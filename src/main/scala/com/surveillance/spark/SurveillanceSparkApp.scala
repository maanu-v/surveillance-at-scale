import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try
import org.apache.spark.sql.streaming.Trigger
import java.util.Base64
import scala.util.parsing.json.JSONObject
import java.net._
import java.io._

object SparkYoloConsumer {

  // ----------------------------
  // Configuration
  // ----------------------------
  val KAFKA_SERVERS = sys.env.getOrElse("KAFKA_SERVERS", "kafka1:29092,kafka2:29093")
  val CAMERA_TOPICS = sys.env.getOrElse("CAMERA_TOPICS", "camera-1-frames,camera-2-frames,camera-3-frames")
  val HDFS_PATH = sys.env.getOrElse("HDFS_PATH", "hdfs://namenode:9000/surveillance/detections")
  val HDFS_CHECKPOINT_PATH = sys.env.getOrElse("HDFS_CHECKPOINT_PATH", "hdfs://namenode:9000/surveillance/checkpoints/yolo-detections")
  val CONFIDENCE_THRESHOLD = sys.env.getOrElse("CONFIDENCE_THRESHOLD", "0.4").toDouble

  // ----------------------------
  // Spark Session
  // ----------------------------
  val spark = SparkSession.builder()
    .appName("YOLO-Spark-Consumer-Scala")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()

  import spark.implicits._

  // ----------------------------
  // Define Kafka Message Schema
  // ----------------------------
  val metadataSchema = StructType(Seq(
    StructField("camId", IntegerType, true),
    StructField("frameId", IntegerType, true),
    StructField("timestamp", StringType, true),
    StructField("frame_shape", ArrayType(IntegerType), true),
    StructField("encoding", StringType, true)
  ))

  val frameMessageSchema = StructType(Seq(
    StructField("metadata", metadataSchema, true),
    StructField("frame_data", StringType, true)
  ))

  // ----------------------------
  // Dummy YOLO Detection Function
  // ----------------------------
  def detectObjects(base64Img: String, camId: Int, frameId: Int, timestamp: String): String = {
    try {
      if (base64Img == null || base64Img.isEmpty) {
        return s"""{
          "metadata": {"camera_id": $camId, "frame_id": $frameId, "timestamp": "$timestamp", "total_detections": 0},
          "detections": []
        }"""
      }

      val url = new URL("http://localhost:8001/process")
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("POST")
      conn.setRequestProperty("Content-Type", "application/json")
      conn.setDoOutput(true)

      val jsonBody = s"""{"image": "$base64Img", "cam_id": $camId, "frame_id": $frameId, "timestamp": "$timestamp"}"""
      val out = new OutputStreamWriter(conn.getOutputStream)
      out.write(jsonBody)
      out.close()

      val in = new BufferedReader(new InputStreamReader(conn.getInputStream))
      val response = Iterator.continually(in.readLine()).takeWhile(_ != null).mkString
      in.close()
      response

    } catch {
      case e: Exception =>
        s"""{"metadata": {"camera_id": $camId, "frame_id": $frameId, "timestamp": "$timestamp", "error": "${e.getMessage}"}, "detections": []}"""
    }
  }

  val detectUDF = udf((base64Img: String, camId: Int, frameId: Int, timestamp: String) =>
    detectObjects(base64Img, camId, frameId, timestamp)
  )

  // ----------------------------
  // Kafka Stream
  // ----------------------------
  val rawStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", CAMERA_TOPICS)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()

  val parsedStream = rawStream.selectExpr("CAST(value AS STRING) as json_value", "topic")
    .withColumn("camera_id", regexp_extract(col("topic"), "camera-(\\d+)-frames", 1).cast(IntegerType))
    .select("json_value", "camera_id")

  val jsonStream = parsedStream
    .withColumn("frame_data_parsed", from_json(col("json_value"), frameMessageSchema))
    .select(
      col("frame_data_parsed.metadata.camId").alias("camId"),
      col("frame_data_parsed.metadata.frameId").alias("frameId"),
      col("frame_data_parsed.metadata.timestamp").alias("timestamp"),
      col("frame_data_parsed.frame_data").alias("frame_data"),
      col("camera_id")
    )

  val detectionsStream = jsonStream.withColumn(
    "detection_results",
    detectUDF(col("frame_data"), col("camId"), col("frameId"), col("timestamp"))
  )

  // ----------------------------
  // Output: HDFS Parquet (partitioned)
  // ----------------------------
  val finalStream = detectionsStream
    .withColumn("parsed_detection", from_json(col("detection_results"),
      StructType(Seq(
        StructField("metadata", StructType(Seq(
          StructField("camera_id", IntegerType),
          StructField("frame_id", IntegerType),
          StructField("timestamp", StringType),
          StructField("total_detections", IntegerType),
          StructField("error", StringType)
        ))),
        StructField("detections", ArrayType(StructType(Seq(
          StructField("class_id", IntegerType),
          StructField("class_name", StringType),
          StructField("confidence", DoubleType)
        ))))
      ))
    ))
    .withColumn("total_detections", col("parsed_detection.metadata.total_detections"))
    .withColumn("detection_timestamp", current_timestamp())
    .withColumn("year", year(current_timestamp()))
    .withColumn("month", month(current_timestamp()))
    .withColumn("day", dayofmonth(current_timestamp()))
    .withColumn("hour", hour(current_timestamp()))

  val query = finalStream
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", HDFS_PATH)
    .option("checkpointLocation", HDFS_CHECKPOINT_PATH)
    .partitionBy("year", "month", "day", "camera_id")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  def main(args: Array[String]): Unit = {
    println(s"🚀 Spark YOLO Consumer Started with topics: $CAMERA_TOPICS")
    println(s"Kafka: $KAFKA_SERVERS | Output: $HDFS_PATH")
    query.awaitTermination()
  }
}

# spark_yolo_consumer.py
import cv2
import numpy as np
import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, explode, split, regexp_extract, current_timestamp, date_format, year, month, dayofmonth, hour
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType, ArrayType
from ultralytics import YOLO
import json
import logging

# ----------------------------
# Initialize Spark
# ----------------------------
spark = SparkSession.builder \
    .appName("YOLO-Spark-Consumer") \
    .getOrCreate()
    # .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \

spark.sparkContext.setLogLevel("WARN")
# ----------------------------
# Load YOLO model
# ----------------------------
print("🤖 Loading YOLO model...")
try:
    yolo_model = YOLO("yolo11n.pt")
    print(f"✅ YOLO model loaded successfully:yolo11n.pt")
except Exception as e:
    print(f"❌ Failed to load YOLO model: {e}")
    print("💡 Make sure ultralytics is installed: pip install ultralytics")
    raise

# ----------------------------
# Schema for existing Kafka messages
# ----------------------------
# Schema matches the producer's message format
metadata_schema = StructType([
    StructField("camId", IntegerType(), True),
    StructField("frameId", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("frame_shape", ArrayType(IntegerType()), True),
    StructField("encoding", StringType(), True)
])

frame_message_schema = StructType([
    StructField("metadata", metadata_schema, True),
    StructField("frame_data", StringType(), True)  # base64 encoded frame
])

# ----------------------------
# UDF: Decode + Run YOLO Detection
# ----------------------------
def detect_objects(base64_img, cam_id, frame_id, timestamp):
    """
    Decode base64 image and run YOLO detection
    Returns JSON string with detection results matching the existing format
    """
    try:
        if not base64_img:
            return json.dumps({
                "metadata": {
                    "camera_id": cam_id,
                    "frame_id": frame_id,
                    "timestamp": timestamp,
                    "detection_timestamp": None,
                    "total_detections": 0,
                    "detection_classes": [],
                    "class_counts": {},
                    "confidence_threshold": CONFIDENCE_THRESHOLD,
                    "nms_threshold": 0.4,
                    "processing_engine": "spark-yolo"
                },
                "detections": []
            })

        # Decode base64 image
        img_bytes = base64.b64decode(base64_img)
        np_arr = np.frombuffer(img_bytes, np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        
        if frame is None:
            raise ValueError("Failed to decode image")

        # Run YOLO prediction
        results = yolo_model.predict(frame, conf=CONFIDENCE_THRESHOLD, verbose=False)
        detections = []
        class_counts = {}

        if results and len(results) > 0 and results[0].boxes is not None:
            for box in results[0].boxes:
                cls_id = int(box.cls)
                cls_name = yolo_model.names[cls_id]
                conf = float(box.conf)
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                
                # Convert to format matching existing system
                detection = {
                    "class_id": cls_id,
                    "class_name": cls_name,
                    "confidence": round(conf, 4),
                    "bbox": {
                        "x": int(x1),
                        "y": int(y1),
                        "width": int(x2 - x1),
                        "height": int(y2 - y1),
                        "center_x": int((x1 + x2) / 2),
                        "center_y": int((y1 + y2) / 2)
                    }
                }
                detections.append(detection)
                
                # Count classes
                class_counts[cls_name] = class_counts.get(cls_name, 0) + 1

        # Create response matching existing detection format
        detection_result = {
            "metadata": {
                "camera_id": cam_id,
                "frame_id": frame_id,
                "timestamp": timestamp,
                "detection_timestamp": timestamp,  # Use same timestamp for simplicity
                "total_detections": len(detections),
                "detection_classes": list(class_counts.keys()),
                "class_counts": class_counts,
                "confidence_threshold": CONFIDENCE_THRESHOLD,
                "nms_threshold": 0.4,
                "processing_engine": "spark-yolo"
            },
            "detections": detections
        }

        return json.dumps(detection_result)
        
    except Exception as e:
        error_result = {
            "metadata": {
                "camera_id": cam_id,
                "frame_id": frame_id,
                "timestamp": timestamp,
                "detection_timestamp": None,
                "total_detections": 0,
                "detection_classes": [],
                "class_counts": {},
                "error": str(e),
                "processing_engine": "spark-yolo"
            },
            "detections": []
        }
        return json.dumps(error_result)

detect_udf = udf(detect_objects, StringType())

# ----------------------------
# Configuration
# ----------------------------
import os

# Get configuration from environment or use defaults
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "kafka1:29092,kafk2:29093")
CAMERA_TOPICS = os.getenv("CAMERA_TOPICS", "camera-1-frames,camera-2-frames,camera-3-frames,camera-4-frames,camera-5-frames,camera-6-frames,camera-7-frames")
YOLO_MODEL = "yolo11n.pt"
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", "0.4"))
HDFS_PATH = os.getenv("HDFS_PATH", "hdfs://namenode:9000/surveillance/detections")
HDFS_CHECKPOINT_PATH = os.getenv("HDFS_CHECKPOINT_PATH", "hdfs://namenode:9000/surveillance/checkpoints/yolo-detections")

print(f"🔧 Configuration:")
print(f"   Kafka Servers: {KAFKA_SERVERS}")
print(f"   Camera Topics: {CAMERA_TOPICS}")
print(f"   YOLO Model: {YOLO_MODEL}")
print(f"   Confidence Threshold: {CONFIDENCE_THRESHOLD}")
print(f"   HDFS Path: {HDFS_PATH}")
print(f"   HDFS Checkpoint Path: {HDFS_CHECKPOINT_PATH}")

# ----------------------------
# Kafka Stream - Multiple Camera Topics
# ----------------------------
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", CAMERA_TOPICS) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse Kafka message and extract camera ID from topic
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_value", "topic") \
    .withColumn("camera_id", regexp_extract(col("topic"), r"camera-(\d+)-frames", 1).cast(IntegerType())) \
    .select("json_value", "camera_id")

# Parse the JSON message using the existing schema
json_stream = parsed_stream \
    .select(from_json(col("json_value"), frame_message_schema).alias("frame_data"), "camera_id") \
    .select("frame_data.*", "camera_id")

# Apply YOLO detection with proper parameters
detections_stream = json_stream.withColumn("detection_results", 
    detect_udf(
        col("frame_data"), 
        col("metadata.camId"),
        col("metadata.frameId"),
        col("metadata.timestamp")
    )
)

# ----------------------------
# Output Options
# ----------------------------

# Option 1: Console Output (for testing/debugging)
def output_to_console():
    query = detections_stream.select(
        col("metadata.camId").alias("camera_id"),
        col("metadata.frameId").alias("frame_id"), 
        col("metadata.timestamp").alias("timestamp"),
        col("detection_results").alias("detections")
    ) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .start()
    
    return query

# Option 2: Publish back to Kafka (to camera-{i}-detections topics)
def output_to_kafka():
    # Transform to match Kafka output format
    kafka_output = detections_stream.select(
        col("metadata.camId").alias("camera_id"),
        col("detection_results").alias("value")
    ).selectExpr(
        "CONCAT('camera-', camera_id, '-detections') as topic",
        "CONCAT('cam', camera_id, '_frame', metadata.frameId, '_detections_spark') as key", 
        "value"
    )
    
    query = kafka_output \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("checkpointLocation", "/tmp/spark-checkpoint-yolo-detections") \
        .outputMode("append") \
        .start()
    
    return query

# Option 3: Save to file system (for batch processing)
def output_to_files():
    query = detections_stream.select(
        col("metadata.camId").alias("camera_id"),
        col("metadata.frameId").alias("frame_id"),
        col("metadata.timestamp").alias("timestamp"), 
        col("detection_results").alias("detections")
    ) \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/tmp/yolo-detections") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-yolo-files") \
    .start()
    
    return query

# Option 4: Save to HDFS in Parquet format with partitioning
def output_to_hdfs_parquet():
    """
    Save detection results to HDFS in Parquet format with date partitioning
    """
    # Parse the JSON detection results for structured storage
    parsed_detections = detections_stream.select(
        col("metadata.camId").alias("camera_id"),
        col("metadata.frameId").alias("frame_id"),
        col("metadata.timestamp").alias("timestamp"),
        from_json(col("detection_results"), StructType([
            StructField("metadata", StructType([
                StructField("camera_id", IntegerType(), True),
                StructField("frame_id", IntegerType(), True),
                StructField("timestamp", StringType(), True),
                StructField("detection_timestamp", StringType(), True),
                StructField("total_detections", IntegerType(), True),
                StructField("detection_classes", ArrayType(StringType()), True),
                StructField("class_counts", StringType(), True),  # Store as JSON string
                StructField("confidence_threshold", DoubleType(), True),
                StructField("nms_threshold", DoubleType(), True),
                StructField("processing_engine", StringType(), True),
                StructField("error", StringType(), True)
            ]), True),
            StructField("detections", ArrayType(StructType([
                StructField("class_id", IntegerType(), True),
                StructField("class_name", StringType(), True),
                StructField("confidence", DoubleType(), True),
                StructField("bbox", StructType([
                    StructField("x", IntegerType(), True),
                    StructField("y", IntegerType(), True),
                    StructField("width", IntegerType(), True),
                    StructField("height", IntegerType(), True),
                    StructField("center_x", IntegerType(), True),
                    StructField("center_y", IntegerType(), True)
                ]), True)
            ])), True)
        ])).alias("parsed_detection")
    )
    
    # Flatten and add partitioning columns
    flattened_detections = parsed_detections.select(
        col("camera_id"),
        col("frame_id"), 
        col("timestamp"),
        col("parsed_detection.metadata.detection_timestamp").alias("detection_timestamp"),
        col("parsed_detection.metadata.total_detections").alias("total_detections"),
        col("parsed_detection.metadata.detection_classes").alias("detection_classes"),
        col("parsed_detection.metadata.class_counts").alias("class_counts"),
        col("parsed_detection.metadata.confidence_threshold").alias("confidence_threshold"),
        col("parsed_detection.metadata.processing_engine").alias("processing_engine"),
        col("parsed_detection.metadata.error").alias("error"),
        col("parsed_detection.detections").alias("detections"),
        current_timestamp().alias("ingestion_timestamp")
    ).withColumn("year", year(col("detection_timestamp"))) \
     .withColumn("month", month(col("detection_timestamp"))) \
     .withColumn("day", dayofmonth(col("detection_timestamp"))) \
     .withColumn("hour", hour(col("detection_timestamp")))
    
    # Write to HDFS with partitioning
    query = flattened_detections \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", HDFS_PATH) \
        .option("checkpointLocation", HDFS_CHECKPOINT_PATH) \
        .partitionBy("year", "month", "day", "camera_id") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    return query

# Option 5: Save to HDFS with flattened detection records (one row per detection)
def output_to_hdfs_parquet_flattened():
    """
    Save detection results to HDFS in Parquet format with flattened structure
    Each detection becomes a separate row for easier analytics
    """
    # Parse the JSON detection results
    parsed_detections = detections_stream.select(
        col("metadata.camId").alias("camera_id"),
        col("metadata.frameId").alias("frame_id"),
        col("metadata.timestamp").alias("timestamp"),
        from_json(col("detection_results"), StructType([
            StructField("metadata", StructType([
                StructField("camera_id", IntegerType(), True),
                StructField("frame_id", IntegerType(), True),
                StructField("timestamp", StringType(), True),
                StructField("detection_timestamp", StringType(), True),
                StructField("total_detections", IntegerType(), True),
                StructField("detection_classes", ArrayType(StringType()), True),
                StructField("confidence_threshold", DoubleType(), True),
                StructField("processing_engine", StringType(), True)
            ]), True),
            StructField("detections", ArrayType(StructType([
                StructField("class_id", IntegerType(), True),
                StructField("class_name", StringType(), True),
                StructField("confidence", DoubleType(), True),
                StructField("bbox", StructType([
                    StructField("x", IntegerType(), True),
                    StructField("y", IntegerType(), True),
                    StructField("width", IntegerType(), True),
                    StructField("height", IntegerType(), True),
                    StructField("center_x", IntegerType(), True),
                    StructField("center_y", IntegerType(), True)
                ]), True)
            ])), True)
        ])).alias("parsed_detection")
    )
    
    # Explode detections array to create one row per detection
    flattened_detections = parsed_detections.select(
        col("camera_id"),
        col("frame_id"),
        col("timestamp"),
        col("parsed_detection.metadata.detection_timestamp").alias("detection_timestamp"),
        col("parsed_detection.metadata.total_detections").alias("total_detections_in_frame"),
        col("parsed_detection.metadata.confidence_threshold").alias("confidence_threshold"),
        col("parsed_detection.metadata.processing_engine").alias("processing_engine"),
        explode(col("parsed_detection.detections")).alias("detection")
    ).select(
        "*",
        col("detection.class_id").alias("class_id"),
        col("detection.class_name").alias("class_name"),
        col("detection.confidence").alias("confidence"),
        col("detection.bbox.x").alias("bbox_x"),
        col("detection.bbox.y").alias("bbox_y"),
        col("detection.bbox.width").alias("bbox_width"),
        col("detection.bbox.height").alias("bbox_height"),
        col("detection.bbox.center_x").alias("bbox_center_x"),
        col("detection.bbox.center_y").alias("bbox_center_y"),
        current_timestamp().alias("ingestion_timestamp")
    ).drop("detection") \
     .withColumn("year", year(col("detection_timestamp"))) \
     .withColumn("month", month(col("detection_timestamp"))) \
     .withColumn("day", dayofmonth(col("detection_timestamp"))) \
     .withColumn("hour", hour(col("detection_timestamp")))
    
    # Write to HDFS with partitioning
    query = flattened_detections \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{HDFS_PATH}_flattened") \
        .option("checkpointLocation", f"{HDFS_CHECKPOINT_PATH}_flattened") \
        .partitionBy("year", "month", "day", "camera_id") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    return query

# ----------------------------
# Main Execution
# ----------------------------
if __name__ == "__main__":
    import sys
    
    # Choose output mode based on argument or default to console
    output_mode = sys.argv[1] if len(sys.argv) > 1 else "console"
    
    print(f"Starting Spark YOLO Consumer with output mode: {output_mode}")
    print(f"Available modes: console, kafka, files, hdfs/parquet, hdfs-flattened/parquet-flattened")
    print(f"Kafka Servers: {KAFKA_SERVERS}")
    print(f"Subscribed Topics: {CAMERA_TOPICS}")
    print("YOLO Model: yolo11n.pt")
    
    try:
        if output_mode == "kafka":
            print("Publishing detections back to Kafka topics...")
            query = output_to_kafka()
        elif output_mode == "files":
            print("Saving detections to file system...")
            query = output_to_files()
        elif output_mode == "hdfs" or output_mode == "parquet":
            print("Saving detections to HDFS in Parquet format...")
            query = output_to_hdfs_parquet()
        elif output_mode == "hdfs-flattened" or output_mode == "parquet-flattened":
            print("Saving detections to HDFS in flattened Parquet format...")
            query = output_to_hdfs_parquet_flattened()
        else:
            print("Outputting detections to console...")
            query = output_to_console()
        
        print("Spark YOLO Consumer started successfully!")
        print("Processing camera frames and running YOLO detection...")
        print("Press Ctrl+C to stop")
        
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStopping Spark YOLO Consumer...")
        query.stop()
    except Exception as e:
        print(f"Error: {e}")
        spark.stop()
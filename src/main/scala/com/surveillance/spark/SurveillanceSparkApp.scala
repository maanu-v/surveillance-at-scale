package com.surveillance.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object SurveillanceSparkApp {
  
  def main(args: Array[String]): Unit = {
    
    // Create Spark configuration
    val conf = new SparkConf()
      .setAppName("Surveillance at Scale")
      .setMaster("spark://spark-master:7077") // Connect to Spark cluster
      .set("spark.executor.memory", "2g")
      .set("spark.executor.cores", "2")
      .set("spark.cores.max", "4")
    
    // Create Spark session
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    
    // Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    println("=" * 60)
    println("Spark Session Created Successfully!")
    println("=" * 60)
    println(s"Spark Version: ${spark.version}")
    println(s"Master URL: ${spark.sparkContext.master}")
    println(s"App Name: ${spark.sparkContext.appName}")
    println(s"App ID: ${spark.sparkContext.applicationId}")
    println("=" * 60)
    
    // Test Spark with a simple operation
    import spark.implicits._
    
    val testData = Seq(
      ("Camera-1", 1080, 1920),
      ("Camera-2", 1080, 1920),
      ("Camera-3", 720, 1280),
      ("Camera-4", 1080, 1920),
      ("Camera-5", 1080, 1920),
      ("Camera-6", 720, 1280),
      ("Camera-7", 1080, 1920)
    ).toDF("camera_id", "height", "width")
    
    println("\nTest DataFrame - Camera Configurations:")
    testData.show()
    
    println(s"\nTotal number of cameras: ${testData.count()}")
    
    // Keep the application running (useful for testing)
    println("\nSpark application is running. Press Ctrl+C to exit.")
    
    // Uncomment below to keep app running indefinitely
    // Thread.sleep(Long.MaxValue)
    
    // Stop Spark session
    spark.stop()
    println("\nSpark session stopped.")
  }
}

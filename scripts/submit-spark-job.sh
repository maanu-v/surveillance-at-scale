#!/bin/bash

# Build and submit Spark application

echo "Building Spark application..."
sbt clean assembly

if [ $? -eq 0 ]; then
    echo "Build successful!"
    
    # Copy JAR to spark-apps directory
    cp target/scala-2.12/surveillance-spark-app.jar data/spark-apps/
    
    echo "Submitting Spark job to cluster..."
    docker exec spark-master /opt/spark/bin/spark-submit \
        --class com.surveillance.spark.SurveillanceSparkApp \
        --master spark://spark-master:7077 \
        --executor-memory 2g \
        --executor-cores 2 \
        --total-executor-cores 4 \
        /opt/spark-apps/surveillance-spark-app.jar
else
    echo "Build failed!"
    exit 1
fi

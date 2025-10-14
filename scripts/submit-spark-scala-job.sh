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
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoint \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --verbose \
        /opt/spark-apps/surveillance-spark-app.jar
else
    echo "Build failed!"
    exit 1
fi

name := "surveillance-at-scale"

version := "0.1.0"

scalaVersion := "2.12.18"

// Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
  "org.apache.hadoop" % "hadoop-client" % "3.3.4" % "provided",
  "org.apache.hadoop" % "hadoop-hdfs-client" % "3.3.4"
)

// Assembly settings for fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

assembly / assemblyJarName := "surveillance-spark-app.jar"

// Set main class
Compile / mainClass := Some("com.surveillance.spark.SurveillanceSparkApp")

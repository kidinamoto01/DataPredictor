name := "DataPredictor"

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"% "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"% "provided"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.12"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.1.0"


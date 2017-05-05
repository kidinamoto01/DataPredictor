name := "DataPredictor"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.0.2"% "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.10" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.0.0"% "provided"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.12"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.26"

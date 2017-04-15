name := "DataPredictor"

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.0"% "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.12"

libraryDependencies += "org.apache.hive" % "hive-jdbc" % "2.1.0"

libraryDependencies ++= Seq(
  ("org.elasticsearch" % "elasticsearch-spark_2.11" % "2.3.3").
    exclude("com.google.guava", "guava").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
    exclude("org.eclipse.jetty.orbit", "javax.servlet").
    exclude("org.slf4j", "slf4j-api")
)


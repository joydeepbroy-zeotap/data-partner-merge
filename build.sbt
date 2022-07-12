name := "data-partner-merge"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  //"io.delta"%"delta-core_2.11"%"0.6.1"
  //"org.apache.spark" %% "spark-core" % "2.4.2"
  "io.delta" %% "delta-core" % "1.0.1",
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "org.apache.spark"%%"spark-avro"%"3.1.1"
 // "com.google.guava"%%"guava"%"23.0"
)
name := "kqi"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.0",
  "org.apache.spark" % "spark-mllib_2.11" % "1.6.0",
  "org.apache.spark" % "spark-graphx_2.11" % "1.6.0"
)
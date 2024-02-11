name := "BigData"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.12"

idePackagePrefix := Some("org.example")

val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
)

name := "DataProcessing"

version := "0.1"

scalaVersion := "2.12.8"

idePackagePrefix := Some("com.baylarov")
val scalaVer = "2.12.8"
val sparkVersion = "3.1.2"
val flinkVersion = "1.12.1"
val esVersion = "7.14.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVer,
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.elasticsearch" %% "elasticsearch-spark-30" % esVersion
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
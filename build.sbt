name := "MLStreaming"

version := "0.1"

scalaVersion := "2.12.13"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.1.1",
  "org.apache.kafka" % "kafka-clients" % "2.8.0"
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
  case _ => MergeStrategy.first
}

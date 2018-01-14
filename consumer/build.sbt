name := "consumer"
version := "1.0"
scalaVersion := "2.12.4"

resolvers += "Confluent Maven" at "http://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.8.0",
  "org.apache.avro" % "avro" % "1.8.2",
  "org.apache.kafka" % "kafka_2.12" % "1.0.0",
  "io.confluent" % "kafka-avro-serializer" % "3.3.1"
)
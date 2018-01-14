package com.github.vsabreu.go_scala_avro

import java.util.Properties

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer

object Main {
  def main(args: Array[String]): Unit = {

    val topic = "go-scala-avro-topic"

    val stream = {
      val props = getProperties()
      val consumer = Consumer.create(new ConsumerConfig(props))

      consumer.createMessageStreamsByFilter(
        new Whitelist(topic), 1, new StringDecoder(), new StringDecoder())(0)
    }

    lazy val iterator = stream.iterator()

    printf("Consumer ready to consume messages from topic %s.\n", topic)

    while(true) {
      if(hasNext) {
        val msg = iterator.next().message()
        println("Got message: " + msg.toString)
      }
    }

    def hasNext: Boolean = {
      try {
        iterator.hasNext
      }
      catch {
        case ex: Exception => ex.printStackTrace()
          false
      }
    }
  }

  def getProperties(): Properties = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", "127.0.0.1:9092")
    p.setProperty("zookeeper.connect", "127.0.0.1:2181")
    p.setProperty("group.id", "scalagroup")
    p.setProperty("auto.commit.enable", "true")
    p.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    p.setProperty("value.deserializer", classOf[KafkaAvroDeserializer].getName)
    p
  }
}

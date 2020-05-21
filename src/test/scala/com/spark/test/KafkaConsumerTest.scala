package com.spark.test

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
object KafkaConsumerTest {
  def main(args: Array[String]): Unit = {
    val props = new Properties();
    props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
    props.put("group.id", "test"); //消费者的组id
    props.put("enable.auto.commit", "false");
    props.put("session.timeout.ms", "30000");
    props.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    );
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    );
    val consumer = new KafkaConsumer[String, String](props);
    consumer.subscribe("MST_444")
    while (true) {
      val records = consumer.poll(100);
      records.foreach { x =>
        }
    }
  }
}

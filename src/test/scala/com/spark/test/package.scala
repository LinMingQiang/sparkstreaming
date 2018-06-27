package com.spark

import kafka.message.MessageAndMetadata
import org.apache.spark.common.util.ConfigurationFactoryTool
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerRecord

package object test {
 // val zookeeper = "solr1,solr2,mongodb3"
  //val brokers="kylin-node2:9092,kylin-node3:9092,kylin-node4:9092"
  val brokers = "kafka1:9092,kafka2:9092,kafka3:9092"
  val outTopic = "test"
  val producerConfig = {
    val p = new java.util.Properties()
    p.setProperty("bootstrap.servers", brokers)
    p.setProperty("key.serializer", classOf[StringSerializer].getName)
    p.setProperty("value.serializer", classOf[StringSerializer].getName)
    p
  }
  val transformFunc = (topic: String, msg: String) => new ProducerRecord[String, String](topic, msg)

  def msgHandle = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)

  def msgHandle2 = (mmd: MessageAndMetadata[String, String]) => ((mmd.topic, mmd.partition, mmd.offset), mmd.message)
  def msgHandle3 = (mmd: MessageAndMetadata[String, String]) => (mmd.topic,mmd.topic,mmd.message)

}
package com.spark

import kafka.message.MessageAndMetadata
import org.apache.spark.common.util.ConfigurationFactoryTool
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerRecord

package object test extends  ConfigurationFactoryTool{
  val zookeeper=""
  val brokers=""
  
  val producerConfig = {
  val p = new java.util.Properties()
  p.setProperty("bootstrap.servers", "127.0.0.1:9092")
  p.setProperty("key.serializer", classOf[StringSerializer].getName)
  p.setProperty("value.serializer", classOf[StringSerializer].getName)
  p
}
  val transformFunc=(topic:String,msg:String) 
  => new ProducerRecord[String, String](topic, msg)
  
  def msgHandle = (mmd: MessageAndMetadata[String, String]) 
  => (mmd.topic, mmd.message)
}
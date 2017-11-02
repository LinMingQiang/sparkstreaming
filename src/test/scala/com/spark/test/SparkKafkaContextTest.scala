package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtil

object SparkKafkaContextTest {
  /**
   * 离线方式 读取kafka数据
   * 测试 SparkKafkaContext
   */
  def main(args: Array[String]): Unit = {
    val skc = new SparkKafkaContext(
      new SparkConf().setMaster("local").setAppName("SparkKafkaContextTest"))
    val kp =Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last")
    val topics = Set("test")
    val kafkadataRdd = skc.kafkaRDD[String, String, StringDecoder, StringDecoder, (String, String)](kp, topics, msgHandle)
    
    
    val rdd2=skc.kafkaRDD(kp, topics, 1000, msgHandle2)
    rdd2.foreach{case((topic,part,offser),msg)=>
      //msg如果是今天的，那就更新offset
    
    }
    
    kafkadataRdd.foreach(println)
    kafkadataRdd.updateOffsets(kp)
    
  }
}
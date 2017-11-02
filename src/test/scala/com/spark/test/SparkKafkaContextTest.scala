package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtil
import org.apache.spark.rdd.RDD


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
    val kafkadataRdd = skc.kafkaRDD(kp, topics, msgHandle2)
    RDD.rddToPairRDDFunctions(kafkadataRdd)
    kafkadataRdd.reduceByKey(_+_)
   // kafkadataRdd.map(f)
 
    
    kafkadataRdd.foreach(println)
    kafkadataRdd.updateOffsets(kp)
    
  }
}
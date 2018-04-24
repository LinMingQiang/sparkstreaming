package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtil
import org.apache.spark.rdd.RDD


object SparkKafkaContextTest {
  /**
   * 离线方式 读取kafka数据
   * 测试 SparkKafkaContext类
   */
  def main(args: Array[String]): Unit = {
    val skc = new SparkKafkaContext(new SparkConf().setMaster("local").setAppName("SparkKafkaContextTest"))
    
    val kp =SparkKafkaContext.getKafkaParam(
        brokers,"groupid","last","consum","test,0,100|test,1,200|test,2,300")

    
    val topics = Set("test")
    val kafkadataRdd = skc.kafkaRDD[((String, Int, Long), String)](kp, topics, msgHandle2)
    
    //RDD.rddToPairRDDFunctions(kafkadataRdd)
    kafkadataRdd.reduceByKey(_+_)
   // kafkadataRdd.map(f)
 
    kafkadataRdd.foreach(println)
    kafkadataRdd.updateOffsets(kp)
    
  }
}
package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD

object SparkKafkaContextTest {
  /**
   * 离线方式 读取kafka数据
   * 测试 SparkKafkaContext类
   */
  def main(args: Array[String]): Unit = {

    val kp = SparkKafkaContext.getKafkaParam(
      brokers, "groupid", "earliest", "earliest", "")
    val skc = new SparkKafkaContext(kp, new SparkConf()
      .setMaster("local")
      .setAppName("SparkKafkaContextTest"))

    val topics = Set("test")
    val kafkadataRdd = skc.kafkaRDD[String,String](topics)

    //RDD.rddToPairRDDFunctions(kafkadataRdd)
    //kafkadataRdd.reduceByKey(_+_)
    // kafkadataRdd.map(f)

    kafkadataRdd.foreach(println)
    //kafkadataRdd.updateOffsets(kp)

  }
}
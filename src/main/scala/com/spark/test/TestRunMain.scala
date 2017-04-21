package com.spark.test

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaSparkStreamManager
import kafka.serializer.StringDecoder
import org.slf4j.LoggerFactory
import org.apache.spark.common.util.Configuration
import org.apache.log4j.PropertyConfigurator

object TestRunMain {
  PropertyConfigurator.configure("conf/log4j.properties")
  def main(args: Array[String]): Unit = {
    runJobWithConf
  }
  def runJob() {
    val scf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(scf)
    val ssc = new StreamingContext(sc, Seconds(5))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last")
    val topics = Set("test")
    val ds = ssc.createDirectStream[(String, String)](kp, topics, null, msgHandle)
    ds.foreachRDD { rdd => rdd.foreach(println) }

    ssc.start()
    ssc.awaitTermination()
  }
  def runJobWithConf() {
    val conf = new ConfigurationTest()
    initConf("conf/config.properties", conf)
    println(conf.getKV())
    val scf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(scf)
    val ssc = new StreamingContext(sc, Seconds(5))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last")
    val topics = Set("test")
    conf.setKafkaParams(kp)
    conf.setTopics(topics)
    
    val ds = ssc.createDirectStream(conf, null, msgHandle)
    ds.foreachRDD { rdd => rdd.foreach(println) }

    ssc.start()
    ssc.awaitTermination()

  }
}
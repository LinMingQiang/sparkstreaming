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
import org.apache.spark.func.tool._
object TestRunMain {
  PropertyConfigurator.configure("conf/log4j.properties")
  def main(args: Array[String]): Unit = {
    runJob
  }
  def runJob() {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Test"))
    val ssc = new StreamingContext(sc, Seconds(20))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id.test2",
      "newgroup.last.earliest"->"earliest",
      "kafka.last.consum" -> "consum")
    val topics = Set("smartadsclicklog")
    val ds = ssc.createDirectStream[(String, String)](kp, topics, msgHandle)
    
    ds.foreachRDD { rdd => 
      println(rdd.count)
      rdd.getRDDOffsets().foreach(println)
      rdd.updateOffsets(kp, "group.id.test")
      System.exit(0)
      }

    ssc.start()
    ssc.awaitTermination()
  }
  def runJobWithConf() {
    val conf = new ConfigurationTest()
    initConf("conf/config.properties", conf)
    initJobConf(conf)
    println(conf.getKV())
    val scf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(scf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val ds = ssc.createDirectStream(conf, null, msgHandle)
    ds.foreachRDD { rdd => rdd.foreach(println) }
    ssc.start()
    ssc.awaitTermination()

  }
  def initJobConf(conf:Configuration){
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last")
    val topics = Set("test")
    conf.setKafkaParams(kp)
    conf.setTopics(topics)
  }
}
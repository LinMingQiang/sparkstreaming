package com.spark.test

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import kafka.serializer.StringDecoder
import org.slf4j.LoggerFactory
import org.apache.spark.common.util.Configuration
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.core.StreamingKafkaContext
import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.common.util.KafkaConfig
import org.apache.spark.common.util.ConfigurationFactoryTool
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka010.DirectKafkaInputDStream
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.Arrays

object StreamingKafkaContextTest {
 val brokers=""
  PropertyConfigurator.configure("conf/log4j.properties")
  def main(args: Array[String]): Unit = {
    run
  }
  def run() {
   
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test")
      .set(SparkKafkaContext.MAX_RATE_PER_PARTITION, "1"))
    var kp = StreamingKafkaContext.getKafkaParam(brokers, "test", "consum", "EARLIEST")
    val ssc = new StreamingKafkaContext(kp,sc, Seconds(5))
    val consumOffset = getConsumerOffset(kp).foreach(println)
    val topics = Set("test1")
    val ds = ssc.createDirectStream[String, String](topics)
    ds.foreachRDD { rdd =>
      println("COUNT : ", rdd.count)
      rdd.foreach(println)
      //使用自带的offset管理
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      ds.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      //使用zookeeper来管理offset
      ssc.updateRDDOffsets("test", rdd)
    }
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * @func 获取上次消费偏移量。
   */
  def getConsumerOffset(kp: Map[String, Object]) = {
    val consumer = new KafkaConsumer[String, String](kp)
    consumer.subscribe(Arrays.asList("test1")); //订阅topic
    consumer.poll(0)
    val parts = consumer.assignment() //获取topic等信息
    val re = parts.map { ps => ps -> consumer.position(ps) }
    consumer.pause(parts)
    re
  }

  /**
   * 使用配置文件的形式
   */
  def runJobWithConf() {
    val conf = new ConfigurationTest()
    ConfigurationFactoryTool.initConf("conf/config.properties", conf)
    initJobConf(conf)
    println(conf.getKV())
    val scf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(scf)
    val ssc = new StreamingKafkaContext(conf.kafkaParams, sc, Seconds(5))
    val ds = ssc.createDirectStream[String, String](conf)
    ds.foreachRDD { rdd => rdd.foreach(println) }
    ssc.start()
    ssc.awaitTermination()

  }
  /**
   * 初始化配置文件
   */
  def initJobConf(conf: KafkaConfig) {
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
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
import kafka.serializer.StringDecoder
object StreamingKafkaContextTest {
  PropertyConfigurator.configure("conf/log4j.properties")
  def main(args: Array[String]): Unit = {
    run
  }
  def run() {
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "testGroupid",
      StreamingKafkaContext.WRONG_GROUP_FROM -> "last", //EARLIEST
      StreamingKafkaContext.CONSUMER_FROM -> "consum"
    )
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("Test")
    )
    val ssc = new StreamingKafkaContext(kp, sc, Seconds(5))
    val topics = Set("smartadsdeliverylog")
    val ds = ssc
      .createDirectStream[
        String,
        String,
        StringDecoder,
        StringDecoder,
        ((String, Int, Long), String)
      ](topics, msgHandle2)
    ds.foreachRDD { rdd =>
      println(rdd.count)
      //rdd.foreach(println)
      //do rdd operate....
      ssc.getRDDOffsets(rdd).foreach(println)
    //ssc.updateRDDOffsets(kp,  "group.id.test", rdd)//如果想要实现 rdd.updateOffsets。这需要重新inputstream（之后会加上）
    }
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 使用配置文件的形式
    */
  def runJobWithConf() {
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last"
    )
    val conf = new KafkaConfig("conf/config.properties", kp)
    val topics = Set("test")
    conf.setTopics(topics)
    val scf = new SparkConf().setMaster("local[2]").setAppName("Test")
    val sc = new SparkContext(scf)
    val ssc = new StreamingKafkaContext(kp, sc, Seconds(5))
    val ds = ssc.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder,
      ((String, Int, Long), String)
    ](conf, msgHandle2)
    ds.foreachRDD { rdd =>
      rdd.foreach(println)
    }
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
      "kafka.last.consum" -> "last"
    )
    val topics = Set("test")
    conf.setKafkaParams(kp)
    conf.setTopics(topics)
  }
}

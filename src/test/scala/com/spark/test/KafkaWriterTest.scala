package com.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.core.StreamingKafkaContext
import org.apache.spark.func.tool._
import kafka.serializer.StringDecoder
object KafkaWriterTest {
  def main(args: Array[String]): Unit = {
    runJob
  }
  def runJob() {
    val brokers = ""
    val sc = new SparkContext(
      new SparkConf().setMaster("local[2]").setAppName("Test")
    )
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "test",
      "kafka.last.consum" -> "consum"
    )
    val topics = Set("test")
    val ssc = new StreamingKafkaContext(kp, sc, Seconds(5))
    ssc.sc.setLogLevel("ERROR")
    val ds = ssc.createDirectStream[String, String](topics)
    ds.foreachRDD { rdd =>
      rdd.foreach(println)
      rdd
        .map(_.value())
        .writeToKafka(producerConfig(brokers), transformFunc(outTopic, _))
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

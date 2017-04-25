package com.spark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.func.tool._
object KafkaWriterTest {
  def main(args: Array[String]): Unit = {
    
  }
  def runJob() {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("Test"))
    val ssc = new StreamingContext(sc, Seconds(5))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "group.id",
      "kafka.last.consum" -> "last")
    val topics = Set("test")
    val ds = ssc.createDirectStream[(String, String)](kp, topics, null, msgHandle)
    ds.foreachRDD { rdd => 
      rdd.map(_._2)
         .writeToKafka(producerConfig, transformFunc("topicname",_))
      rdd.updateOffsets(kp, "group.id")}

    ssc.start()
    ssc.awaitTermination()
  }
}
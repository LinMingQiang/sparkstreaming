package com.spark.test

import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.SparkConf
object SparkKafkaContextTest {
  def main(args: Array[String]): Unit = {
    val sc=new SparkKafkaContext(new SparkConf().setMaster("local[*]").setAppName("SparkKafkaContextTest"))
    var kp = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "test",
      "newgroup.last.earliest"->"earliest",
      "kafka.last.consum" -> "last")
    val topics = Set("smartadsdeliverylog")
    
    val rdd=sc.kafkaRDD(kp, topics, msgHandle)
        rdd.map{x=>x._1}.foreach(println)
        rdd.updateOffsets(kp, "test")
  }
}
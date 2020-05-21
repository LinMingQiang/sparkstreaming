package com.test

import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.core.SparkKafkaConfsKey
import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.kafka.common.TopicPartition
import org.apache.spark.kafka.util.KafkaCluster
import org.apache.spark.core.SparkKafkaContext
object Test extends SparkKafkaConfsKey {
  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092"
    val groupId = "test"
    val topics = Set("test")
    val props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("group.id", groupId);
    props.put("enable.auto.commit", "false"); //自动commit
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("key.deserializer.encoding", "UTF8");
    props.put("value.deserializer.encoding", "UTF8");
    props.put("heartbeat.interval.ms", "6000");

    //如果需要使用ssl验证，需要设置一下四个参数
//    props.put(SparkKafkaContext.DRIVER_SSL_TRUSTSTORE_LOCATION, "/mnt/kafka-key/client.truststore.jks")
//    props.put(SparkKafkaContext.DRIVER_SSL_KEYSTORE_LOCATION, "/mnt/kafka-key/client.keystore.jks")
//    props.put(SparkKafkaContext.EXECUTOR_SSL_TRUSTSTORE_LOCATION, "client.truststore.jks")
//    props.put(SparkKafkaContext.EXECUTOR_SSL_KEYSTORE_LOCATION, "client.keystore.jks")

    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    val skc = new SparkKafkaContext[String, String](props.toMap, sc)
    // skc.kc.getLastestOffset(topics).foreach(println)

    //val kc = new KafkaCluster(props.toMap)
    // val consum = skc.kc.getConsumerOffet(topics)
    // val last = skc.kc.getLastestOffset(topics)
    // last.map { case (tp, l) => println(s"""${tp},(${l},${consum(tp)},${l - consum(tp)})"""); (l - consum(tp)) }
    skc.kc.updateOffset(skc.kc.getEarleastOffset(topics))
    val rdd = skc.createKafkaRDD(topics, 100)
rdd.foreach(println)
    //skc.updateOffset(rdd)
  }
}
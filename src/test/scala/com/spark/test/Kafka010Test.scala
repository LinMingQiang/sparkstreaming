package com.kakfa.offset.test

import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.Properties
import org.apache.kafka.common.TopicPartition
import java.util.ArrayList
import java.util.Arrays
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object Kafka010Test {
  val broker = "192.168.0.234:9092,192.168.0.235:9092,192.168.0.236:9092"
  val topics = Set("test010")
  def main(args: Array[String]): Unit = {
    //spark-streaming-kafka-0-10_2.11 版本
    /*val props = new Properties();
    props.put("bootstrap.servers", broker);
    props.put("group.id", "test");
    props.put("enable.auto.commit", "false"); //自动commit
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("key.deserializer.encoding", "UTF8");
    props.put("value.deserializer.encoding", "UTF8");
    val kp = props.toMap[String,Object]
    val lastoffset = getLastOffset
    val sc = new SparkContext(new SparkConf().setAppName("ss").setMaster("local"))
    val offsetRanges = lastoffset.map { case (tp, of) => OffsetRange(tp, 0, of) }.toArray
    val rdd = KafkaUtils.createRDD[String, String](
      sc,
      kp.asJava,
      offsetRanges,
      LocationStrategies.PreferConsistent)
    rdd.foreach { cr => println(cr.value()) }*/
  }
  //kafka 010 获取最新偏移量
  //kafka 010 不提供kafkaCluster类，这个类只有在spark-streaming-kafka-0-8 才有
  def getLastOffset() = {
    val props = new Properties();
    props.put("bootstrap.servers", broker);
    props.put("group.id", "test");
    props.put("enable.auto.commit", "false"); //自动commit
    props.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    );
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer"
    );
    props.put("key.deserializer.encoding", "UTF8");
    props.put("value.deserializer.encoding", "UTF8");
    val c = new KafkaConsumer[String, String](props)
    c.subscribe(Arrays.asList("test010")); //订阅topic
    //val d = c.poll(0) //拉取数据，延迟为10000.也就是缓存里面的10000前的数据，如果为0的话就全部取
    val parts = c.assignment() //获取topic等信息
    c.pause(parts)
    //以下是表示从哪里开始读取
    //c.seekToBeginning(new ArrayList[TopicPartition]());//把offset设置到最开始
    //c.seek(partition, offset)//把某个分区的offset设置到哪个位置
    c.seekToEnd(parts) //从最新开始取
    val re = parts.map { ps =>
      ps -> c.position(ps)
    }
    re
  }
}

package org.apache.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.KafkaSparkContextManager
import scala.reflect.ClassTag
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
class SparkKafkaContext(sconf:SparkConf){
  val sc=new SparkContext(sconf)
  def kafkaRDD[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag](
      kp:Map[String, String],
      topics: Set[String],
      msgHandle: (MessageAndMetadata[K, V]) => R)={
    KafkaSparkContextManager.createKafkaRDD[K, V, KD, VD, R](sc, kp, topics, null, msgHandle)
  }
  def kafkaRDD[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag](
      kp:Map[String, String],
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long],
      msgHandle: (MessageAndMetadata[K, V]) => R)={
    KafkaSparkContextManager.createKafkaRDD[K, V, KD, VD, R](sc, kp, topics, fromOffset, msgHandle)
  }
  def kafkaRDD[R: ClassTag](
      kp:Map[String, String],
      topics: Set[String],
      msgHandle: (MessageAndMetadata[String, String]) => R)={
    KafkaSparkContextManager.createKafkaRDD[String, String, StringDecoder, StringDecoder, R](sc, kp, topics, null, msgHandle)
  }
  def kafkaRDD[R: ClassTag](
      kp:Map[String, String],
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long],
      msgHandle: (MessageAndMetadata[String, String]) => R)={
    KafkaSparkContextManager.createKafkaRDD[String, String, StringDecoder, StringDecoder, R](sc, kp, topics, fromOffset, msgHandle)
  }
}
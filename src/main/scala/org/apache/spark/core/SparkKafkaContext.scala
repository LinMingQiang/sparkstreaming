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
import org.apache.spark.rdd.RDD
class SparkKafkaContext {
  var sc: SparkContext = null
  def this(sc: SparkContext) {
    this()
    this.sc = sc
  }
  def this(conf: SparkConf) {
    this()
    sc = new SparkContext(conf)
  }
  def sparkcontext() = sc
  /**
   *
   */
  def broadcast[T: ClassTag](value: T) = {
    sc.broadcast(value)
  }
  
  def getRDDOffset[T](rdd:RDD[T])={
    KafkaSparkContextManager.getRDDConsumerOffsets(rdd)
  }
  def getLastOffset(topics: Set[String], kp: Map[String, String])={
    KafkaSparkContextManager.getLatestOffsets(topics, kp)
  }
  /**
   * 将当前的topic的groupid更新至最新的offsets
   */
  def updataOffsetToLastest(topics: Set[String], kp: Map[String, String]) = {
    val lastestOffsets = KafkaSparkContextManager.getLatestOffsets(topics, kp)
    KafkaSparkContextManager.updateConsumerOffsets(kp, lastestOffsets)
    lastestOffsets
  }
 
  /**
   * 为 sc提供更新offset的功能
   */
  def updateRDDOffsets[T](kp: Map[String, String], groupId: String, rdd: RDD[T]) {
    KafkaSparkContextManager.updateRDDOffset(kp, groupId, rdd)
  }
  /**
   * 为 sc提供更新offset的功能
   */
  def updateConsumerOffsets(kp: Map[String, String], offsets: Map[TopicAndPartition, Long]) {
    KafkaSparkContextManager.updateConsumerOffsets(kp, offsets)
  }
  /**
   * 创建 kafkaDataRDD
   */
  def kafkaRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    msgHandle: (MessageAndMetadata[K, V]) => R) = {
    KafkaSparkContextManager.createKafkaRDD[K, V, KD, VD, R](sc, kp, topics, null, msgHandle)
  }
    /**
   * 创建 kafkaDataRDD
   */
  def kafkaRDD[R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    msgHandle: (MessageAndMetadata[String, String]) => R) = {
    KafkaSparkContextManager.createKafkaRDD[String, String, StringDecoder, StringDecoder, R](sc, kp, topics, null, msgHandle)
  }
  /**
   * 创建kafkaRDD 但是提供fromOffset
   */
  def kafkaRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    msgHandle: (MessageAndMetadata[K, V]) => R) = {
    KafkaSparkContextManager.createKafkaRDD[K, V, KD, VD, R](sc, kp, topics, fromOffset, msgHandle)
  }
  /**
   * 现在读取条数（每个分区）
   */
  def kafkaRDD[R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    maxMessagesPerPartition: Int,
    msgHandle: (MessageAndMetadata[String, String]) => R) = {
    KafkaSparkContextManager.createKafkaRDD[String, String, StringDecoder, StringDecoder, R](sc, kp, topics, null, maxMessagesPerPartition, msgHandle)
  }
}
object SparkKafkaContext extends SparkKafkaConfsKey {

}
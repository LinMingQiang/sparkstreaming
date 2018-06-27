package org.apache.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka.SparkContextKafkaManager
import scala.reflect.ClassTag
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
/**
 * @author LMQ
 * @description 用于替代SparkContext。但sparkContext的很多功能没写，可以自己添加，或者直接拿sparkcontext使用
 * @description 此类主要是用于 创建 kafkaRDD 。
 * @description 创建的kafkaRDD提供更新偏移量的能力
 */
class SparkKafkaContext {
  var sparkcontext: SparkContext = null
  def this(sparkcontext: SparkContext) {
    this()
    this.sparkcontext = sparkcontext
  }
  def this(conf: SparkConf) {
    this()
    sparkcontext = new SparkContext(conf)
  }
  def broadcast[T: ClassTag](value: T) = {
    sparkcontext.broadcast(value)
  }

  /**
   * @author LMQ
   * @description 获取rdd的偏移量
   * @description 这个方法要求RDD继承 HasOffsetRanges
   */
  def getRDDOffset[T](rdd: RDD[T]) = {
    SparkContextKafkaManager.getRDDConsumerOffsets(rdd)
  }
  /**
   * @author LMQ
   * @description 获取topics的最新的offset
   */
  def getLastOffset(
    topics: Set[String],
    kp: Map[String, String]) = {
    SparkContextKafkaManager.getLatestOffsets(topics, kp)
  }
  /**
   * @author LMQ
   * @description 将当前的topic的偏移量更新至最新。（相当于丢掉未处理的数据）
   * @return lastestOffsets ：返回最新的offset
   */
  def updateOffsetToLastest(
    topics: Set[String],
    kp: Map[String, String]) = {
    val lastestOffsets = SparkContextKafkaManager.getLatestOffsets(topics, kp)
    SparkContextKafkaManager.updateConsumerOffsets(kp, lastestOffsets)
    lastestOffsets
  }
  /**
   * @author LMQ
   * @description 将当前的topic的偏移量更新至最旧。
   * @return lastestOffsets ：返回最新的offset
   */
  def updateOffsetToEarliest(
    topics: Set[String],
    kp: Map[String, String]) = {
    val earliestOffsets = SparkContextKafkaManager.getEarliestOffsets(topics, kp)
    SparkContextKafkaManager.updateConsumerOffsets(kp, earliestOffsets)
    earliestOffsets
  }
  /**
   * @author LMQ
   * @description 为 sc提供更新offset的功能
   */
  def updateRDDOffsets[T](
    kp: Map[String, String],
    groupId: String,
    rdd: RDD[T]) {
    SparkContextKafkaManager.updateRDDOffset(kp, groupId, rdd)
  }
  /**
   * @author LMQ
   * @description 为 sc提供更新offset的功能
   */
  def updateConsumerOffsets(
    kp: Map[String, String],
    offsets: Map[TopicAndPartition, Long]) {
    SparkContextKafkaManager.updateConsumerOffsets(kp, offsets)
  }
  /**
   * @author LMQ
   * @description 创建一个kafkaRDD。从kafka拉取数据
   * @param kp：kafka配置参数
   * @param topics： topics
   * @param msgHandle：拉取哪些kafka数据
   */
  def kafkaRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    msgHandle: (MessageAndMetadata[K, V]) => R) = {
    SparkContextKafkaManager
      .createKafkaRDD[K, V, KD, VD, R](
        sparkcontext, kp, topics, null, msgHandle)
  }
  /**
   * @author LMQ
   * @description 创建一个kafkaRDD。从kafka拉取数据
   * @param kp：kafka配置参数
   * @param topics： topics
   * @attention 这里没有传 msgHandle 则使用默认的msgHandle （输出为Tuple2(topic,msg）
   */
  def kafkaRDD(
    kp: Map[String, String],
    topics: Set[String]) = {
    SparkContextKafkaManager
      .createKafkaRDD[String, String, StringDecoder, StringDecoder, (String, String)](
        sparkcontext, kp, topics, null)
  }
  /**
   * @author LMQ
   * @description 创建一个kafkaRDD。从kafka拉取数据
   * @param kp：kafka配置参数
   * @param topics： topics
   * @param msgHandle：拉取哪些kafka数据
   */
  def kafkaRDD[R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    msgHandle: (MessageAndMetadata[String, String]) => R) = {
    SparkContextKafkaManager
      .createKafkaRDD[String, String, StringDecoder, StringDecoder, R](
        sparkcontext, kp, topics, null, msgHandle)
  }
  /**
   * @author LMQ
   * @description 创建一个kafkaRDD。从kafka拉取数据
   * @param kp：kafka配置参数
   * @param topics： topics
   * @param fromOffset: 拉取数据的起始offset
   * @param msgHandle：拉取哪些kafka数据
   */
  def kafkaRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    msgHandle: (MessageAndMetadata[K, V]) => R) = {
    SparkContextKafkaManager
      .createKafkaRDD[K, V, KD, VD, R](
        sparkcontext, kp, topics, fromOffset, msgHandle)
  }
  /**
   * @author LMQ
   * @description 创建一个kafkaRDD。从kafka拉取数据
   * @param kp：kafka配置参数
   * @param topics： topics
   * @param fromOffset: 拉取数据的起始offset
   */
  def kafkaRDD(
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long]) = {
    SparkContextKafkaManager
      .createKafkaRDD[String, String, StringDecoder, StringDecoder, (String, String)](
        sparkcontext, kp, topics, fromOffset)
  }
  /**
   * @author LMQ
   * @description 创建一个kafkaRDD。从kafka拉取数据
   * @param kp：kafka配置参数
   * @param topics： topics
   * @param maxMessagesPerPartition:每个分区最多拉取多少条
   * @attention 这里没有传 msgHandle 则使用默认的msgHandle （输出为Tuple2(topic,msg）
   */
  def kafkaRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    maxMessagesPerPartition: Int,
    msgHandle: (MessageAndMetadata[K, V]) => R) = {
    SparkContextKafkaManager
      .createKafkaRDD2[K, V, KD, VD, R](
        sparkcontext, kp, topics, null, maxMessagesPerPartition, msgHandle)
  }
  /**
   * @author LMQ
   * @description 创建一个kafkaRDD。从kafka拉取数据
   * @param kp：kafka配置参数
   * @param topics： topics
   * @param maxMessagesPerPartition:每个分区最多拉取多少条
   * @attention 这里没有传 msgHandle 则使用默认的msgHandle （输出为Tuple2(topic,msg）
   */
  def kafkaRDD(
    kp: Map[String, String],
    topics: Set[String],
    maxMessagesPerPartition: Int) = {
    SparkContextKafkaManager
      .createKafkaRDD2[String, String, StringDecoder, StringDecoder, (String, String)](
        sparkcontext, kp, topics, null, maxMessagesPerPartition)
  }
}
object SparkKafkaContext extends SparkKafkaConfsKey {

}
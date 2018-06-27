package org.apache.spark.core

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import scala.reflect.ClassTag
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.StreamingKafkaManager
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.common.util.Configuration
import org.apache.spark.streaming.kafka.SparkContextKafkaManager
import org.apache.spark.rdd.RDD
import org.apache.spark.common.util.KafkaConfig
import kafka.serializer.Decoder
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * @author LMQ
 * @description 用于替代StreamingContext。但StreamingContext的很多功能没写，可以自己添加，或者直接拿StreamingContext使用
 * @description 此类主要是用于读取kafka数据， 创建 Dstream 。
 * @description 目前只提供createDirectStream的方式读取kafka
 */
class StreamingKafkaContext {
  var streamingContext: StreamingContext = null
  var sc: SparkContext = null

  def this(streamingContext: StreamingContext) {
    this()
    this.streamingContext = streamingContext
    this.sc = streamingContext.sparkContext
  }
  def this(sc: SparkContext, batchDuration: Duration) {
    this()
    this.sc = sc
    streamingContext = new StreamingContext(sc, batchDuration)
  }
  def start() {
    streamingContext.start()
  }
  def awaitTermination() {
    streamingContext.awaitTermination
  }
  /**
   * @author LMQ
   * @description 将当前的topic的偏移量更新至最新。（相当于丢掉未处理的数据）
   * @return lastestOffsets ：返回最新的offset
   */
  def updataOffsetToLastest(
    topics: Set[String],
    kp: Map[String, String]) = {
    val lastestOffsets = getLastOffset(topics, kp)
    SparkContextKafkaManager.updateConsumerOffsets(kp, lastestOffsets)
    lastestOffsets
  }
  /**
   * @author LMQ
   * @description 获取最新的offset
   * @return lastestOffsets ：返回最新的offset
   */
  def getLastOffset(
    topics: Set[String],
    kp: Map[String, String]) = {
    SparkContextKafkaManager
      .getLatestOffsets(topics, kp)
  }
  /**
   * @author LMQ
   * @description 更新rdd的offset
   */
  def updateRDDOffsets[T](
    kp: Map[String, String],
    groupId: String,
    rdd: RDD[T]) {
    SparkContextKafkaManager.updateRDDOffset(kp, groupId, rdd)
  }
  /**
   * @author LMQ
   * @description 更新rdd的offset
   */
  def updateRDDOffsets[T](
    kp: Map[String, String],
    rdd: RDD[T]) {
    if (kp.contains("group.id")) {
      val groupid = kp.get("group.id").get
      SparkContextKafkaManager.updateRDDOffset(kp, groupid, rdd)
    } else println("No Group Id To UpdateRDDOffsets")
  }
  /**
   * @author LMQ
   * @description 获取rdd的offset
   */
  def getRDDOffsets[T](rdd: RDD[T]) = {
    StreamingKafkaManager.getRDDConsumerOffsets(rdd)
  }
   /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param kp:kafka配置
   * @param topics：topic列表
   * @param fromOffset ： 读取数据的offset起点
   * @param msgHandle ：读取kafka数据的方法
   */
  def createDirectStream(
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long]
    )= {
    StreamingKafkaManager
    .createDirectStream[String, String](
        streamingContext, kp, topics, fromOffset)
  }
     /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param kp:kafka配置
   * @param topics：topic列表
   * @param fromOffset ： 读取数据的offset起点
   * @param msgHandle ：读取kafka数据的方法
   */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long]
    ): InputDStream[ConsumerRecord[K,V]] = {
    StreamingKafkaManager
    .createDirectStream[K, V](
        streamingContext, kp, topics, fromOffset)
  }
  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param kp:kafka配置
   * @param topics：topic列表
   * @param msgHandle ：读取kafka数据的方法
   */
  def createDirectStream[K: ClassTag, V: ClassTag](
    kp: Map[String, String],
    topics: Set[String]
    ): InputDStream[ConsumerRecord[K,V]] = {
    StreamingKafkaManager
    .createDirectStream[K, V](
        streamingContext, kp, topics, null)
  }
    /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param kp:kafka配置
   * @param topics：topic列表
   * @param msgHandle ：读取kafka数据的方法
   */
  def createDirectStream(
    kp: Map[String, String],
    topics: Set[String]
    ): InputDStream[ConsumerRecord[String, String]] = {
    StreamingKafkaManager
    .createDirectStream[String, String](
        streamingContext, kp, topics, null)
  }
  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param conf ： 包含kp和topics
   * @param fromOffset ： 读取数据的offset起点
   * @param msgHandle ：读取kafka数据的方法
   */
  def createDirectStream(
    conf: KafkaConfig,
    fromOffset: Map[TopicAndPartition, Long]
    ): InputDStream[ConsumerRecord[String, String]]  = {
    StreamingKafkaManager
    .createDirectStream[String, String](
        streamingContext, conf, fromOffset)
  }
  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param conf ： 包含kp和topics
   * @param fromOffset ： 读取数据的offset起点
   * @param msgHandle ：读取kafka数据的方法
   */
  def createDirectStream[K: ClassTag, V: ClassTag](
    conf: KafkaConfig,
    fromOffset: Map[TopicAndPartition, Long]
    ): InputDStream[ConsumerRecord[K,V]]  = {
    StreamingKafkaManager
    .createDirectStream[K, V](
        streamingContext, conf, fromOffset)
  }
  /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param conf ： 包含kp和topics
   * @param msgHandle ：读取kafka数据的方法
   */
  def createDirectStream[K: ClassTag, V: ClassTag](
    conf: KafkaConfig
    ): InputDStream[ConsumerRecord[String, String]]  = {
    StreamingKafkaManager
    .createDirectStream[String, String](
        streamingContext, conf, null)
  }
    /**
   * @author LMQ
   * @description 从kafka使用direct的方式获取数据
   * @param conf ： 包含kp和topics
   * @param msgHandle ：读取kafka数据的方法
   */
  def createDirectStream(
    conf: KafkaConfig
    ): InputDStream[ConsumerRecord[String, String]] = {
    StreamingKafkaManager
    .createDirectStream[String, String](
        streamingContext, conf, null)
  }
}
object StreamingKafkaContext extends SparkKafkaConfsKey {

}
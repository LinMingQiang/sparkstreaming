package org.apache.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.reflect.ClassTag
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.SparkKafkaManager
import org.apache.spark.streaming.kafka.KafkaDataRDD

/**
  * @author LMQ
  * @description 用于替代SparkContext。但sparkContext的很多功能没写，可以自己添加，或者直接拿sparkcontext使用
  * @description 此类主要是用于 创建 kafkaRDD 。
  * @description 创建的kafkaRDD提供更新偏移量的能力
  */
class SparkKafkaContext(var kp: Map[String, String]) {
  var sparkcontext: SparkContext = null
  lazy val skm = new SparkKafkaManager(kp) //这个类似于KafkaCluster
  lazy val conf = sparkcontext.getConf
  def this(kp: Map[String, String], sparkcontext: SparkContext) {
    this(kp)
    this.sparkcontext = sparkcontext
  }
  def this(kp: Map[String, String], conf: SparkConf) {
    this(kp)
    sparkcontext = new SparkContext(conf)
  }
  def this(kp: Map[String, String], master: String, appName: String) {
    this(kp)
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName(appName)
    sparkcontext = new SparkContext(conf)
  }
  def this(kp: Map[String, String], appName: String) {
    this(kp)
    val conf = new SparkConf()
    conf.setAppName(appName)
    sparkcontext = new SparkContext(conf)
  }
  def broadcast[T: ClassTag](value: T) = {
    sparkcontext.broadcast(value)
  }
  def setKafkaParam(kp: Map[String, String]) {
    this.kp = kp
    skm.setKafkaParam(kp)
  }

  /**
    * @author LMQ
    * @description 获取rdd的偏移量
    * @description 这个方法要求RDD继承 HasOffsetRanges
    */
  def getRDDOffset[T](rdd: RDD[T]) = {
    skm.getRDDConsumerOffsets(rdd)
  }

  /**
    * @author LMQ
    * @description 获取topics的最新的offset
    */
  def getLastOffset(topics: Set[String]) = {
    skm.getLatestOffsets(topics)
  }

  /**
    * @author LMQ
    * @description 将当前的topic的偏移量更新至最新。（相当于丢掉未处理的数据）
    * @return lastestOffsets ：返回最新的offset
    */
  def updateOffsetToLastest(topics: Set[String], kp: Map[String, String]) = {
    val lastestOffsets = skm.getLatestOffsets(topics)
    skm.updateConsumerOffsets(lastestOffsets)
    lastestOffsets
  }

  /**
    * @author LMQ
    * @description 将当前的topic的偏移量更新至最旧。
    * @return lastestOffsets ：返回最新的offset
    */
  def updateOffsetToEarliest(topics: Set[String]) = {
    val earliestOffsets = skm.getEarliestOffsets(topics)
    skm.updateConsumerOffsets(earliestOffsets)
    earliestOffsets
  }

  /**
    * @author LMQ
    * @description 为 sc提供更新offset的功能
    */
  def updateRDDOffsets[T](groupId: String, rdd: RDD[T]) {
    skm.updateRDDOffset(groupId, rdd)
  }

  /**
    * @author LMQ
    * @description 为 sc提供更新offset的功能
    */
  def updateConsumerOffsets(offsets: Map[TopicAndPartition, Long]) {
    skm.updateConsumerOffsets(offsets)
  }

  /**
    * @author LMQ
    * @description 创建一个kafkaRDD。从kafka拉取数据
    * @param topics： topics
    */
  def kafkaRDD[K: ClassTag, V: ClassTag](topics: Set[String]) = {
    skm.createKafkaRDD[K, V](this, topics, null)
  }

  /**
    * @author LMQ
    * @description 创建一个kafkaRDD。从kafka拉取数据
    * @param topics： topics
    * @param fromOffset: 拉取数据的起始offset
    */
  def kafkaRDD[K: ClassTag, V: ClassTag](
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long]) = {
    skm
      .createKafkaRDD[K, V](this, topics, fromOffset)
  }

  /**
    * @author LMQ
    * @description 创建一个kafkaRDD。从kafka拉取数据
    * @param topics： topics
    * @param fromOffset: 拉取数据的起始offset
    */
  def kafkaRDD(topics: Set[String],
               fromOffset: Map[TopicAndPartition, Long]) = {
    skm
      .createKafkaRDD[String, String](this, topics, fromOffset)
  }

  /**
    * @author LMQ
    * @description 创建一个kafkaRDD。从kafka拉取数据
    * @param topics： topics
    * @param maxMessagesPerPartition:每个分区最多拉取多少条
    * @attention 这里没有传 msgHandle 则使用默认的msgHandle （输出为Tuple2(topic,msg）
    */
  def kafkaRDD[K: ClassTag, V: ClassTag](topics: Set[String],
                                         maxMessagesPerPartition: Int) = {
    skm
      .createKafkaRDD[K, V](this, topics, null, maxMessagesPerPartition)
  }

  /**
    * @author LMQ
    * @description 创建一个kafkaRDD。从kafka拉取数据
    * @time 2019-06-08
    * @param maxMessagesPerPartition : 控制各个分区获取的条数限制，为了ratecontroller准备
    */
  def kafkaRDD[K: ClassTag, V: ClassTag](
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long],
      maxMessagesPerPartition: Option[Map[TopicAndPartition, Long]]) = {
    skm
      .createKafkaRDD[K, V](this, topics, fromOffset, maxMessagesPerPartition)
  }

  /**
    * @author LMQ
    * @description 创建一个kafkaRDD。从kafka拉取数据
    * @param topics： topics
    * @param maxMessagesPerPartition:每个分区最多拉取多少条
    * @attention 这里没有传 msgHandle 则使用默认的msgHandle （输出为Tuple2(topic,msg）
    */
  def kafkaRDD(topics: Set[String], maxMessagesPerPartition: Int) = {
    skm
      .createKafkaRDD[String, String](this,
                                      topics,
                                      null,
                                      maxMessagesPerPartition)
  }
}
object SparkKafkaContext extends SparkKafkaConfsKey {}

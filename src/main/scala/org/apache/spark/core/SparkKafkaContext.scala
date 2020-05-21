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
import org.apache.spark.kafka.util.KafkaCluster
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkException
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scala.collection.mutable.HashMap
import kafka.serializer.Decoder
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkContext
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import java.{ util => ju }
import org.apache.spark.streaming.kafka010.KafkaRDD
import org.apache.spark.kafka.util.KafkaCluster
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import scala.beans.BeanProperty

/**
 * @author LMQ
 * @description 用于替代SparkContext。但sparkContext的很多功能没写，可以自己添加，或者直接拿sparkcontext使用
 * @description 此类主要是用于 创建 kafkaRDD 。
 * @description 创建的kafkaRDD提供更新偏移量的能力
 */
class SparkKafkaContext[K: ClassTag, V: ClassTag] extends SparkKafkaConfsKey{
  var sparkcontext: SparkContext = null
  var kc: KafkaCluster[K, V] = null
  lazy val conf = sparkcontext.getConf
  def initKafkaCluster(kp: Map[String, String]) = {
    if (kc == null) {
      kc = new KafkaCluster(kp)
    }
  }
  def this(kp: Map[String, String],
           sparkcontext: SparkContext) {
    this()
    initKafkaCluster(kp)
    this.sparkcontext = sparkcontext
  }
  def this(kp: Map[String, String],
           conf: SparkConf) {
    this()
    initKafkaCluster(kp)
    sparkcontext = new SparkContext(conf)
  }
  def this(kp: Map[String, String],
           master: String,
           appName: String) {
    this()
    val conf = new SparkConf()
    initKafkaCluster(kp)
    conf.setMaster(master)
    conf.setAppName(appName)
    sparkcontext = new SparkContext(conf)
  }
  def this(kp: Map[String, String],
           appName: String) {
    this()
    initKafkaCluster(kp)
    val conf = new SparkConf()
    conf.setAppName(appName)
    sparkcontext = new SparkContext(conf)
  }
  def broadcast[T: ClassTag](value: T) = {
    sparkcontext.broadcast(value)
  }

  /**
   * @author LMQ
   * @time 2018.11.01
   * @desc 自定义读取数据区间
   */
  def createKafkaRDD(offsetRanges: Array[OffsetRange]) = {
    new KafkaRDD[K, V](
      sparkcontext,
      kc.fixKafkaExcutorParams,
      offsetRanges,
      ju.Collections.emptyMap[TopicPartition, String](),
      true)
  }
  /**
   * @author LMQ
   * @time 2018-11-05
   * @desc 更获取kafkardd，限制读取条数（默认限制10000，如果想要不限制可以设置成0）
   */
  def createKafkaRDD(topics: Set[String],
                     perParLimit: Long = 10000) = {
    new KafkaRDD[K, V](
      sparkcontext,
      kc.fixKafkaExcutorParams,
      kc.getOffsetRange(topics, perParLimit),
      ju.Collections.emptyMap[TopicPartition, String](),
      true)
  }
  /**
   * @author LMQ
   * @time 2018-11-05
   * @desc 更新offset
   */
  def updateOffset[T](rdd: RDD[T]) {
    val untilOffset = rdd
      .asInstanceOf[HasOffsetRanges]
      .offsetRanges
      .map { x => (x.topicPartition(), x.untilOffset) }.toMap
    kc.updateOffset(untilOffset)
  }
  /**
   * @author LMQ
   * @time 2018-11-05
   * @desc 更新offset
   */
  def updateOffset(offsetRanges: Array[OffsetRange]) {
    val untilOffset =
      offsetRanges
        .map { x => (x.topicPartition(), x.untilOffset) }.toMap

    kc.updateOffset(untilOffset)
  }
  /**
   * @author LMQ
   * @time 2018-11-05
   * @desc 更新offset
   */
  def updateOffset(last: Map[TopicPartition, Long]) {
    kc.updateOffset(last)
  }
}
object SparkKafkaContext  extends SparkKafkaConfsKey {
  
}

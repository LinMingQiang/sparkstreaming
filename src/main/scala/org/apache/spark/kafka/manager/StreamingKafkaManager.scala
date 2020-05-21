package org.apache.spark.streaming.kafka

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkException
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scala.collection.mutable.HashMap
import org.apache.spark.common.util.Configuration
import kafka.serializer.Decoder
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.kafka.manager.SparkKafkaManagerBase
import org.apache.spark.common.util.KafkaConfig
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.LocationStrategies
import java.{util => ju}
import scala.collection.JavaConverters._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * @author LMQ
  * @time 2018.03.07
  * @description 用于spark streaming 读取kafka数据
  */
private[spark] class StreamingKafkaManager(override var kp: Map[String, String])
    extends SparkKafkaManagerBase {
  logname = "StreamingKafkaManager"

  /**
    * @author LMQ
    * @description 创建一个kafka的Dstream。
    * @param ssc ： 一个StreamingContext
    * @param topics ： kakfa的topic列表
    * @param fromOffset ： 如果想自己自定义从指定的offset开始读的话，传入这个值
    */
  def createDirectStream[K: ClassTag, V: ClassTag](
      ssc: StreamingContext,
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long]
  ): InputDStream[ConsumerRecord[K, V]] = {
    if (kp == null || !kp.contains(GROUPID))
      throw new SparkException(
        s"kafkaParam is Null or ${GROUPID} is not setted")
    val groupId = kp.get(GROUPID).get
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val fromWhere =
          if (kp.contains(CONSUMER_FROM)) kp.get(CONSUMER_FROM).get
          else DEFUALT_FROM
        fromWhere.toUpperCase match {
          case LAST     => getLatestOffsets(topics)
          case EARLIEST => getEarliestOffsets(topics)
          case CONSUM   => getConsumerOffset(groupId, topics)
          case _ =>
            log.error(
              s"""${CONSUMER_FROM} must LAST or CONSUM,defualt is LAST""");
            getLatestOffsets(topics)
        }
      } else fromOffset
    KafkaUtils.createDirectStream[K, V](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[K, V](
        topics.asJavaCollection,
        fixKafkaExcutorParams,
        consumerOffsets.map {
          case (tp, lo) =>
            (new TopicPartition(tp.topic, tp.partition), new java.lang.Long(lo))
        }.asJava
      )
    )
  }

  /**
    * @author LMQ
    * @description 创建一个kafka的Dstream。使用配置文件的方式。kp和topic统一放入KafkaConfiguration
    * @param ssc ： 一个StreamingContext
    * @param conf : 配置信息 (不知道怎么配，可以看示例)
    * @param fromOffset ： 如果想自己自定义从指定的offset开始读的话，传入这个值
    */
  def createDirectStream[K: ClassTag, V: ClassTag](
      ssc: StreamingContext,
      conf: KafkaConfig,
      fromOffset: Map[TopicAndPartition, Long]
  ): InputDStream[ConsumerRecord[K, V]] = {
    if (conf.kpIsNull || conf.tpIsNull) {
      throw new SparkException(
        s"Configuration s kafkaParam is Null or Topics is not setted")
    }
    val kp = conf.getKafkaParams()
    if (!kp.contains(GROUPID) && !conf.containsKey(GROUPID))
      throw new SparkException(
        s"Configuration s kafkaParam is Null or ${GROUPID} is not setted")
    val groupId =
      if (kp.contains(GROUPID)) kp.get(GROUPID).get
      else conf.get(GROUPID)
    val topics = conf.topics
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val fromWhere =
          if (kp.contains(CONSUMER_FROM)) kp.get(CONSUMER_FROM).get
          else if (conf.containsKey(CONSUMER_FROM)) conf.get(CONSUMER_FROM)
          else DEFUALT_FROM
        fromWhere.toUpperCase match {
          case LAST     => getLatestOffsets(topics)
          case EARLIEST => getEarliestOffsets(topics)
          case CONSUM   => getConsumerOffset(groupId, topics)
          case _ =>
            log.error(
              s"""${CONSUMER_FROM} must LAST or CONSUM,defualt is LAST""");
            getLatestOffsets(topics)
        }
      } else fromOffset
    KafkaUtils.createDirectStream[K, V](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[K, V](
        topics.asJavaCollection,
        fixKafkaExcutorParams,
        consumerOffsets.map {
          case (tp, lo) =>
            (new TopicPartition(tp.topic, tp.partition), new java.lang.Long(lo))
        }.asJava
      )
    )
  }

}

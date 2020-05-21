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
import org.apache.spark.SparkContext
import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.kafka.manager.SparkKafkaManagerBase
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import java.{util => ju}

private[spark] class SparkKafkaManager(override var kp: Map[String, String])
    extends SparkKafkaManagerBase {
  logname = "SparkContextKafkaManager"

  /**
    * @author LMQ
    * @time 2018.03.07
    * @description 创建一个 kafkaDataRDD
    * @description 这个kafkaDataRDD是自己定义的，可以自己添加很多自定义的功能（如：更新offset）
    * @description 读取kafka数据有四种方式：
    *  1 ： 从最新开始  = LAST
    * 				2 ：从上次消费开始  = CONSUM
    * 				3:从最早的offset开始消费  = EARLIEST
    * 				4：从自定义的offset开始  = CUSTOM
    * @attention maxMessagesPerPartitionKEY：这个参数。是放在sparkconf里面，或者是kp里面。如果两个都没配置，那默认是没有限制，
    * 						这样可能会导致一次性读取的数据量过大。也可以使用另一个带有maxMessagesPerPartition参数的方法来读取
    */
  def createKafkaRDD[K: ClassTag, V: ClassTag](
      sc: SparkKafkaContext,
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long]): KafkaDataRDD[K, V] = {
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
          case CONSUM   => getConsumerOffset(groupId, topics)
          case EARLIEST => getEarliestOffsets(topics)
          case CUSTOM   => getSelfOffsets()
          case _ =>
            log.info(
              s"""${CONSUMER_FROM} must LAST or CONSUM,defualt is LAST""");
            getLatestOffsets(topics)
        }
      } else fromOffset
    val maxMessagesPerPartition =
      if (kp.contains(MAX_RATE_PER_PARTITION))
        kp.get(MAX_RATE_PER_PARTITION).get.toInt
      else sc.conf.getInt(MAX_RATE_PER_PARTITION, 0) //0表示没限制
    val untilOffsets = clamp(latestLeaderOffsets(consumerOffsets),
                             consumerOffsets,
                             maxMessagesPerPartition)
    val offsetRange = getOffsetRange(consumerOffsets, untilOffsets)
    new KafkaDataRDD[K, V](sc,
                           fixKafkaExcutorParams,
                           offsetRange,
                           ju.Collections.emptyMap[TopicPartition, String](),
                           true)
  }

  /**
    * @author LMQ
    * @time 2018.03.07
    * @description 创建一个 kafkaDataRDD
    * @description 这个kafkaDataRDD是自己定义的，可以自己添加很多自定义的功能（如：更新offset）
    * @description 读取kafka数据有四种方式：
    *  1 ： 从最新开始  = LAST
    * 				2 ：从上次消费开始  = CONSUM
    * 				3:从最早的offset开始消费  = EARLIEST
    * 				4：从自定义的offset开始  = CUSTOM
    * @param maxMessagesPerPartition ： 限制读取的kafka数据量（每个分区多少数据）
    */
  def createKafkaRDD[K: ClassTag, V: ClassTag](
      sc: SparkKafkaContext,
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long],
      maxMessagesPerPartition: Int): KafkaDataRDD[K, V] = {
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
          case CONSUM   => getConsumerOffset(groupId, topics)
          case EARLIEST => getEarliestOffsets(topics)
          case CUSTOM   => getSelfOffsets()
          case _ =>
            log.info(
              s"""${CONSUMER_FROM} must LAST or CONSUM,defualt is LAST""");
            getLatestOffsets(topics)
        }
      } else fromOffset
    val untilOffsets = clamp(latestLeaderOffsets(consumerOffsets),
                             consumerOffsets,
                             maxMessagesPerPartition)
    val offsetRange = getOffsetRange(consumerOffsets, untilOffsets)
    new KafkaDataRDD[K, V](sc,
                           fixKafkaExcutorParams,
                           offsetRange,
                           ju.Collections.emptyMap[TopicPartition, String](),
                           true)
  }

  /**
    * @author LMQ
    * @time 2018.03.07
    * @description 创建一个 kafkaDataRDD
    * @description 这个kafkaDataRDD是自己定义的，可以自己添加很多自定义的功能（如：更新offset）
    * @description 读取kafka数据有四种方式：
    *  1 ： 从最新开始  = LAST
    * 				2 ：从上次消费开始  = CONSUM
    * 				3:从最早的offset开始消费  = EARLIEST
    * 				4：从自定义的offset开始  = CUSTOM
    * @param maxMessagesPerPartition ： 限制读取的kafka数据量（每个分区多少数据）
    */
  def createKafkaRDD[K: ClassTag, V: ClassTag](
      sc: SparkKafkaContext,
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long],
      maxMessagesPerPartition: Option[Map[TopicAndPartition, Long]])
    : KafkaDataRDD[K, V] = {
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
          case CONSUM   => getConsumerOffset(groupId, topics)
          case EARLIEST => getEarliestOffsets(topics)
          case CUSTOM   => getSelfOffsets()
          case _ =>
            log.info(
              s"""${CONSUMER_FROM} must LAST or CONSUM,defualt is LAST""");
            getLatestOffsets(topics)
        }
      } else fromOffset
    val untilOffsets = clamp(latestLeaderOffsets(consumerOffsets),
                             consumerOffsets,
                             maxMessagesPerPartition)

    val offsetRange = getOffsetRange(consumerOffsets, untilOffsets)
    new KafkaDataRDD[K, V](sc,
                           fixKafkaExcutorParams,
                           offsetRange,
                           ju.Collections.emptyMap[TopicPartition, String](),
                           true)

  }

  /**
    * @author LMQ
    * @description 获取自定义的offset值
    * @description 这个方法主要适用于 ： 如果程序想要重算某个时间点或者从指定的offset开始
    * 							例如程序需要重算今天的所有数据（前提你记录了今天凌晨的offset。这个我之后会有程序来提供每天记录所有topic凌晨的offset）
    * 						 https://github.com/LinMingQiang/kafka-util
    * @attention 这个方法只有在 配置 kafka.consumer.from = CUSTOM才生效
    *            同时必须要有一个 kafka.offset 的配置
    *            具体的 数据格式在readme.md里面有解释
    *
    */
  private def getSelfOffsets() = {
    var consumerOffsets = new HashMap[TopicAndPartition, Long]()
    var todayOffsets = kp.get(KAFKA_OFFSET).get.split('|')
    for (offset <- todayOffsets) {
      val offsets = offset.split(",")
      consumerOffsets.put(new TopicAndPartition(offsets(0), offsets(1).toInt),
                          offsets(2).toLong)
    }
    consumerOffsets.toMap
  }

  /**
    * @author LMQ
    * @description 获取最新的数据偏移量
    * @description 这个方法主要是spark在用，所以就不写进kafkatool的类里面了
    */
  def latestLeaderOffsets(consumerOffsets: Map[TopicAndPartition, Long])
    : Map[TopicAndPartition, LeaderOffset] = {
    val o = kc.getLatestLeaderOffsets(consumerOffsets.keySet)
    if (o.isLeft) {
      throw new SparkException(o.left.toString)
    } else {
      o.right.get
    }
  }

  /**
    * @author LMQ
    * @func 获取offsetrange
    */
  def getOffsetRange(
      consumerOffsets: Map[TopicAndPartition, Long],
      untilOffsets: Map[TopicAndPartition, KafkaCluster.LeaderOffset]) = {
    untilOffsets.map {
      case (tp, of) =>
        if (consumerOffsets.contains(tp)) {
          OffsetRange(tp.topic, tp.partition, consumerOffsets(tp), of.offset)
        } else {
          OffsetRange(tp.topic, tp.partition, 0, of.offset) //一个新分区，默认从0开始
        }
    }.toArray
  }

  /**
    * @author LMQ
    * @description 计算读取kafka的数据到哪返回一个Map[TopicAndPartition, LeaderOffset]
    * 							代表每个topic.partition->offset
    * @description 由maxMessagesPerPartition限制最多读取多少数据
    */
  def clamp(
      lastOffsets: Map[TopicAndPartition, LeaderOffset],
      currentOffsets: Map[TopicAndPartition, Long],
      maxMessagesPerPartition: Int): Map[TopicAndPartition, LeaderOffset] = {
    if (maxMessagesPerPartition > 0) {
      lastOffsets.map {
        case (tp, lo) =>
          if (currentOffsets.contains(tp))
            tp -> lo.copy(offset =
              Math.min(currentOffsets(tp) + maxMessagesPerPartition, lo.offset))
          else
            tp -> lo.copy(offset = Math.min(maxMessagesPerPartition, lo.offset)) //新增一个分区时

      }
    } else lastOffsets
  }

  /**
    * @author LMQ
    * @description 增加了速率控制
    */
  def clamp(lastOffsets: Map[TopicAndPartition, LeaderOffset],
            currentOffsets: Map[TopicAndPartition, Long],
            maxMessagesPerPartition: Option[Map[TopicAndPartition, Long]])
    : Map[TopicAndPartition, LeaderOffset] = {

    maxMessagesPerPartition
      .map { mmp =>
        mmp.map {
          case (tp, messages) =>
            val uo = lastOffsets(tp)
            if (currentOffsets.contains(tp))
              tp -> uo.copy(
                offset = Math.min(currentOffsets(tp) + messages, uo.offset))
            else
              tp -> uo.copy(offset = Math.min(messages, uo.offset)) //新增一个分区时
        }
      }
      .getOrElse(lastOffsets)
  }

}
object SparkKafkaManager {
  def apply(kp: Map[String, String]) = new SparkKafkaManager(kp)

}


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

private[spark] object KafkaSparkContextManager
    extends KafkaSparkTool {
  val lastOrConsum: String = "LAST"
  logname = "KafkaSparkContextManager"
  /**
   * 创建一个 kafkaDataRDD
   * 这个kafkaDataRDD是自己定义的，可以自己添加很多自定义的功能（如：更新offset）
   * 读取kafka数据有三种方式：
   * 1 ： 从最新开始 last
   * 2 ：从上次消费开始 consum
   * 3 ：从自定义的offset开始 CUSTOM
   */
  def createKafkaRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    sc: SparkContext,
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    messageHandler: MessageAndMetadata[K, V] => R) = {
    if (kp == null || !kp.contains(GROUP_ID))
      throw new SparkException(s"kafkaParam is Null or ${GROUP_ID} is not setted")
    instance(kp)
    val groupId = kp.get(GROUP_ID).get
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last = if (kp.contains(LAST_OR_CONSUMER)) kp.get(LAST_OR_CONSUMER).get
        else lastOrConsum
        last.toUpperCase match {
          case "LAST"   => getLatestOffsets(topics, kp)
          case "CONSUM" => getConsumerOffset(kp, groupId, topics)
          case "CUSTOM" => getSelfOffsets(kp)
          case _        => log.info(s"""${LAST_OR_CONSUMER} must LAST or CONSUM,defualt is LAST"""); getLatestOffsets(topics, kp)
        }
      } else fromOffset
    //consumerOffsets.foreach(x=>log.info(x.toString))
    val maxMessagesPerPartition = if (kp.contains(maxMessagesPerPartitionKEY))
      kp.get(maxMessagesPerPartitionKEY).get.toInt
    else sc.getConf.getInt(maxMessagesPerPartitionKEY, 0) //0表示没限制
    val untilOffsets = clamp(latestLeaderOffsets(consumerOffsets), consumerOffsets, maxMessagesPerPartition)

    val kd = KafkaRDD[K, V, KD, VD, R](
      sc,
      kp,
      consumerOffsets,
      untilOffsets,
      messageHandler)
    new KafkaDataRDD[K, V, KD, VD, R](kd)
  }
  /**
   * 自定义一个 maxMessagesPerPartition
   */
  def createKafkaRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    sc: SparkContext,
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    maxMessagesPerPartition: Int,
    messageHandler: MessageAndMetadata[K, V] => R) = {
    if (kp == null || !kp.contains(GROUP_ID))
      throw new SparkException(s"kafkaParam is Null or ${GROUP_ID} is not setted")
    instance(kp)
    val groupId = kp.get(GROUP_ID).get
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last = if (kp.contains(LAST_OR_CONSUMER)) kp.get(LAST_OR_CONSUMER).get
        else lastOrConsum
        last.toUpperCase match {
          case "LAST"   => getLatestOffsets(topics, kp)
          case "CONSUM" => getConsumerOffset(kp, groupId, topics)
          case _        => log.info(s"""${LAST_OR_CONSUMER} must LAST or CONSUM,defualt is LAST"""); getLatestOffsets(topics, kp)
        }
      } else fromOffset
    val untilOffsets = clamp(latestLeaderOffsets(consumerOffsets), consumerOffsets, maxMessagesPerPartition)

    val kd = KafkaRDD[K, V, KD, VD, R](
      sc,
      kp,
      consumerOffsets,
      untilOffsets,
      messageHandler)
    new KafkaDataRDD[K, V, KD, VD, R](kd)
  }
  /**
   * 自定义offset读取点
   */
  private def getSelfOffsets(kp: Map[String, String]) = {
    var consumerOffsets = new HashMap[TopicAndPartition, Long]()
    var todayOffsets = kp.get("kafka.offset").get.split('|')
    for (offset <- todayOffsets) {
      val offsets = offset.split(",")
      consumerOffsets.put(new TopicAndPartition(offsets(0), offsets(1).toInt), offsets(2).toLong)
    }
    consumerOffsets.toMap
  }
  /**
   * 最新的数据偏移量
   */
  def latestLeaderOffsets(consumerOffsets: Map[TopicAndPartition, Long]): Map[TopicAndPartition, LeaderOffset] = {
    val o = kc.getLatestLeaderOffsets(consumerOffsets.keySet)
    if (o.isLeft) {
      throw new SparkException(o.left.toString)
    } else {
      o.right.get
    }
  }
 /**
  * 获取 untiloffset
  */
  def clamp(leaderOffsets: Map[TopicAndPartition, LeaderOffset],
            currentOffsets: Map[TopicAndPartition, Long],
            maxMessagesPerPartition: Int): Map[TopicAndPartition, LeaderOffset] = {
    if (maxMessagesPerPartition > 0) {
      leaderOffsets.map {
        case (tp, lo) =>
          tp -> lo.copy(offset = Math.min(currentOffsets(tp) + maxMessagesPerPartition, lo.offset))
      }
    } else leaderOffsets
  }

  /**
   * get Kafka Data Handle
   */
  def msgHandle = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message)
  /**
   * get RDD Offset
   */
  def getRDDConsumerOffsets[T](rdd: RDD[T]) = {
    var consumoffsets = Map[TopicAndPartition, Long]()
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      consumoffsets += ((topicAndPartition, offsets.untilOffset))
    }
    consumoffsets
  }
  /**
   * update RDD Offset
   */
  def updateRDDOffset[T](kp: Map[String, String], groupId: String, rdd: RDD[T]) {
    val offsets = getRDDConsumerOffsets(rdd)
    updateConsumerOffsets(kp, groupId, offsets)
  }
  //将当前的topic的groupid更新至最新的offsets
  def updataOffsetToLastest(topics: Set[String], kp: Map[String, String]) = {
    val lastestOffsets = KafkaSparkContextManager.getLatestOffsets(topics, kp)
    updateConsumerOffsets(kp, kp.get("group.id").get, lastestOffsets)
    lastestOffsets
  }
}
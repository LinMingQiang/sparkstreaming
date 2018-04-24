
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

private[spark] object SparkContextKafkaManager
    extends SparkKafkaManagerBase{
  logname = "SparkContextKafkaManager"
  /**
   * @author LMQ
   * @time 2018.03.07
   * @description 创建一个 kafkaDataRDD
   * @description 这个kafkaDataRDD是自己定义的，可以自己添加很多自定义的功能（如：更新offset）
   * @description 读取kafka数据有四种方式：
   * @param 1 ： 从最新开始  = LAST
   * 				2 ：从上次消费开始  = CONSUM
   * 				3:从最早的offset开始消费  = EARLIEST
   * 				4：从自定义的offset开始  = CUSTOM
   * @attention maxMessagesPerPartitionKEY：这个参数。是放在sparkconf里面，或者是kp里面。如果两个都没配置，那默认是没有限制，
   * 						这样可能会导致一次性读取的数据量过大。也可以使用另一个带有maxMessagesPerPartition参数的方法来读取
   */
  def createKafkaRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    sc: SparkContext,
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    messageHandler: MessageAndMetadata[K, V] => R = msgHandle) = {
    if (kp == null || !kp.contains(GROUP_ID))
      throw new SparkException(s"kafkaParam is Null or ${GROUP_ID} is not setted")
    val groupId = kp.get(GROUP_ID).get
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last = if (kp.contains(KAFKA_CONSUMER_FROM)) kp.get(KAFKA_CONSUMER_FROM).get
                   else defualtFrom
        last.toUpperCase match {
          case "LAST"     => getLatestOffsets(topics, kp)
          case "CONSUM"   => getConsumerOffset(kp, groupId, topics)
          case "EARLIEST" => getEarliestOffsets(topics, kp)
          case "CUSTOM"   => getSelfOffsets(kp)
          case _          => log.info(s"""${KAFKA_CONSUMER_FROM} must LAST or CONSUM,defualt is LAST"""); getLatestOffsets(topics, kp)
        }
      } else fromOffset
      
    //consumerOffsets.foreach(x=>log.info(x.toString))
    val maxMessagesPerPartition = if (kp.contains(maxMessagesPerPartitionKEY)) kp.get(maxMessagesPerPartitionKEY).get.toInt
                                  else sc.getConf.getInt(maxMessagesPerPartitionKEY, 0) //0表示没限制
    val untilOffsets = clamp(latestLeaderOffsets(kp,0,consumerOffsets), consumerOffsets, maxMessagesPerPartition)

    val kd = KafkaRDD[K, V, KD, VD, R](
      sc,
      kp,
      consumerOffsets,
      untilOffsets,
      messageHandler)
    new KafkaDataRDD[K, V, KD, VD, R](kd)
  }
  /**
   * @author LMQ
   * @time 2018.03.07
   * @description 创建一个 kafkaDataRDD
   * @description 这个kafkaDataRDD是自己定义的，可以自己添加很多自定义的功能（如：更新offset）
   * @description 读取kafka数据有四种方式：
   * @param 1 ： 从最新开始  = LAST
   * 				2 ：从上次消费开始  = CONSUM
   * 				3:从最早的offset开始消费  = EARLIEST
   * 				4：从自定义的offset开始  = CUSTOM
   * @param maxMessagesPerPartition ： 限制读取的kafka数据量（每个分区多少数据）
   */
  def createKafkaRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    sc: SparkContext,
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    maxMessagesPerPartition: Int,
    messageHandler: MessageAndMetadata[K, V] => R = msgHandle) = {
    if (kp == null || !kp.contains(GROUP_ID))
      throw new SparkException(s"kafkaParam is Null or ${GROUP_ID} is not setted")
    instance(kp)
    val groupId = kp.get(GROUP_ID).get
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last = if (kp.contains(KAFKA_CONSUMER_FROM)) kp.get(KAFKA_CONSUMER_FROM).get
        else defualtFrom
        last.toUpperCase match {
          case "LAST"     => getLatestOffsets(topics, kp)
          case "CONSUM"   => getConsumerOffset(kp, groupId, topics)
          case "EARLIEST" => getEarliestOffsets(topics, kp)
          case "CUSTOM"   => getSelfOffsets(kp)
          case _          => log.info(s"""${KAFKA_CONSUMER_FROM} must LAST or CONSUM,defualt is LAST"""); getLatestOffsets(topics, kp)
        }
      } else fromOffset
    val untilOffsets = clamp(latestLeaderOffsets(kp,0,consumerOffsets), consumerOffsets, maxMessagesPerPartition)

    val kd = KafkaRDD[K, V, KD, VD, R](
      sc,
      kp,
      consumerOffsets,
      untilOffsets,
      messageHandler)
    new KafkaDataRDD[K, V, KD, VD, R](kd)
  }
  /**
   * @author LMQ
   * @description 获取自定义的offset值
   * @description 这个方法主要适用于 ： 如果程序想要重算某个时间点或者从指定的offset开始
   * 							例如程序需要重算今天的所有数据（前提你记录了今天凌晨的offset。这个我之后会有程序来提供每天记录所有topic凌晨的offset）
   * @attention 这个方法只有在 配置 kafka.consumer.from = CUSTOM才生效
   *            同时必须要有一个 kafka.offset 的配置
   *            具体的 数据格式在readme.md里面有解释
   *            
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
   * @author LMQ
   * @description 计算读取kafka的数据到哪返回一个Map[TopicAndPartition, LeaderOffset]
   * 							代表每个topic.partition->offset
   * @description 由maxMessagesPerPartition限制最多读取多少数据
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
}
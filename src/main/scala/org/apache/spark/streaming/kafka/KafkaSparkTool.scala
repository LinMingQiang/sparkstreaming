package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.rdd.RDD
import java.util.Properties
import org.apache.spark.Logging
import org.slf4j.LoggerFactory
/**
 * @author LMQ
 * @description 读取kafka，操作offset等操作工具
 * @description 只用于spark包使用
 */
private[spark] trait KafkaSparkTool {
  var logname = "KafkaSparkTool" //外部可重写
  lazy val defualtFrom: String = "LAST"
  lazy val log = LoggerFactory.getLogger(logname)
  var kc: KafkaCluster = null
  val GROUP_ID = "group.id"
  val KAFKA_CONSUMER_FROM = "kafka.consumer.from" //kakfa读取起点(CONSUM / LAST / EARLIEST / CUSTOM)
  val WRONG_GROUP_FROM = "wrong.groupid.from" //新用户或者过期用户 重新读取的点 （最新或者最旧）
  val maxMessagesPerPartitionKEY = "spark.streaming.kafka.maxRatePerPartition"
  /**
   * @author LMQ
   * @description init KafkaCluster
   */
  def instance(
    kp: Map[String, String]) {
    if (kc == null) kc = new KafkaCluster(kp)
  }
  /**
   * @author LMQ
   * @description 获取kakfa消费者的offset
   * @param kp ： kafka的配置
   * @param groupId：组名，用于唯一识别kakfa消费者身份
   * @param topics ：操作的topic列表
   * @attention 有几种情况要注意：
   *  1 ：如果这个groupid从未消费过
   *  2 :如果消费过，但是消费者记录的偏移量过期，或者超过了LAST。
   *  以上两种情况
   *  从何读起将取决于配置 （wrong.groupid.from）= > ( LAST / EARLIEST )
   */
  def getConsumerOffset(
    kp: Map[String, String],
    groupId: String,
    topics: Set[String]) = {
    instance(kp)
    var offsets: Map[TopicAndPartition, Long] = Map()
    topics.foreach { topic =>
      var hasConsumed = true //是否消费过  ,true为消费过
      val partitionsE = kc.getPartitions(Set(topic)) //获取patition信息
      if (partitionsE.isLeft) throw new SparkException("get kafka partition failed:")
      val partitions = partitionsE.right.get
      //过期或者是新的groupid从哪开始读取
      val last_earlies = if (kp.contains(WRONG_GROUP_FROM)) {
        kp.get(WRONG_GROUP_FROM).get.toUpperCase()
      } else {
        log.warn("-- Use LAST Offset (set 'wrong.groupid.from') -- ")
        "LAST"
      }
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions) //获取这个topic的每个patition的消费信息      
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) {
        offsets ++= (getEffectiveOffset(kc, kp, partitions, consumerOffsetsE, last_earlies))
      } else {
        offsets ++= (getNewGroupIdOffset(groupId, partitions, kp,last_earlies))
      }
    }
    offsets
  }
   /**
   * @author LMQ
   * @time 2018-04-04
   * @func 获取一个新grouid的offset （last_earlies决定是从最新还是最旧）
   */
  def getNewGroupIdOffset(
    groupId: String,
    partitions: Set[TopicAndPartition],
    kp: Map[String, String],
    last_earlies: String)= {
    log.warn(" NEW  GROUP   ID  : " + groupId)
    log.warn(" NEW  GROUP  FROM : " + last_earlies)
    var newgroupOffsets = last_earlies match {
      case "EARLIEST" => kc.getEarliestLeaderOffsets(partitions).right.get
      case _          => kc.getLatestLeaderOffsets(partitions).right.get
    }
    //解决冷启动问题，更新一个初始为0的偏移量记录。下次启动就不会是新group了
    val newOffset = newgroupOffsets.map { case (tp, offset) => (tp -> offset.offset) }
    updateConsumerOffsets(kp, groupId, newOffset)
    newOffset
  }
  /**
   * @author LMQ
   * @time 2018-04-04
   * @func 获取有效的offset
   */
  def getEffectiveOffset(
    kc: KafkaCluster,
    kp: Map[String, String],
    partitions: Set[TopicAndPartition],
    consumerOffsetsE: Either[KafkaCluster.Err, Map[TopicAndPartition, Long]],
    last_earlies: String) = {
    val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get //获取最早的偏移量
    val partLastOffsets = kc.getLatestLeaderOffsets(partitions).right
    val consumerOffsets = consumerOffsetsE.right.get
    //消费的偏移量和最早的偏移量做比较（因为kafka有过期，如果太久没消费，）
    consumerOffsets.map({
      case (tp, n) =>
        //现在数据在什么offset上
        val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
        val lastoffset = partLastOffsets.get(tp).offset
        if (n > lastoffset || n < earliestLeaderOffset) { //如果offset超过了最新的//消费过，但是过时了，就从最新开始消费
          log.warn("-- Consumer offset is OutTime --- " + tp + "->" + n)
          last_earlies match {
            case "EARLIEST" => (tp -> earliestLeaderOffset)
            case _          => (tp -> lastoffset)
          }
        } else {
          (tp -> n)
        }
    })
  }

  /**
   * @author LMQ
   * @description 更新消费者的offset至zookeeper
   */
  def updateConsumerOffsets(
    kp: Map[String, String],
    groupId: String,
    offsets: Map[TopicAndPartition, Long]): Unit = {
    instance(kp)
    val o = kc.setConsumerOffsets(groupId, offsets)
    if (o.isLeft)
      log.error(s"Error updating the offset to Kafka cluster: ${o.left.get}")
  }
  /**
   * @author LMQ
   * @description 更新消费者的offset至zookeeper
   */
  def updateConsumerOffsets(
    kp: Map[String, String],
    offsets: Map[TopicAndPartition, Long]): Unit = {
    val groupId = kp.get(GROUP_ID).get
    updateConsumerOffsets(kp, groupId, offsets)
  }
  /**
   * @author LMQ
   * @description 获取最新的offset
   */
  def getLatestOffsets(
    topics: Set[String],
    kafkaParams: Map[String, String]) = {
    instance(kafkaParams)
    var fromOffsets = (for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (kc.getLatestLeaderOffsets(topicPartitions)).right
    } yield {
      val fromOffsets = leaderOffsets.map {
        case (tp, lo) =>
          (tp, lo.offset)
      }
      fromOffsets
    }).fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok)
    fromOffsets
  }
  /**
   * @author LMQ
   * @description 获取最早的offset
   */
  def getEarliestOffsets(
    topics: Set[String],
    kafkaParams: Map[String, String]) = {
    instance(kafkaParams)
    var fromOffsets = (for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (kc.getEarliestLeaderOffsets(topicPartitions)).right
    } yield {
      val fromOffsets = leaderOffsets.map {
        case (tp, lo) =>
          (tp, lo.offset)
      }
      fromOffsets
    }).fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok)
    fromOffsets
  }
  /**
   *
   */
  def findLeaders(kp: Map[String, String], topics: Set[TopicAndPartition]) = {
    if (kc == null) {
      kc = new KafkaCluster(kp)
    }
    kc.findLeaders(topics).fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok)
  }
  /**
   *
   */
  def latestLeaderOffsets(
    kp: Map[String, String],
    retries: Int,
    currentOffsets: Map[TopicAndPartition, Long]
    ): Map[TopicAndPartition, LeaderOffset] = {
    instance(kp)
    val o = kc.getLatestLeaderOffsets(currentOffsets.keySet)
    if (o.isLeft) {
      val err = o.left.get.toString
      if (retries <= 0) {
        throw new SparkException(err)
      } else {
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        latestLeaderOffsets(kp, retries - 1, currentOffsets)
      }
    } else {
      o.right.get
    }
  }
  /**
   * @author LMQ
   * @description 将某个groupid的偏移量更新至最新的offset
   * @description 主要是用于过滤脏数据。如果kakfa某个时间段进来很多废弃的数据，你想跳过这些数据，可以在程序开始的时候使用这个方法来跳过数据
   */
  def updataOffsetToLastest(topics: Set[String], kp: Map[String, String]) = {
    val lastestOffsets = getLatestOffsets(topics, kp)
    updateConsumerOffsets(kp, kp.get("group.id").get, lastestOffsets)
    lastestOffsets
  }
}
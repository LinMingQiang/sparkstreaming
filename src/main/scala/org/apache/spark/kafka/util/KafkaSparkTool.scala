package org.apache.spark.kafka.util

import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.slf4j.LoggerFactory
import org.apache.spark.core.SparkKafkaConfsKey
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.log4j.PropertyConfigurator

/**
  * @author LMQ
  * @description 读取kafka，操作offset等操作工具
  * @description 只用于spark包使用
  */
private[spark] trait KafkaSparkTool extends SparkKafkaConfsKey {
  var logname = "KafkaSparkTool" //外部可重写
  lazy val log = LoggerFactory.getLogger(logname)
  var kp: Map[String, String]
  lazy val kc: KafkaCluster = new KafkaCluster(kp)

  /**
    * @author LMQ
    * @func 重置kafka配置参数
    */
  def setKafkaParam(kp: Map[String, String]) {
    this.kp = kp
  }

  /**
    * @author LMQ
    * @description 获取kakfa消费者的offset
    * @param groupId：组名，用于唯一识别kakfa消费者身份
    * @param topics ：操作的topic列表
    * 有几种情况要注意：
    *  1 ：如果这个groupid从未消费过
    *  2 :如果消费过，但是消费者记录的偏移量过期，或者超过了LAST。
    *  以上两种情况
    *  从何读起将取决于配置 （wrong.groupid.from）= > ( LAST / EARLIEST )
    */
  def getConsumerOffset(groupId: String, topics: Set[String]) = {
    var offsets: Map[TopicAndPartition, Long] = Map()
    topics.foreach { topic =>
      var hasConsumed = true //是否消费过  ,true为消费过
      val partitionsE = kc.getPartitions(Set(topic)) //获取patition信息
      if (partitionsE.isLeft)
        throw new SparkException("get kafka partition failed:")
      val partitions = partitionsE.right.get
      //过期或者是新的groupid从哪开始读取
      val last_earlies = if (kp.contains(WRONG_GROUP_FROM)) {
        kp.get(WRONG_GROUP_FROM).get.toUpperCase()
      } else {
        log.warn("-- Use LAST Offset (set 'wrong.groupid.from') -- ")
        "LAST"
      }
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions) //获取这个topic的每个patition的消费信息
      if (consumerOffsetsE.isRight) {
        offsets ++= (getEffectiveOffset(partitions,
                                        consumerOffsetsE.right.get,
                                        last_earlies))
      } else {
        offsets ++= (getNewGroupIdOffset(groupId, partitions, last_earlies))
      }
    }
    offsets
  }

  /**
    * @author LMQ
    * @time 2018-04-04
    * @func 获取一个新grouid的offset （last_earlies决定是从最新还是最旧）
    */
  def getNewGroupIdOffset(groupId: String,
                          partitions: Set[TopicAndPartition],
                          last_earlies: String) = {
    log.warn(" NEW  GROUP   ID  : " + groupId)
    log.warn(" NEW  GROUP  FROM : " + last_earlies)
    var newgroupOffsets = last_earlies match {
      case "EARLIEST" => kc.getEarliestLeaderOffsets(partitions).right.get
      case _          => kc.getLatestLeaderOffsets(partitions).right.get
    }
    //解决冷启动问题，更新一个初始为0的偏移量记录。下次启动就不会是新group了
    val newOffset = newgroupOffsets.map {
      case (tp, offset) => (tp -> offset.offset)
    }
    updateConsumerOffsets(groupId, newOffset)
    newOffset
  }

  /**
    * @author LMQ
    * @time 2018-04-04
    * @func 获取有效的offset
    */
  def getEffectiveOffset(partitions: Set[TopicAndPartition],
                         consumerOffsetMap: Map[TopicAndPartition, Long],
                         last_earlies: String) = {
    val earliestinfoE = kc.getEarliestLeaderOffsets(partitions)
    if (earliestinfoE.isLeft)
      throw new SparkException(earliestinfoE.left.toString())
    val earliestOffsetsMap = earliestinfoE.right.get //获取最早的偏移量
    val lastinfoE = kc.getLatestLeaderOffsets(partitions)
    if (lastinfoE.isLeft) throw new SparkException(lastinfoE.left.toString())
    val lastOffsetsMap = lastinfoE.right.get
    //消费的偏移量和最早的偏移量做比较（因为kafka有过期，如果太久没消费，）
    lastOffsetsMap.map {
      case (tp, lastLeaderOffset) =>
        val lastOffset = lastLeaderOffset.offset
        val earliestOffset = earliestOffsetsMap(tp).offset
        if (consumerOffsetMap.contains(tp)) {
          val consumerOffset = consumerOffsetMap(tp)
          if (consumerOffset > lastOffset || consumerOffset < earliestOffset) { //如果offset超过了最新的//消费过，但是过时了，就从最新开始消费
            log.warn(
              s""""Consumer offset is outTime : ${tp} -> (Consumer : ${consumerOffset},Earliest:${earliestOffset},Last:${lastOffset})""")
            log.warn("-- RESET OFFSET FROM  --- " + last_earlies)
            last_earlies match {
              case "EARLIEST" => (tp -> earliestOffset)
              case _          => (tp -> lastOffset)
            }
          } else (tp -> consumerOffset)

        } else { //增加了新分区
          last_earlies match {
            case "EARLIEST" => (tp -> earliestOffset)
            case _          => (tp -> lastOffset)
          }
        }
    }
  }

  /**
    * @author LMQ
    * @description 更新消费者的offset至zookeeper
    */
  def updateConsumerOffsets(groupId: String,
                            offsets: Map[TopicAndPartition, Long]) = {
    val o = kc.setConsumerOffsets(groupId, offsets)
    if (o.isLeft)
      log.error(s"Error updating the offset to Kafka cluster: ${o.left.get}")
    o
  }

  /**
    * @author LMQ
    * @description 更新消费者的offset至zookeeper
    */
  def updateConsumerOffsets(offsets: Map[TopicAndPartition, Long])
    : Either[KafkaCluster.Err, Map[TopicAndPartition, Short]] = {
    val groupId = kp.get(GROUPID).get
    updateConsumerOffsets(groupId, offsets)
  }

  /**
    * @author LMQ
    * @description 更新消费者的offset至zookeeper
    */
  def updateConsumerOffsets(offsets: String): Unit = {
    val groupId = kp.get(GROUPID).get
    val offsetArr = offsets
      .split('|')
      .map { offsetStr =>
        val offsetArr = offsetStr.split(",")
        new TopicAndPartition(offsetArr(0), offsetArr(1).toInt) -> offsetArr(2).toLong
      }
      .toMap
    updateConsumerOffsets(groupId, offsetArr)
  }

  /**
    * @author LMQ
    * @description 获取最新的offset
    */
  def getLatestOffsets(topics: Set[String]) = {
    var fromOffsets = (for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (kc.getLatestLeaderOffsets(topicPartitions)).right
    } yield {
      val fromOffsets = leaderOffsets.map {
        case (tp, lo) =>
          (tp, lo.offset)
      }
      fromOffsets
    }).fold(errs => throw new SparkException(errs.mkString("\n")), ok => ok)
    fromOffsets
  }

  /**
    * @author LMQ
    * @description 获取最新的offset,带上host信息
    */
  def getLatestLeaderOffsets(topics: Set[String]) = {
    val o = kc.getLatestLeaderOffsets(kc.getPartitions(topics).right.get)
    // Either.fold would confuse @tailrec, do it manually
    o.right.get
  }

  /**
    * @author LMQ
    * @description 获取最早的offset
    */
  def getEarliestOffsets(topics: Set[String]) = {
    var fromOffsets = (for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (kc.getEarliestLeaderOffsets(topicPartitions)).right
    } yield {
      val fromOffsets = leaderOffsets.map {
        case (tp, lo) =>
          (tp, lo.offset)
      }
      fromOffsets
    }).fold(errs => throw new SparkException(errs.mkString("\n")), ok => ok)
    fromOffsets
  }

  /**
    * @author LMQ
    * @func 解决偶尔读不到kafka信息的情况
    */
  def latestLeaderOffsets(retries: Int,
                          currentOffsets: Map[TopicAndPartition, Long])
    : Map[TopicAndPartition, LeaderOffset] = {
    val o = kc.getLatestLeaderOffsets(currentOffsets.keySet)
    // Either.fold would confuse @tailrec, do it manually
    if (o.isLeft) {
      val err = o.left.get.toString
      if (retries <= 0) {
        throw new SparkException(err)
      } else {
        Thread.sleep(kc.config.refreshLeaderBackoffMs)
        latestLeaderOffsets(retries - 1, currentOffsets)
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
  def updataOffsetToLastest(topics: Set[String]) = {
    val lastestOffsets = getLatestOffsets(topics)
    updateConsumerOffsets(kp.get(GROUPID).get, lastestOffsets)
    lastestOffsets
  }

  /**
    * @author LMQ
    * @description 将某个groupid的偏移量更新至最早的offset
    * @description
    */
  def updataOffsetToEarliest(topics: Set[String]) = {
    val earliestOffset = getEarliestOffsets(topics)
    updateConsumerOffsets(kp.get(GROUPID).get, earliestOffset)
    earliestOffset
  }

  /**
    * @author LMQ
    * @description 将某个groupid的偏移量更新至自定义的offset位置
    * @description offsets的格式为 ：  topicname1,partNum1,offset1|topicname1,partNum2,offset2|....
    */
  def updataOffsetToCustom(offsets: String) = {
    updateConsumerOffsets(offsets)
  }
}

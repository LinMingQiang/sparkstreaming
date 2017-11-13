package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.rdd.RDD
import java.util.Properties
import org.apache.spark.Logging
import org.slf4j.LoggerFactory
private[spark] trait KafkaSparkTool {
  var logname = "KafkaSparkTool" //外部可重写
  lazy val log = LoggerFactory.getLogger(logname)
  var kc: KafkaCluster = null
  val GROUP_ID = "group.id"
  val LAST_OR_CONSUMER = "kafka.last.consum"
  val LAST_OR_EARLIEST = "newgroup.last.earliest" //新用户或者过期用户 重新读取的点 （最新或者最旧）
  val maxMessagesPerPartitionKEY = "spark.streaming.kafka.maxRatePerPartition"
  /**
   * init KafkaCluster
   */
  def instance(kp: Map[String, String]) {
    if (kc == null) kc = new KafkaCluster(kp)
  }
  /**
   * get consumer offset
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
      val last_earlies = if (kp.contains(LAST_OR_EARLIEST)) { kp.get(LAST_OR_EARLIEST).get.toUpperCase() } else "LAST"
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions) //获取这个topic的每个patition的消费信息      
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) {
        val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get //获取最早的偏移量
        val partLastOffsets = kc.getLatestLeaderOffsets(partitions).right
        val consumerOffsets = consumerOffsetsE.right.get
        //过期或者是新的groupid从哪开始读取//为了防止丢失，建议从最旧的开始
        var newgroupOffsets = last_earlies match {
          case "EARLIEST" => earliestLeaderOffsets
          case _          => kc.getLatestLeaderOffsets(partitions).right.get
        }
        //消费的偏移量和最早的偏移量做比较（因为kafka有过期，如果太久没消费，）
        consumerOffsets.foreach({
          case (tp, n) =>
            //现在数据在什么offset上
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            val lastoffset = partLastOffsets.get(tp).offset
            if (n > lastoffset || n < earliestLeaderOffset) { //如果offset超过了最新的//消费过，但是过时了，就从最新开始消费
              if (kp.contains(LAST_OR_EARLIEST)) {
                kp.get(LAST_OR_EARLIEST).get.toUpperCase() match {
                  case "EARLIEST" => offsets += (tp -> earliestLeaderOffset)
                  case _          => offsets += (tp -> lastoffset)
                }
              }
            } else offsets += (tp -> n) //消费者的offsets正常
        })
      } else {
        log.warn(" New Group ID : " + groupId)
        log.warn(" Last_Earlies : " + last_earlies)
        var newgroupOffsets = last_earlies match {
          case "EARLIEST" => kc.getEarliestLeaderOffsets(partitions).right.get
          case _          => kc.getLatestLeaderOffsets(partitions).right.get
        }
        newgroupOffsets.foreach {
          case (tp, offset) =>
            offsets += (tp -> offset.offset)
        }
        //解决冷启动问题，更新一个初始为0的偏移量记录。下次启动就不会是新group了
        updateConsumerOffsets(kp, groupId, newgroupOffsets.map { case (tp, offset) => (tp -> 0L) })
      }
    }
    offsets
  }
  /**
   * 更新offset by group id
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
   * 更新offset
   */
  def updateConsumerOffsets(
    kp: Map[String, String],
    offsets: Map[TopicAndPartition, Long]): Unit = {
    instance(kp)
    val groupId = kp.get(GROUP_ID).get
    val o = kc.setConsumerOffsets(groupId, offsets)
    if (o.isLeft)
      println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
  }
  /*
 *  "largest"/"smallest"
 * 
 */
  def getLatestOffsets(topics: Set[String], kafkaParams: Map[String, String]) = {
    instance(kafkaParams)
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    var fromOffsets = (for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
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
   * 将当前的topic的groupid更新至最新的offsets
   */
  def updataOffsetToLastest(topics: Set[String], kp: Map[String, String]) = {
    val lastestOffsets = KafkaSparkContextManager.getLatestOffsets(topics, kp)
    updateConsumerOffsets(kp, kp.get("group.id").get, lastestOffsets)
    lastestOffsets
  }
}
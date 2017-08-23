package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.rdd.RDD
import java.util.Properties
import org.apache.spark.Logging
import org.slf4j.LoggerFactory
trait KafkaSparkTool{
  val logname="KafkaSparkTool"
  lazy val log=LoggerFactory.getLogger(logname)
  var kc: KafkaCluster = null
  val GROUP_ID="group.id"
  val LAST_OR_CONSUMER="kafka.last.consum"
  val LAST_OR_EARLIEST="newgroup.last.earliest"
  val maxMessagesPerPartitionKEY="spark.streaming.kafka.maxRatePerPartition"
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
      val last_earlies=if(kp.contains(LAST_OR_EARLIEST)){kp.get(LAST_OR_EARLIEST).get.toUpperCase()} else "LAST"
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions) //获取这个topic的每个patition的消费信息      
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) {
        val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get//获取最早的偏移量
        val consumerOffsets = consumerOffsetsE.right.get
         //过期或者是新的groupid从哪开始读取//为了防止丢失，建议从最旧的开始
        var newgroupOffsets= last_earlies match{
            case "EARLIEST"=>earliestLeaderOffsets
            case  _ => kc.getLatestLeaderOffsets(partitions).right.get
        }
        //消费的偏移量和最早的偏移量做比较（因为kafka有过期，如果太久没消费，）
        consumerOffsets.foreach({case (tp, n) =>
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            if (n < earliestLeaderOffset) {
              offsets += (tp -> newgroupOffsets.get(tp).get.offset)
            } else offsets += (tp -> n) 
        })
      } else {
        log.warn(" New Group ID : " + groupId)
        var newgroupOffsets= last_earlies match{
            case "EARLIEST"=>kc.getEarliestLeaderOffsets(partitions).right.get
            case _ => kc.getLatestLeaderOffsets(partitions).right.get
          }
        newgroupOffsets.foreach { case (tp, offset) => 
          offsets += (tp -> offset.offset) }
        //解决冷启动问题，更新一个初始为0的偏移量记录
        updateConsumerOffsets(kp, groupId, newgroupOffsets.map { case (tp, offset) => (tp -> 0L) })
      }
    }
    offsets
  }
  def updateConsumerOffsets(kp: Map[String, String], groupId: String, offsets: Map[TopicAndPartition, Long]): Unit = {
    instance(kp)
    val o = kc.setConsumerOffsets(groupId, offsets)
    if (o.isLeft)
      log.error(s"Error updating the offset to Kafka cluster: ${o.left.get}")
  }
   def updateConsumerOffsets(kp: Map[String, String], offsets: Map[TopicAndPartition, Long]): Unit = {
    instance(kp)
    val groupId=kp.get(GROUP_ID).get
    val o = kc.setConsumerOffsets(groupId, offsets)
    if (o.isLeft)
      println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
  }
/*
 * two parm "largest"/"smallest"
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

}
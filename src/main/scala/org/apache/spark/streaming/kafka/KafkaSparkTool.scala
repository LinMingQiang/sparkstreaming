package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.rdd.RDD
import java.util.Properties
trait KafkaSparkTool {
  var kc: KafkaCluster = null
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
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions) //获取这个topic的每个patition的消费信息      
      if (consumerOffsetsE.isLeft) hasConsumed = false
      if (hasConsumed) {
        val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(partitions).right.get
        val consumerOffsets = consumerOffsetsE.right.get
        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为latestLeaderOffsets          
        consumerOffsets.foreach({
          case (tp, n) =>
            //现在数据在什么offset上
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            if (n < earliestLeaderOffset) {
              //消费过，但是过时了，就从最新开始消费
              val latestLeaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get(tp).offset
              offsets += (tp -> latestLeaderOffsets)
            } else offsets += (tp -> n) //消费者的offsets正常
        })
      } else { // 没有消费过 ，这是一个新的消费group id
        println(">>>  这是一个新的kafka group id  <<< : " + groupId)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        leaderOffsets = kc.getLatestLeaderOffsets(partitions).right.get
        leaderOffsets.foreach { case (tp, offset) => offsets += (tp -> offset.offset) }
        //解决冷启动问题。
        updateConsumerOffsets(kp, groupId, leaderOffsets.map { case (tp, offset) => (tp -> offset.offset) })
      }
    }
    offsets
  }
  def updateConsumerOffsets(kp: Map[String, String], groupId: String, offsets: Map[TopicAndPartition, Long]): Unit = {
    instance(kp)
    val o = kc.setConsumerOffsets(groupId, offsets)
    if (o.isLeft)
      println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
  }
/*
 * two parm "largest"/"smallest"
 * 
 */
  def getLatestOffsets(topics: Set[String], kafkaParams: Map[String, String]) = {
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
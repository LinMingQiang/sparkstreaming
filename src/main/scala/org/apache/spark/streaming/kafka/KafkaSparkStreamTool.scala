package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
private[spark]
object KafkaSparkStreamTool extends KafkaSparkTool {
  def findLeaders(kp: Map[String, String], topics: Set[TopicAndPartition]) = {
    if (kc == null) {
      kc = new KafkaCluster(kp)
    }
    kc.findLeaders(topics).fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok)
  }
  def latestLeaderOffsets(
    kp: Map[String, String],
    retries: Int,
    currentOffsets: Map[TopicAndPartition, Long]): Map[TopicAndPartition, LeaderOffset] = {
    if (kc == null) {
      kc = new KafkaCluster(kp)
    }
    val o = kc.getLatestLeaderOffsets(currentOffsets.keySet)
    // Either.fold would confuse @tailrec, do it manually
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
}
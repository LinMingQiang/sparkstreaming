package org.apache.spark.func.tool

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaSparkStreamManager

class rddkafkaFunClass[T](rdd: RDD[T]) {
  def updateOffsets(kp: Map[String, String], groupid: String) {
    KafkaSparkStreamManager.updateRDDOffset(kp, groupid, rdd)
  }
  def getRDDOffsets() = {
    KafkaSparkStreamManager.getRDDConsumerOffsets(rdd)
  }
}
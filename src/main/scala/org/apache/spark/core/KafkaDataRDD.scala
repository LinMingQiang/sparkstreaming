package org.apache.spark.streaming.kafka

import scala.reflect.ClassTag
import kafka.serializer.Decoder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.kafka010.KafkaRDD
import org.apache.kafka.clients.consumer.ConsumerRecord


/**
 * @author LMQ
 * @description 自定义一个kafkaRDD
 * @description 具备的特性： 使rdd具备更新offset的能力
 * @description 当然也可以再重写一些其他特性
 */
private[kafka] 
class KafkaDataRDD[K: ClassTag, V: ClassTag](prev: KafkaRDD[K, V])
    extends RDD[ConsumerRecord[K, V]](prev) with HasOffsetRanges {

  def updateOffsets(kp: Map[String, String], groupid: String) {
    StreamingKafkaManager.updateRDDOffset(kp, groupid, this)
  }
  def updateOffsets(kp: Map[String, String]): Boolean = {
    if (kp.contains("group.id")) {
      updateOffsets(kp, kp("group.id"))
      true
    } else {
      false
    }
  }
  def getRDDOffsets() = { StreamingKafkaManager.getRDDConsumerOffsets(this) }
  override def offsetRanges() = prev.offsetRanges
  override def getPartitions: Array[Partition] = prev.getPartitions
  override def getPreferredLocations(thePart: Partition): Seq[String] = prev.getPreferredLocations(thePart)
  override def compute(split: Partition, context: TaskContext) = prev.compute(split, context)
}
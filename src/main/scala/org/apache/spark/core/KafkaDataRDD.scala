package org.apache.spark.streaming.kafka

import scala.reflect.ClassTag
import kafka.serializer.Decoder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
private[kafka]
class KafkaDataRDD[
  K: ClassTag,
  V: ClassTag,
  U <: Decoder[_]: ClassTag,
  T <: Decoder[_]: ClassTag,
  R: ClassTag](prev: KafkaRDD[K,V,U,T,R]) 
extends RDD[R](prev) with HasOffsetRanges{
  /**
   * 使rdd具备更新offset的能力
   */
  def updateOffsets(kp: Map[String, String], groupid: String){
    KafkaSparkStreamManager.updateRDDOffset(kp, groupid, this)
  }
  def updateOffsets(kp: Map[String, String]):Boolean={
    if(kp.contains("group.id")){
      KafkaSparkStreamManager.updateRDDOffset(kp, kp("group.id"), this)
      true
    }else{
      false 
    }
  }
  def getRDDOffsets() = {KafkaSparkStreamManager.getRDDConsumerOffsets(this)}
  override def offsetRanges()=prev.offsetRanges
  override def getPartitions: Array[Partition] = prev.getPartitions
  override def getPreferredLocations(thePart: Partition): Seq[String] = prev.getPreferredLocations(thePart)
  override def compute(split: Partition, context: TaskContext) =prev.compute(split, context)
}
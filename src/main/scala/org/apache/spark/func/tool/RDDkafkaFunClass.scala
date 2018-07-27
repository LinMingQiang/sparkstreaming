package org.apache.spark.func.tool

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.StreamingKafkaManager
/**
 * @author LMQ
 * @description 为rdd增加以下隐式转换。
 * @attention 如果要使用这部分的功能，需要你继承这个类。
 * 					  但由于我现在大部分使用的是自定义的  KafkaRDD 。这部分其实用不到。
 * 					Dstream里面的RDD 更新offset的操作已经放在StreamingKafkaContext类里面了
 */
class rddkafkaFunClass[T](rdd: RDD[T]) {
/*  def updateOffsets(kp: Map[String, String], groupid: String){
    StreamingKafkaManager.updateRDDOffset(kp, groupid, rdd)
  }
  def updateOffsets(kp: Map[String, String]):Boolean={
    if(kp.contains("group.id")){
      StreamingKafkaManager.updateRDDOffset(kp, kp("group.id"), rdd)
      true
    }else{
      false 
    }
  }
  def getRDDOffsets() = {
    StreamingKafkaManager.getRDDConsumerOffsets(rdd)
  }*/
}
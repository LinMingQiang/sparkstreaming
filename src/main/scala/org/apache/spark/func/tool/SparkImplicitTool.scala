package org.apache.spark.func.tool

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaSparkStreamManager
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.common.util.Configuration
import org.apache.spark.rdd.RDD

trait SparkImplicitTool {
  implicit class sscFunc(ssc: StreamingContext) {
    def createDirectStream[R: ClassTag](
      kp: Map[String, String],
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long],
      msgHandle: (MessageAndMetadata[String, String]) => R) :InputDStream[R]={
      KafkaSparkStreamManager.createDirectStream
      [String, String, StringDecoder, StringDecoder, R](ssc,kp,topics, fromOffset, msgHandle)
    }
    def createDirectStream[R: ClassTag](
      conf: Configuration,
      fromOffset: Map[TopicAndPartition, Long],
      msgHandle: (MessageAndMetadata[String, String]) => R) :InputDStream[R]={
      KafkaSparkStreamManager.createDirectStream
      [String, String, StringDecoder, StringDecoder, R](ssc,conf, fromOffset, msgHandle)
    }
  }
  implicit class rddkafkaFun[T](rdd:RDD[T]){
    def updateOffsets(kp:Map[String,String],groupid:String){
      KafkaSparkStreamManager.updateRDDOffset(kp, groupid, rdd)
    }
    def getRDDOffsets()={
      KafkaSparkStreamManager.getRDDConsumerOffsets(rdd)
    }
  }
}
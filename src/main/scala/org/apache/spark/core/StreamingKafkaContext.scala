package org.apache.spark.core

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration
import scala.reflect.ClassTag
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.KafkaSparkStreamManager
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.common.util.Configuration

class StreamingKafkaContext {
   var ssc:StreamingContext=null
   def this(ssc:StreamingContext){
     this()
     this.ssc=ssc
   }
   def this(sc:SparkContext,batchDuration: Duration){
     this()
     ssc=new StreamingContext(sc,batchDuration)
   }
      def createDirectStream[R: ClassTag](
      kp: Map[String, String],
      topics: Set[String],
      fromOffset: Map[TopicAndPartition, Long],
      msgHandle: (MessageAndMetadata[String, String]) => R) :InputDStream[R]={
      KafkaSparkStreamManager.createDirectStream
      [String, String, StringDecoder, StringDecoder, R](ssc,kp,topics, fromOffset, msgHandle)
    }
   def createDirectStream[R: ClassTag](
      kp: Map[String, String],
      topics: Set[String],
      msgHandle: (MessageAndMetadata[String, String]) => R) :InputDStream[R]={
      KafkaSparkStreamManager.createDirectStream
      [String, String, StringDecoder, StringDecoder, R](ssc,kp,topics, null, msgHandle)
    }
    def createDirectStream[R: ClassTag](
      conf: Configuration,
      fromOffset: Map[TopicAndPartition, Long],
      msgHandle: (MessageAndMetadata[String, String]) => R) :InputDStream[R]={
      KafkaSparkStreamManager.createDirectStream
      [String, String, StringDecoder, StringDecoder, R](ssc,conf, fromOffset, msgHandle)
    }
     def createDirectStream[R: ClassTag](
      conf: Configuration,
      msgHandle: (MessageAndMetadata[String, String]) => R) :InputDStream[R]={
      KafkaSparkStreamManager.createDirectStream
      [String, String, StringDecoder, StringDecoder, R](ssc,conf, null, msgHandle)
    }
}
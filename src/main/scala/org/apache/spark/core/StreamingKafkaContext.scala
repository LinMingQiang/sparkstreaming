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
import org.apache.spark.streaming.kafka.KafkaSparkContextManager
import org.apache.spark.rdd.RDD

class StreamingKafkaContext {
  var ssc: StreamingContext = null
  def this(ssc: StreamingContext) {
    this()
    this.ssc = ssc
  }
  def this(skc:SparkKafkaContext, batchDuration: Duration){
    this()
    ssc = new StreamingContext(skc.sc, batchDuration)
  }
  def this(sc: SparkContext, batchDuration: Duration) {
    this()
    ssc = new StreamingContext(sc, batchDuration)
  }
  def start(){
    ssc.start()
  }
  def awaitTermination(){
    ssc.awaitTermination
  }
  //将当前的topic的groupid更新至最新的offsets
  def updataOffsetToLastest(topics: Set[String], kp: Map[String, String]) = {
    val lastestOffsets = KafkaSparkContextManager.getLatestOffsets(topics, kp)
    KafkaSparkContextManager.updateConsumerOffsets(kp, lastestOffsets)
    lastestOffsets
  }
  def updateRDDOffsets[T](kp: Map[String, String], groupId: String, rdd: RDD[T]) {
    KafkaSparkContextManager.updateRDDOffset(kp, groupId, rdd)
  }
  def getRDDOffsets[T](rdd:RDD[T]) = {
    KafkaSparkStreamManager.getRDDConsumerOffsets(rdd)
  }
  
  def createDirectStream[R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    KafkaSparkStreamManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](ssc, kp, topics, fromOffset, msgHandle)
  }
  def createDirectStream[R: ClassTag](
    kp: Map[String, String],
    topics: Set[String],
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    KafkaSparkStreamManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](ssc, kp, topics, null, msgHandle)
  }
  def createDirectStream[R: ClassTag](
    conf: Configuration,
    fromOffset: Map[TopicAndPartition, Long],
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    KafkaSparkStreamManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](ssc, conf, fromOffset, msgHandle)
  }
  def createDirectStream[R: ClassTag](
    conf: Configuration,
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    KafkaSparkStreamManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](ssc, conf, null, msgHandle)
  }
}
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
import org.apache.spark.common.util.KafkaConfiguration

class StreamingKafkaContext {
  var ssc: StreamingContext = null
  var sc: SparkContext = null

  def this(ssc: StreamingContext) {
    this()
    this.ssc = ssc
    this.sc = ssc.sparkContext
  }
  def this(sc: SparkContext, batchDuration: Duration) {
    this()
    this.sc = sc
    ssc = new StreamingContext(sc, batchDuration)
  }
  def start() {
    ssc.start()
  }
  def awaitTermination() {
    ssc.awaitTermination
  }
  //将当前的topic的groupid更新至最新的offsets
  def updataOffsetToLastest(topics: Set[String], kp: Map[String, String]) = {
    val lastestOffsets = KafkaSparkContextManager.getLatestOffsets(topics, kp)
    KafkaSparkContextManager.updateConsumerOffsets(kp, lastestOffsets)
    lastestOffsets
  }
  def getLastOffset(topics: Set[String], kp: Map[String, String])={
    KafkaSparkContextManager.getLatestOffsets(topics, kp)
  }
  /**
   * 更新rdd的offset
   */
  def updateRDDOffsets[T](
    kp: Map[String, String],
    groupId: String,
    rdd: RDD[T]) {
    KafkaSparkContextManager.updateRDDOffset(kp, groupId, rdd)
  }
  
  def getRDDOffsets[T](rdd: RDD[T]) = {
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
    conf: KafkaConfiguration,
    fromOffset: Map[TopicAndPartition, Long],
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    KafkaSparkStreamManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](ssc, conf, fromOffset, msgHandle)
  }
  def createDirectStream[R: ClassTag](
    conf: KafkaConfiguration,
    msgHandle: (MessageAndMetadata[String, String]) => R): InputDStream[R] = {
    KafkaSparkStreamManager.createDirectStream[String, String, StringDecoder, StringDecoder, R](ssc, conf, null, msgHandle)
  }
}
object StreamingKafkaContext extends SparkKafkaConfsKey {
/**
   * 更新rdd的offset
   */
  def updateRDDOffsets[T](
    kp: Map[String, String],
    rdd: RDD[T]) {
    if (kp.contains("group.id")) {
      val groupid = kp.get("group.id").get
      KafkaSparkContextManager.updateRDDOffset(kp, groupid, rdd)
    }else println("No Group Id To UpdateRDDOffsets")
  }
}
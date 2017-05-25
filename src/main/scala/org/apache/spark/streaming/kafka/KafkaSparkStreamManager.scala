
package org.apache.spark.streaming.kafka
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkException
import kafka.message.MessageAndMetadata
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scala.collection.mutable.HashMap
import org.apache.spark.common.util.Configuration
import kafka.serializer.Decoder
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.InputDStream
private[spark]
object KafkaSparkStreamManager
extends KafkaSparkTool { 
  val lastOrConsum:String="LAST"
  override val logname="KafkaSparkStreamManager"
  /**
   * common create DStream 
   * 
   */
  def createDirectStream[
    K: ClassTag,
    V: ClassTag, 
    KD <: Decoder[K]: ClassTag, 
    VD <: Decoder[V]: ClassTag, 
    R: ClassTag](
    ssc: StreamingContext,
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long],
    msghandle: (MessageAndMetadata[K, V]) => R =msgHandle): InputDStream[R] = {
    if (kp==null || !kp.contains(GROUP_ID))
      throw new SparkException(s"kafkaParam is Null or ${GROUP_ID} is not setted")
    instance(kp)
    val groupId = kp.get(GROUP_ID).get
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last =if (kp.contains(LAST_OR_CONSUMER)) kp.get(LAST_OR_CONSUMER).get
                  else lastOrConsum
        last.toUpperCase match {
          case "LAST"   => getLatestOffsets(topics, kp)
          case "CONSUM" => getConsumerOffset(kp, groupId, topics)
          case _          => log.info(s"""${LAST_OR_CONSUMER} must LAST or CONSUM,defualt is LAST""");getLatestOffsets(topics, kp)
        }
      } else fromOffset
    consumerOffsets.foreach(x=>log.info(x.toString))
    KafkaUtils.createDirectStream[K, V, KD, VD, R](
      ssc,
      kp,
      consumerOffsets,
      msghandle)
  }
  /**
   * when you dont want use kafkaParam to create
   * you want use configuration to create
   */
  def createDirectStream[
    K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    ssc: StreamingContext,
    conf: Configuration,
    fromOffset: Map[TopicAndPartition, Long],
    msghandle: (MessageAndMetadata[K, V]) => R): InputDStream[R] = {
    if (conf.kpIsNull ||conf.tpIsNull) {
      throw new SparkException(s"Configuration s kafkaParam is Null or Topics is not setted")
    }
    val kp = conf.getKafkaParams()
    if (!kp.contains(GROUP_ID) && !conf.containsKey(GROUP_ID))
      throw new SparkException(s"Configuration s kafkaParam is Null or ${GROUP_ID} is not setted")
    instance(kp)
    val groupId = if(kp.contains(GROUP_ID)) kp.get(GROUP_ID).get
                  else conf.get(GROUP_ID)
    val topics=conf.topics
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last =if (kp.contains(LAST_OR_CONSUMER)) kp.get(LAST_OR_CONSUMER).get
                  else if(conf.containsKey(LAST_OR_CONSUMER)) conf.get(LAST_OR_CONSUMER)
                  else lastOrConsum
        last.toUpperCase match {
          case "LAST"   => getLatestOffsets(topics, kp)
          case "CONSUM" => getConsumerOffset(kp, groupId, topics)
          case _          => getLatestOffsets(topics, kp)
        }
      } else fromOffset
      consumerOffsets.foreach(x=>log.info(x.toString))
      KafkaUtils.createDirectStream[K, V, KD, VD, R](
      ssc,
      kp,
      consumerOffsets,
      msghandle)
 }
  def msgHandle = (mmd: MessageAndMetadata[String, String]) 
  => (mmd.topic, mmd.message)
  /**
   * get RDD offset
   */
  def getRDDConsumerOffsets[T](rdd: RDD[T]) = {
    var consumoffsets = Map[TopicAndPartition, Long]()
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      consumoffsets += ((topicAndPartition, offsets.untilOffset))
    }
    consumoffsets
  }
  /**
   * update RDD Offset
   */
  def updateRDDOffset[T](kp: Map[String, String], groupId: String, rdd: RDD[T]) {
    val offsets = getRDDConsumerOffsets(rdd)
    updateConsumerOffsets(kp, groupId, offsets)
  }

}
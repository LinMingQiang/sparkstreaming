
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
import com.spark.common.conf.util.Configuration
import com.spark.common.conf.util.configurationKey
import kafka.serializer.Decoder
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.InputDStream
object KafkaSparkStreamManager
    extends KafkaSparkTool
    with configurationKey { 
  def createDirectStream[
    K: ClassTag,
    V: ClassTag, 
    KD <: Decoder[K]: ClassTag, 
    VD <: Decoder[V]: ClassTag, 
    R: ClassTag](
    ssc: StreamingContext,
    kp: Map[String, String],
    topics: Set[String],
    fromOffset: Map[TopicAndPartition, Long] = null,
    msghandle: (MessageAndMetadata[K, V]) => R = msgHandle)(implicit from:String): InputDStream[R] = {
    if (!kp.contains(GROUP_ID))
      throw new SparkException(s"Configuration s kafkaParam is Null or ${GROUP_ID} is not setted")
    val groupId = kp.get(GROUP_ID).get
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last =if (kp.contains(LAST_OP_COMSUMER)) kp.get(LAST_OP_COMSUMER)
                  else from
        last match {
          case "LASTED"   => getLatestOffsets(topics, kp)
          case "COMSUMER" => getConsumerOffset(kp, groupId, topics)
          case _          => getLatestOffsets(topics, kp)
        }
      } else fromOffset
    KafkaUtils.createDirectStream[K, V, KD, VD, R](
      ssc,
      kp,
      consumerOffsets,
      msghandle)
  }
  def createDirectStreams[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    ssc: StreamingContext,
    conf: Configuration,
    fromOffset: Map[TopicAndPartition, Long] = null,
    topics: Set[String],
    msghandle: (MessageAndMetadata[K, V]) => R = msgHandle)(implicit from:String): InputDStream[R] = {
    if (conf.kpIsNull) {
      throw new SparkException(s"Configuration s kafkaParam is Null or ${GROUP_ID} is not setted")
    }
    val kp = conf.getKafkaParams()
    if (!kp.contains(GROUP_ID) && conf.containsKey(GROUP_ID))
      throw new SparkException(s"Configuration s kafkaParam is Null or ${GROUP_ID} is not setted")
    val groupId = if(kp.contains(GROUP_ID)) kp.get(GROUP_ID).get
                   else conf.get(GROUP_ID)
    val consumerOffsets: Map[TopicAndPartition, Long] =
      if (fromOffset == null) {
        val last =if (kp.contains(LAST_OP_COMSUMER)) kp.get(LAST_OP_COMSUMER)
                  else if(conf.containsKey(LAST_OP_COMSUMER)) conf.get(LAST_OP_COMSUMER)
                  else from
        last match {
          case "LASTED"   => getLatestOffsets(topics, kp)
          case "COMSUMER" => getConsumerOffset(kp, groupId, topics)
          case _          => getLatestOffsets(topics, kp)
        }
      } else fromOffset
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
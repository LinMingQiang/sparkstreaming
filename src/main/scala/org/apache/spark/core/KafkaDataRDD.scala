package org.apache.spark.streaming.kafka

import scala.reflect.ClassTag
import kafka.serializer.Decoder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.core.SparkKafkaConfsKey

/**
  * @author LMQ
  * @description 自定义一个kafkaRDD
  * @description 具备的特性： 使rdd具备更新offset的能力
  * @description 当然也可以再重写一些其他特性
  */
class KafkaDataRDD[K: ClassTag,
                   V: ClassTag,
                   U <: Decoder[_]: ClassTag,
                   T <: Decoder[_]: ClassTag,
                   R: ClassTag](@transient sc: SparkKafkaContext,
                                kafkaParams: Map[String, String],
                                offsetRanges: Array[OffsetRange],
                                leaders: Map[TopicAndPartition, (String, Int)],
                                messageHandler: MessageAndMetadata[K, V] => R)
    extends KafkaRDD[K, V, U, T, R](sc.sparkcontext,
                                    kafkaParams,
                                    offsetRanges,
                                    leaders,
                                    messageHandler)
    with SparkKafkaConfsKey {

  /**
    * @author LMQ
    * @desc 更新offset至zk
    */
  def updateOffsets(groupId: String) {
    sc.updateRDDOffsets(groupId, this)
  }

  /**
    * @author LMQ
    * @desc 更新offset至zk
    */
  def updateOffsets() {
    sc.updateRDDOffsets(kafkaParams(GROUPID), this)
  }

  /**
    * @author LMQ
    * @desc 獲取當前rdd的offset
    */
  def getRDDOffsets() = { sc.getRDDOffset(this) }

}
object KafkaDataRDD {

  /**
    *
    */
  def apply[K: ClassTag,
            V: ClassTag,
            U <: Decoder[_]: ClassTag,
            T <: Decoder[_]: ClassTag,
            R: ClassTag](sc: SparkKafkaContext,
                         kafkaParams: Map[String, String],
                         fromOffsets: Map[TopicAndPartition, Long],
                         untilOffsets: Map[TopicAndPartition, LeaderOffset],
                         messageHandler: MessageAndMetadata[K, V] => R)
    : KafkaDataRDD[K, V, U, T, R] = {
    val leaders = untilOffsets.map {
      case (tp, lo) =>
        tp -> (lo.host, lo.port)
    }.toMap

    val offsetRanges = fromOffsets.map {
      case (tp, fo) =>
        val uo = untilOffsets(tp)
        OffsetRange(tp.topic, tp.partition, fo, uo.offset)
    }.toArray
    new KafkaDataRDD[K, V, U, T, R](sc,
                                    kafkaParams,
                                    offsetRanges,
                                    leaders,
                                    messageHandler)
  }

}

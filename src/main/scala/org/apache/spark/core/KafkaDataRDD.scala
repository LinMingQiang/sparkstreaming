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
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.kafka010.KafkaRDD
import org.apache.kafka.common.TopicPartition
import java.{util => ju}
import org.apache.spark.core.SparkKafkaConfsKey

/**
  * @author LMQ
  * @description 自定义一个kafkaRDD
  * @description 具备的特性： 使rdd具备更新offset的能力
  * @description 当然也可以再重写一些其他特性
  */
class KafkaDataRDD[K: ClassTag, V: ClassTag](
    @transient var sc: SparkKafkaContext,
    @transient var kafkaParam: ju.Map[String, Object],
    offsetRanges: Array[OffsetRange],
    preferredHosts: java.util.Map[TopicPartition, String],
    useConsumerCache: Boolean)
    extends KafkaRDD[K, V](sc.sparkcontext,
                           kafkaParam,
                           offsetRanges,
                           preferredHosts,
                           useConsumerCache)
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
    sc.updateRDDOffsets(kafkaParam.get(GROUPID).toString(), this)
  }

  /**
    * @author LMQ
    * @desc 更新offset至zk
    */
  def updateOffsets(kp: Map[String, String]): Boolean = {
    if (kp.contains(GROUPID)) {
      updateOffsets(kp(GROUPID))
      true
    } else {
      false
    }
  }

  /**
    * @author LMQ
    * @desc 獲取當前rdd的offset
    */
  def getRDDOffsets() = { sc.getRDDOffset(this) }

}
object KafkaDataRDD {
  def apply[K: ClassTag, V: ClassTag](
      sc: SparkKafkaContext,
      kafkaParams: ju.Map[String, Object],
      offsetRanges: Array[OffsetRange],
      preferredHosts: ju.Map[TopicPartition, String],
      useConsumerCache: Boolean): KafkaDataRDD[K, V] = {
    new KafkaDataRDD[K, V](sc,
                           kafkaParams,
                           offsetRanges,
                           preferredHosts,
                           useConsumerCache)
  }
}

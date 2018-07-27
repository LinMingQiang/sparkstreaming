package org.apache.streaming.core
import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.streaming.Duration
import scala.reflect.ClassTag

/**
 * @func 与StreamingKafkaContext不同，这里不使用SparkStreaming来实现流的功能，而是用while true来实现
 */
class SparkKafkaStreamContext(var kp:Map[String,String]) {
  var sc: SparkKafkaContext = null
  var batchDuration: Duration = null
  def this(sc: SparkKafkaContext, batchDuration: Duration) {
    this(sc.kp)
    this.sc = sc
    this.batchDuration = batchDuration
  }
  def createKafkaStreamRDD(
    kp:     Map[String, String],
    topics: Set[String]) = {
    val getRDDFunc = {
      sc.kafkaRDD[String,String](topics)
    }
    new KafkaDirectStreamRDD(getRDDFunc, batchDuration)
  }
  def createKafkaStreamRDD[K:ClassTag,V:ClassTag](
    kp:     Map[String, String],
    topics: Set[String]) = {
    val getRDDFunc = {
      sc.kafkaRDD[K,V](topics)
    }
    new KafkaDirectStreamRDD(getRDDFunc, batchDuration)
  }
}
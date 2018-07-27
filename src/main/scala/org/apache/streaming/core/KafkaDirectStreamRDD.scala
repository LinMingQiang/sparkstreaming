package org.apache.streaming.core
import kafka.message.MessageAndMetadata
import scala.reflect.ClassTag
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import java.util.Date

/**
 * @author LinMingQiang
 * @time 2018-07-07
 * @func 不使用sparkstreaming的方式来做实时，而是用while ture的方式来做。
 * 	SparkStreaming有几个问题：1：Job的append，导致数据的延迟处理。
 * 													  2：多余的等待时间，例如batchtime为5s，但是数据处理只用了1s，会导致程序有多余的4s在处于等待。
 * 而使用while true的死循环的方式已经在生产上验证是可行的。其实sparkstreaming也是timer的方式来执行的
 */
class KafkaDirectStreamRDD[R: ClassTag](
  getRDDFunc: => RDD[R],
  batchDuration:Duration
  ) {
  def startAndWait[T](func: RDD[R] => T) {
    while (true) {
      val startTime=new Date().getTime
      val rdd = getRDDFunc
      func(rdd)
      val endTime=new Date().getTime
      val useTime=endTime-startTime
      if(useTime <= batchDuration.milliseconds/3){//如果计算所花的时间是设置时间的1/5那就程序睡眠
        Thread.sleep(batchDuration.milliseconds/2-useTime)
      }
      
    }
  }
}
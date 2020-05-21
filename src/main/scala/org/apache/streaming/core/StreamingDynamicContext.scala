package org.apache.streaming.core
import org.apache.spark.core.SparkKafkaContext
import org.apache.spark.streaming.Duration
import kafka.serializer.Decoder
import scala.reflect.ClassTag
import java.util.Date
import kafka.message.MessageAndMetadata
import org.slf4j.LoggerFactory

/**
  * @author LMQ
  * @func 与StreamingKafkaContext不同，这里不使用SparkStreaming来实现流的功能，
  */
class StreamingDynamicContext {
  val log = LoggerFactory.getLogger("StreamingDynamicContext")
  var sc: SparkKafkaContext = null
  var emptyDataWaitTime: Duration = null //dstream没有数据的时候等待时长
  var stop = false
  def this(sc: SparkKafkaContext, emptyDataWaitTime: Duration) {
    this()
    this.sc = sc
    this.emptyDataWaitTime = emptyDataWaitTime
  }

  var kafkaDstream: KafkaDynamicDStream[_, _, _, _, _] = null

  /**
    * @author LMQ
    * @time 20190531
    * @desc 启动流式程序,这里借鉴了streamingContext的流式原理，没有使用Timer定时器
    */
  def start() {
    if (kafkaDstream == null) {
      log.error("kafkaDstream is Null ")
    } else {
      for {
        rateController <- kafkaDstream.rateController
      } this.sc.sparkcontext.addSparkListener(rateController) //添加监听器
      while (!stop) {
        val startTime = new Date().getTime
        val hasData = kafkaDstream.generateJob()
        val endTime = new Date().getTime
        kafkaDstream.onBatchCompleted()
        //一个batch结束
        if (!hasData) {
          val costTime = endTime - startTime
          if (emptyDataWaitTime.milliseconds > costTime)
            Thread.sleep(emptyDataWaitTime.milliseconds - costTime)
        }
      }
    }
  }

  /**
    * @author LMQ
    * @time 20190531
    * @desc 目前只支持了Kafka的数据源
    */
  def createKafkaDstream[K: ClassTag,
                         V: ClassTag,
                         KD <: Decoder[K]: ClassTag,
                         VD <: Decoder[V]: ClassTag,
                         R: ClassTag](
      topics: Set[String],
      msghandle: (MessageAndMetadata[K, V]) => R) = {
    val r =
      new KafkaDirectInputDStream[K, V, KD, VD, R](this, msghandle, topics)
    this.kafkaDstream = r
    r
  }
}

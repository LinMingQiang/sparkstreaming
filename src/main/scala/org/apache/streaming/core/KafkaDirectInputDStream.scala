package org.apache.streaming.core
import kafka.message.MessageAndMetadata
import scala.reflect.ClassTag
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import java.util.Date
import org.apache.spark.streaming.scheduler.KafkaRateController
import org.apache.spark.streaming.scheduler.PIDRateEstimator

/**
  * @author LinMingQiang
  * @time 2019-06-01
  * @desc 不使用sparkstreaming的方式来做实时
  */
private[streaming] class KafkaDirectInputDStream[K: ClassTag,
                                                 V: ClassTag,
                                                 KD <: Decoder[K]: ClassTag,
                                                 VD <: Decoder[V]: ClassTag,
                                                 R: ClassTag](
    ssc: StreamingDynamicContext,
    msghandle: (MessageAndMetadata[K, V]) => R,
    var topics: Set[String])
    extends KafkaDynamicDStream[K, V, KD, VD, R] {

  /**
    * @author LMQ
    * @time 20190613
    * @desc 由于我的配置  maxRateLimitPerPartition 代表的就是每个分区最多取多少条，而不是每个分区每秒最多多少条，所以在这里要做下处理，至少为1
    *
    */
  private val maxRateLimitPerPartition: Int = {
    val maxRate = ssc.sc.sparkcontext.getConf.getInt(
      "spark.streaming.kafka.maxRatePerPartition",
      0) /// (ssc.emptyDataWaitTime.milliseconds / 1000).toInt)
    if (maxRate > 0)
      Math.max(1, maxRate / (ssc.emptyDataWaitTime.milliseconds / 1000).toInt)
    else maxRate
  }

  /**
    * @author LMQ
    * @time 20190613
    * @desc 限制各个分区最多拉取多少条，而不是每秒多少
    *
    */
  val maxLimitPerPartition = ssc.sc.sparkcontext.getConf
    .getLong("spark.streaming.kafka.maxRatePerPartition", 0)

  /**
    * @author LMQ
    * @time 2019 06 13
    * @desc Asynchronously maintains & sends new rate limits to the receiver through the receiver tracker.
    */
  override val rateController: Option[KafkaRateController] = {
    if (ssc.sc.conf.getBoolean("spark.streaming.backpressure.enabled", false)) {
      Some(
        new KafkaRateController(
          PIDRateEstimator.create(ssc.sc.conf, ssc.emptyDataWaitTime)))
    } else {
      None
    }
  }

  /**
    * @author LMQ
    * @time 20190613
    * @desc 支持用户在执行流式处理的时候，动态地更改topic。//之后会添加更改offset的方法，
    */
  def setTopics(topics: Set[String]) = {
    this.topics = topics
  }

  /**
    * @author LMQ
    * @time 20190604
    * @desc 获取kafka的RDD，这里如果想自己实现也可以，类似DStream里面的compute
    */
  override def batchRDD() = {
    val lastOffset = ssc.sc.getLastOffset(topics)
    val kafkardd = ssc.sc.kafkaRDD[K, V, KD, VD, R](
      topics,
      currentOffsets,
      maxMessagesPerPartition(lastOffset),
      msghandle)
    rateController.foreach { x =>
      x.setCurrentElems(kafkardd.count())
    }
    currentOffsets = kafkardd.getRDDOffsets()
    //设置批次启动时间
    rateController.foreach { x =>
      x.setBatchSubmitTime(new Date().getTime)
    }
    kafkardd
  }

  /**
    * @author LMQ
    * @desc 当前批次结束时调用，用于计算rate
    */
  override def onBatchCompleted() {
    rateController.foreach { x =>
      x.onBatchCompleted()
    }
  }

  /**
    * @author LMQ
    * @time 2019-06-09
    * @desc 获取每个分区最大拉取条数
    */
  def maxMessagesPerPartition(lastOffset: Map[TopicAndPartition, Long])
    : Option[Map[TopicAndPartition, Long]] = {
    val estimatedRateLimit = rateController.map(_.getLatestRate().toInt)
    // calculate a per-partition rate limit based on current lag
    val effectiveRateLimitPerPartition =
      estimatedRateLimit.filter(_ > 0) match {
        case Some(rate) =>
          val lagPerPartition = lastOffset.map {
            case (tp, offset) =>
              tp -> Math.max(offset - currentOffsets(tp), 0)
          }
          val totalLag = lagPerPartition.values.sum

          lagPerPartition.map {
            case (tp, lag) =>
              // 防止速率降到0的时候，返回None，导致直接取到last的数据，所以这里加了Math.max。我看源码里面是没有这个处理的。
              val backpressureRate =
                Math.max(Math.round(lag / totalLag.toFloat * rate), 1)
              tp -> (if (maxRateLimitPerPartition > 0) {
                       Math.min(backpressureRate, maxRateLimitPerPartition)
                     } else backpressureRate)
          }
        case None =>
          lastOffset.map { case (tp, offset) => tp -> maxRateLimitPerPartition }
      }
    if (effectiveRateLimitPerPartition.values.sum > 0) {
      val secsPerBatch = ssc.emptyDataWaitTime.milliseconds.toDouble / 1000
      Some(effectiveRateLimitPerPartition.map {
        case (tp, limit) =>
          if (maxLimitPerPartition > 0)
            tp -> Math.min(maxLimitPerPartition, (secsPerBatch * limit).toLong)
          else tp -> (secsPerBatch * limit).toLong
      })
    } else {
      None
    }
  }
}

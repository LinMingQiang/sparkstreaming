package org.apache.streaming.core

import scala.reflect.ClassTag
import org.apache.spark.streaming.kafka.KafkaDataRDD
import kafka.serializer.Decoder
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.scheduler.KafkaRateController

abstract class KafkaDynamicDStream[K: ClassTag,
                                   V: ClassTag,
                                   KD <: Decoder[K]: ClassTag,
                                   VD <: Decoder[V]: ClassTag,
                                   R: ClassTag] {

  /**
    * @author LMQ
    * @time 2019-06-13
    * @desc 专属于kafka的rateController
    */
  val rateController: Option[KafkaRateController] = null

  /**
    * @author LMQ
    * @time 2019-06-13
    * @desc  记录当前的offset的起。会有个问题，如果在foreachRDD中的代码出现错误，数据其实是没有消费的，但是在这里会默认batch开始就已经消费了。所以会导致丢数据，如果
    *      foreachRDD里面一直出现error但是都catch了，会导致这一批数据直接丢掉，想不丢掉得自己实现  batchRDD，或者在catch里面去手动 setCurrentOffsets
    */
  var currentOffsets: Map[TopicAndPartition, Long] = null

  /**
    * @author LMQ
    * @time 2019-06-13
    * @desc 允许用户修改offset的起点，
    */
  def setCurrentOffsets(currentOffsets: Map[TopicAndPartition, Long]) {
    this.currentOffsets = currentOffsets
  }

  /**
    * @author LMQ
    * @time 2019-06-13
    * @desc 就是用户自己的batch逻辑部分。默认返回false
    * @return true ： 立即执行下个batch job （例如kafka中还有堆积的数据时，可以选择立刻执行下个数据）
    * 				 false ： 暂缓执行下一个batch job （例如当 kafka中没有数据进来时，可以暂缓job执行时间）
    */
  var computeFunc: (KafkaDataRDD[K, V, KD, VD, R] => Boolean) =
    (rdd: KafkaDataRDD[K, V, KD, VD, R]) => false //是否立刻执行下个批次

  /**
    * @author LMQ
    * @time 2019-06-13
    * @desc 贴合spark，方便使用理解
    */
  def foreachRDD(computeFunc: KafkaDataRDD[K, V, KD, VD, R] => Boolean) {
    this.computeFunc = computeFunc
  }

  /**
    * @author LMQ
    * @time 2019-06-13
    * @desc 我本来是先以此来命名的，因为这个更符合我设计这个代码的初衷，就是针对batch而不是rdd
    */
  def foreachBatch(computeFunc: KafkaDataRDD[K, V, KD, VD, R] => Boolean) {
    this.computeFunc = computeFunc
  }

  /**
    * @author LMQ
    * @time 2019-06-13
    * @desc 用来返回批次的rdd ，（参考源码中 compute 方法）
    */
  def batchRDD(): KafkaDataRDD[K, V, KD, VD, R] // 用来获取当前批次的kafkardd
  /**
    * @author LMQ
    * @time 2019-06-13
    * @desc 真正执行job，参考源码
    */
  def generateJob() = computeFunc(batchRDD) //执行job
  /**
    * @author LMQ
    * @time 2019-06-13
    * @desc 当batch结束后，做一些操作
    */
  def onBatchCompleted(): Unit
}

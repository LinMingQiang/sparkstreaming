package org.apache.streaming.core

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

/**
  * @author LMQ
  * @desc 只支持kafka。暂时不考虑支持其他的数据源。接口先定义。为了方便foreachRDD{ } ，
  * 				否则无法直接使用kafkardd。这个接口暂时不用，等之后有想要扩张其他的数据源的时候再开发。暂时使用的是KafkaDynamicDStream接口
  */
abstract class DynamicDstream[R: ClassTag] {
  var computeFunc = (rdd: RDD[R]) => {
    false //是否立刻执行下个批次
  }

  def foreachRDD(computeFunc: RDD[R] => Boolean) {
    this.computeFunc = computeFunc
  }
  def batchRDD(): RDD[R] //获取当前批次的rdd 。类似于 compute
  def generateJob() = computeFunc(batchRDD) //执行计算操作

}

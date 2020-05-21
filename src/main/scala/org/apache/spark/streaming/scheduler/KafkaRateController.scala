package org.apache.spark.streaming.scheduler

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import java.util.Date
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration
import scala.collection.mutable.ArrayBuffer
import scala.beans.BeanProperty

class KafkaRateController(val rateEstimator: PIDRateEstimator)
    extends SparkListener {
  var jobUseTime = new ArrayBuffer[Long] //实际的逻辑运行时间，但是一个streaming可以有多个job，要累加起来
  var jobStartTime = -1L
  var jobLastEndTime = -1L
  var scheduleDelay = new ArrayBuffer[Long]
  var rateLimit: Long = 0L
  @BeanProperty
  var batchSubmitTime = -1L //batch的启动时间
  @BeanProperty
  var currentElems = 0L

  /**
    * @author LinMingQiang
    * @time 2019-06-16
    * @desc job开始时执行，一个active一个job
    */
  override def onJobStart(jobStart: SparkListenerJobStart) {
    jobStartTime = new Date().getTime
    if (jobLastEndTime > 0) {
      scheduleDelay.+=(jobStartTime - jobLastEndTime)
    } else {
      scheduleDelay.+=(jobStartTime - batchSubmitTime)
    }
  }

  /**
    * @author LinMingQiang
    * @time 2019-06-16
    * @desc job结束时执行，一个active一个job
    */
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    jobLastEndTime = new Date().getTime
    jobUseTime.+=(jobLastEndTime - jobStartTime)
  }

  /**
    * @author LinMingQiang
    * @time 2019-06-16
    * @desc 计算下一批数据拉取速率
    */
  private def computeRate(time: Long,
                          elems: Long,
                          workDelay: Long,
                          waitDelay: Long) {
    rateEstimator.compute(time, elems, workDelay, waitDelay).foreach { x =>
      rateLimit = x.toLong
    }

  }

  /**
    * @author LinMingQiang
    * @time 2019-06-16
    * @desc 当前批次结束时计算速率和初始化
    */
  def onBatchCompleted() {
    val totalJobUseTime = jobUseTime.sum
    val totalScheduleDelay = scheduleDelay.sum
    computeRate(new Date().getTime,
                currentElems,
                totalJobUseTime,
                totalScheduleDelay)
    jobUseTime.clear()
    scheduleDelay.clear()
    jobLastEndTime = -1L

  }
  def getLatestRate(): Long = rateLimit

}

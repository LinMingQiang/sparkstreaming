package org.apache.spark.streaming.scheduler

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import java.util.Date
import org.apache.spark.scheduler.SparkListenerJobStart
import scala.collection.mutable.ArrayBuffer

class KafkaRateController(val rateEstimator: PIDRateEstimator)
    extends SparkListener {
  var jobUseTime = new ArrayBuffer[Long] //实际的逻辑运行时间，但是一个streaming可以有多个job，要累加起来
  var jobStartTime = -1L
  var jobLastEndTime = -1L
  var batchSubmitTime = -1L //batch的启动时间
  var scheduleDelay = new ArrayBuffer[Long]
  var rateLimit: Long = 0L
  var currentElems = 0L

  def setBatchSubmitTime(batchSubmitTime: Long) {
    this.batchSubmitTime = batchSubmitTime
  }

  def setCurrentElems(currentElems: Long) {
    this.currentElems = currentElems
  }
  override def onJobStart(jobStart: SparkListenerJobStart) {
    jobStartTime = new Date().getTime
    if (jobLastEndTime > 0) {
      scheduleDelay.+=(jobStartTime - jobLastEndTime)
    } else {
      scheduleDelay.+=(jobStartTime - batchSubmitTime)
    }
  }
  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    jobLastEndTime = new Date().getTime
    jobUseTime.+=(jobLastEndTime - jobStartTime)
  }

  /**
    * Compute the new rate limit and publish it asynchronously.
    */
  private def computeAndPublish(time: Long,
                                elems: Long,
                                workDelay: Long,
                                waitDelay: Long) {
    rateEstimator.compute(time, elems, workDelay, waitDelay).foreach { x =>
      rateLimit = x.toLong
    }

  }

  def onBatchCompleted() {
    val totalJobUseTime = jobUseTime.sum
    val totalScheduleDelay = scheduleDelay.sum
    computeAndPublish(new Date().getTime,
                      currentElems,
                      totalJobUseTime,
                      totalScheduleDelay)
    jobUseTime.clear()
    scheduleDelay.clear()
    jobLastEndTime = -1L

  }
  def getLatestRate(): Long = rateLimit

}

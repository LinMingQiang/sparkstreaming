package org.apache.spark.streaming.scheduler

import org.apache.spark.streaming.Duration
import org.apache.spark.SparkConf

class PIDRateEstimator(
    batchIntervalMillis: Long,
    proportional: Double,
    integral: Double,
    derivative: Double,
    minRate: Double
) {

  private var firstRun: Boolean = true
  private var latestTime: Long = -1L
  private var latestRate: Double = -1D
  private var latestError: Double = -1L
  def getLatestRate() = {
    latestRate
  }
  require(
    batchIntervalMillis > 0,
    s"Specified batch interval $batchIntervalMillis in PIDRateEstimator is invalid.")
  require(
    proportional >= 0,
    s"Proportional term $proportional in PIDRateEstimator should be >= 0.")
  require(integral >= 0,
          s"Integral term $integral in PIDRateEstimator should be >= 0.")
  require(derivative >= 0,
          s"Derivative term $derivative in PIDRateEstimator should be >= 0.")
  require(minRate > 0, s"Minimum rate in PIDRateEstimator should be > 0")

  def compute(
      time: Long, // in milliseconds
      numElements: Long,
      processingDelay: Long, // in milliseconds
      schedulingDelay: Long // in milliseconds
  ): Option[Double] = {
    this.synchronized {
      if (time > latestTime && numElements > 0 && processingDelay > 0) {

        // in seconds, should be close to batchDuration
        val delaySinceUpdate = (time - latestTime).toDouble / 1000
        // in elements/second
        val processingRate = numElements.toDouble / processingDelay * 1000
        // In our system `error` is the difference between the desired rate and the measured rate
        // based on the latest batch information. We consider the desired rate to be latest rate,
        // which is what this estimator calculated for the previous batch.
        // in elements/second
        val error = latestRate - processingRate
        // The error integral, based on schedulingDelay as an indicator for accumulated errors.
        // A scheduling delay s corresponds to s * processingRate overflowing elements. Those
        // are elements that couldn't be processed in previous batches, leading to this delay.
        // In the following, we assume the processingRate didn't change too much.
        // From the number of overflowing elements we can calculate the rate at which they would be
        // processed by dividing it by the batch interval. This rate is our "historical" error,
        // or integral part, since if we subtracted this rate from the previous "calculated rate",
        // there wouldn't have been any overflowing elements, and the scheduling delay would have
        // been zero.
        // (in elements/second)
        val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis

        // in elements/(second ^ 2)
        val dError = (error - latestError) / delaySinceUpdate

        val newRate = (latestRate - proportional * error -
          integral * historicalError -
          derivative * dError).max(minRate)
        latestTime = time
        if (firstRun) {
          latestRate = processingRate
          latestError = 0D
          firstRun = false
          None
        } else {
          latestRate = newRate
          latestError = error
          Some(newRate)
        }
      } else {
        None
      }
    }
  }
}
object PIDRateEstimator {

  /**
    * Return a new `RateEstimator` based on the value of
    * `spark.streaming.backpressure.rateEstimator`.
    *
    * The only known and acceptable estimator right now is `pid`.
    *
    * @return An instance of RateEstimator
    * @throws IllegalArgumentException if the configured RateEstimator is not `pid`.
    */
  def create(conf: SparkConf, batchInterval: Duration) =
    conf.get("spark.streaming.backpressure.rateEstimator", "pid") match {
      case "pid" =>
        val proportional =
          conf.getDouble("spark.streaming.backpressure.pid.proportional", 1.0)
        val integral =
          conf.getDouble("spark.streaming.backpressure.pid.integral", 0.2)
        val derived =
          conf.getDouble("spark.streaming.backpressure.pid.derived", 0.0)
        val minRate =
          conf.getDouble("spark.streaming.backpressure.pid.minRate", 100)
        new PIDRateEstimator(batchInterval.milliseconds,
                             proportional,
                             integral,
                             derived,
                             minRate)
      case estimator =>
        throw new IllegalArgumentException(
          s"Unknown rate estimator: $estimator")
    }
}

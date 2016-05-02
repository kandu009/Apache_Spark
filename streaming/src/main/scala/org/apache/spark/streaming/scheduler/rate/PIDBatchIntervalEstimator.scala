/*
author: rkandur
*/

package org.apache.spark.streaming.scheduler.rate

import org.apache.spark.internal.Logging

/**
 * Implements a proportional-integral-derivative (PID) controller which acts on
 * the batch size of the data fed into Spark Streaming. It works by calculating 
 * an error, difference between the measured processing rate and the desired values.
 * @see https://en.wikipedia.org/wiki/PID_controller
 *
 * @param proportional how much the correction should depend on the current
 *        error. This term usually provides the bulk of correction and should be positive or zero.
 *        A value too large would make the controller overshoot the setpoint, while a small value
 *        would make the controller too insensitive. The default value is 1.
 * @param integral how much the correction should depend on the accumulation
 *        of past errors. This value should be positive or 0. This term accelerates the movement
 *        towards the desired value, but a large value may lead to overshooting. The default value
 *        is 0.2.
 * @param derivative how much the correction should depend on a prediction
 *        of future errors, based on current rate of change. This value should be positive or 0.
 *        This term is not used very often, as it impacts stability of the system. The default
 *        value is 0.
 */
private[streaming] class PIDBatchIntervalEstimator(
    minBatchInterval: Long, // in milliseconds
    proportional: Double,
    integral: Double,
    derivative: Double
  ) extends BatchIntervalEstimator with Logging {

  private var firstRun: Boolean = true
  private var latestTime: Long = -1L
  private var latestRate: Double = -1D
  private var latestError: Double = -1L
  private var latestBatchInterval: Long = -1L

  require(
    proportional >= 0,
    s"Proportional term $proportional in BatchIntervalEstimator should be >= 0.")
  require(
    integral >= 0,
    s"Integral term $integral in BatchIntervalEstimator should be >= 0.")
  require(
    derivative >= 0,
    s"Derivative term $derivative in BatchIntervalEstimator should be >= 0.")
  
  logInfo(s"Created BatchIntervalEstimator with proportional = $proportional, integral = $integral, " +
    s"derivative = $derivative")

  def compute(
      time: Long, // in milliseconds
      numElements: Long,
      processingDelay: Long, // in milliseconds
      schedulingDelay: Long // in milliseconds
    ): Option[Long] = {
    
    logTrace(s"\ntime = $time, # records = $numElements, " +
      s"processing time = $processingDelay, scheduling delay = $schedulingDelay")
    
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
        val historicalError = schedulingDelay.toDouble * processingRate / minBatchInterval

        // in elements/(second ^ 2)
        val dError = (error - latestError) / delaySinceUpdate

        val newRate = (latestRate - proportional * error -
                                    integral * historicalError -
                                    derivative * dError)

        logTrace(s"""
            | latestRate = $latestRate, error = $error
            | latestError = $latestError, historicalError = $historicalError
            | delaySinceUpdate = $delaySinceUpdate, dError = $dError
            """.stripMargin)

        latestTime = time

        if (firstRun) {
          latestBatchInterval = minBatchInterval
          latestRate = processingRate
          latestError = 0D
          firstRun = false
          logTrace("First run, rate estimation skipped")
          Some(latestBatchInterval)
        } else {
          latestBatchInterval = (((newRate*latestBatchInterval)/latestRate).toLong).max(minBatchInterval)
          latestRate = newRate
          latestError = error
          logTrace(s"New rate = $newRate")
          Some(latestBatchInterval)
        }

      } else {
        logTrace("BatchSize estimation skipped")
        None
      }
    }
  }
}
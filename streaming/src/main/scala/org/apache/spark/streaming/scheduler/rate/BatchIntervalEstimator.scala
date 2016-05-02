/*
author: rkandur
*/

package org.apache.spark.streaming.scheduler.rate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Duration

private[streaming] trait BatchIntervalEstimator extends Serializable {

  /**
   * @param time The timestamp of the current batch interval that just finished
   * @param elements The number of records that were processed in this batch
   * @param processingDelay The time in ms that took for the job to complete
   * @param schedulingDelay The time in ms that the job spent in the scheduling queue
   */
  def compute(
      time: Long,
      elements: Long,
      processingDelay: Long,
      schedulingDelay: Long): Option[Long]
}

object BatchIntervalEstimator {

  def create(conf: SparkConf, minBatchInterval: Duration): BatchIntervalEstimator =
    conf.get("spark.streaming.batchsizecontrol.batchIntervalEstimator", "pid") match {
      case "pid" =>
        val proportional = conf.getDouble("spark.streaming.batchsizecontrol.pid.proportional", 1.0)
        val integral = conf.getDouble("spark.streaming.batchsizecontrol.pid.integral", 0.2)
        val derived = conf.getDouble("spark.streaming.batchsizecontrol.pid.derived", 0.0)
        new PIDBatchIntervalEstimator(minBatchInterval.milliseconds, proportional, integral, derived)

      case "gradient" =>
        val threshold = conf.getLong("spark.streaming.batchsizecontrol.gradient.threshold", 25) // TODO: RK: Check appropriate threshold
        val stepSize = conf.getLong("spark.streaming.batchsizecontrol.gradient.stepSize", 100)
        new GradientBatchIntervalEstimator(minBatchInterval.milliseconds, threshold, stepSize)

      case unknown =>
        throw new IllegalArgumentException(s"Unknown batch size estimator: $unknown")
    }
}

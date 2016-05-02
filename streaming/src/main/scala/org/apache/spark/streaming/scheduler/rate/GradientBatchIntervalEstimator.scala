/*
author: rkandur
*/

package org.apache.spark.streaming.scheduler.rate

import org.apache.spark.internal.Logging

/**
 * Implements a Gradient based batch interval adaptor. It works by calculating 
 * the gradient of the past two batches (delta in processing_delay/delta in batch_interval).
 * If the gradient is > gradientThreshold, batchInterval is increased by a stepSize
 * Else, it is decreased by the stepSize.
 * 
 * @param minBatchInterval minimum batch interval which should be maintained
 * @param gradientThreshold threshold which is used to identify if the current batch interval should
 *        be increased or decreased based on the current system load
 * @param stepSize by how much should the step size be increased or decreased after each iteration
 */
private[streaming] class GradientBatchIntervalEstimator(
    minBatchInterval: Long, // in milliseconds
    gradientThreshold: Long,
    stepSize: Long
  ) extends BatchIntervalEstimator with Logging {

  private var firstRun: Boolean = true
  private var secondRun: Boolean = false
  private var latestBatchInterval: Long = -1L
  private var previousBatchInterval: Long = -1L // last but one batch interval
  private var latestProcessingDelay: Long = -1L
  private var previousProcessingDelay: Long = -1L // last but one processing delay

  def compute(
      time: Long, // in milliseconds
      numElements: Long,
      processingDelay: Long, // in milliseconds
      schedulingDelay: Long // in milliseconds
    ): Option[Long] = {
    
    logTrace(s"\ntime = $time, # records = $numElements, " +
      s"processing time = $processingDelay, scheduling delay = $schedulingDelay")
    
    this.synchronized {
      
      if (numElements > 0 && processingDelay > 0) {

        if (firstRun && !secondRun) {
          latestBatchInterval = minBatchInterval
          latestProcessingDelay = processingDelay
          firstRun = false
          secondRun = true
          logTrace("First run, rate estimation skipped")
          Some(latestBatchInterval)
        } else if (secondRun && !firstRun) {
          previousBatchInterval = latestBatchInterval
          previousProcessingDelay = latestProcessingDelay
          latestBatchInterval = minBatchInterval
          latestProcessingDelay = processingDelay
          secondRun = false
          logTrace("Second run, rate estimation skipped")
          Some(latestBatchInterval)
        } else {
          val newBatchInterval = if(((previousProcessingDelay-latestProcessingDelay)/(previousBatchInterval-latestBatchInterval)) > gradientThreshold) latestBatchInterval-stepSize else latestBatchInterval+stepSize
          logTrace(s"New batch = $newBatchInterval")
          previousBatchInterval = latestBatchInterval
          previousProcessingDelay = latestProcessingDelay
          latestBatchInterval = newBatchInterval.max(minBatchInterval)
          latestProcessingDelay = processingDelay
          Some(latestBatchInterval)
        }

      } else {
        logTrace("BatchSize estimation skipped")
        None
      }
    }
  }
}
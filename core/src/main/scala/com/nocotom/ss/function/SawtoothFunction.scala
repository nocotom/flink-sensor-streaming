package com.nocotom.ss.function

import java.util

import com.nocotom.ss.model.{DataPoint, TimePoint}
import com.nocotom.ss.model.Point._
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed

import scala.collection.JavaConversions._
import scala.math.BigDecimal.RoundingMode

/**
  * y(t) = A[t - floor(t)]
  * Where:
  *   A - amplitude
  */
class SawtoothFunction(private val stepsAmount: Int = 10, private val amplitude: BigDecimal = 1)
  extends RichMapFunction[TimePoint, DataPoint[BigDecimal]]
    with ListCheckpointed[BigDecimal] {

  private var currentStep : BigDecimal = 0

  override def map(timePoint : TimePoint): DataPoint[BigDecimal] = {
    val phase = currentStep / stepsAmount * amplitude
    currentStep += 1
    currentStep = currentStep % stepsAmount
    timePoint.withValue(phase.setScale(4, RoundingMode.HALF_UP))
  }

  override def restoreState(state: util.List[BigDecimal]): Unit = {
    for (stateEntry <- state) {
      currentStep = stateEntry
    }
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[BigDecimal] = {
    util.Collections.singletonList(currentStep)
  }
}

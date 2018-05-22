package com.nocotom.ss.function

import java.{lang, util}

import com.nocotom.ss.point.{DataPoint, TimePoint}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed

import scala.collection.JavaConversions._
import scala.math.BigDecimal.RoundingMode

/**
  * y(t) = Asin(2πft + φ)
  * Where:
  *   A - amplitude
  *   f - frequency
  *   φ - phase
  */
class SineFunction(private val amplitude : BigDecimal = 1, private val frequency : BigDecimal = 1, private val phase : BigDecimal = 0, private val stepsAmount: Int = 20)
  extends RichMapFunction[TimePoint, DataPoint[BigDecimal]]
    with ListCheckpointed[lang.Integer] {

  private var currentStep = 0

  override def map(timePoint: TimePoint): DataPoint[BigDecimal] = {
    val radians = 2 * math.Pi * frequency * currentStep / stepsAmount + phase
    currentStep += 1
    currentStep = currentStep % stepsAmount

    val result = amplitude * math.sin(radians.doubleValue())
    timePoint.withValue(result.setScale(4, RoundingMode.HALF_UP))
  }

  override def restoreState(state: util.List[lang.Integer]): Unit = {
    for (stateEntry <- state) {
      currentStep = stateEntry
    }
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[lang.Integer] = {
    util.Collections.singletonList(currentStep)
  }
}
package com.nocotom.ss.function

import java.{lang, util}

import com.nocotom.ss.model.DataPoint
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed

import scala.collection.JavaConversions._

/**
  * y(t) = Asin(2πft + φ)
  * Where:
  *   A - amplitude
  *   f - frequency
  *   φ - phase
  */
class SineFunction(private val amplitude : BigDecimal = 1, private val frequency : BigDecimal = 1, private val phase : BigDecimal = 0, private val stepsAmount: Int = 10)
  extends RichMapFunction[DataPoint[BigDecimal], DataPoint[BigDecimal]]
    with ListCheckpointed[lang.Integer] {

  private var currentStep = 0

  override def map(dataPoint: DataPoint[BigDecimal]): DataPoint[BigDecimal] = {
    val radians = 2 * math.Pi * frequency * currentStep / stepsAmount + phase
    currentStep = currentStep % stepsAmount + 1

    val result = amplitude * math.sin(radians.doubleValue())
    dataPoint.withNewValue(result)
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
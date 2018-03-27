package com.nocotom.ss.function

import java.{lang, util}

import com.nocotom.ss.model.DataPoint
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed

import scala.collection.JavaConversions._

/**
  * y(t) = t - floor(t)
  */
class SawtoothFunction(private val stepsAmount: Int = 10)
  extends RichMapFunction[DataPoint[BigDecimal], DataPoint[BigDecimal]]
    with ListCheckpointed[lang.Integer] {

  private var currentStep = 0

  override def map(dataPoint : DataPoint[BigDecimal]): DataPoint[BigDecimal] = {
    val phase = currentStep / stepsAmount
    currentStep = currentStep % stepsAmount + 1
    dataPoint.withNewValue(phase)
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

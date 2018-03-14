package com.nocotom.ss.map

import com.nocotom.ss.model.DataPoint
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

class SawtoothFunction(private val stepsAmount: Int)
  extends RichMapFunction[DataPoint[BigDecimal], DataPoint[BigDecimal]]
    with CheckpointedFunction {

  private val STATE_KEY = "sawtooth_state"
  private var currentStep = 0

  override def map(dataPoint : DataPoint[BigDecimal]): DataPoint[BigDecimal] = {
    val phase = currentStep / stepsAmount
    currentStep += 1
    currentStep = currentStep % stepsAmount
    dataPoint.withNewValue(BigDecimal(phase))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    this.getState.update(currentStep)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    currentStep = this.getState.value()
  }

  private def getState : ValueState[Int] = {
    getRuntimeContext.getState(new ValueStateDescriptor(STATE_KEY, classOf[Int]))
  }
}

package com.nocotom.ss.function

import com.nocotom.ss.model.DataPoint
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

/**
  * y(t) = Asin(2πft + φ)
  * Where:
  *   A - amplitude
  *   f - frequency
  *   φ - phase
  */
class SineFunction(private val amplitude : BigDecimal = 1, private val frequency : BigDecimal = 1, private val phase : BigDecimal = 0, private val stepsAmount: Int = 10)
  extends RichMapFunction[DataPoint[BigDecimal], DataPoint[BigDecimal]]
    with CheckpointedFunction {

  private val STATE_KEY = "SINE_FUNCTION_STATE"
  private var currentStep = 0

  override def map(dataPoint: DataPoint[BigDecimal]): DataPoint[BigDecimal] = {
    val radians = 2 * math.Pi * frequency * currentStep / stepsAmount + phase
    currentStep = currentStep % stepsAmount + 1

    val result = amplitude * math.sin(radians.doubleValue())
    dataPoint.withNewValue(result)
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
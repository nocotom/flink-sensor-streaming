package com.nocotom.ss.source

import com.nocotom.ss.model.DataPoint
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark

import scala.concurrent.duration.FiniteDuration

class TimestampSource(private val period: FiniteDuration, private val slowdownFactor : Int)
  extends RichParallelSourceFunction[DataPoint[BigDecimal]]
    with CheckpointedFunction {

  private val STATE_KEY = "TIMESTAMP_SOURCE_STATE"
  private lazy val locker = new Locker()
  private var currentTime : Long = 0

  override def cancel(): Unit = locker.open()

  override def run(sourceContext: SourceFunction.SourceContext[DataPoint[BigDecimal]]): Unit = {
    while(!locker.await(period)){
      sourceContext.collectWithTimestamp(new DataPoint[BigDecimal](currentTime, BigDecimal(0)), currentTime)
      sourceContext.emitWatermark(new Watermark(currentTime))
      currentTime += period.toMillis
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    this.getState.update(currentTime)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    currentTime = this.getState.value()
  }

  private def getState : ValueState[Long] = {
    getRuntimeContext.getState(new ValueStateDescriptor(STATE_KEY, classOf[Long]))
  }
}

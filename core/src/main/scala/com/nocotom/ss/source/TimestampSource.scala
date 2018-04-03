package com.nocotom.ss.source

import java.{lang, util}

import com.nocotom.ss.model.{DataPoint, TimePoint}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark

import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration

class TimestampSource(private val period: FiniteDuration)
  extends RichParallelSourceFunction[TimePoint]
    with ListCheckpointed[lang.Long] {

  private lazy val gate = new Gate()
  private var currentTime : Long = 0L

  override def open(parameters: Configuration): Unit = {
    if(currentTime == 0){
      currentTime = now
    }
  }

  override def cancel(): Unit = gate.open()

  override def run(sourceContext: SourceFunction.SourceContext[TimePoint]): Unit = {
    val lock = sourceContext.getCheckpointLock

    while(!gate.await(period)){
      lock.synchronized({
        sourceContext.collectWithTimestamp(TimePoint(currentTime), currentTime)
        sourceContext.emitWatermark(new Watermark(currentTime))
        currentTime += period.toMillis
      })
    }
  }

  override def restoreState(state: util.List[lang.Long]): Unit = {
    for (stateEntry <- state) {
      currentTime = stateEntry
    }
  }

  private def now = System.currentTimeMillis

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[lang.Long] = {
    util.Collections.singletonList(currentTime)
  }
}

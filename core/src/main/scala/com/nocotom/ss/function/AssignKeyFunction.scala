package com.nocotom.ss.function

import com.nocotom.ss.model.{DataPoint, KeyedDataPoint}
import com.nocotom.ss.model.Point._
import org.apache.flink.api.common.functions.MapFunction

import scala.math.ScalaNumber

class AssignKeyFunction[T <: ScalaNumber](val key: String) extends MapFunction[DataPoint[T], KeyedDataPoint[T]]{
  override def map(dataPoint: DataPoint[T]): KeyedDataPoint[T] = {
    dataPoint.withKey(key)
  }
}

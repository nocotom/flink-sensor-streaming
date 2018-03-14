package com.nocotom.ss.model

import scala.concurrent.duration.FiniteDuration
import scala.math.ScalaNumber

class DataPoint[T >: ScalaNumber](val timestamp: Long, val value: T) {

  def this(timestamp: FiniteDuration, value: T){
    this(timestamp.toMillis, value)
  }

  def withNewValue(newValue: T): DataPoint[T] ={
    new DataPoint(this.timestamp, newValue)
  }
}

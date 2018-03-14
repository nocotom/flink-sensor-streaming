package com.nocotom.ss.model

import scala.concurrent.duration.FiniteDuration

class DataPoint[T](val timestamp: Long, val value: T) {

  def this(timestamp: FiniteDuration, value: T){
    this(timestamp.toMillis, value)
  }
}

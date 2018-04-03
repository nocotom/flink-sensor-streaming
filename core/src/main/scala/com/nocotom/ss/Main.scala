package com.nocotom.ss

import com.nocotom.ss.function.{AssignKeyFunction, SawtoothFunction, SineFunction}
import com.nocotom.ss.sink.InfluxDbSink
import com.nocotom.ss.source.TimestampSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

import scala.concurrent.duration._

object Main {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val timestampStream = env
      .addSource(new TimestampSource(1.seconds))
      .name("source")

    // Simulate temperature sensor
    val sawtoothStream = timestampStream
      .map(new SawtoothFunction(amplitude = 3))
      .name("sawtooth(point)")

    val tempStream = sawtoothStream
      .map(new AssignKeyFunction[BigDecimal]("temp"))
      .name("assignKey(point)")

    // Simulate humidity sensor
    val sineStream = timestampStream
      .map(new SineFunction(amplitude = 3))
      .name("sine(point)")

    val humidityStream = sineStream
      .map(new AssignKeyFunction[BigDecimal]("humidity"))
      .name("assignKey(humidity)")

    val sensorStream = tempStream.union(humidityStream)

    //sensorStream.print()

    sensorStream
      .addSink(new InfluxDbSink[BigDecimal]("sensors"));

    env.execute()
  }
}

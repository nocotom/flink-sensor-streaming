package com.nocotom.ss.sink

import java.util.concurrent.TimeUnit

import com.nocotom.ss.model.KeyedDataPoint
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.influxdb.dto.Point
import org.influxdb.{InfluxDB, InfluxDBFactory}

import scala.math.ScalaNumber

class InfluxDbSink[T <: ScalaNumber](val measurement: String) extends RichSinkFunction[KeyedDataPoint[T]] {

  @transient
  private var influxDB: InfluxDB = _
  private var databaseName: String = _

  override def open(parameters: Configuration): Unit = {
    databaseName = parameters.getString("db", "sensors")
    influxDB = InfluxDBFactory.connect(
      parameters.getString("url", "http://localhost:8086"),
      parameters.getString("user", "admin"),
      parameters.getString("password", "admin"))

    influxDB.createDatabase(databaseName)
    influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS)
  }

  override def close(): Unit = {
    influxDB.close()
  }

  override def invoke(dataPoint: KeyedDataPoint[T]): Unit = {
    val point: Point = Point.measurement(measurement)
      .time(dataPoint.timestamp, TimeUnit.MILLISECONDS)
      .addField("value", dataPoint.value.doubleValue())
      .tag("key", dataPoint.key)
      .build()

    influxDB.write(databaseName, "autogen", point)
  }
}

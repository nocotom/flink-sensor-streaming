package com.nocotom.ss.sink

import java.util.concurrent.TimeUnit

import com.nocotom.ss.point.KeyedDataPoint
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.influxdb.dto.Point
import org.influxdb.{InfluxDB, InfluxDBFactory}

import scala.math.ScalaNumber

class InfluxDbSink[T <: ScalaNumber](val measurement: String) extends RichSinkFunction[KeyedDataPoint[T]] {

  @transient
  private var influxDB: InfluxDB = _
  private var databaseName: String = _

  override def open(configuration: Configuration): Unit = {
    val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    databaseName = parameters.get("influx.db", "sensors")
    influxDB = InfluxDBFactory.connect(
      parameters.get("influx.url", "http://localhost:8086"),
      parameters.get("influx.user", "admin"),
      parameters.get("influx.password", "admin"))

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

# Apache Flink - Sensor Streaming

This project is an exemplary Apache Flink application for testing purposes. 

## Prerequisites

* Scala 2.11+
* Maven
* Docker compose

## Running locally

There is prepared the ``docker-compose.yaml`` file which allows you to run the following services:

* ``InfluxDb`` - storage for processed events (sink)
* ``Grafana`` - data visualization
* ``Job Manager`` - Flink's job manager
* ``Task Manager`` - Flink's task manager

To run all services, execute the following command:

```bash
docker-compose up -d
```

To crate Flink's job (java library), execute the following command:

```bash
mvn package
```

To deploy job, go to the [submit page](http://localhost:8081/#/submit), select compiled java library 
and submit job with the following program arguments:

```
--influx.url http://influxdb:8086
```

Finally, go to the [Grafana page](http://localhost:3000), login as ``admin:admin`` and go to the ``Sensors`` dashboard.
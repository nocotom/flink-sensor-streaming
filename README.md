# Apache Flink - Sensor Streaming

This project is an exemplary Apache Flink application for testing purposes. 

## Prerequisites

* Scala 2.11+
* Maven
* Docker compose
* Flink 1.6+

## Running locally

There is prepared the ``docker-compose.yaml`` file which allows you to run the following services:

* ``InfluxDb`` - storage for processed events (sink)
* ``Grafana`` - data visualization

To run all services, execute the following command:

```bash
docker-compose up -d
```

To crate Flink's job (java library), execute the following command:

```bash
mvn package
```

To deploy job, run the following command:

```bash
flink run -d core\target\sensor-streaming-core-1.0-SNAPSHOT.jar
```

Finally, go to the [Grafana page](http://localhost:3000), login as ``admin:admin`` and go to the ``Sensors`` dashboard.
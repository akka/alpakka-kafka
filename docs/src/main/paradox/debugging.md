# Debugging

Debugging setups with the Alpakka Kafka Connector will be required at times. This page collects a few ideas to start out with in case the connector does not behave as you expected.

## Logging with SLF4J

Akka, Akka Streams and thus the Alpakka Kafka Connector support to use the [SLF4J logging API](https://www.slf4j.org/) by adding Akka's SLF4J module and an SLF4J compatible logging framework, eg. [Logback](http://logback.qos.ch/).

The Kafka client library used by the Alpakka Kafka connector uses SLF4J, as well.

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-slf4j_$scala.binary.version$
  version=$akka.version$
  group2=ch.qos.logback
  artifact2=logback-classic
  version2=1.2.3
}

To enable Akka SLF4J logging, configure Akka in `application.conf` as below. Refer to the [Akka documentation](https://doc.akka.io/docs/akka/current/logging.html#slf4j) for details.

```hocon
akka {
  loggers = ["akka.event.slf4j.Slf4jLogger"]
  loglevel = "DEBUG"
  logging-filter = "akka.event.slf4j.Slf4jLoggingFilter"
}
```

## Receive logging

In case you're debugging the internals in the Kafka Consumer actor, you might want to enable receive logging to see all messages it receives. To lower the log message volume, change the Kafka poll interval to  something larger, eg. 300 ms.

```hocon
akka {
  actor {
    debug.receive = true
  }
  kafka.consumer {
    poll-interval = 300ms
  }
}
```

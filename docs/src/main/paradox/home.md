# Alpakka Kafka connector

The [Alpakka project](https://developer.lightbend.com/docs/alpakka/current/) is an open source initiative to implement stream-aware and reactive integration pipelines for Java and Scala. It is built on top of [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html), and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and stream-oriented programming, with built-in support for backpressure. Akka Streams is a [Reactive Streams](https://www.reactive-streams.org/) and JDK 9+ [java.util.concurrent.Flow](https://docs.oracle.com/javase/10/docs/api/java/util/concurrent/Flow.html)-compliant implementation and therefore [fully interoperable](https://doc.akka.io/docs/akka/current/general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.

This **Alpakka Kafka connector** lets you connect [Apache Kafka](https://kafka.apache.org/) to Akka Streams. It was formerly known as **Akka Streams Kafka** and even **Reactive Kafka**.

The examples in this documentation use

* Alpakka Kafka connector $version$ ([Github](https://github.com/akka/reactive-kafka))
* Scala $scalaBinaryVersion$
* @extref[Akka Streams](akka-docs:scala/stream/index.html) $akkaVersion$ ([Github](https://github.com/akka/akka))
* @extref[Apache Kafka](kafka-docs:index.html) $kafkaVersion$ ([Github](https://github.com/apache/kafka))

Release notes are found at [Github releases](https://github.com/akka/reactive-kafka/releases).

### Reported issues

[Issues at Github](https://github.com/akka/reactive-kafka/issues)

## Dependencies

@@dependency [sbt,Maven,Gradle] {
  group=com.typesafe.akka
  artifact=akka-stream-kafka_$scalaBinaryVersion$
  version=$version$
}


## Scala and Java APIs

There are two separate packages named `akka.kafka.scaladsl` and `akka.kafka.javadsl` 
with the API for Scala and Java. These packages contain `Producer` and `Consumer`
classes with factory methods for the various Akka Streams `Flow`, `Sink` and `Source`
that are producing or consuming messages to/from Kafka.


## Contributing

Please feel free to contribute to Alpakka and the Alpakka Kafka connector by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/reactive-kafka/blob/master/CONTRIBUTING.md) to learn how it can be done.

We want Akka and Alpakka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://github.com/akka/reactive-kafka/blob/master/CODE_OF_CONDUCT.md). 


@@ toc { .main depth=3 }

@@@ index

* [Producer](producer.md)
* [Consumer](consumer.md)
* [Error Handling](errorhandling.md)
* [At-Least-Once Delivery](atleastonce.md)

@@@

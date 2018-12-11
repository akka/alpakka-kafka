# Overview

The [Alpakka project](https://developer.lightbend.com/docs/alpakka/current/) is an open source initiative to implement stream-aware and reactive integration pipelines for Java and Scala. It is built on top of @extref[Akka Streams](akka-docs:stream/index.html), and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and stream-oriented programming, with built-in support for backpressure. Akka Streams is a [Reactive Streams](https://www.reactive-streams.org/) and JDK 9+ @extref[java.util.concurrent.Flow](java-docs:docs/api/java.base/java/util/concurrent/Flow.html)-compliant implementation and therefore @extref[fully interoperable](akka-docs:general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.

This **Alpakka Kafka connector** lets you connect [Apache Kafka](https://kafka.apache.org/) to Akka Streams. It was formerly known as **Akka Streams Kafka** and even **Reactive Kafka**.

## Project Info

@@project-info{ projectId="core" }

## Matching Kafka Versions

|Kafka  | Akka version | Alpakka Kafka Connector
|-------|--------------|-------------------------
|[2.1.x](https://dist.apache.org/repos/dist/release/kafka/2.1.0/RELEASE_NOTES.html) | 2.5.x        | @ref:[release 1.0-RC1](release-notes/1.0-RC1.md)
|2.0.x  | 2.5.x        | @ref:[release 1.0-M1](release-notes/1.0-M1.md)
|1.1.x  | 2.5.x        | [release 0.20+](https://github.com/akka/reactive-kafka/releases)
|1.0.x  | 2.5.x        | [release 0.20+](https://github.com/akka/reactive-kafka/releases)
|0.11.x | 2.5.x        | [release 0.19](https://github.com/akka/reactive-kafka/milestone/19?closed=1)
|0.11.x | 2.4.x        | [release 0.18](https://github.com/akka/reactive-kafka/milestone/18?closed=1)


## Dependencies

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-stream-kafka_$scala.binary.version$
  version=$project.version$
}

This connector depends on Akka 2.5.x and note that it is important that all `akka-*` dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems with transient dependencies causing an unlucky mix of versions.

The table below shows Alpakka Kafka's direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="core" }

* Akka Streams $akka.version$ @extref[documentation](akka-docs:stream/index.html) and [sources](https://github.com/akka/akka)
* Apache Kafka client $kafka.version$ @extref[documentation](kafka-docs:index.html) and [sources](https://github.com/apache/kafka)


## Scala and Java APIs

There are two separate packages named `akka.kafka.scaladsl` and `akka.kafka.docs.javadsl`
with the API for Scala and Java. These packages contain `Producer` and `Consumer`
classes with factory methods for the various Akka Streams `Flow`, `Sink` and `Source`
that are producing or consuming messages to/from Kafka.


## Contributing

Please feel free to contribute to Alpakka and the Alpakka Kafka connector by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/reactive-kafka/blob/master/CONTRIBUTING.md) to learn how it can be done.

We want Akka and Alpakka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://github.com/akka/reactive-kafka/blob/master/CODE_OF_CONDUCT.md).


@@@ index

* [release notes](release-notes/index.md)

@@@

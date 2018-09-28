# Alpakka Kafka Documentation

The [Alpakka project](https://developer.lightbend.com/docs/alpakka/current/) is an open source initiative to implement stream-aware and reactive integration pipelines for Java and Scala. It is built on top of [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html), and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and stream-oriented programming, with built-in support for backpressure. Akka Streams is a [Reactive Streams](https://www.reactive-streams.org/) and JDK 9+ [java.util.concurrent.Flow](https://docs.oracle.com/javase/10/docs/api/java/util/concurrent/Flow.html)-compliant implementation and therefore [fully interoperable](https://doc.akka.io/docs/akka/current/general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.

This **Alpakka Kafka connector** lets you connect [Apache Kafka](https://kafka.apache.org/) to Akka Streams. It was formerly known as **Akka Streams Kafka** and even **Reactive Kafka**.

@@toc { .main depth=2 }

@@@ index

* [overview](home.md)
* [Producer](producer.md)
* [Consumer](consumer.md)
* [Consumer Metadata](consumer-metadata.md)
* [Error Handling](errorhandling.md)
* [At-Least-Once Delivery](atleastonce.md)
* [Transactions](transactions.md)
* [debug](debugging.md)
* [Snapshots](snapshots.md)

@@@

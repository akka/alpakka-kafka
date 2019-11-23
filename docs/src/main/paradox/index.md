# Alpakka Kafka Documentation

The [Alpakka project](https://developer.lightbend.com/docs/alpakka/current/) is an open source initiative to implement stream-aware and reactive integration pipelines for Java and Scala. It is built on top of @extref[Akka Streams](akka:stream/index.html), and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and stream-oriented programming, with built-in support for backpressure. Akka Streams is a [Reactive Streams](https://www.reactive-streams.org/) and JDK 9+ @extref[java.util.concurrent.Flow](java-docs:docs/api/java.base/java/util/concurrent/Flow.html)-compliant implementation and therefore @extref[fully interoperable](akka:general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.

This **Alpakka Kafka connector** lets you connect [Apache Kafka](https://kafka.apache.org/) to Akka Streams. It was formerly known as **Akka Streams Kafka** and even **Reactive Kafka**.

@@toc { .main depth=2 }

@@@ index

* [overview](home.md)
* [Producer](producer.md)
* [Consumer](consumer.md)
* [Discovery](discovery.md)
* [Error Handling](errorhandling.md)
* [At-Least-Once Delivery](atleastonce.md)
* [Transactions](transactions.md)
* [deser](serialization.md)
* [debug](debugging.md)
* [test](testing.md)
* [test-testcontainers](testing-testcontainers.md)
* [in prod](production.md)
* [Snapshots](snapshots.md)

@@@

# Overview

The [Alpakka project](https://doc.akka.io/libraries/alpakka/current/) is an open source initiative to implement stream-aware and reactive integration pipelines for Java and Scala. It is built on top of @extref[Akka Streams](akka:stream/index.html), and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and stream-oriented programming, with built-in support for backpressure. Akka Streams is a [Reactive Streams](https://www.reactive-streams.org/) and JDK @extref[java.util.concurrent.Flow](java-docs:docs/api/java.base/java/util/concurrent/Flow.html)-compliant implementation and therefore @extref[fully interoperable](akka:general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.

This **Alpakka Kafka connector** lets you connect [Apache Kafka](https://kafka.apache.org/) to Akka Streams. It was formerly known as **Akka Streams Kafka** and even **Reactive Kafka**.

## Project Info

@@project-info{ projectId="core" }

## Matching Kafka Versions

See all releases in [GitHub releases](https://github.com/akka/alpakka-kafka/releases).

| Kafka client                                                               | Scala Versions   | Akka version    | Alpakka Kafka Connector
|----------------------------------------------------------------------------|------------------|-----------------|-------------------------
| 4.1.0                                                                      | 3.3, 2.13        | 2.10.5+         | [release 8.0.0](https://github.com/akka/alpakka-kafka/releases/tag/v8.0.0)
| 3.9.1                                                                      | 3.3, 2.13        | 2.10.5+         | [release 7.0.4](https://github.com/akka/alpakka-kafka/releases/tag/v7.0.4)
| 3.9.0                                                                      | 3.3, 2.13        | 2.10.5+         | [release 7.0.3](https://github.com/akka/alpakka-kafka/releases/tag/v7.0.3)
| 3.7.1                                                                      | 3.3, 2.13        | 2.10.5+         | [release 7.0.2](https://github.com/akka/alpakka-kafka/releases/tag/v7.0.2)
| 3.7.1                                                                      | 3.3, 2.13        | 2.10.0-2.10.4   | [release 7.0.0](https://github.com/akka/alpakka-kafka/releases/tag/v7.0.0)
| 3.7.0                                                                      | 3.3, 2.13        | 2.9.0+          | [release 6.0.0](https://github.com/akka/alpakka-kafka/releases/tag/v6.0.0)
| 3.5.1                                                                      | 3.3, 2.13        | 2.9.0+          | [release 5.0.0](https://github.com/akka/alpakka-kafka/releases/tag/v5.0.0)
| 3.3.1                                                                      | 2.13, 2.12       | 2.7.0+          | [release 4.0.0](https://github.com/akka/alpakka-kafka/releases/tag/v4.0.0)
| 3.0.1                                                                      | 2.13             | 2.6.18+         | [release 3.0.1](https://github.com/akka/alpakka-kafka/releases/tag/v3.0.0)
| [3.0.0](https://blogsarchive.apache.org/kafka/entry/what-s-new-in-apache6) | 2.13             | 2.6.18+         | [release 3.0.0 RC1](https://github.com/akka/alpakka-kafka/releases/tag/v3.0.0-RC1)
| [2.7.0](https://archive.apache.org/dist/kafka/2.7.0/RELEASE_NOTES.html)    | 2.13, 2.12       | 2.6.14+         | @ref:[release 2.1.0](release-notes/2.1.x.md)
| [2.4.1](https://archive.apache.org/dist/kafka/2.4.1/RELEASE_NOTES.html)    | 2.13, 2.12, 2.11 | 2.5.31+, 2.6.6+ | @ref:[release 2.0.5](release-notes/2.0.x.md)
| [2.4.1](https://archive.apache.org/dist/kafka/2.4.1/RELEASE_NOTES.html)    | 2.13, 2.12, 2.11 | 2.5.30+, 2.6.6+ | @ref:[release 2.0.4](release-notes/2.0.x.md)
| [2.4.1](https://archive.apache.org/dist/kafka/2.4.1/RELEASE_NOTES.html)    | 2.13, 2.12, 2.11 | 2.5.30+, 2.6.3+ | @ref:[release 2.0.3](release-notes/2.0.x.md)
| [2.4.0](https://archive.apache.org/dist/kafka/2.4.0/RELEASE_NOTES.html)    | 2.13, 2.12, 2.11 | 2.5.23+, 2.6.x  | @ref:[release 2.0.0](release-notes/2.0.x.md)
| [2.1.1](https://archive.apache.org/dist/kafka/2.1.1/RELEASE_NOTES.html)    | 2.13, 2.12, 2.11 | 2.5.x           | @ref:[release 1.0.4](release-notes/1.0.x.md#1-0-4)
| [2.1.1](https://archive.apache.org/dist/kafka/2.1.1/RELEASE_NOTES.html)    | 2.12, 2.11       | 2.5.x           | @ref:[release 1.0.1](release-notes/1.0.x.md#1-0-1)
| [2.1.0](https://archive.apache.org/dist/kafka/2.1.0/RELEASE_NOTES.html)    | 2.12, 2.11       | 2.5.x           | @ref:[release 1.0](release-notes/1.0.x.md#1-0)
| 1.1.x                                                                      | 2.12, 2.11       | 2.5.x           | [release 0.20+](https://github.com/akka/alpakka-kafka/releases)
| 1.0.x                                                                      | 2.12, 2.11       | 2.5.x           | [release 0.20+](https://github.com/akka/alpakka-kafka/releases)
| 0.11.x                                                                     | 2.12, 2.11       | 2.5.x           | [release 0.19](https://github.com/akka/alpakka-kafka/milestone/19?closed=1)

@@@ note

As Kafka's client protocol negotiates the version to use with the Kafka broker, you may use a Kafka client version that is different than the Kafka broker's version.

These clients can communicate with brokers that are version 2.1.0 or newer. Older or newer brokers may not support certain features. You will receive an UnsupportedVersionException when invoking an API that is not available on the running broker version.

Check even Confluent's [Versions and Interoperability](https://docs.confluent.io/platform/current/installation/versions-interoperability.html) page for more details. 

-- @extref:[Javadoc for `KafkaConsumer`](kafka:/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)

@@@

## Dependencies

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

Additionally, add the dependencies as below.

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-stream-kafka_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2="$akka.version$"
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

This connector depends on Akka $akka.version$ and note that it is important that all `akka-*` dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems with transient dependencies causing an unlucky mix of versions.

Alpakka Kafka APIs accept a typed @apidoc[akka.actor.typed.ActorSystem] or a classic @apidoc[akka.actor.ActorSystem] because both implement the @apidoc[akka.actor.ClassicActorSystemProvider] @scala[trait]@java[interface].
There are some Alpakka Kafka APIs that only accept classic a @apidoc[akka.actor.ActorRef], such as the @ref[rebalance listener](./consumer-rebalance.md) API, but otherwise there is no difference between running Alpakka Kafka and any other Akka Streams implementation with a typed @apidoc[akka.actor.typed.ActorSystem]. 
For more information on Akka classic and typed interoperability read the @extref[Coexistence](akka:/typed/coexisting.html) page of the Akka Documentation.

The table below shows Alpakka Kafka's direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="core" }

* Akka Streams $akka.version$ @extref[documentation](akka:stream/index.html) and [sources](https://github.com/akka/akka)
* Apache Kafka client $kafka.version$ @extref[documentation](kafka:/documentation#index) and [sources](https://github.com/apache/kafka)


## Scala and Java APIs

Following Akka's conventions there are two separate packages named `akka.kafka.scaladsl` and `akka.kafka.javadsl`
with the API for Scala and Java. These packages contain `Producer` and `Consumer`
classes with factory methods for the various Akka Streams `Flow`, `Sink` and `Source`
that are producing or consuming messages to/from Kafka.


## Examples

A few self-contained examples using Alpakka are available as [Alpakka Samples](https://github.com/akka/alpakka-samples/).

To read and see how others use Alpakka see the [Alpakka documentation's Webinars, Presentations and Articles](https://doc.akka.io/libraries/alpakka/current/other-docs/webinars-presentations-articles.html) listing.


## Contributing

Please feel free to contribute to Alpakka and the Alpakka Kafka connector by reporting issues you identify, or by suggesting changes to the code. Please refer to our [contributing instructions](https://github.com/akka/alpakka-kafka/blob/main/CONTRIBUTING.md) to learn how it can be done.

We want Akka and Alpakka to strive in a welcoming and open atmosphere and expect all contributors to respect our [code of conduct](https://akka.io/conduct).


@@@ index

* [release notes](release-notes/index.md)

@@@

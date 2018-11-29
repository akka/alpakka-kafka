Alpakka Kafka Connector [![scaladex-badge][]][scaladex] [![maven-central-badge][]][maven-central] [![travis-badge][]][travis] [![gitter-badge][]][gitter]
=======================

[scaladex]:            https://index.scala-lang.org/akka/alpakka-kafka
[scaladex-badge]:      https://index.scala-lang.org/akka/alpakka-kafka/latest.svg
[maven-central]:       https://maven-badges.herokuapp.com/maven-central/com.typesafe.akka/akka-stream-kafka_2.12
[maven-central-badge]: https://maven-badges.herokuapp.com/maven-central/com.typesafe.akka/akka-stream-kafka_2.12/badge.svg
[travis]:              https://travis-ci.org/akka/alpakka-kafka
[travis-badge]:        https://travis-ci.org/akka/alpakka-kafka.svg?branch=master
[gitter]:              https://gitter.im/akka/alpakka-kafka
[gitter-badge]:        https://badges.gitter.im/akka/alpakka-kafka.svg


Systems don't come alone. In the modern world of microservices and cloud deployment, new components must interact with legacy systems, making integration an important key to success. Reactive Streams give us a technology-independent tool to let these heterogeneous systems communicate without overwhelming each other.

The Alpakka project is an open source initiative to implement stream-aware, reactive, integration pipelines for Java and Scala. It is built on top of [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html), and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and stream-oriented programming, with built-in support for backpressure. Akka Streams is a [Reactive Streams](https://www.reactive-streams.org/) and JDK 9+ [java.util.concurrent.Flow](https://docs.oracle.com/javase/10/docs/api/java/util/concurrent/Flow.html)-compliant implementation and therefore [fully interoperable](https://doc.akka.io/docs/akka/current/general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.

This repository contains the sources for the **Alpakka Kafka connector**. Which lets you connect [Apache Kafka](https://kafka.apache.org/) to Akka Streams. It was formerly known as **Akka Streams Kafka** and even **Reactive Kafka**.

Akka Stream connectors to other technologies are listed in the [Alpakka repository](https://github.com/akka/alpakka).


Documentation
-------------

- [Alpakka reference](https://developer.lightbend.com/docs/alpakka/current/) documentation 

- **[Alpakka Kafka connector reference](https://doc.akka.io/docs/akka-stream-kafka/current/) documentation**

To keep up with the latest Alpakka releases check out [Alpakka releases](https://github.com/akka/alpakka/releases) and [Alpakka Kafka connector releases](https://github.com/akka/alpakka-kafka/releases).


Community
---------

You can join these groups and chats to discuss and ask Akka and Alpakka related questions:

- Forums: [discuss.lightbend.com](https://discuss.lightbend.com/c/akka/streams-and-alpakka)
- Chat room about **Akka and Alpakka**: [![gitter: akka/akka](https://img.shields.io/badge/gitter%3A-akka%2Fakka-blue.svg?style=flat-square)](https://gitter.im/akka/akka)
- Chat room about **Alpakka Kafka connector**: [![gitter: akka/alpakka-kafka](https://img.shields.io/badge/gitter%3A-alpakka--kafka-blue.svg?style=flat-square)](https://gitter.im/akka/alpakka-kafka)
- Issue tracker: [![github: akka/alpakka-kafka](https://img.shields.io/badge/github%3A-issues-blue.svg?style=flat-square)](https://github.com/akka/alpakka-kafka/issues)

In addition to that, you may enjoy following:

- The [Akka Team Blog](https://akka.io/blog/)
- [@akkateam](https://twitter.com/akkateam) on Twitter
- Questions tagged [#alpakka on StackOverflow](https://stackoverflow.com/questions/tagged/alpakka)
- Questions tagged [**#alpakka-kafka** on StackOverflow](https://stackoverflow.com/questions/tagged/alpakka-kafka)

The Kafka connector was originally created as **Reactive Kafka** by [<img src="https://softwaremill.com/images/header-main-logo.3449d6a3.svg" alt="SoftwareMill logo" height="25">](https://softwaremill.com).


Contributing
------------

[Lightbend](https://www.lightbend.com/) is committed to Alpakka and has an Alpakka team working on it.

Contributions are *very* welcome! The Alpakka team appreciates community contributions by both those new to Alpakka and those more experienced.
Alpakka depends on the community to to keep up with the ever-growing number of technologies with which to integrate. Please step up and share the successful Akka Stream integrations you implement with the Alpakka community.

If you find an issue that you'd like to see fixed, the quickest way to make that happen is to implement the fix and submit a pull request.

Refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file for more details about the workflow, and general hints on how to prepare your pull request.

You can also ask for clarifications or guidance in GitHub issues directly, or in the [akka/dev](https://gitter.im/akka/dev) chat if a more real time communication would be of benefit.



Caveat Emptor
-------------

Alpakka components are not always binary compatible between releases. API changes that are not backward compatible might be introduced as we refine and simplify based on your feedback. A module may be dropped in any release without prior deprecation. 

Support for the Alpakka Kafka connector is available via [Lightbend subscription](https://www.lightbend.com/subscription). 

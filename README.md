Alpakka Kafka [![gh-actions-badge][]][gh-actions]
=============

[gh-actions]:          https://github.com/akka/alpakka-kafka/actions
[gh-actions-badge]:    https://github.com/akka/alpakka-kafka/workflows/CI/badge.svg?branch=main


Systems don't come alone. In the modern world of microservices and cloud deployment, new components must interact with legacy systems, making integration an important key to success. Reactive Streams give us a technology-independent tool to let these heterogeneous systems communicate without overwhelming each other.

The Alpakka project is an open source initiative to implement stream-aware, reactive, integration pipelines for Java and Scala. It is built on top of [Akka Streams](https://doc.akka.io/libraries/akka-core/current/stream/index.html), and has been designed from the ground up to understand streaming natively and provide a DSL for reactive and stream-oriented programming, with built-in support for backpressure. Akka Streams is a [Reactive Streams](https://www.reactive-streams.org/) and JDK 9+ [java.util.concurrent.Flow](https://docs.oracle.com/javase/10/docs/api/java/util/concurrent/Flow.html)-compliant implementation and therefore [fully interoperable](https://doc.akka.io/libraries/akka-core/current/general/stream/stream-design.html#interoperation-with-other-reactive-streams-implementations) with other implementations.

This repository contains the sources for the **Alpakka Kafka connector**. Which lets you connect [Apache Kafka](https://kafka.apache.org/) to Akka Streams. It was formerly known as **Akka Streams Kafka** and even **Reactive Kafka**.

Akka Stream connectors to other technologies are listed in the [Alpakka repository](https://github.com/akka/alpakka).


Documentation
-------------

- [Alpakka reference](https://doc.akka.io/libraries/alpakka/current/) documentation

- **[Alpakka Kafka connector reference](https://doc.akka.io/libraries/alpakka-kafka/current/) documentation**

To keep up with the latest Alpakka releases check out [Alpakka releases](https://github.com/akka/alpakka/releases) and [Alpakka Kafka releases](https://github.com/akka/alpakka-kafka/releases).


Community
---------

You can join these groups and chats to discuss and ask Akka and Alpakka related questions:

- Forums: [discuss.lightbend.com](https://discuss.lightbend.com/c/akka/streams-and-alpakka)
- Issue tracker: [![github: akka/alpakka-kafka](https://img.shields.io/badge/github%3A-issues-blue.svg?style=flat-square)](https://github.com/akka/alpakka-kafka/issues)

In addition to that, you may enjoy following:

- The [Akka Team Blog](https://akka.io/blog/)
- [@akkateam](https://twitter.com/akkateam) on Twitter
- Questions tagged [#alpakka on StackOverflow](https://stackoverflow.com/questions/tagged/alpakka)
- Questions tagged [**#alpakka** on StackOverflow](https://stackoverflow.com/questions/tagged/alpakka)

The Kafka connector was originally created as **Reactive Kafka** by [<img src="https://files.softwaremill.com/logo/logo.svg" alt="SoftwareMill logo" height="25">](https://softwaremill.com).


Contributing
------------

The Akka family of projects is managed by teams at [Lightbend](https://www.lightbend.com/) with help from the community.

Contributions are *very* welcome! Lightbend appreciates community contributions by both those new to Alpakka and those more experienced.

Alpakka depends on the community to keep up with the ever-growing number of technologies with which to integrate. Please step up and share the successful Akka Stream integrations you implement with the Alpakka community.

If you find an issue that you'd like to see fixed, the quickest way to make that happen is to implement the fix and submit a pull request.

Refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file for more details about the workflow, and general hints on how to prepare your pull request.

You can also ask for clarifications or guidance in GitHub issues directly.

Caveat Emptor
-------------

Alpakka components are not always binary compatible between releases. API changes that are not backward compatible might be introduced as we refine and simplify based on your feedback. A module may be dropped in any release without prior deprecation. 

License
-------

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).

Tests and documentation are under a separate license, see the LICENSE file in each documentation and test root directory for details.

# Introduction

Akka Streams Kafka, also known as Reactive Kafka, is an [Akka Streams](http://doc.akka.io/docs/akka/current/scala/stream/index.html) connector for [Apache Kafka](https://kafka.apache.org/).

## scaladsl and javadsl

There are two separate packages named `akka.kafka.scaladsl` and `akka.kafka.javadsl` 
with the API for Scala and Java. These packages contain `Producer` and `Consumer`
classes with factory methods for the various Akka Streams `Flow`, `Sink` and `Source`
that are producing or consuming messages to/from Kafka.

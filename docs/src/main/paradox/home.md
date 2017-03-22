# Akka Streams Kafka

Akka Streams Kafka, also known as Reactive Kafka, is an Akka Streams connector for Apache Kafka.

The examples in this documentation use

* Akka Streams Kakfka $version$ ([Github](https://github.com/akka/reactive-kafka))
* Scala $scalaBinaryVersion$
* @extref[Akka Streams](akka-docs:scala/stream/index.html) $akkaVersion$ ([Github](https://github.com/akka/akka))
* @extref[Apache Kafka](kafka-docs:index.html) $kafkaVersion$ ([Apache Git](https://git-wip-us.apache.org/repos/asf?p=kafka.git))


## Dependencies

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.typesafe.akka</groupId>
      <artifactId>akka-stream-kafka_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@ vars
    ```
    dependencies {
      compile group: "com.typesafe.akka", name: "akka-stream-kafka_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@



## scaladsl and javadsl

There are two separate packages named `akka.kafka.scaladsl` and `akka.kafka.javadsl` 
with the API for Scala and Java. These packages contain `Producer` and `Consumer`
classes with factory methods for the various Akka Streams `Flow`, `Sink` and `Source`
that are producing or consuming messages to/from Kafka.


@@ toc { .main depth=3 }

@@@ index

* [Producer](producer.md)
* [Consumer](consumer.md)

@@@

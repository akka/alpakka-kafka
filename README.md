Reactive Streams for Kafka
====
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.softwaremill/reactive-kafka_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.softwaremill/reactive-kafka_2.11)  
If you have questions or are working on a pull request or just curious, please feel welcome to join the chat room: [![Join the chat at https://gitter.im/softwaremill/reactive-kafka](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/softwaremill/reactive-kafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


[Reactive Streams](http://www.reactive-streams.org) wrapper for [Apache Kafka](https://kafka.apache.org/).  

Initiated by [SoftwareMill](https://softwaremill.com)

Supports Kafka 0.8.2.1

Available at Maven Central for Scala 2.10 and 2.11:

````scala
libraryDependencies += "com.softwaremill" %% "reactive-kafka" % "0.6.0"
````

Testing
----
Tests require Apache Kafka and Zookeeper to be available on localhost:9092 and localhost:2181

Example usage
----

```Scala
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.ReactiveKafka
import com.softwaremill.react.kafka.ProducerProps
import com.softwaremill.react.kafka.ConsumerProps
import kafka.serializer.{StringDecoder, StringEncoder}

implicit val actorSystem = ActorSystem("ReactiveKafka")
implicit val materializer = ActorMaterializer()

val kafka = new ReactiveKafka()
val publisher = kafka.consume(ConsumerProps("localhost:9092", "localhost:2181", "lowercaseStrings", "groupName", new StringDecoder()))
val subscriber = kafka.publish(ProducerProps("localhost:9092", "uppercaseSettings", "groupName", new StringEncoder()))

Source(publisher).map(_.toUpperCase).to(Sink(subscriber)).run()
```

Controlling consumer start offset
----

By default a new consumer will start reading from the beginning of a topic. If you want to start reading from the end,
you can use alternative way to create a consumer:
```Scala
  val publisher = kafka.consumeFromEnd(topic, groupId, new StringDecoder())
````

Tuning
----

KafkaActorSubscriber and KafkaActorPublisher have their own thread pools, configured in `reference.conf`.
You can tune them by overriding `kafka-publisher-dispatcher.thread-pool-executor` and
`kafka-subscriber-dispatcher.thread-pool-executor` in your `application.conf` file.

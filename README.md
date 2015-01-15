Reactive Streams for Kafka
====

[Reactive Streams](http://www.reactive-streams.org) wrapper for [Apache Kafka](https://kafka.apache.org/).

Initiated by [SoftwareMill](https://softwaremill.com)

Supports Kafka 0.8.2-beta

Available at Maven Central for Scala 2.10 and 2.11:

    libraryDependencies += "com.softwaremill" %% "reactive-kafka" % "0.2.0"

Testing
----
Tests require Apache Kafka and Zookeeper to be available on localhost:9092 and localhost:2181

Example usage
----

```Scala
import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.ReactiveKafka

implicit val materializer = FlowMaterializer()
implicit  val actorSystem = ActorSystem("ReactiveKafka")

val kafka = new ReactiveKafka(host = "localhost:9092", zooKeeperHost = "localhost:2181")
val publisher = kafka.consume("lowercaseStrings", "groupName")
val subscriber = kafka.publish("uppercaseStrings", "groupName")


Source(publisher).map(_.toUpperCase).to(Sink(subscriber)).run()
```

Tuning
----

KafkaActorSubscriber and KafkaActorPublisher have their own thread pools, configured in `reference.conf`.
You can tune them by overriding `kafka-publisher-dispatcher.thread-pool-executor` and
`kafka-subscriber-dispatcher.thread-pool-executor` in your `application.conf` file.

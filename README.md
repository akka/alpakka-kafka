Reactive Streams for Kafka
====
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.softwaremill/reactive-kafka_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.softwaremill/reactive-kafka_2.11)  
If you have questions or are working on a pull request or just curious, please feel welcome to join the chat room: [![Join the chat at https://gitter.im/softwaremill/reactive-kafka](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/softwaremill/reactive-kafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


[Reactive Streams](http://www.reactive-streams.org) wrapper for [Apache Kafka](https://kafka.apache.org/).  

Initiated by [SoftwareMill](https://softwaremill.com)

Supports Kafka 0.8.2.1

Available at Maven Central for Scala 2.10 and 2.11:

````scala
libraryDependencies += "com.softwaremill" %% "reactive-kafka" % "0.7.2"
````

Testing
----
Tests require Apache Kafka and Zookeeper to be available on localhost:9092 and localhost:2181

Example usage
----

#### Scala
```Scala
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import kafka.serializer.{StringDecoder, StringEncoder}
import com.softwaremill.react.kafka.{ReactiveKafka, ProducerProperties, ConsumerProperties}

implicit val actorSystem = ActorSystem("ReactiveKafka")
implicit val materializer = ActorMaterializer()

val kafka = new ReactiveKafka()
val publisher = kafka.consume(ConsumerProperties(
  brokerList = "localhost:9092",
  zooKeeperHost = "localhost:2181",
  topic = "lowercaseStrings",
  groupId = "groupName",
  decoder = new StringDecoder()
))
val subscriber = kafka.publish(ProducerProperties(
  brokerList = "localhost:9092",
  topic = "uppercaseStrings",
  clientId = "groupName",
  encoder = new StringEncoder()
))

Source(publisher).map(_.toUpperCase).to(Sink(subscriber)).run()
```

#### Java
```Java
String zooKeeperHost = "localhost:2181";
String brokerList = "localhost:9092";

ReactiveKafka kafka = new ReactiveKafka();
ActorSystem system = ActorSystem.create("ReactiveKafka");
ActorMaterializer materializer = ActorMaterializer.create(system);

ConsumerProperties<Byte[], String> cp =
   new PropertiesBuilder.Consumer(brokerList, zooKeeperHost, "topic", "groupId", new StringDecoder(null))
      .build();

Publisher<KeyValueKafkaMessage<Byte[], String>> publisher = kafka.consume(cp, system);

ProducerProperties<String> pp = new PropertiesBuilder.Producer(brokerList, zooKeeperHost, "topic", new StringEncoder(null))
        .build();

Subscriber<String> subscriber = kafka.publish(pp, system);

Source.from(publisher).map(KeyValueKafkaMessage::msg).to(Sink.create(subscriber)).run(materializer);
```

Passing configuration properties to Kafka
----
In order to set your own custom Kafka parameters, you can construct `ConsumerProperties` and `ProducerProperties` using
some of their provided methods in a builder-pattern-style DSL, for example:  
```Scala
val consumerProperties = ConsumerProperties(
  "localhost:9092",
  "localhost:2181",
  "topic",
  "groupId",
  new StringDecoder()
)
  .consumerTimeoutMs(timeInMs = 100)
  .kafkaOffsetsStorage(dualCommit = true)
  .setProperty("some.kafka.property", "value") 
```
The `ProducerProperties` class offers a similar API.

Controlling consumer start offset
----

By default a new consumer will start reading from the beginning of a topic. If you want to start reading from the end,
you can specify this on your `ConsumerProperties`:
```Scala
  val consumerProperties = ConsumerProperties(...).readFromEndOfStream()
````

Working with actors
----
Since we are based upon akka-stream, the best way to handle errors is to leverage Akka's error handling and lifecycle 
management capabilities. Producers and consumers are in fact actors. 

#### Obtaining actor references
`ReactiveKafka` comes with a few methods allowing working on the actor level. You can let it create Props to let your 
own supervisor create these actor as children, or you can  directly create actors at the top level of supervision. 
Here are a some examples:  

```Scala
import akka.actor.{Props, ActorRef, Actor, ActorSystem}
import akka.stream.ActorMaterializer
import kafka.serializer.{StringEncoder, StringDecoder}
import com.softwaremill.react.kafka.{ReactiveKafka, ProducerProperties, ConsumerProperties}

// inside an Actor:
implicit val materializer = ActorMaterializer()

val kafka = new ReactiveKafka()
// publisher
val publisherProperties = ConsumerProperties(
  brokerList = "localhost:9092",
  zooKeeperHost = "localhost:2181",
  topic = "lowercaseStrings",
  groupId = "groupName",
  decoder = new StringDecoder()
)
val publisherActorProps: Props = kafka.consumerActorProps(publisherProperties)
val publisherActor: ActorRef = context.actorOf(publisherActorProps)
// or:
val topLevelPublisherActor: ActorRef = kafka.consumerActor(publisherProperties)

// subscriber
val subscriberProperties = ProducerProperties(
  brokerList = "localhost:9092",
  topic = "uppercaseStrings",
  encoder = new StringEncoder()
)
val subscriberActorProps: Props = kafka.producerActorProps(subscriberProperties)
val subscriberActor: ActorRef = context.actorOf(subscriberActorProps)
// or:
val topLevelSubscriberActor: ActorRef = kafka.producerActor(subscriberProperties)
```

#### Handling errors
When a publisher (consumer) fails to load more elements from Kafka, it calls `onError()` on all of its subscribers. 
The error will be handled depending on subscriber implementation.  
When a subscriber (producer) fails to get more elements from upstream due to an error, it is no longer usable. 
It will throw an exception and close all underlying resource (effectively: the Kafka connection). 
If there's a problem with putting elements into Kafka, only an exception will be thrown. 
This mechanism allows custom handling and keeping the subscriber working.  
Example of custom error handling for a Kafka Sink:
```Scala
val sourceDecider: Supervision.Decider = {
  case _ => Supervision.Resume // Your error handling
}

Source(publisher)
  .map(_.message().toUpperCase)
  .to(Sink(subscriber).withAttributes(ActorAttributes.supervisionStrategy(sourceDecider)))
  .run()
```
#### Cleaning up
If you want to manually stop a publisher or a subscriber, you have to send an appropriate message to the underlying
actor. `KafkaActorPublisher` must receive a `ActorPublisherMessage.Cancel`, where `KafkaActorSubscriber` must receive
a `ActorSubscriberMessage.OnComplete`.

#### Manual Commit (version 0.8 and above)
Current version supports manual commit only when committing to Zookeeper. Support for Committing to Kafka-based storage is
in progress.  
In order to be able to achieve "at-least-once" delivery, you can use following API to obtain an additional Sink, when
you can stream back messages that you processed. An underlying actor will periodically flush offsets of these messages as committed.  
Example:  

```Scala
    import scala.concurrent.duration._
    import akka.actor.ActorSystem
    import akka.stream.ActorMaterializer
    import akka.stream.scaladsl.Source
    import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}

    implicit val actorSystem = ActorSystem("ReactiveKafka")
    implicit val materializer = ActorMaterializer()

    val kafka = new ReactiveKafka()
    val consumerProperties = ConsumerProperties(
      brokerList = "localhost:9092",
      zooKeeperHost = "localhost:2181",
      topic = "lowercaseStrings",
      groupId = "groupName",
      decoder = new StringDecoder())
    .commitInterval(5 seconds) // flush interval
    
    val consumerWithOffsetSink = kafka.consumeWithOffsetSink(consumerProperties)
    Source(consumerWithOffsetSink.publisher)
    .map(processMessage(_)) // your message processing
    .to(consumerWithOffsetSink.offsetCommitSink) // stream back for commit
    .run()
```

Tuning
----

KafkaActorSubscriber and KafkaActorPublisher have their own thread pools, configured in `reference.conf`.
You can tune them by overriding `kafka-publisher-dispatcher.thread-pool-executor` and
`kafka-subscriber-dispatcher.thread-pool-executor` in your `application.conf` file.  
Alternatively you can provide your own dispatcher name. It can be passed to appropriate variants of factory methods in
`ReactiveKafka`: `publish()`, `producerActor()`, `producerActorProps()` or `consume()`, `consumerActor()`, `consumerActorProps()`.

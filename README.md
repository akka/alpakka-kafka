Reactive Streams for Kafka
====
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.typesafe.akka/akka-stream-kafka_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.typesafe.akka/akka-stream-kafka_2.11)
If you have questions or are working on a pull request or just curious, please feel welcome to join the chat room: [![Join the chat at https://gitter.im/akka/reactive-kafka](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akka/reactive-kafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


[Akka Streams](http://doc.akka.io/docs/akka/2.4.3/scala/stream/index.html) connector for [Apache Kafka](https://kafka.apache.org/).

Created and maintained by 
[<img src="https://softwaremill.com/img/logo2x.png" alt="SoftwareMill logo" height="25">](https://softwaremill.com)

## New API: 0.11-M2

Supports Kafka 0.9.0.x

This version of `akka-stream-kafka` depends on Akka 2.4.3 and Scala 2.11.8. 

Available at Maven Central for Scala 2.11:

````scala
libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "0.11-M2"
````

Example usage
----

#### Scala

Producer Settings:

```Scala
import akka.kafka.scaladsl._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.ByteArraySerializer

  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers("localhost:9092")
```

Produce messages:

```Scala
  Source(1 to 10000)
    .map(_.toString)
    .map(elem => new ProducerRecord[Array[Byte], String]("topic1", elem))
    .to(Producer.plainSink(producerSettings))
```

Produce messages in a flow:

```Scala
  Source(1 to 10000)
    .map(elem => Producer.Message(new ProducerRecord[Array[Byte], String]("topic1", elem.toString), elem))
    .via(Producer.flow(producerSettings))
    .map { result =>
      val record = result.message.record
      println(s"${record.topic}/${record.partition} ${result.offset}: ${record.value} (${result.message.passThrough}")
      result
    }
```

Consumer Settings:

```Scala
import akka.kafka.scaladsl._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig

val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer, 
    Set("topic1"))
  .withBootstrapServers("localhost:9092")
  .withGroupId("group1")
  .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
```

Consume messages and store a representation, including offset, in DB:

```Scala
  db.loadOffset().foreach { fromOffset =>
    val settings = consumerSettings
      .withFromOffset(new TopicPartition("topic1", 1), fromOffset)
    Consumer.plainSource(settings)
      .mapAsync(1)(db.save)
  }
```

Consume messages at-most-once:

```Scala
  Consumer.atMostOnceSource(consumerSettings.withClientId("client1"))
    .mapAsync(1) { record =>
      rocket.launch(record.value)
    }
```

Consume messages at-least-once:

```Scala
  Consumer.committableSource(consumerSettings.withClientId("client1"))
    .mapAsync(1) { msg =>
      db.update(msg.value).flatMap(_ => msg.committableOffset.commit())
    }
```

Connect a Consumer to Producer:

```Scala
  Consumer.committableSource(consumerSettings.withClientId("client1"))
    .map(msg =>
      Producer.Message(new ProducerRecord[Array[Byte], String]("topic2", msg.value), msg.committableOffset))
    .to(Producer.commitableSink(producerSettings))
```

Consume messages at-least-once, and commit in batches:

```Scala
  Consumer.committableSource(consumerSettings.withClientId("client1"))
    .mapAsync(1) { msg =>
      db.update(msg.value).map(_ => msg.committableOffset)
    }
    .batch(max = 10, first => CommittableOffsetBatch.empty.updated(first)) { (batch, elem) =>
      batch.updated(elem)
    }
    .mapAsync(1)(_.commit())
```

Additional examples are available in 
[ConsumerExamples.scala](https://github.com/akka/reactive-kafka/blob/v0.11-M2/core/src/test/scala/examples/ConsumerExample.scala)


#### Java

Java API is not included in 0.11-M2. It will be added soon.


Configuration
----

The configuration properties are defined in [reference.conf](https://github.com/akka/reactive-kafka/blob/v0.11-M2/core/src/main/resources/reference.conf)

Testing
----
Tests require Apache Kafka and Zookeeper to be available on `localhost:9092` and `localhost:2181`. Note that `auto.create.topics.enable` should be `true`.

## Old API: 0.10.0

Supports Kafka 0.9.0.x
**For Kafka 0.8** see [this branch](https://github.com/softwaremill/reactive-kafka/tree/0.8).

Available at Maven Central for Scala 2.11:

````scala
libraryDependencies += "com.softwaremill.reactivekafka" %% "reactive-kafka-core" % "0.10.0"
````

Example usage
----

#### Scala
```Scala
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.KafkaMessages._
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import com.softwaremill.react.kafka.{ProducerMessage, ConsumerProperties, ProducerProperties, ReactiveKafka}
import org.reactivestreams.{ Publisher, Subscriber }

implicit val actorSystem = ActorSystem("ReactiveKafka")
implicit val materializer = ActorMaterializer()

val kafka = new ReactiveKafka()
val publisher: Publisher[StringConsumerRecord] = kafka.consume(ConsumerProperties(
 bootstrapServers = "localhost:9092",
 topic = "lowercaseStrings",
 groupId = "groupName",
 valueDeserializer = new StringDeserializer()
))
val subscriber: Subscriber[StringProducerMessage] = kafka.publish(ProducerProperties(
  bootstrapServers = "localhost:9092",
  topic = "uppercaseStrings",
  valueSerializer = new StringSerializer()
))

Source.fromPublisher(publisher).map(m => ProducerMessage(m.value().toUpperCase))
  .to(Sink.fromSubscriber(subscriber)).run()
```

#### Java
```java
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public void run() {
String brokerList = "localhost:9092";

ReactiveKafka kafka = new ReactiveKafka();
ActorSystem system = ActorSystem.create("ReactiveKafka");
ActorMaterializer materializer = ActorMaterializer.create(system);

StringDeserializer deserializer = new StringDeserializer();
ConsumerProperties<String> cp =
   new PropertiesBuilder.Consumer(brokerList, "topic", "groupId", deserializer)
      .build();

Publisher<ConsumerRecord<String, String>> publisher = kafka.consume(cp, system);

StringSerializer serializer = new StringSerializer();
ProducerProperties<String, String> pp = new PropertiesBuilder.Producer(
   brokerList,
   "topic",
   serializer,
   serializer).build();

Subscriber<ProducerMessage<String, String>> subscriber = kafka.publish(pp, system);
Source.fromPublisher(publisher).map(this::toProdMessage)
  .to(Sink.fromSubscriber(subscriber)).run(materializer);
}

private ProducerMessage<String, String> toProdMessage(ConsumerRecord<String, String> record) {
  return KeyValueProducerMessage.apply(record.key(), record.value());
}
```

Passing configuration properties to Kafka
----
In order to set your own custom Kafka parameters, you can construct `ConsumerProperties` and `ProducerProperties` using
some of their provided methods in a builder-pattern-style DSL, for example:  
```Scala
import org.apache.kafka.common.serialization.StringDeserializer
import com.softwaremill.react.kafka.ConsumerProperties

val consumerProperties = ConsumerProperties(
  "localhost:2181",
  "topic",
  "groupId",
  new StringDeserializer()
)
  .readFromEndOfStream()
  .consumerTimeoutMs(300)
  .commitInterval(2 seconds)
  .setProperty("some.kafka.property", "value") 
```
The `ProducerProperties` class offers a similar API.

Controlling consumer start offset
----

By default a new consumer will start reading from the beginning of a topic, fetching all uncommitted messages. 
If you want to start reading from the end, you can specify this on your `ConsumerProperties`:
```Scala
  val consumerProperties = ConsumerProperties(...).readFromEndOfStream()
````

Working with actors
----
Since we are based upon akka-stream, the best way to handle errors is to leverage Akka's error handling and lifecycle 
management capabilities. Producers and consumers are in fact actors. 

#### Obtaining actor references
`ReactiveKafka` comes with a few methods allowing working on the actor level. You can let it create `Props` to let your 
own supervisor create these actors as children, or you can  directly create actors at the top level of supervision. 
Here are some examples:  

```Scala
import akka.actor.{Props, ActorRef, Actor, ActorSystem}
import akka.stream.ActorMaterializer
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import com.softwaremill.react.kafka.{ReactiveKafka, ProducerProperties, ConsumerProperties}

// inside an Actor:
implicit val materializer = ActorMaterializer()

val kafka = new ReactiveKafka()
// consumer
val consumerProperties = ConsumerProperties(
  bootstrapServers = "localhost:9092",
  topic = "lowercaseStrings",
  groupId = "groupName",
  valueDeserializer = new StringDeserializer()
)
val consumerActorProps: Props = kafka.consumerActorProps(consumerProperties)
val publisherActor: ActorRef = context.actorOf(consumerActorProps)
// or:
val topLevelPublisherActor: ActorRef = kafka.consumerActor(consumerActorProps)

// subscriber
val producerProperties = ProducerProperties(
  bootstrapServers = "localhost:9092",
  topic = "uppercaseStrings",
  new StringSerializer()
)
val producerActorProps: Props = kafka.producerActorProps(producerProperties)
val subscriberActor: ActorRef = context.actorOf(producerActorProps)
// or:
val topLevelSubscriberActor: ActorRef = kafka.producerActor(producerProperties)
```

#### Handling errors
When a consumer or a producer fails to read/write from Kafka, the error is unrecoverable and requires that
the connection be terminated. This will be performed automatically and the `KafkaActorSubscriber` / `KafkaActorPublisher`
which failed will be stopped. You can use `DeathWatch` to detect such failures in order to restart your stream.
Additionally, when a producer fails, it will signal `onError()` to stop the rest of stream.

Example of monitoring routine:
```Scala
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.softwaremill.react.kafka.KafkaMessages._
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, ReactiveKafka}

class Handler extends Actor {
  implicit val materializer = ActorMaterializer()

  def createSupervisedSubscriberActor() = {
    val kafka = new ReactiveKafka()

    // subscriber
    val subscriberProperties = ProducerProperties(
      bootstrapServers = "localhost:9092",
      topic = "uppercaseStrings",
      valueSerializer = new StringSerializer()
    )
    val subscriberActorProps: Props = kafka.producerActorProps(subscriberProperties)
    val subscriberActor = context.actorOf(subscriberActorProps)
    context.watch(subscriberActor)
  }

  override def receive: Receive = {
    case Terminated(actorRef) => // your custom handling
  }
  
  // Rest of the Actor's body
}
```
#### Cleaning up
If you want to manually stop a publisher or a subscriber, you have to send an appropriate message to the underlying
actor. `KafkaActorPublisher` must receive a `KafkaActorPublisher.Stop`, whereas `KafkaActorSubscriber` must receive a `ActorSubscriberMessage.OnComplete`.
If you're using a `PublisherWithCommitSink` returned from `ReactiveKafka.consumeWithOffsetSink()`, you must call its 
`cancel()` method in order to gracefully close all underlying resources.

#### Manual Commit (version 0.8 and above)
In order to be able to achieve "at-least-once" delivery, you can use following API to obtain an additional Sink, where
you can stream back messages that you processed. An underlying actor will periodically flush offsets of these messages as committed. 
Example:  

```Scala
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.softwaremill.react.kafka.KafkaMessages._
import akka.stream.scaladsl.Source
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}

implicit val actorSystem = ActorSystem("ReactiveKafka")
implicit val materializer = ActorMaterializer()

val kafka = new ReactiveKafka()
val consumerProperties = ConsumerProperties(
  bootstrapServers = "localhost:9092",
  topic = "lowercaseStrings",
  groupId = "groupName",
  valueDeserializer = new StringDeserializer())
.commitInterval(5 seconds) // flush interval
    
val consumerWithOffsetSink = kafka.consumeWithOffsetSink(consumerProperties)
Source.fromPublisher(consumerWithOffsetSink.publisher)
  .map(processMessage(_)) // your message processing
  .to(consumerWithOffsetSink.offsetCommitSink) // stream back for commit
  .run()
```
Tuning
----

`KafkaActorSubscriber` and `KafkaActorPublisher` have their own thread pools, configured in `reference.conf`.
You can tune them by overriding `kafka-publisher-dispatcher.thread-pool-executor` and
`kafka-subscriber-dispatcher.thread-pool-executor` in your `application.conf` file.  
Alternatively, you can provide your own dispatcher name. It can be passed to appropriate variants of factory methods in
`ReactiveKafka`: `publish()`, `producerActor()`, `producerActorProps()` or `consume()`, `consumerActor()`, `consumerActorProps()`.

Testing
----
Tests require Apache Kafka and Zookeeper to be available on `localhost:9092` and `localhost:2181`. Note that `auto.create.topics.enable` should be `true`.

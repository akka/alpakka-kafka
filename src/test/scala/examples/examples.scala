package examples

import com.softwaremill.react.kafka.ConsumerProperties
import com.softwaremill.react.kafka.KafkaMessage.StringKafkaMessage
import kafka.serializer.{StringDecoder, StringEncoder}
import org.reactivestreams.{Publisher, Subscriber}

/**
 * Code samples for the documentation.
 */
object examples {

  def simple(): Unit = {

    import akka.actor.ActorSystem
    import akka.stream.ActorMaterializer
    import akka.stream.scaladsl.{Sink, Source}
    import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, ReactiveKafka}

    implicit val actorSystem = ActorSystem("ReactiveKafka")
    implicit val materializer = ActorMaterializer()

    val kafka = new ReactiveKafka()
    val publisher: Publisher[StringKafkaMessage] = kafka.consume(ConsumerProperties(
      brokerList = "localhost:9092",
      zooKeeperHost = "localhost:2181",
      topic = "lowercaseStrings",
      groupId = "groupName",
      decoder = new StringDecoder()
    ))
    val subscriber: Subscriber[String] = kafka.publish(ProducerProperties(
      brokerList = "localhost:9092",
      topic = "uppercaseStrings",
      clientId = "groupName",
      encoder = new StringEncoder()
    ))

    Source(publisher).map(_.msg.toUpperCase).to(Sink(subscriber)).run()
  }

  def handling(): Unit = {
    import akka.actor.{Actor, ActorRef, ActorSystem, Props}
    import akka.stream.ActorMaterializer
    import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, ReactiveKafka}

    class Handler extends Actor {
      implicit val actorSystem = ActorSystem("ReactiveKafka")
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
        clientId = "groupName",
        encoder = new StringEncoder()
      )
      val subscriberActorProps: Props = kafka.producerActorProps(subscriberProperties)
      val subscriberActor: ActorRef = context.actorOf(subscriberActorProps)
      // or:
      val topLevelSubscriberActor: ActorRef = kafka.producerActor(subscriberProperties)

      override def receive: Receive = {
        case _ =>
      }
    }
  }

  def consumerProperties() = {
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
  }
}

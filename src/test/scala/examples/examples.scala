package examples

import akka.actor.Actor.Receive
import akka.actor.{Props, ActorRef, Actor, ActorSystem}
import akka.stream.ActorMaterializer
import akka.stream.actor.WatermarkRequestStrategy
import com.softwaremill.react.kafka.{ConsumerProps, ProducerProps, ReactiveKafka}
import kafka.serializer.{StringDecoder, StringEncoder}

/**
 * Code samples for the documentation.
 */
object examples {

  def simple(): Unit = {

    import com.softwaremill.react.kafka.ReactiveKafka
    import com.softwaremill.react.kafka.ProducerProps
    import com.softwaremill.react.kafka.ConsumerProps
    import akka.actor.ActorSystem
    import akka.stream.ActorMaterializer
    import akka.stream.scaladsl.{Sink, Source}

    implicit val actorSystem = ActorSystem("ReactiveKafka")
    implicit val materializer = ActorMaterializer()

    val kafka = new ReactiveKafka()
    val publisher = kafka.consume(ConsumerProps("localhost:9092", "localhost:2181", "lowercaseStrings", "groupName", new StringDecoder()))
    val subscriber = kafka.publish(ProducerProps("localhost:9092", "uppercaseSettings", "groupName", new StringEncoder()))

    Source(publisher).map(_.toUpperCase).to(Sink(subscriber)).run()
  }

  def handling(): Unit = {
    class Handler extends Actor {
      implicit val actorSystem = ActorSystem("ReactiveKafka")
      implicit val materializer = ActorMaterializer()

      val kafka = new ReactiveKafka()
      // publisher
      val publisherProps = ConsumerProps("localhost:9092", "localhost:2181", "lowercaseStrings", "groupName", new StringDecoder())
      val publisherActorProps: Props = kafka.consumerActorProps(publisherProps)
      val publisherActor: ActorRef = context.actorOf(publisherActorProps)
      // or:
      val topLevelPublisherActor: ActorRef = kafka.consumerActor(publisherProps)

      // subscriber
      val subscriberProps = ProducerProps("localhost:9092", "uppercaseSettings", "groupName", new StringEncoder())
      val subscriberActorProps: Props = kafka.producerActorProps(subscriberProps)
      val subscriberActor: ActorRef = context.actorOf(subscriberActorProps)
      // or:
      val topLevelSubscriberActor: ActorRef = kafka.producerActor(subscriberProps)

      override def receive: Receive = {
        case _ =>
      }
    }
  }
}

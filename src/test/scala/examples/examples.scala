package examples

import kafka.serializer.{StringEncoder, StringDecoder}

/**
 * Code samples for the documentation.
 */
object examples {

  def simple(): Unit = {

    import com.softwaremill.react.kafka.ReactiveKafka
    import com.softwaremill.react.kafka.{ProducerProperties, ConsumerProperties}
    import akka.actor.ActorSystem
    import akka.stream.ActorMaterializer
    import akka.stream.scaladsl.{Sink, Source}

    implicit val actorSystem = ActorSystem("ReactiveKafka")
    implicit val materializer = ActorMaterializer()

    val kafka = new ReactiveKafka()
    val publisher = kafka.consume(ConsumerProperties("localhost:9092", "localhost:2181", "lowercaseStrings", "groupName", new StringDecoder()))
    val subscriber = kafka.publish(ProducerProperties("localhost:9092", "uppercaseSettings", "groupName", new StringEncoder()))

    Source(publisher).map(_.toUpperCase).to(Sink(subscriber)).run()
  }

  def handling(): Unit = {
    import akka.actor.{Props, ActorRef, Actor, ActorSystem}
    import com.softwaremill.react.kafka.ReactiveKafka
    import com.softwaremill.react.kafka.{ProducerProperties, ConsumerProperties}
    import akka.stream.ActorMaterializer

    class Handler extends Actor {
      implicit val actorSystem = ActorSystem("ReactiveKafka")
      implicit val materializer = ActorMaterializer()

      val kafka = new ReactiveKafka()
      // publisher
      val publisherProps = ConsumerProperties("localhost:9092", "localhost:2181", "lowercaseStrings", "groupName", new StringDecoder())
      val publisherActorProps: Props = kafka.consumerActorProps(publisherProps)
      val publisherActor: ActorRef = context.actorOf(publisherActorProps)
      // or:
      val topLevelPublisherActor: ActorRef = kafka.consumerActor(publisherProps)

      // subscriber
      val subscriberProps = ProducerProperties("localhost:9092", "uppercaseSettings", "groupName", new StringEncoder())
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

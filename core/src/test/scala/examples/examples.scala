package examples

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Terminated, OneForOneStrategy, SupervisorStrategy}
import com.softwaremill.react.kafka.{ProducerMessage, ConsumerProperties}
import com.softwaremill.react.kafka.KafkaMessages._
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import org.reactivestreams.{Publisher, Subscriber}
import scala.language.postfixOps

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
  }

  def handling(): Unit = {
    import akka.actor.{Actor, ActorRef, ActorSystem, Props}
    import akka.stream.ActorMaterializer
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
    }
  }

  def consumerProperties() = {
    val consumerProperties = ConsumerProperties(
      bootstrapServers = "localhost:9092",
      "topic",
      "groupId",
      new StringDeserializer()
    )
      .consumerTimeoutMs(timeInMs = 100)
      .setProperty("some.kafka.property", "value")
  }

  def processMessage[T](msg: T) = {
    msg
  }

  def manualCommit() = {
    import scala.concurrent.duration._
    import akka.actor.ActorSystem
    import akka.stream.ActorMaterializer
    import akka.stream.scaladsl.Source
    import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}

    implicit val actorSystem = ActorSystem("ReactiveKafka")
    implicit val materializer = ActorMaterializer()

    val kafka = new ReactiveKafka()
    val consumerProperties = ConsumerProperties(
      bootstrapServers = "localhost:9092",
      topic = "lowercaseStrings",
      groupId = "groupName",
      valueDeserializer = new StringDeserializer()
    )
      .commitInterval(5 seconds) // flush interval

    val consumerWithOffsetSink = kafka.consumeWithOffsetSink(consumerProperties)
    Source.fromPublisher(consumerWithOffsetSink.publisher)
      .map(processMessage(_)) // your message processing
      .to(consumerWithOffsetSink.offsetCommitSink) // stream back for commit
      .run()
  }
}

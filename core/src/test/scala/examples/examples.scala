package examples

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{OneForOneStrategy, SupervisorStrategy}
import akka.stream.{ActorAttributes, Supervision}
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.{ProducerMessage, ConsumerProperties}
import com.softwaremill.react.kafka.KafkaMessages._
import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.kafka.common.serialization.{StringSerializer, ByteArrayDeserializer, StringDeserializer}
import org.reactivestreams.{Publisher, Subscriber}
import scala.language.postfixOps
import scala.concurrent.duration._
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
      keyDeserializer = new StringDeserializer(),
      valueDeserializer = new StringDeserializer()
    ))
    val subscriber: Subscriber[StringProducerMessage] = kafka.publish(ProducerProperties(
      bootstrapServers = "localhost:9092",
      topic = "uppercaseStrings",
      keySerializer = new StringSerializer(),
      valueSerializer = new StringSerializer()
    ))

    Source(publisher).map(m => ProducerMessage(m.key(), m.value().toUpperCase)).to(Sink(subscriber)).run()
  }

  def handling(): Unit = {
    import akka.actor.{Actor, ActorRef, ActorSystem, Props}
    import akka.stream.ActorMaterializer
    import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, ReactiveKafka}

    class Handler extends Actor {
      implicit val materializer = ActorMaterializer()

      override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
        case exception => Resume // Your custom error handling
      }

      def createSupervisedSubscriberActor() = {
        val kafka = new ReactiveKafka()

        // subscriber
        val subscriberProperties = ProducerProperties(
          bootstrapServers = "localhost:9092",
          topic = "uppercaseStrings",
          keySerializer = new StringSerializer(),
          valueSerializer = new StringSerializer()
        )
        val subscriberActorProps: Props = kafka.producerActorProps(subscriberProperties)
        context.actorOf(subscriberActorProps)
      }

      override def receive: Receive = {
        case _ =>
      }
    }
  }

  def consumerProperties() = {
    val consumerProperties = ConsumerProperties(
      bootstrapServers = "localhost:9092",
      "topic",
      "groupId",
      new ByteArrayDeserializer(),
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
      keyDeserializer = new ByteArrayDeserializer(),
      valueDeserializer = new StringDeserializer()
    )
      .commitInterval(5 seconds) // flush interval

    val consumerWithOffsetSink = kafka.consumeWithOffsetSink(consumerProperties)
    Source(consumerWithOffsetSink.publisher)
      .map(processMessage(_)) // your message processing
      .to(consumerWithOffsetSink.offsetCommitSink) // stream back for commit
      .run()
  }
}

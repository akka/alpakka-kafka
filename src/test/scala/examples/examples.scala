package examples

import akka.stream.{ActorAttributes, Supervision}
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.ConsumerProperties
import com.softwaremill.react.kafka.KafkaMessages.StringKafkaMessage
import kafka.serializer.{StringDecoder, StringEncoder}
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
      encoder = new StringEncoder()
    ))

    Source(publisher).map(_.message().toUpperCase).to(Sink(subscriber)).run()
  }

  def errorHandling(): Unit = {
    import akka.actor.{Actor, ActorRef, ActorSystem, Props}
    import akka.stream.ActorMaterializer
    import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, ReactiveKafka}

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
      encoder = new StringEncoder()
    ))

    val sinkDecider: Supervision.Decider = {
      case _ => Supervision.Resume // Your error handling
    }

    Source(publisher)
      .map(_.message().toUpperCase)
      .to(Sink(subscriber).withAttributes(ActorAttributes.supervisionStrategy(sinkDecider)))
      .run()
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
      brokerList = "localhost:9092",
      zooKeeperHost = "localhost:2181",
      topic = "lowercaseStrings",
      groupId = "groupName",
      decoder = new StringDecoder()
    )
      .commitInterval(5 seconds) // flush interval

    val consumerWithOffsetSink = kafka.consumeWithOffsetSink(consumerProperties)
    Source(consumerWithOffsetSink.publisher)
      .map(processMessage(_)) // your message processing
      .to(consumerWithOffsetSink.offsetCommitSink) // stream back for commit
      .run()
  }
}

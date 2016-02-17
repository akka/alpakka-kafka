package examples

import akka.NotUsed
import com.softwaremill.react.kafka.KafkaMessages._
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerMessage}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

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
    val source: Source[StringConsumerRecord, NotUsed] = kafka.graphStageSource(ConsumerProperties(
      bootstrapServers = "localhost:9092",
      topic = "lowercaseStrings",
      groupId = "groupName",
      valueDeserializer = new StringDeserializer()
    ))
    val sink: Sink[StringProducerMessage, NotUsed] = kafka.graphStageSink(ProducerProperties(
      bootstrapServers = "localhost:9092",
      topic = "uppercaseStrings",
      valueSerializer = new StringSerializer()
    ))

    source.map(m => ProducerMessage(m.value().toUpperCase)).to(sink).run()
    ()
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
    import akka.actor.ActorSystem
    import akka.stream.ActorMaterializer
    import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}

    import scala.concurrent.duration._

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

    val consumerWithOffsetSink = kafka.sourceWithOffsetSink(consumerProperties)
    consumerWithOffsetSink.source
      .map(processMessage(_)) // your message processing
      .to(consumerWithOffsetSink.offsetCommitSink) // stream back for commit
      .run()
  }
}

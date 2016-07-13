package akka.kafka.benchmarks

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
case class ReactiveKafkaConsumerTestFixture[T](topic: String, msgCount: Int, source: Source[T, Control])

object ReactiveKafkaConsumerFixtures extends PerfFixtureHelpers {

  private def createConsumerSettings(kafkaHost: String)(implicit actorSystem: ActorSystem) =
    ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(kafkaHost)
    .withGroupId(randomId())
    .withClientId(randomId())
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def plainSources(kafkaHost: String)(from: Int, upto: Int, hop: Int)
                  (implicit actorSystem: ActorSystem) = FixtureGen[ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]](from, upto, hop, msgCount => {
      val topic = randomId()
      fillTopic(kafkaHost, topic, msgCount)
      val settings = createConsumerSettings(kafkaHost)
      val source = Consumer.plainSource(settings, Subscriptions.topics(topic))
      ReactiveKafkaConsumerTestFixture(topic, msgCount, source)
    })

  def commitableSources(kafkaHost: String)(from: Int, upto: Int, hop: Int)
                  (implicit actorSystem: ActorSystem) = FixtureGen[ReactiveKafkaConsumerTestFixture[CommittableMessage[Array[Byte], String]]](from, upto, hop, msgCount => {
    val topic = randomId()
    fillTopic(kafkaHost, topic, msgCount)
    val settings = createConsumerSettings(kafkaHost)
    val source = Consumer.committableSource(settings, Subscriptions.topics(topic))
    ReactiveKafkaConsumerTestFixture(topic, msgCount, source)
  })

}

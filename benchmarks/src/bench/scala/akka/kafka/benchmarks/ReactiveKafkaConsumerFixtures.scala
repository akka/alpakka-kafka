package akka.kafka.benchmarks

import java.util.UUID
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

case class ReactiveKafkaConsumerTestFixture(topic: String, msgCount: Int, source: Source[ConsumerRecord[Array[Byte], String], Control])

object ReactiveKafkaConsumerFixtures extends BenchmarkHelpers {

  def randomId() = UUID.randomUUID().toString

  def filledTopics(kafkaHost: String, axisName: String)(from: Int, upto: Int, hop: Int)
                  (implicit actorSystem: ActorSystem) = FixtureGen[ReactiveKafkaConsumerTestFixture](from, upto, hop, msgCount => {
      val topic = randomId()
      fillTopic(kafkaHost, topic, msgCount)
      val settings =
        ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)
        .withBootstrapServers(kafkaHost)
        .withGroupId(randomId())
        .withClientId(randomId())
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      val source = Consumer.plainSource(settings, Subscriptions.topics(topic))
      ReactiveKafkaConsumerTestFixture(topic, msgCount, source)
    })
}

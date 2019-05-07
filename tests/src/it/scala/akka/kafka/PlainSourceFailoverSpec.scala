package akka.kafka

import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import com.spotify.docker.client.DefaultDockerClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

object PlainSourceFailoverSpec {
  // the following system properties are provided by the sbt-docker-compose plugin
  val KafkaBootstrapServers = (1 to BuildInfo.kafkaScale).map(i => sys.props(s"kafka_${i}_9094")).mkString(",")
  val Kafka1Port = sys.props("kafka_1_9094_port").toInt
  val Kafka2ContainerId = sys.props("kafka_2_9094_id")
}

class PlainSourceFailoverSpec extends ScalatestKafkaSpec(PlainSourceFailoverSpec.Kafka1Port) with WordSpecLike with ScalaFutures with Matchers {
  import PlainSourceFailoverSpec._

  override def bootstrapServers = KafkaBootstrapServers

  val docker = new DefaultDockerClient("unix:///var/run/docker.sock")

  implicit val pc = PatienceConfig(30.seconds, 100.millis)

  "plain source" should {

    "not lose any messages when a Kafka node dies" in assertAllStagesStopped {

      val totalMessages = 1000 * 10
      val partitions = 1

      waitUntilCluster() {
        _.nodes().get().size == BuildInfo.kafkaScale
      }

      val topic = createTopic(suffix = 0, partitions, replication = 3)
      val groupId = createGroupId(0)

      val consumerConfig = consumerDefaults
        .withGroupId(groupId)
        .withProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100") // default was 5 * 60 * 1000 (five minutes)

      val consumer = Consumer.plainSource(consumerConfig, Subscriptions.topics(topic))
        .takeWhile(_.value().toInt < totalMessages, inclusive = true)
        .scan(0)((c, _) => c + 1)
        .map { i =>
          if (i % 1000 == 0) log.info(s"Received [$i] messages so far.")
          i
        }
        .runWith(Sink.last)

      waitUntilConsumerSummary(groupId) {
        case singleConsumer :: Nil => singleConsumer.assignment.topicPartitions.size == partitions
      }

      val result = Source(1 to totalMessages)
        .map { i =>
          if (i % 1000 == 0) log.info(s"Sent [$i] messages so far.")
          i.toString
        }
        .map(number => new ProducerRecord(topic, partition0, DefaultKey, number))
        .map { c =>
          if (c.value().toInt == totalMessages / 2) {
            log.info("Stopping one Kafka container")
            docker.stopContainer(Kafka2ContainerId, 0)
          }
          c
        }
        .runWith(Producer.plainSink(producerDefaults))

      result.futureValue
      consumer.futureValue shouldBe totalMessages
    }
  }
}

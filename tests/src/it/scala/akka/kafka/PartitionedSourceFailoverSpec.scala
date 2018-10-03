package akka.kafka

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer, ScalatestKafkaSpec}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import com.spotify.docker.client.DefaultDockerClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

object PartitionedSourceFailoverSpec {
  // the following system properties are provided by the sbt-docker-compose plugin
  val KafkaBootstrapServers = (1 to BuildInfo.kafkaScale).map(i => sys.props(s"kafka_${i}_9094")).mkString(",")
  val Kafka1Port = sys.props("kafka_1_9094_port").toInt
  val Kafka2ContainerId = sys.props("kafka_2_9094_id")
}

class PartitionedSourceFailoverSpec extends ScalatestKafkaSpec(PartitionedSourceFailoverSpec.Kafka1Port) with WordSpecLike with ScalaFutures with Matchers {
  import PartitionedSourceFailoverSpec._

  override def bootstrapServers = KafkaBootstrapServers

  val docker = new DefaultDockerClient("unix:///var/run/docker.sock")

  implicit val pc = PatienceConfig(30.seconds, 1.second)

  final val logSentMessages: Long => Long = i => {
    if (i % 1000 == 0) log.info(s"Sent [$i] messages so far.")
    i
  }

  final def logReceivedMessages(tp: TopicPartition): Long => Long = i => {
    if (i % 1000 == 0) log.info(s"$tp: Received [$i] messages so far.")
    i
  }

  "partitioned source" should {

    "not lose any messages when a Kafka node dies" in assertAllStagesStopped {

      val totalMessages = 1000 * 10L

      waitUntilCluster() {
        _.nodes().get().size == BuildInfo.kafkaScale
      }

      val partitions = 4

      val topic = createTopic(0, partitions, replication = 3)
      val groupId = createGroupId(0)

      val consumerConfig = consumerDefaults
        .withGroupId(groupId)
        .withProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100") // default was 5 * 60 * 1000 (five minutes)

      val control = Consumer.plainPartitionedSource(consumerConfig, Subscriptions.topics(topic))
        .groupBy(partitions, _._1)
        .mapAsync(8) { case (tp, source) =>
          log.info(s"Sub-source for ${tp}")
          source
            .scan(0L)((c, _) => c + 1)
            .map(logReceivedMessages(tp))
            .runWith(Sink.last)
            .map { res =>
              log.info(s"$tp: Received [$res] messages in total.")
              res
            }
        }
        .mergeSubstreams
        .scan(0L)((c, subValue) => c + subValue)
        .toMat(Sink.last)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()

      waitUntilConsumerGroup(groupId) {
        _.consumers match {
          case Some(consumers) if consumers.nonEmpty => true
          case _ => false
        }
      }

      val result = Source(0L until totalMessages)
        .map(logSentMessages)
        .map { number =>
          if (number == totalMessages / 2) {
            log.warn(s"Stopping one Kafka container [$Kafka2ContainerId] after [$number] messages")
            docker.stopContainer(Kafka2ContainerId, 0)
          }
          number
        }
        .map(number => new ProducerRecord(topic, (number % partitions).toInt, DefaultKey, number.toString))
        .runWith(Producer.plainSink(producerDefaults))

      result.futureValue shouldBe Done
      sleep(2.seconds)
      control.drainAndShutdown().futureValue shouldBe totalMessages
    }
   }
}

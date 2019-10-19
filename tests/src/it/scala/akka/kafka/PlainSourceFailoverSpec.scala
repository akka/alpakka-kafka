package akka.kafka

import akka.kafka.scaladsl.{Consumer, Producer, SpecBase}
import akka.kafka.testkit.internal.TestcontainersKafka.TestcontainersKafkaSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.TopicConfig
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}
import org.testcontainers.containers.GenericContainer

import scala.concurrent.duration._

class PlainSourceFailoverSpec extends SpecBase with TestcontainersKafkaPerClassLike with WordSpecLike with ScalaFutures with Matchers {
  implicit val pc = PatienceConfig(45.seconds, 100.millis)

  override def testcontainersSettings = TestcontainersKafkaSettings(
    numBrokers = 3,
    internalTopicsReplicationFactor = 2
  )

  "plain source" should {
    "not lose any messages when a Kafka node dies" in assertAllStagesStopped {
      val broker2: GenericContainer[_] = brokerContainers(1)
      val broker2ContainerId: String = broker2.getContainerId

      val totalMessages = 1000 * 10
      val partitions = 1

      // TODO: This is probably not necessary anymore since the testcontainer setup blocks until all brokers are online.
      // TODO: However it is nice reassurance to hear from Kafka itself that the cluster is formed.
      waitUntilCluster() {
        _.nodes().get().size == testcontainersSettings.numBrokers
      }

      val topic = createTopic(0, partitions, replication = 3, Map(
        // require at least two replicas be in sync before acknowledging produced record
        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG -> "2"
      ))
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

      val producerConfig = producerDefaults.withProperties(
        // require acknowledgement from at least min in sync replicas (2).  default is 1
        ProducerConfig.ACKS_CONFIG -> "all"
      )

      val result = Source(1 to totalMessages)
        .map { i =>
          if (i % 1000 == 0) log.info(s"Sent [$i] messages so far.")
          i.toString
        }
        .map(number => new ProducerRecord(topic, partition0, DefaultKey, number))
        .map { number =>
          if (number.value().toInt == totalMessages / 2) {
            log.warn(s"Stopping one Kafka container [$broker2ContainerId] after [$number] messages")
            broker2.stop()
          }
          number
        }
        .runWith(Producer.plainSink(producerConfig))

      result.futureValue
      log.info("Actual messages received [{}], total messages sent [{}]", consumer.futureValue, totalMessages)
      // assert that we receive at least the number of messages we sent, there could be more due to retries
      assert(consumer.futureValue >= totalMessages)
    }
  }
}

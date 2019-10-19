package akka.kafka

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Consumer, Producer, SpecBase}
import akka.kafka.testkit.internal.TestcontainersKafka.TestcontainersKafkaSettings
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}
import org.testcontainers.containers.GenericContainer

import scala.concurrent.Future
import scala.concurrent.duration._

class PartitionedSourceFailoverSpec extends SpecBase with TestcontainersKafkaPerClassLike with WordSpecLike with ScalaFutures with Matchers {
  implicit val pc = PatienceConfig(45.seconds, 1.second)

  final val logSentMessages: Long => Long = i => {
    if (i % 1000 == 0) log.info(s"Sent [$i] messages so far.")
    i
  }

  final def logReceivedMessages(tp: TopicPartition): Long => Long = i => {
    if (i % 1000 == 0) log.info(s"$tp: Received [$i] messages so far.")
    i
  }

  override def testcontainersSettings = TestcontainersKafkaSettings(
    numBrokers = 3,
    internalTopicsReplicationFactor = 2
  )

  "partitioned source" should {
    "not lose any messages when a Kafka node dies" in assertAllStagesStopped {
      val broker2: GenericContainer[_] = brokerContainers(1)
      val broker2ContainerId: String = broker2.getContainerId

      val totalMessages = 1000 * 10L
      val partitions = 4

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

      val control: DrainingControl[Long] = Consumer.plainPartitionedSource(consumerConfig, Subscriptions.topics(topic))
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
        !_.members().isEmpty
      }

      val producerConfig = producerDefaults.withProperties(
        // require acknowledgement from at least min in sync replicas (2).  default is 1
        ProducerConfig.ACKS_CONFIG -> "all"
      )

      val result: Future[Done] = Source(0L until totalMessages)
        .map(logSentMessages)
        .map { number =>
          if (number == totalMessages / 2) {
            log.warn(s"Stopping one Kafka container [$broker2ContainerId] after [$number] messages")
            broker2.stop()
          }
          number
        }
        .map(number => new ProducerRecord(topic, (number % partitions).toInt, DefaultKey, number.toString))
        .runWith(Producer.plainSink(producerConfig))

      result.futureValue shouldBe Done
      log.info("Actual messages received [{}], total messages sent [{}]", control.drainAndShutdown().futureValue, totalMessages)
      // assert that we receive at least the number of messages we sent, there could be more due to retries
      assert(control.drainAndShutdown().futureValue >= totalMessages)
    }
  }
}

package akka.kafka

import java.util
import java.util.{Arrays, Properties, UUID}

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer, ScalatestKafkaSpec}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.spotify.docker.client.DefaultDockerClient
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, DescribeClusterResult, NewTopic}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.annotation.tailrec
import scala.concurrent.duration._

object RebalanceSpec {
  val partition0 = 0
  val defaultKey = "key"

  def adminClient(hosts: String) = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, hosts)
    AdminClient.create(config)
  }

  def createRealTopic(hosts: String, partitions: Int, replication: Int): String = {
    val topicName = UUID.randomUUID().toString

    val configs = new util.HashMap[String, String]()
    val createResult = adminClient(hosts).createTopics(Arrays.asList(
      new NewTopic(topicName, partitions, replication.toShort).configs(configs)))
    createResult.all()
    topicName
  }

  def waitUntilCluster(hosts: String)(predicate: DescribeClusterResult => Boolean): Unit = {
    val MaxTries = 10
    val Sleep = 100.millis

    @tailrec def checkCluster(triesLeft: Int): Unit = {
      val cluster = adminClient(hosts).describeCluster()
      if (!predicate(cluster)) {
        if (triesLeft > 0) {
          Thread.sleep(Sleep.toMillis)
          checkCluster(triesLeft - 1)
        }
        else {
          throw new Error("Failure while waiting for wanted cluster state")
        }
      }
    }

    checkCluster(MaxTries)
  }

  val KafkaHosts = (1 to 3).map(i => sys.props(s"kafka_${i}_9094")).mkString(",")
  val Kafka1Port = sys.props("kafka_1_9094_port").toInt
  val Kafka2ContainerId = sys.props("kafka_2_9094_id")
}

class RebalanceSpec extends ScalatestKafkaSpec(RebalanceSpec.Kafka1Port) with WordSpecLike with ScalaFutures with Matchers {
  import RebalanceSpec._

  val docker = new DefaultDockerClient("unix:///var/run/docker.sock")

  implicit val pc = PatienceConfig(30.seconds, 100.millis)

  "alpakka kafka" should {

    "not lose any messages during a rebalance" in {

      val totalMessages = 1000 * 10

      waitUntilCluster(KafkaHosts) {
        _.nodes().get().size == 3
      }

      val topic = createRealTopic(KafkaHosts, partitions = 1, replication = 3)

      val producerSettings = ProducerSettings(system, new StringSerializer, new IntegerSerializer)
        .withBootstrapServers(KafkaHosts)

      val groupId = UUID.randomUUID().toString
      val clientId = UUID.randomUUID().toString

      val consumerSettings = ConsumerSettings(system, new StringDeserializer, new IntegerDeserializer)
        .withBootstrapServers(KafkaHosts)
        .withGroupId(groupId)
        .withClientId(clientId)

      val consumer = Consumer.plainSource(consumerSettings, Subscriptions.topics(topic))
        .takeWhile(_.value() < totalMessages)
        .scan(0)((c, _) => c + 1)
        .map { i =>
          if (i % 1000 == 0) system.log.info(s"Received [$i] messages so far.")
          i
        }
        .runWith(Sink.last)

      // Give some time for the consumer to establish connection to kafka
      Thread.sleep(5000)

      val result = Source(0 to totalMessages)
        .map { i =>
          if (i % 1000 == 0) system.log.info(s"Sent [$i] messages so far.")
          i
        }
        .map(number => new ProducerRecord(topic, partition0, defaultKey, new Integer(number)))
        .map { c =>
          if (c.value() == totalMessages / 2) {
            system.log.info("Stopping one Kafka container")
            docker.stopContainer(Kafka2ContainerId, 0)
          }
          c
        }
        .runWith(Producer.plainSink(producerSettings))

      result.futureValue
      consumer.futureValue shouldBe totalMessages
    }
  }
}

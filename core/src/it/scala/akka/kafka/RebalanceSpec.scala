package akka.kafka

import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import com.spotify.docker.client.DefaultDockerClient
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

object RebalanceSpec {
  def createTopic(): String = UUID.randomUUID().toString
  val partition0 = 0
  val defaultKey = "key"
}

class RebalanceSpec extends TestKit(ActorSystem("RebalanceSpec")) with WordSpecLike with BeforeAndAfterAll with ScalaFutures with Matchers {
  import RebalanceSpec._

  val kafkaHost = sys.props("kafka_1_9094")
  val containerId = sys.props("kafka_2_9094_id")

  val docker = new DefaultDockerClient("unix:///var/run/docker.sock")

  implicit val pc = PatienceConfig(10.seconds, 100.millis)
  implicit val mat = ActorMaterializer()

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  "alpakka kafka" should {

    "not loose any mesasges during rebalance" in {

      val producerSettings = ProducerSettings(system, new StringSerializer, new IntegerSerializer)
        .withBootstrapServers(kafkaHost)

      val topic = createTopic()
      val totalMessages = 1000 * 10

      val producer = producerSettings.createKafkaProducer()
      producer.send(new ProducerRecord(topic, partition0, defaultKey, -1))

      val group = UUID.randomUUID().toString
      val client = UUID.randomUUID().toString

      val consumerSettings = ConsumerSettings(system, new StringDeserializer, new IntegerDeserializer)
        .withBootstrapServers(kafkaHost)
        .withGroupId(group)
        .withClientId(client)

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
            docker.stopContainer(containerId, 0)
          }
          c
        }
        .runWith(Producer.plainSink(producerSettings))

      result.futureValue
      consumer.futureValue shouldBe totalMessages
    }
  }
}

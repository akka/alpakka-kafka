/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka._
import akka.kafka.test.Utils._
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink, Tcp}
import net.manub.embeddedkafka.EmbeddedKafkaConfig

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ReconnectSpec extends SpecBase(kafkaPort = KafkaPorts.ReconnectSpec) {

  val proxyPort = KafkaPorts.ReconnectSpecProxy

  def createKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort, zooKeeperPort,
      Map(
        "offsets.topic.replication.factor" -> "1"
      ))

  "A killed Kafka server" must {

    "work for a Producer" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroup(1)

      givenInitializedTopic(topic1)

      val producerSettings = producerDefaults.withBootstrapServers(s"localhost:$proxyPort")

      // start a TCP proxy forwarding to Kafka and start producing messages
      val (proxyBinding, proxyKillSwtich) = createProxy()
      Await.ready(proxyBinding, remainingOrDefault)

      val messagesProduced = 100
      produce(topic1, 1 to messagesProduced, producerSettings)
      val proxyConnection = proxyKillSwtich.futureValue

      // construct a consumer directly to Kafka and request a first message and kill the proxy-Kafka connection
      val (_, probe) = createProbe(consumerDefaults.withGroupId(group1), topic1)
      probe.requestNext() should be("1")
      proxyConnection.shutdown()
      sleep(100.millis)

      // expect some radio silence
      probe.expectNoMessage(1.second)

      // create new proxy and expect all other messages
      val (proxyBinding2, _) = createProxy()
      probe.request(messagesProduced.toLong)
      probe.expectNextN((2 to messagesProduced).map(_.toString))

      // shut down
      probe.cancel()
      proxyBinding2.map(_.unbind()).futureValue
    }

    "work for a Consumer" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroup(1)

      givenInitializedTopic(topic1)

      // produce messages directly to Kafka
      val messagesProduced = 100
      produce(topic1, 1 to messagesProduced)

      // create a TCP proxy and set up a consumer through it
      val (proxyBinding, proxyKillSwtich) = createProxy()
      Await.ready(proxyBinding, remainingOrDefault)
      val consumerSettings = consumerDefaults
        .withGroupId(group1)
        .withBootstrapServers(s"localhost:$proxyPort")
      val (control, probe) = createProbe(consumerSettings, topic1)

      // expect an element and kill the proxy
      probe.requestNext() should be("1")
      proxyKillSwtich.futureValue.shutdown()
      sleep(100.millis)

      // expect some radio silence
      probe.expectNoMessage(1.second)

      // create new proxy and expect all other messages
      val (proxyBinding2, _) = createProxy()
      probe.request(messagesProduced.toLong)
      probe.expectNextN((2 to messagesProduced).map(_.toString))

      // shut down
      control.shutdown().futureValue
      proxyBinding2.map(_.unbind()).futureValue
    }

  }

  /** Create a proxy so it can be shut down.
    */
  def createProxy(): (Future[Tcp.ServerBinding], Future[UniqueKillSwitch]) = {
    // Create a proxy so it can be shut down
    val (proxyBinding, connection) = Tcp().bind("localhost", proxyPort).toMat(Sink.head)(Keep.both).run()
    val proxyKsFut = connection.map(
      _.handleWith(
        Tcp()
          .outgoingConnection("localhost", kafkaPort)
          .viaMat(KillSwitches.single)(Keep.right)
      )
    )
    (proxyBinding, proxyKsFut)
  }
}

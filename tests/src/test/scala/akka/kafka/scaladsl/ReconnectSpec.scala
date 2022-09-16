/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.Done
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{KillSwitches, OverflowStrategy, UniqueKillSwitch}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ReconnectSpec extends SpecBase with TestcontainersKafkaLike {

  val proxyPort = 9034

  "A Producer" must {

    "continue to work when there is another Kafka port available" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)

      // start a TCP proxy forwarding to Kafka
      val (_, proxyKillSwitch) = createProxy()

      val messagesProduced = 100
      val firstBatch = 10
      val messages = (1 to messagesProduced).map(_.toString)
      // start a producer flow with a queue as source
      val producer: SourceQueueWithComplete[String] = Source
        .queue[String](messagesProduced, OverflowStrategy.backpressure)
        .map(msg => new ProducerRecord(topic1, partition0, DefaultKey, msg))
        .to(Producer.plainSink(producerDefaults.withBootstrapServers(s"localhost:$proxyPort")))
        .run()

      def offerInOrder(msgs: Seq[String]): Future[_] =
        if (msgs.isEmpty) Future.successful(Done)
        else producer.offer(msgs.head).flatMap(_ => offerInOrder(msgs.tail))

      // put one batch into the stream
      offerInOrder(messages.take(firstBatch))

      // construct a consumer directly to Kafka and request messages and kill the proxy-Kafka connection
      val (_, probe) = createProbe(consumerDefaults.withGroupId(group1), topic1)
      probe.request(messagesProduced.toLong)
      probe.expectNextN(messages.take(firstBatch))

      Await.result(proxyKillSwitch, remainingOrDefault).shutdown()

      probe.expectNoMessage(500.millis)

      offerInOrder(messages.drop(firstBatch))

      probe.expectNextN(messagesProduced.toLong - firstBatch) should be(messages.drop(firstBatch))

      // shut down
      producer.complete()
      probe.cancel()
    }
  }

  "A Consumer" must {

    "continue to work when there is another Kafka port available" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)

      // produce messages directly to Kafka
      val messagesProduced = 100
      produce(topic1, 1 to messagesProduced)

      // create a TCP proxy and set up a consumer through it
      val (_, proxyKillSwitch) = createProxy()
      val consumerSettings = consumerDefaults
        .withGroupId(group1)
        .withBootstrapServers(s"localhost:$proxyPort")
      val (control, probe) = createProbe(consumerSettings, topic1)

      // expect an element and kill the proxy
      probe.requestNext() should be("1")
      Await.result(proxyKillSwitch, remainingOrDefault).shutdown()
      sleep(100.millis)

      probe.request(messagesProduced.toLong)
      probe.expectNextN((2 to messagesProduced).map(_.toString))

      // shut down
      Await.ready(control.shutdown(), remainingOrDefault)
    }

    "pick up again when the Kafka server comes back up" in assertAllStagesStopped {
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)

      // produce messages
      val messagesProduced = 100
      produce(topic1, 1 to messagesProduced)

      // create a consumer
      val (control, probe) =
        createProbe(consumerDefaults.withGroupId(group1), topic1)

      // expect an element and make Kafka brokers unavailable
      probe.requestNext() should be("1")

      // stop Kafka broker process
      stopKafka()
      sleep(1.second)

      // by now all messages have arrived in the consumer
      probe.request(99)
      probe.receiveWithin(100.millis) should be((2 to 100).map(_.toString))

      // expect silence
      probe.request(1)
      probe.expectNoMessage(1.second)

      // start Kafka broker process produce another round
      startKafka()

      sleep(1.second) // Got some messages dropped during startup
      produce(topic1, 101 to 200)

      probe.request(messagesProduced.toLong * 2)
      // because we are using a plainSink sometimes the disconnect will reset the consumer group generation and when the
      // consumer reconnects it will seek back to the earliest offset 0 (because `auto.offset.reset=earliest`).
      // therefore if we take the last `messageProduced` messages it should always be 101 to 200.
      probe.receiveWithin(5.second).takeRight(messagesProduced) should be((101 to 200).map(_.toString))

      // shut down
      Await.ready(control.shutdown(), remainingOrDefault)
    }
  }

  /**
   * Create a proxy so it can be shut down.
   */
  def createProxy(): (Tcp.ServerBinding, Future[UniqueKillSwitch]) = {
    // Create a proxy so it can be shut down
    val (proxyBinding, connection) =
      Tcp().bind("localhost", proxyPort).toMat(Sink.head)(Keep.both).run()
    val proxyKsFut = connection.map(
      _.handleWith(
        Tcp()
          .outgoingConnection(brokerContainers.head.getHost, kafkaPort)
          .viaMat(KillSwitches.single)(Keep.right)
      )
    )
    (Await.result(proxyBinding, remainingOrDefault), proxyKsFut)
  }
}

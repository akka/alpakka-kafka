/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ProducerMessage}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

class TestkitSamplesSpec
    extends TestKit(ActorSystem("example"))
    with FlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {
  implicit val mat: Materializer = ActorMaterializer()

  implicit val patience = PatienceConfig(3.seconds, 100.millis)

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "Without broker testing" should "be possible" in assertAllStagesStopped {
    val topic = "topic"
    val targetTopic = "target-topic"
    val groupId = "group1"
    val startOffset = 100L
    val partition = 0

    // #factories
    import akka.kafka.testkit.{ConsumerResultFactory, ProducerResultFactory}
    import akka.kafka.testkit.scaladsl.ConsumerControlFactory

    // create elements emitted by the mocked Consumer
    val elements = immutable.Seq(
      ConsumerResultFactory.committableMessage(
        new ConsumerRecord(topic, partition, startOffset, "key", "value 1"),
        ConsumerResultFactory.committableOffset(groupId, topic, partition, startOffset, "metadata")
      ),
      ConsumerResultFactory.committableMessage(
        new ConsumerRecord(topic, partition, startOffset + 1, "key", "value 2"),
        ConsumerResultFactory.committableOffset(groupId, topic, partition, startOffset + 1, "metadata 2")
      )
    )

    // create a source imitating the Consumer.committableSource
    val mockedKafkaConsumerSource: Source[ConsumerMessage.CommittableMessage[String, String], Consumer.Control] =
      Source(elements)
        .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)

    // create a source imitating the Producer.flexiFlow
    val mockedKafkaProducerFlow: Flow[ProducerMessage.Envelope[String, String, CommittableOffset],
                                      ProducerMessage.Results[String, String, CommittableOffset],
                                      NotUsed] =
      Flow[ProducerMessage.Envelope[String, String, CommittableOffset]]
        .map {
          case msg: ProducerMessage.Message[String, String, CommittableOffset] =>
            ProducerResultFactory.result(msg)
          case other => throw new Exception(s"excluded: $other")
        }

    // run the flow as if it was connected to a Kafka broker
    val (control, streamCompletion) = mockedKafkaConsumerSource
      .map(
        msg =>
          ProducerMessage.Message(
            new ProducerRecord[String, String](targetTopic, msg.record.value),
            msg.committableOffset
        )
      )
      .via(mockedKafkaProducerFlow)
      .map(_.passThrough)
      .groupedWithin(10, 500.millis)
      .map(CommittableOffsetBatch(_))
      .mapAsync(3)(_.commitScaladsl())
      .toMat(Sink.ignore)(Keep.both)
      .run()
    // #factories

    Await.result(streamCompletion, 2.seconds)
    streamCompletion.futureValue should be(Done)
  }
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.kafka.internal.KafkaConsumerActor.Internal
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.Producer
import akka.kafka.testkit.ConsumerResultFactory
import akka.kafka.testkit.scaladsl.{ConsumerControlFactory, Slf4jToAkkaLoggingAdapter}
import akka.kafka.tests.scaladsl.LogCapturing
import akka.kafka._
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.{ActorAttributes, Supervision}
import akka.testkit.{TestKit, TestProbe}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext

class CommittingProducerSinkSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with IntegrationPatience
    with Eventually
    with Repeated
    with LogCapturing {

  import CommittingProducerSinkSpec.FakeConsumer

  def this() = this(ActorSystem())

  override def afterAll(): Unit = shutdown(system)

  val log: Logger = LoggerFactory.getLogger(getClass)

  // used by the .log(...) stream operator
  implicit val adapter: LoggingAdapter = new Slf4jToAkkaLoggingAdapter(log)

  implicit val ec: ExecutionContext = _system.dispatcher

  val groupId = "group1"
  val topic = "topic1"
  val partition = 1
  val partition2 = 2

  "committable producer sink" should "produce, and commit after interval" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val commitInterval = 200.millis
    val committerSettings = CommitterSettings(system).withMaxInterval(commitInterval)

    val control = Source(elements)
      .concat(Source.maybe) // keep the source alive
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    consumer.actor.expectNoMessage(commitInterval)
    val commitMsg = consumer.actor.expectMsgClass(classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "produce, and commit after interval with pass-through messages" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "skip"),
      consumer.message(partition, "send")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val commitInterval = 200.millis
    val committerSettings = CommitterSettings(system).withMaxInterval(commitInterval)

    val control = Source(elements)
      .concat(Source.maybe) // keep the source alive
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        if (msg.record.value == "skip") {
          ProducerMessage.passThrough[String, String, ConsumerMessage.CommittableOffset](msg.committableOffset)
        } else {
          ProducerMessage.single(
            new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
            msg.committableOffset
          )
        }
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    consumer.actor.expectNoMessage(commitInterval)
    val commitMsg = consumer.actor.expectMsgClass(classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (1)
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "produce, and commit when batch size is reached" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system).withMaxBatch(2L).withMaxInterval(10.seconds)

    val control = Source(elements)
      .concat(Source.maybe) // keep the source alive
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    val commitMsg = consumer.actor.expectMsgClass(500.millis, classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "produce, and commit when batch size is reached with pass-through messages" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system).withMaxBatch(2L).withMaxInterval(10.seconds)

    val control = Source(elements)
      .concat(Source.maybe) // keep the source alive
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.passThrough[String, String, ConsumerMessage.CommittableOffset](msg.committableOffset)
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    val commitMsg = consumer.actor.expectMsgClass(500.millis, classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (0)
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "produce, and commit when batch size is reached with multi-messages" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producerRecordsPerInput = 2
    val totalProducerRecords = elements.size * producerRecordsPerInput

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system).withMaxBatch(elements.size.longValue())

    val control = Source(elements)
      .concat(Source.maybe)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.multi(
          (1 to producerRecordsPerInput)
            .map(n => new ProducerRecord("targetTopic", msg.record.key, msg.record.value)),
          msg.committableOffset
        )
      }
      .toMat(
        Producer.committableSink(producerSettings, committerSettings)
      )(DrainingControl.apply)
      .run()

    val commitMsg = consumer.actor.expectMsgClass(classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + elements.size)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (totalProducerRecords.longValue())
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "only commit when batch size is reached with empty multi-messages" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)
    val message = consumer.message(partition, "increment the offset")

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system).withMaxBatch(1)

    val control = Source
      .single(message)
      .concat(Source.maybe)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.multi(immutable.Seq.empty[ProducerRecord[String, String]], msg.committableOffset)
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    val commitMsg = consumer.actor.expectMsgClass(classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 1)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size 0
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "produce, and commit when the next offset is observed" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system)
      .withMaxBatch(1L)
      .withCommitWhen(CommitWhen.NextOffsetObserved)

    val control = Source(elements)
      .concat(Source.maybe) // keep the source alive
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    val commitMsg = consumer.actor.expectMsgClass(500.millis, classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 1)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "produce, and commit on completion" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    // choose a large commit interval so that completion happens before
    val largeCommitInterval = 30.seconds
    val committerSettings = CommitterSettings(system).withMaxInterval(largeCommitInterval)

    val control = Source(elements)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    // expect the commit to reach the actor within 1 second
    val commitMsg = consumer.actor.expectMsgClass(1.second, classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "produce, and commit on delayed completion" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val commitInterval = 5.seconds
    val committerSettings = CommitterSettings(system).withMaxInterval(commitInterval)

    val control = Source(elements)
      .concat(Source.maybe) // keep the source alive
      .idleTimeout(50.millis)
      .recoverWithRetries(1, {
        case _ => Source.empty
      })
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    val commitMsg = consumer.actor.expectMsgClass(1.second, classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "produce, and commit on upstream failure" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val commitInterval = 5.seconds
    val committerSettings = CommitterSettings(system).withMaxInterval(commitInterval)

    val control = Source(elements)
      .concat(Source.maybe) // keep the source alive
      .idleTimeout(50.millis)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    val commitMsg = consumer.actor.expectMsgClass(1.second, classOf[Internal.CommitWithoutReply])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().failed.futureValue shouldBe a[java.util.concurrent.TimeoutException]
  }

  it should "time out for missing producer reply" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    // this producer does not auto complete messages
    val producer = new MockProducer[String, String](false, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system).withMaxBatch(1L)

    val control = Source(elements)
      .concat(Source.maybe)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    while (!producer.completeNext()) {}

    val commitMsg = consumer.actor.expectMsgClass(classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 1)
    consumer.actor.reply(Done)

    // TODO how should not getting a produce callback be handled?
    while (!producer.completeNext()) {}

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().failed.futureValue shouldBe an[akka.kafka.CommitTimeoutException]
  }

  it should "choose to ignore producer errors" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](false, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system).withMaxBatch(1L)

    val control = Source(elements)
      .concat(Source.maybe)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(
        Producer
          .committableSink(producerSettings, committerSettings)
          .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
      )(DrainingControl.apply)
      .run()

    // fail the first message
    while (!producer.errorNext(new RuntimeException("let producing fail"))) {}
    consumer.actor.expectNoMessage(100.millis)

    // second message succeeds and its offset gets committed
    while (!producer.completeNext()) {}
    val commitMsg = consumer.actor.expectMsgClass(classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "choose to ignore producer errors and shut down cleanly" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](false, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    // choose a large commit interval so that completion happens before
    val largeCommitInterval = 30.seconds
    val committerSettings = CommitterSettings(system).withMaxInterval(largeCommitInterval)

    val control = Source(elements)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(
        Producer
          .committableSink(producerSettings, committerSettings)
          .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
      )(DrainingControl.apply)
      .run()

    // fail the first message
    while (!producer.errorNext(new RuntimeException("let producing fail"))) {}
    consumer.actor.expectNoMessage(100.millis)

    // second message succeeds and its offset gets committed
    while (!producer.completeNext()) {}

    // expect the commit to reach the actor within 1 second because the source completed, which should trigger commit
    val commitMsg = consumer.actor.expectMsgClass(1.second, classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)
    consumer.actor.reply(Done)

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "fail for commit timeout" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system).withMaxBatch(2L)

    val control = Source(elements)
      .concat(Source.maybe)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    val commitMsg = consumer.actor.expectMsgClass(classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)

    eventually {
      producer.history.asScala should have size (2)
    }
    control.drainAndShutdown().failed.futureValue shouldBe an[akka.kafka.CommitTimeoutException]
  }

  it should "ignore commit timeout" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system).withMaxBatch(2L)

    val control = Source(elements)
      .concat(Source.maybe)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(
        Producer
          .committableSink(producerSettings, committerSettings)
          .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
      )(DrainingControl.apply)
      .run()

    val commitMsg = consumer.actor.expectMsgClass(classOf[Internal.Commit])
    commitMsg.tp shouldBe new TopicPartition(topic, partition)
    commitMsg.offsetAndMetadata.offset() shouldBe (consumer.startOffset + 2)

    eventually {
      producer.history.asScala should have size (2)
    }

    // commit failure is ignored
    control.drainAndShutdown().futureValue shouldBe Done
  }

  it should "not commit next offset after failure if it hasn't been observed" in assertAllStagesStopped {
    val consumer = FakeConsumer(groupId, topic, startOffset = 1616L)

    val elements = immutable.Seq(
      consumer.message(partition, "value 1"),
      consumer.message(partition, "value 2")
    )

    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system)
      .withMaxBatch(1L)
      .withCommitWhen(CommitWhen.NextOffsetObserved)

    val control = Source(elements)
      .concat(Source.maybe)
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        if (msg eq elements(1)) throw new RuntimeException("error")
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(
        Producer
          .committableSink(producerSettings, committerSettings)
          .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
      )(DrainingControl.apply)
      .run()

    consumer.actor.expectNoMessage(10.millis)

    eventually {
      producer.history.asScala should have size (1)
    }

    ScalaFutures.whenReady(control.drainAndShutdown().failed) { e =>
      e shouldBe a[RuntimeException]
    }
  }

  it should "shut down without elements" in assertAllStagesStopped {
    val producer = new MockProducer[String, String](true, new StringSerializer, new StringSerializer)
    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withProducer(producer)
    val committerSettings = CommitterSettings(system).withMaxInterval(1.second)

    val control = Source
      .maybe[ConsumerMessage.CommittableMessage[String, String]]
      .viaMat(ConsumerControlFactory.controlFlow())(Keep.right)
      .map { msg =>
        ProducerMessage.single(
          new ProducerRecord("targetTopic", msg.record.key, msg.record.value),
          msg.committableOffset
        )
      }
      .toMat(Producer.committableSink(producerSettings, committerSettings))(DrainingControl.apply)
      .run()

    control.drainAndShutdown().futureValue shouldBe Done
  }
}

object CommittingProducerSinkSpec {
  final case class FakeConsumer(groupId: String, topic: String, startOffset: Long)(implicit system: ActorSystem) {
    val offset = new AtomicLong(startOffset)
    val actor = TestProbe("kafkaConsumerActor")
    val fakeCommitter: KafkaAsyncConsumerCommitterRef =
      new KafkaAsyncConsumerCommitterRef(actor.ref, 100.millis)(system.dispatcher)

    def message(partition: Int, value: String): ConsumerMessage.CommittableMessage[String, String] =
      ConsumerResultFactory.committableMessage(
        new ConsumerRecord(topic, partition, startOffset, "key", value),
        CommittableOffsetImpl(
          ConsumerResultFactory.partitionOffset(groupId, topic, partition, offset.getAndIncrement()),
          "metadata"
        )(fakeCommitter)
      )
  }
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.CompletableFuture

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{GroupTopicPartition, PartitionOffset, PartitionOffsetCommittedMarker}
import akka.kafka.ProducerMessage._
import akka.kafka.scaladsl.Producer
import akka.kafka.tests.scaladsl.LogCapturing
import akka.kafka.{ConsumerMessage, ProducerMessage, ProducerSettings}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.stream.{ActorAttributes, Supervision}
import akka.testkit.TestKit
import akka.{Done, NotUsed}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.mockito
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ProducerSpec(_system: ActorSystem)
    extends TestKit(_system)
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with LogCapturing {

  def this() =
    this(
      ActorSystem("ProducerSpec",
                  ConfigFactory
                    .parseString("""akka.stream.materializer.debug.fuzzing-mode = on""")
                    .withFallback(ConfigFactory.load()))
    )

  override def afterAll(): Unit = shutdown(system)

  implicit val ec = _system.dispatcher

  val checksum = java.lang.Long.valueOf(-1)
  val group = "group"

  type K = String
  type V = String
  type Record = ProducerRecord[K, V]
  type Msg = Message[String, String, NotUsed.type]
  type TxMsg = Message[K, V, ConsumerMessage.PartitionOffsetCommittedMarker]

  def recordAndMetadata(seed: Int) =
    new ProducerRecord("test", seed.toString, seed.toString) ->
    new RecordMetadata(new TopicPartition("test", seed),
                       seed.toLong,
                       seed.toLong,
                       System.currentTimeMillis(),
                       checksum,
                       -1,
                       -1)

  def toMessage(tuple: (Record, RecordMetadata)) = Message(tuple._1, NotUsed)
  private[kafka] def toTxMessage(tuple: (Record, RecordMetadata), committer: CommittedMarker) = {
    val consumerMessage = ConsumerMessage
      .PartitionOffset(GroupTopicPartition(group, tuple._1.topic(), 1), tuple._2.offset())
    val partitionOffsetCommittedMarker =
      PartitionOffsetCommittedMarker(consumerMessage.key,
                                     consumerMessage.offset,
                                     committer,
                                     fromPartitionedSource = false)
    ProducerMessage.Message(
      tuple._1,
      partitionOffsetCommittedMarker
    )
  }

  def result(r: Record, m: RecordMetadata) = Result(m, ProducerMessage.Message(r, NotUsed))
  val toResult = (result _).tupled

  def recordValues(values: V*): ((ProducerRecord[String, String], RecordMetadata)) => Boolean = {
    case (r, _) =>
      values.contains(r.value())
  }

  val settings =
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withEosCommitInterval(10.milliseconds)

  def testProducerFlow[P](mock: ProducerMock[K, V],
                          closeOnStop: Boolean = true): Flow[Message[K, V, P], Result[K, V, P], NotUsed] = {
    val pSettings = settings.withProducer(mock.mock).withCloseProducerOnStop(closeOnStop)
    Flow
      .fromGraph(
        new DefaultProducerStage[K, V, P, Message[K, V, P], Result[K, V, P]](pSettings)
      )
      .mapAsync(1)(identity)
  }

  def testTransactionProducerFlow[P](
      mock: ProducerMock[K, V],
      closeOnStop: Boolean = true
  ): Flow[Envelope[K, V, P], Results[K, V, P], NotUsed] = {
    val pSettings = settings.withProducerFactory(_ => mock.mock).withCloseProducerOnStop(closeOnStop)
    Flow
      .fromGraph(
        new TransactionalProducerStage[K, V, P](pSettings, "transactionalId")
      )
      .mapAsync(1)(identity)
  }

  "Producer" should "send one message and shutdown the producer gracefully" in {
    assertAllStagesStopped {
      val input = recordAndMetadata(1)

      val client = {
        val inputMap = Map(input)
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis)(x => Try { inputMap(x) }))
      }
      val committer = new CommittedMarkerMock

      val (source, sink) = TestSource
        .probe[TxMsg]
        .via(testProducerFlow(client))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val txMsg = toTxMessage(input, committer.mock)
      source.sendNext(txMsg)
      sink.requestNext()

      // we must wait for the producer to be asynchronously assigned before observing interactions with the mock
      awaitAssert(client.verifySend(times(1)))

      source.sendComplete()
      sink.expectComplete()

      client.verifyClosed()
      client.verifyNoMoreInteractions()
    }
  }

  it should "work with a provided Producer" in {
    assertAllStagesStopped {
      val input = 1 to 10 map { recordAndMetadata(_)._1 }

      val mockProducer = new MockProducer[String, String](true, null, null)

      val fut: Future[Done] = Source(input).runWith(Producer.plainSink(settings.withProducer(mockProducer)))

      Await.result(fut, Duration.apply("1 second"))
      mockProducer.close()
      mockProducer.history().asScala.toVector shouldEqual input
    }
  }

  it should "emit confirmation in same order as inputs" in {
    assertAllStagesStopped {
      val input = 1 to 3 map recordAndMetadata

      val client = {
        val inputMap = input.toMap
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis)(x => Try { inputMap(x) }))
      }
      val probe = Source(input.map(toMessage))
        .via(testProducerFlow(client))
        .runWith(TestSink.probe)

      probe
        .request(10)
        .expectNextN(input.map(toResult))
        .expectComplete()

      client.verifyClosed()
      client.verifySend(atLeastOnce())
      client.verifyNoMoreInteractions()
    }
  }

  it should "in case of source error complete emitted messages and push error" in assertAllStagesStopped {
    val input = 1 to 10 map recordAndMetadata

    val client = {
      val inputMap = input.toMap
      new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis)(x => Try { inputMap(x) }))
    }
    val (source, sink) = TestSource
      .probe[Msg]
      .via(testProducerFlow(client))
      .toMat(TestSink.probe)(Keep.both)
      .run()

    input.map(toMessage).foreach(source.sendNext)
    sink.requestNext()
    val sourceError = new Exception()
    source.sendError(sourceError)

    // Here we can not be sure that all messages from source delivered to producer
    // because of buffers in akka-stream and faster error pushing that ignores buffers
    sink.expectError(sourceError)

    client.verifyClosed()
    client.verifySend(atLeastOnce())
    client.verifyNoMoreInteractions()
  }

  it should "fail stream and force-close producer in callback on send failure" in {
    assertAllStagesStopped {
      val input = 1 to 3 map recordAndMetadata
      val error = new Exception("Something wrong in kafka")

      val client = {
        val inputMap = input.toMap
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis) { msg =>
          if (msg.value() == "2") Failure(error)
          else Success(inputMap(msg))
        })
      }
      val (source, sink) = TestSource
        .probe[Msg]
        .via(testProducerFlow(client))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(100)
      input.map(toMessage).foreach(source.sendNext)

      source.expectCancellation()

      client.verifyForceClosedInCallback()
      client.verifySend(atLeastOnce())
      client.verifyNoMoreInteractions()
    }
  }

  it should "stop emitting messages after encountering a send failure" in {
    assertAllStagesStopped {
      val input = 1 to 3 map recordAndMetadata
      val error = new Exception("Something wrong in kafka")

      val client = {
        val inputMap = input.toMap
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis) { msg =>
          if (msg.value() == "2") Failure(error)
          else Success(inputMap(msg))
        })
      }
      val (source, sink) = TestSource
        .probe[Msg]
        .via(testProducerFlow(client))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      input.map(toMessage).foreach(source.sendNext)

      sink
        .request(100)
        .expectNextN(input.filter(recordValues("1")).map(toResult))
        .expectError(error)

      source.expectCancellation()

      client.verifyForceClosedInCallback()
      client.verifySend(atLeastOnce())
      client.verifyNoMoreInteractions()
    }
  }

  it should "resume stream and gracefully close producer on send failure if specified by supervision-strategy" in {
    assertAllStagesStopped {
      val input = 1 to 3 map recordAndMetadata
      val error = new Exception("Something wrong in kafka")

      val client = {
        val inputMap = input.toMap
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis) { msg =>
          if (msg.value() == "2") Failure(error)
          else Success(inputMap(msg))
        })
      }
      val (source, sink) = TestSource
        .probe[Msg]
        .via(
          testProducerFlow(client).withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.resumingDecider))
        )
        .toMat(TestSink.probe)(Keep.both)
        .run()

      input.map(toMessage).foreach(source.sendNext)
      source.sendComplete()

      sink
        .request(100)
        .expectNextN(input.filter(recordValues("1", "3")).map(toResult))
        .expectComplete()

      client.verifyClosed()
      client.verifySend(atLeastOnce())
      client.verifyNoMoreInteractions()
    }
  }

  it should "fail stream on exception of producer send" in {
    assertAllStagesStopped {
      val input = 1 to 3 map recordAndMetadata

      val client = new ProducerMock[K, V](ProducerMock.handlers.fail)
      val probe = Source(input.map(toMessage))
        .via(testProducerFlow(client))
        .runWith(TestSink.probe)

      probe
        .request(10)
        .expectError()

      client.verifyClosed()
      client.verifySend(atLeastOnce())
      client.verifyNoMoreInteractions()
    }
  }

  it should "close client and complete in case of cancellation of outlet" in {
    assertAllStagesStopped {
      val input = 1 to 3 map recordAndMetadata

      val client = {
        val inputMap = input.toMap
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(5.seconds)(x => Try { inputMap(x) }))
      }
      val (source, sink) = TestSource
        .probe[Msg]
        .via(testProducerFlow(client))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(10)
      input.map(toMessage).foreach(source.sendNext)

      sink.cancel()
      source.expectCancellation()

      client.verifyClosed()
    }
  }

  it should "not close the producer if closeProducerOnStop is false" in {
    assertAllStagesStopped {
      val input = 1 to 3 map recordAndMetadata

      val client = {
        val inputMap = input.toMap
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis)(x => Try { inputMap(x) }))
      }
      val probe = Source(input.map(toMessage))
        .via(testProducerFlow(client, closeOnStop = false))
        .runWith(TestSink.probe)

      probe
        .request(10)
        .expectNextN(input.map(toResult))
        .expectComplete()

      client.verifySend(atLeastOnce())
      client.verifyNoMoreInteractions()
    }
  }

  it should "not close the producer on failure if closeProducerOnStop is false" in {
    assertAllStagesStopped {
      val input = 1 to 3 map recordAndMetadata
      val error = new Exception("Something wrong in kafka")

      val client = new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis) { _ =>
        Failure(error)
      })

      val (source, sink) = TestSource
        .probe[Msg]
        .via(testProducerFlow(client, closeOnStop = false))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(100)
      input.map(toMessage).foreach(source.sendNext)

      source.expectCancellation()

      client.verifySend(atLeastOnce())
      client.verifyNoMoreInteractions()
    }
  }

  it should "not initialize and begin transaction when there are no messages" in {
    assertAllStagesStopped {
      val client = new ProducerMock[K, V](ProducerMock.handlers.fail)

      val probe = Source
        .empty[Msg]
        .via(testTransactionProducerFlow(client))
        .runWith(TestSink.probe)

      probe
        .request(1)
        .expectComplete()

      client.verifyNoMoreInteractions()
    }
  }

  it should "initialize and begin a transaction when first run" in {
    assertAllStagesStopped {
      val input = recordAndMetadata(1)

      val client = {
        val inputMap = Map(input)
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis)(x => Try { inputMap(x) }))
      }
      val committer = new CommittedMarkerMock

      val (source, sink) = TestSource
        .probe[TxMsg]
        .via(testTransactionProducerFlow(client))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val txMsg = toTxMessage(input, committer.mock)
      source.sendNext(txMsg)
      sink.requestNext()

      awaitAssert(client.verifyTxInitialized(), 2.second)

      source.sendComplete()
      sink.expectComplete()
    }
  }

  it should "commit the current transaction at commit interval" in {
    assertAllStagesStopped {
      val input = recordAndMetadata(1)

      val client = {
        val inputMap = Map(input)
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis)(x => Try { inputMap(x) }))
      }
      val committer = new CommittedMarkerMock

      val (source, sink) = TestSource
        .probe[TxMsg]
        .via(testTransactionProducerFlow(client))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val txMsg = toTxMessage(input, committer.mock)
      source.sendNext(txMsg)
      sink.requestNext()

      awaitAssert(client.verifyTxCommit(txMsg.passThrough), 2.second)
      awaitAssert(committer.verifyOffsets(txMsg.passThrough), 2.second)

      source.sendComplete()
      sink.expectComplete()
    }
  }

  it should "commit the current transaction even if all messages are filtered out" in {
    assertAllStagesStopped {
      val input = recordAndMetadata(1)

      val client = {
        val inputMap = Map(input)
        new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis)(x => Try { inputMap(x) }))
      }
      val committer = new CommittedMarkerMock

      val (source, sink) = TestSource
        .probe[TxMsg]
        .map(msg => ProducerMessage.passThrough[K, V, PartitionOffset](msg.passThrough))
        .via(testTransactionProducerFlow(client))
        .toMat(TestSink.probe)(Keep.both)
        .run()

      val txMsg = toTxMessage(input, committer.mock)
      source.sendNext(txMsg)
      sink.requestNext()

      awaitAssert(client.verifyTxCommit(txMsg.passThrough), 2.second)
      awaitAssert(committer.verifyOffsets(txMsg.passThrough), 2.second)

      source.sendComplete()
      sink.expectComplete()
    }
  }

  it should "commit the current transaction gracefully on shutdown" in {
    val input = recordAndMetadata(1)

    val client = {
      val inputMap = Map(input)
      new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis)(x => Try { inputMap(x) }))
    }
    val committedMarker = new CommittedMarkerMock

    val (source, sink) = TestSource
      .probe[TxMsg]
      .via(testTransactionProducerFlow(client))
      .toMat(TestSink.probe)(Keep.both)
      .run()

    val txMsg: TxMsg = toTxMessage(input, committedMarker.mock)
    source.sendNext(txMsg)
    sink.requestNext()

    client.verifySend(atLeastOnce())

    awaitAssert(client.verifyTxCommitWhenShutdown(txMsg.passThrough), 2.second)
    awaitAssert(committedMarker.verifyOffsets(txMsg.passThrough), 2.second)

    source.sendComplete()
    sink.expectComplete()
    client.verifyClosed()
  }

  it should "abort the current transaction on failure" in {
    val input = recordAndMetadata(1)

    val client = {
      val inputMap = Map(input)
      new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100.millis)(x => Try { inputMap(x) }))
    }
    val committedMarker = new CommittedMarkerMock

    val (source, sink) = TestSource
      .probe[TxMsg]
      .via(testTransactionProducerFlow(client))
      .toMat(Sink.lastOption)(Keep.both)
      .run()

    val txMsg = toTxMessage(input, committedMarker.mock)
    source.sendNext(txMsg)

    awaitAssert(client.verifyTxInitialized())

    source.sendError(new Exception())

    // Here we can not be sure that all messages from source delivered to producer
    // because of buffers in akka-stream and faster error pushing that ignores buffers
    // TODO: we can await a tx to be initialized before sending the error (which means the producer was assigned and first msg processed). does that invalidate this test?

    Await.ready(sink, remainingOrDefault)
    sink.value should matchPattern {
      case Some(Failure(_)) =>
    }

    client.verifyTxAbort()
    client.verifyClosed()
  }
}

object ProducerMock {
  type Handler[K, V] = (ProducerRecord[K, V], Callback) => Future[RecordMetadata]
  object handlers {
    def fail[K, V]: Handler[K, V] = (_, _) => throw new Exception("Should not be called")
    def delayedMap[K, V](
        delay: FiniteDuration
    )(f: ProducerRecord[K, V] => Try[RecordMetadata])(implicit as: ActorSystem): Handler[K, V] = { (record, _) =>
      implicit val ec = as.dispatcher
      val promise = Promise[RecordMetadata]()
      as.scheduler.scheduleOnce(delay) {
        promise.complete(f(record))
        ()
      }
      promise.future
    }
  }
}

class ProducerMock[K, V](handler: ProducerMock.Handler[K, V])(implicit ec: ExecutionContext) {
  var closed = false
  val mock = {
    val result = Mockito.mock(classOf[KafkaProducer[K, V]])
    Mockito
      .when(result.send(mockito.ArgumentMatchers.any[ProducerRecord[K, V]], mockito.ArgumentMatchers.any[Callback]))
      .thenAnswer(new Answer[java.util.concurrent.Future[RecordMetadata]] {
        override def answer(invocation: InvocationOnMock) = {
          val record = invocation.getArguments()(0).asInstanceOf[ProducerRecord[K, V]]
          val callback = invocation.getArguments()(1).asInstanceOf[Callback]
          handler(record, callback).onComplete {
            case Success(value) if !closed => callback.onCompletion(value, null)
            case Success(_) => callback.onCompletion(null, new Exception("Kafka producer already closed"))
            case Failure(ex: Exception) => callback.onCompletion(null, ex)
            case Failure(throwableUnsupported) => throw new Exception("Throwable failure are not supported")
          }
          val result = new CompletableFuture[RecordMetadata]()
          result.completeExceptionally(new Exception("Not implemented yet"))
          result
        }
      })
    Mockito
      .when(result.close(mockito.ArgumentMatchers.any[java.time.Duration]))
      .thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock) =
          closed = true
      })
    result
  }

  def verifySend(mode: VerificationMode) =
    Mockito
      .verify(mock, mode)
      .send(mockito.ArgumentMatchers.any[ProducerRecord[K, V]], mockito.ArgumentMatchers.any[Callback])

  def verifyClosed() = {
    Mockito.verify(mock).flush()
    Mockito.verify(mock).close(mockito.ArgumentMatchers.any[java.time.Duration])
  }

  def verifyForceClosedInCallback() = {
    val inOrder = Mockito.inOrder(mock)
    // the force close from the async callback `failStageCb`
    inOrder.verify(mock).close(mockito.ArgumentMatchers.any[java.time.Duration])
    // the flush and close from `closeProducer`
    inOrder.verify(mock).flush()
    inOrder.verify(mock).close(mockito.ArgumentMatchers.any[java.time.Duration])
  }

  def verifyNoMoreInteractions() =
    Mockito.verifyNoMoreInteractions(mock)

  def verifyTxInitialized() = {
    val inOrder = Mockito.inOrder(mock)
    inOrder.verify(mock).initTransactions()
    inOrder.verify(mock).beginTransaction()
  }

  def verifyTxCommit(po: ConsumerMessage.PartitionOffset) = {
    val inOrder = Mockito.inOrder(mock)
    val offsets = Map(new TopicPartition(po.key.topic, po.key.partition) -> new OffsetAndMetadata(po.offset + 1)).asJava
    inOrder.verify(mock).sendOffsetsToTransaction(offsets, po.key.groupId)
    inOrder.verify(mock).commitTransaction()
    inOrder.verify(mock).beginTransaction()
  }

  def verifyTxCommitWhenShutdown(po: ConsumerMessage.PartitionOffset) = {
    val inOrder = Mockito.inOrder(mock)
    val offsets = Map(new TopicPartition(po.key.topic, po.key.partition) -> new OffsetAndMetadata(po.offset + 1)).asJava
    inOrder.verify(mock).sendOffsetsToTransaction(offsets, po.key.groupId)
    inOrder.verify(mock).commitTransaction()
  }

  def verifyTxAbort() = {
    val inOrder = Mockito.inOrder(mock)
    inOrder.verify(mock).abortTransaction()
    inOrder.verify(mock).flush()
    inOrder.verify(mock).close(mockito.ArgumentMatchers.any[java.time.Duration])
  }
}

class CommittedMarkerMock {
  val mock = Mockito.mock(classOf[CommittedMarker])
  when(
    mock.committed(mockito.ArgumentMatchers.any[Map[TopicPartition, OffsetAndMetadata]])
  ).thenAnswer(new Answer[Future[Done]] {
    override def answer(invocation: InvocationOnMock): Future[Done] =
      Future.successful(Done)
  })

  private[kafka] def verifyOffsets(pos: ConsumerMessage.PartitionOffsetCommittedMarker*): Future[Done] =
    Mockito
      .verify(mock, Mockito.only())
      .committed(
        mockito.ArgumentMatchers.eq(
          pos.map(p => new TopicPartition(p.key.topic, p.key.partition) -> new OffsetAndMetadata(p.offset + 1)).toMap
        )
      )
}

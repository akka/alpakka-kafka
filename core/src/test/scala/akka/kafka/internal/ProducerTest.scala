/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util.Date
import java.util.concurrent.{CompletableFuture, TimeUnit}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage._
import akka.kafka.ProducerSettings
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.scaladsl.Flow
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import akka.kafka.test.Utils._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.mockito.Matchers._
import org.mockito.Mockito
import Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.BeforeAndAfterAll

class ProducerTest(_system: ActorSystem)
    extends TestKit(_system) with FlatSpecLike
    with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem())

  override def afterAll(): Unit = shutdown(system)

  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(_system).withFuzzing(true)
  )
  implicit val stageStoppingTimeout = StageStoppingTimeout(15.seconds)
  implicit val ec = _system.dispatcher

  type K = String
  type V = String
  type Record = ProducerRecord[K, V]
  type Msg = Message[String, String, NotUsed.type]

  def recordAndMetadata(seed: Int) = {
    new ProducerRecord("test", seed.toString, seed.toString) ->
      new RecordMetadata(new TopicPartition("test", seed), seed.toLong, seed.toLong, new Date().getTime, java.lang.Long.valueOf(-1), -1, -1)
  }

  def toMessage(tuple: (Record, RecordMetadata)) = Message(tuple._1, NotUsed)
  def result(r: Record, m: RecordMetadata) = Result(m, Message(r, NotUsed))
  val toResult = (result _).tupled

  val settings = ProducerSettings(system, new StringSerializer, new StringSerializer)

  def testProducerFlow[P](mock: ProducerMock[K, V], closeOnStop: Boolean = true): Flow[Message[K, V, P], Result[K, V, P], NotUsed] =
    Flow.fromGraph(new ProducerStage[K, V, P](settings.closeTimeout, closeOnStop, () => mock.mock))
      .mapAsync(1)(identity)

  "Producer" should "not send messages when source is empty" in {
    assertAllStagesStopped {
      val client = new ProducerMock[K, V](ProducerMock.handlers.fail)

      val probe = Source
        .empty[Msg]
        .via(testProducerFlow(client))
        .runWith(TestSink.probe)

      probe
        .request(1)
        .expectComplete()

      client.verifySend(never())
      client.verifyClosed()
      client.verifyNoMoreInteractions()
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
      .toMat(Sink.lastOption)(Keep.both)
      .run()

    input.map(toMessage).foreach(source.sendNext)
    source.sendError(new Exception())

    // Here we can not be sure that all messages from source delivered to producer
    // because of buffers in akka-stream and faster error pushing that ignores buffers

    Await.ready(sink, remainingOrDefault)
    sink.value should matchPattern {
      case Some(Failure(_)) =>
    }

    client.verifyClosed()
    client.verifySend(atLeastOnce())
    client.verifyNoMoreInteractions()
  }

  it should "fail stream in case of fail to send message" in {
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
}

object ProducerMock {
  type Handler[K, V] = (ProducerRecord[K, V], Callback) => Future[RecordMetadata]
  object handlers {
    def fail[K, V]: Handler[K, V] = (_, _) => throw new Exception("Should not be called")
    def delayedMap[K, V](delay: FiniteDuration)(f: ProducerRecord[K, V] => Try[RecordMetadata])(implicit as: ActorSystem): Handler[K, V] = {
      (record, _) =>
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
    Mockito.when(result.send(any[ProducerRecord[K, V]], any[Callback])).thenAnswer(new Answer[java.util.concurrent.Future[RecordMetadata]] {
      override def answer(invocation: InvocationOnMock) = {
        val record = invocation.getArguments()(0).asInstanceOf[ProducerRecord[K, V]]
        val callback = invocation.getArguments()(1).asInstanceOf[Callback]
        handler(record, callback).onComplete {
          case Success(value) if !closed => callback.onCompletion(value, null)
          case Success(value) if closed => callback.onCompletion(null, new Exception("Kafka producer already closed"))
          case Failure(ex: Exception) => callback.onCompletion(null, ex)
          case Failure(throwableUnsupported) => throw new Exception("Throwable failure are not supported")
        }
        val result = new CompletableFuture[RecordMetadata]()
        result.completeExceptionally(new Exception("Not implemented yet"))
        result
      }
    })
    Mockito.when(result.close(any[Long], any[TimeUnit])).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock) = {
        closed = true
      }
    })
    result
  }

  def verifySend(mode: VerificationMode) = {
    Mockito.verify(mock, mode).send(any[ProducerRecord[K, V]], any[Callback])
  }

  def verifyClosed() = {
    Mockito.verify(mock).flush()
    Mockito.verify(mock).close(any[Long], any[TimeUnit])
  }

  def verifyNoMoreInteractions() = {
    Mockito.verifyNoMoreInteractions(mock)
  }
}

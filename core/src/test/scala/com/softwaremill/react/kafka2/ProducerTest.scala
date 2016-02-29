package com.softwaremill.react.kafka2

import java.util.concurrent.{CompletableFuture, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.testkit.scaladsl.{TestSource, TestSink}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.testkit.TestKit
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.language.postfixOps
import scala.util.{Try, Failure, Success}

/**
 * @author Alexey Romanchuk
 */
class ProducerTest(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers
    with MockitoSugar {
  def this() = this(ActorSystem())

  implicit val m = ActorMaterializer(ActorMaterializerSettings(_system))
  implicit val ec = _system.dispatcher

  type K = String
  type V = String
  type Record = ProducerRecord[K, V]

  def recordAndMetadata(seed: Int) = {
    new ProducerRecord("test", seed.toString, seed.toString) ->
      new RecordMetadata(new TopicPartition("test", seed), seed.toLong, seed.toLong)

  }

  "Producer" should "not send messages when source is empty" in {
    val client = new ProducerMock[K, V](ProducerMock.handlers.fail)

    val probe = Source
      .empty[Record]
      .via(Producer(() => client.mock))
      .runWith(TestSink.probe)

    probe
      .request(1)
      .expectComplete()

    client.verifySend(never())
    client.verifyClosed()
    client.verifyNoMoreInteractions()
    ()
  }

  it should "emit confirmation in same order as inputs" in {
    val input = 1 to 3 map recordAndMetadata

    val client = {
      val inputMap = input.toMap
      new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100 millis)(x => Try{ inputMap(x) }))
    }
    val probe = Source
      .fromIterator(() => input.map(_._1).toIterator)
      .via(Producer(() => client.mock))
      .mapAsync(1)(identity)
      .runWith(TestSink.probe)

    probe
      .request(10)
      .expectNextN(input)
      .expectComplete()

    client.verifyClosed()
    client.verifySend(atLeastOnce())
    client.verifyNoMoreInteractions()
    ()
  }

  it should "fail fast in case of source error" in {
    val input = 1 to 3 map recordAndMetadata

    val client = {
      val inputMap = input.toMap
      new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100 millis)(x => Try{ inputMap(x) }))
    }
    val (source, sink) = TestSource
      .probe[Record]
      .via(Producer(() => client.mock))
      .mapAsync(1)(identity)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    sink.request(100)
    input.map(_._1).foreach(source.sendNext)
    source.sendError(new Exception())

    sink.expectError()

    client.verifyClosed()
    client.verifySend(atLeastOnce())
    client.verifyNoMoreInteractions()
  }

  it should "fail future in case of fail to send message and did not fail stage" in {
    val input = 1 to 3 map recordAndMetadata
    val error = new Exception("Something wrong in kafka")

    val client = {
      val inputMap = input.toMap
      new ProducerMock[K, V](ProducerMock.handlers.delayedMap(100 millis) { msg =>
        if (msg.value() == "2") Failure(error)
        else Success(inputMap(msg))
      })
    }
    val (source, sink) = TestSource
      .probe[Record]
      .via(Producer(() => client.mock))
      .map { f => Await.ready(f, 1 second); f.value.get }
      .toMat(TestSink.probe)(Keep.both)
      .run()

    sink.request(100)
    input.map(_._1).foreach(source.sendNext)
    source.sendComplete()

    val expectedResult = input.map {
      case x @ (msg, _) if msg.value() == "2" => Failure(error)
      case x => Success(x)
    }
    sink
      .expectNextN(expectedResult)
      .expectComplete()

    client.verifyClosed()
    client.verifySend(atLeastOnce())
    client.verifyNoMoreInteractions()
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
          case Failure(ex) => ???
        }
        val result = new CompletableFuture[RecordMetadata]()
        result.completeExceptionally(new Exception("Not implemented yet"))
        result
      }
    })
    Mockito.when(result.close(any[Long], any[TimeUnit])).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock) = {
        closed = true
        ()
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

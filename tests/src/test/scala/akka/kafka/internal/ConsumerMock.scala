/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicBoolean

import akka.testkit.TestKit
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.mockito.{ArgumentMatchers, Mockito}

import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.collection.immutable.Seq
import scala.concurrent.duration._

object ConsumerMock {
  type OnCompleteHandler = Map[TopicPartition, OffsetAndMetadata] => (Map[TopicPartition, OffsetAndMetadata], Exception)

  def closeTimeout = 500.millis

  trait CommitHandler {
    def onCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit
    def onComplete(): Unit
    def allComplete(): Boolean
    def allComplete(minOffset: Long): Boolean
  }

  class NotImplementedHandler extends CommitHandler {
    def onCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit = ???
    def onComplete(): Unit = ???
    def allComplete(): Boolean = ???
    def allComplete(minOffset: Long): Boolean = ???
  }

  class LogHandler(val onCompleteHandler: OnCompleteHandler = offsets => (offsets, null)) extends CommitHandler {
    var calls: Seq[(Map[TopicPartition, OffsetAndMetadata], OffsetCommitCallback)] = Seq.empty
    var completed: Seq[(Map[TopicPartition, OffsetAndMetadata], OffsetCommitCallback)] = Seq.empty
    var maxOffset: Map[TopicPartition, OffsetAndMetadata] = Map.empty
    def onCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit =
      this.synchronized(calls :+= ((offsets, callback)))

    def onComplete(): Unit = this.synchronized {
      calls.filterNot(completed.contains).foreach {
        case call @ (offsets, callback) =>
          val (newOffsets, exception) = onCompleteHandler(offsets)
          callback.onComplete(newOffsets.asJava, exception)
          maxOffset = offsets.map {
            case (tp, thisOffset) =>
              val lastMaxOffset = maxOffset.getOrElse(tp, thisOffset)
              tp -> Seq(lastMaxOffset, thisOffset).maxBy(_.offset())
          }
          completed :+= call
      }
    }

    def allComplete(): Boolean = calls.size == completed.size
    def allComplete(minOffset: Long): Boolean = allComplete() && maxOffset.forall(_._2.offset() >= minOffset)
  }
}
class ConsumerMock[K, V](handler: ConsumerMock.CommitHandler = new ConsumerMock.NotImplementedHandler) {
  private var responses = collection.immutable.Queue.empty[Seq[ConsumerRecord[K, V]]]
  private var pendingSubscriptions = List.empty[(List[String], ConsumerRebalanceListener)]
  private var assignment = Set.empty[TopicPartition]
  private var messagesRequested = false
  val releaseCommitCallbacks = new AtomicBoolean()
  val mock = {
    val result = Mockito.mock(classOf[KafkaConsumer[K, V]])
    Mockito
      .when(result.poll(ArgumentMatchers.any[java.time.Duration]))
      .thenAnswer(new Answer[ConsumerRecords[K, V]] {
        override def answer(invocation: InvocationOnMock) = ConsumerMock.this.synchronized {
          pendingSubscriptions.foreach {
            case (topics, callback) =>
              val tps = topics.map { t =>
                new TopicPartition(t, 1)
              }
              assignment ++= tps
              callback.onPartitionsAssigned(tps.asJavaCollection)
          }
          pendingSubscriptions = List.empty
          val records = if (messagesRequested) {
            responses.dequeueOption
              .map {
                case (element, remains) =>
                  responses = remains
                  element
                    .groupBy(x => new TopicPartition(x.topic(), x.partition()))
                    .map {
                      case (topicPart, messages) => (topicPart, messages.asJava)
                    }
              }
              .getOrElse(Map.empty)
          } else Map.empty[TopicPartition, java.util.List[ConsumerRecord[K, V]]]
          // emulate commit callbacks in poll thread like in a real KafkaConsumer
          if (releaseCommitCallbacks.get()) {
            handler.onComplete()
          }
          new ConsumerRecords[K, V](records.asJava, java.util.Map.of())
        }
      })
    Mockito
      .when(
        result.commitAsync(ArgumentMatchers.any[java.util.Map[TopicPartition, OffsetAndMetadata]],
                           ArgumentMatchers.any[OffsetCommitCallback])
      )
      .thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock) = {
          val offsets = invocation.getArgument[java.util.Map[TopicPartition, OffsetAndMetadata]](0)
          val callback = invocation.getArgument[OffsetCommitCallback](1)
          handler.onCommitAsync(offsets.asScala.toMap, callback)
          ()
        }
      })
    Mockito
      .when(
        result.subscribe(ArgumentMatchers.any[java.util.List[String]], ArgumentMatchers.any[ConsumerRebalanceListener])
      )
      .thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock) = {
          val topics = invocation.getArgument[java.util.List[String]](0)
          val callback = invocation.getArgument[ConsumerRebalanceListener](1)
          pendingSubscriptions :+= (topics.asScala.toList -> callback)
          ()
        }
      })
    Mockito
      .when(result.resume(ArgumentMatchers.any[java.util.Collection[TopicPartition]]))
      .thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock) = {
          messagesRequested = true
          ()
        }
      })
    Mockito
      .when(result.pause(ArgumentMatchers.any[java.util.Collection[TopicPartition]]))
      .thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock) = {
          messagesRequested = false
          ()
        }
      })
    Mockito
      .when(result.assignment())
      .thenAnswer(new Answer[java.util.Set[TopicPartition]] {
        override def answer(invocation: InvocationOnMock) = assignment.asJava
      })
    result
  }

  def enqueue(records: Seq[ConsumerRecord[K, V]]): Unit =
    synchronized {
      responses :+= records
    }

  def verifyClosed(mode: VerificationMode = Mockito.times(1)): Unit =
    verify(mock, mode).close(CloseOptions.timeout(ConsumerMock.closeTimeout.toJava))

  def verifyPoll(mode: VerificationMode = Mockito.atLeastOnce()): ConsumerRecords[K, V] =
    verify(mock, mode).poll(ArgumentMatchers.any[java.time.Duration])

  def assignPartitions(tps: Set[TopicPartition]): Unit =
    tps.groupBy(_.topic()).foreach {
      case (topic, localTps) =>
        pendingSubscriptions
          .find {
            case (topics: List[String], _) =>
              topics.contains(topic)
          }
          .get
          ._2
          .onPartitionsAssigned(localTps.asJavaCollection)
    }

  def revokePartitions(tps: Set[TopicPartition]): Unit =
    tps.groupBy(_.topic()).foreach {
      case (topic, localTps) =>
        pendingSubscriptions
          .find {
            case (topics: List[String], _) =>
              topics.contains(topic)
          }
          .get
          ._2
          .onPartitionsRevoked(localTps.asJavaCollection)
    }

  def releaseAndAwaitCommitCallbacks(testkit: TestKit, minOffset: Long): Unit = {
    releaseCommitCallbacks.set(true)
    testkit.awaitCond(handler.allComplete(minOffset))
  }

  def releaseAndAwaitCommitCallbacks(testkit: TestKit): Unit = {
    releaseCommitCallbacks.set(true)
    testkit.awaitCond(handler.allComplete())
  }
}

class FailingConsumerMock[K, V](throwable: Throwable, failOnCallNumber: Int*) extends ConsumerMock[K, V] {
  var callNumber = 0

  Mockito
    .when(mock.poll(ArgumentMatchers.any[java.time.Duration]))
    .thenAnswer(new Answer[ConsumerRecords[K, V]] {
      override def answer(invocation: InvocationOnMock): ConsumerRecords[K, V] = FailingConsumerMock.this.synchronized {
        callNumber = callNumber + 1
        if (failOnCallNumber.contains(callNumber))
          throw throwable
        else
          new ConsumerRecords[K, V](Map.empty[TopicPartition, java.util.List[ConsumerRecord[K, V]]].asJava,
                                    java.util.Map.of())
      }
    })
}

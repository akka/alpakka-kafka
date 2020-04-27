/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

import java.util.concurrent.atomic.AtomicBoolean

import akka.util.JavaDurationConverters._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.mockito.{ArgumentMatchers, Mockito}

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.concurrent.duration._

object ConsumerMock {
  //type CommitHandler = (Map[TopicPartition, OffsetAndMetadata], OffsetCommitCallback) => Unit
  type OnCompleteHandler = Map[TopicPartition, OffsetAndMetadata] => (Map[TopicPartition, OffsetAndMetadata], Exception)

  def closeTimeout = 500.millis

  trait CommitHandler {
    def sendCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit
    def processOnComplete(): Unit
  }

  class NotImplementedHandler extends CommitHandler {
    def sendCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit = ???
    def processOnComplete(): Unit = ???
  }

  class LogHandler(val onCompleteHandler: OnCompleteHandler = offsets => (offsets, null)) extends CommitHandler {
    var calls: Seq[(Map[TopicPartition, OffsetAndMetadata], OffsetCommitCallback)] = Seq.empty
    def sendCommitAsync(offsets: Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit =
      this.synchronized {
        calls :+= ((offsets, callback))
      }

    def processOnComplete(): Unit = this.synchronized {
      calls.foreach {
        case (offsets, callback) =>
          val (newOffsets, exception) = onCompleteHandler(offsets)
          callback.onComplete(newOffsets.asJava, exception)
      }
      calls = Seq.empty
    }
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
            handler match {
              case h: ConsumerMock.LogHandler => h.processOnComplete()
              case _ => ()
            }
          }
          new ConsumerRecords[K, V](records.asJava)
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
          handler.sendCommitAsync(offsets.asScala.toMap, callback)
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

  def enqueue(records: Seq[ConsumerRecord[K, V]]) =
    synchronized {
      responses :+= records
    }

  def verifyClosed(mode: VerificationMode = Mockito.times(1)) =
    verify(mock, mode).close(ConsumerMock.closeTimeout.asJava)

  def verifyPoll(mode: VerificationMode = Mockito.atLeastOnce()) =
    verify(mock, mode).poll(ArgumentMatchers.any[java.time.Duration])

  def assignPartitions(tps: Set[TopicPartition]) =
    tps.groupBy(_.topic()).foreach {
      case (topic, localTps) =>
        pendingSubscriptions.find(_._1 == topic).get._2.onPartitionsAssigned(localTps.asJavaCollection)
    }

  def revokePartitions(tps: Set[TopicPartition]) =
    tps.groupBy(_.topic()).foreach {
      case (topic, localTps) =>
        pendingSubscriptions.find(_._1 == topic).get._2.onPartitionsRevoked(localTps.asJavaCollection)
    }
}

class FailingConsumerMock[K, V](throwable: Throwable, failOnCallNumber: Int*) extends ConsumerMock[K, V] {
  var callNumber = 0

  Mockito
    .when(mock.poll(ArgumentMatchers.any[java.time.Duration]))
    .thenAnswer(new Answer[ConsumerRecords[K, V]] {
      override def answer(invocation: InvocationOnMock) = FailingConsumerMock.this.synchronized {
        callNumber = callNumber + 1
        if (failOnCallNumber.contains(callNumber))
          throw throwable
        else new ConsumerRecords[K, V](Map.empty[TopicPartition, java.util.List[ConsumerRecord[K, V]]].asJava)
      }
    })
}

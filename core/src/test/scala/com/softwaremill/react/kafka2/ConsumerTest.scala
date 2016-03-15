package com.softwaremill.react.kafka2

import java.util.{Map => JMap}

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.scalatest.{FlatSpecLike, Matchers}

import scala.collection.JavaConversions._
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * @author Alexey Romanchuk
 */
object ConsumerTest {
  def record(seed: Int) = new ConsumerRecord[String, String]("topic", 1, seed.toLong, seed.toString, seed.toString)
  def commit(record: ConsumerRecord[_, _]) = Map(
    new TopicPartition(record.topic(), record.partition()) -> new OffsetAndMetadata(record.offset())
  )

  def noopFlow[K, V] = Flow[ConsumerRecord[K, V]].map { r =>
    Map(new TopicPartition(r.topic(), r.partition()) -> new OffsetAndMetadata(r.offset()))
  }
  def testFlow[I, O](implicit as: ActorSystem) = Flow.fromSinkAndSourceMat(TestSink.probe[I], TestSource.probe[O])(Keep.both)

  def graph[Key, Value, FlowMat, SinkMat](
    consumer: KafkaConsumer[Key, Value],
    processing: Flow[ConsumerRecord[Key, Value], Map[TopicPartition, OffsetAndMetadata], FlowMat],
    confirmation: Sink[Future[Map[TopicPartition, OffsetAndMetadata]], SinkMat]
  )(implicit m: Materializer) = {
    val graph = GraphDSL.create(
      new ManualCommitConsumer[Key, Value](() => consumer),
      processing,
      confirmation
    )(Tuple3.apply) { implicit b => (kafka, processing, confirmation) =>
        import GraphDSL.Implicits._

        kafka.messages ~> processing ~> kafka.commit
        kafka.confirmation ~> confirmation
        ClosedShape
      }
    RunnableGraph.fromGraph(graph).run()
  }
}

class ConsumerTest(_system: ActorSystem)
    extends TestKit(_system)
    with FlatSpecLike
    with Matchers {
  def this() = this(ActorSystem())

  implicit val m = ActorMaterializer(ActorMaterializerSettings(_system).withFuzzing(true))
  implicit val ec = _system.dispatcher

  type K = String
  type V = String
  type Record = ConsumerRecord[K, V]
  type CommitInfo = Map[TopicPartition, OffsetAndMetadata]

  import ConsumerTest._
  it should "complete graph when stream control.stop called" in {
    val mock = new ConsumerMock[K, V]()
    val (control, (in, out), sink) = graph(mock.mock, testFlow[Record, CommitInfo], TestSink.probe)

    sink.request(100)
    in.expectSubscription()

    control.stop()
    sink.expectNoMsg(200 millis)
    in.expectComplete()
    mock.verifyClosed(never())

    out.sendComplete()
    sink.expectComplete()
    mock.verifyClosed()
    ()
  }

  it should "complete graph when processing flow send complete" in {
    val mock = new ConsumerMock[K, V]()
    val (_, (in, out), sink) = graph(mock.mock, testFlow[Record, CommitInfo], TestSink.probe)

    sink.request(100)

    in.cancel()
    mock.verifyClosed(never())

    out.sendComplete()
    sink.expectComplete()
    mock.verifyClosed()
    ()
  }

  it should "complete graph when confirmation sink canceled" in {
    val mock = new ConsumerMock[K, V]()
    val (_, (in, out), sink) = graph(mock.mock, testFlow[Record, CommitInfo], TestSink.probe)

    sink.request(100)

    sink.cancel()
    out.expectCancellation()
    mock.verifyClosed(never())

    in.cancel()
    Thread.sleep(100) //we probably need some kind of Future related to lifecycle of graph shape instance
    mock.verifyClosed()
    ()
  }

  def checkMessagesReceiving(msgss: Seq[Seq[ConsumerRecord[K, V]]]) = {
    val mock = new ConsumerMock[K, V]()
    val (control, (in, out), _) = graph(mock.mock, testFlow[Record, CommitInfo], TestSink.probe)

    in.request(msgss.map(_.size).sum.toLong)
    msgss.foreach(mock.enqueue)
    in.expectNextN(msgss.flatten)

    control.stop()
    out.sendComplete()
    ()
  }

  val messages = (1 to 10000).map(record)
  it should "emit messages received as one big chunk" in {
    checkMessagesReceiving(Seq(messages))
  }

  it should "emit messages received as medium chunks" in {
    checkMessagesReceiving(messages.grouped(97).to[Seq])
  }

  it should "emit messages received as one message per chunk" in {
    checkMessagesReceiving(messages.grouped(1).to[Seq])
  }

  it should "emit messages received with empty some messages" in {
    checkMessagesReceiving(
      messages
      .grouped(97)
      .map(x => Seq(Seq.empty, x))
      .flatten
      .to[Seq]
    )
  }

  it should "call commitAsync for commit message and then complete future" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (_, (_, commitIn), confirmationOut) = graph(mock.mock, testFlow[Record, CommitInfo], TestSink.probe)

    //send message to commit
    confirmationOut.request(100)
    commitIn.sendNext(commit(record(1)))

    //expect future and one call to commitAsync
    val confirmation = confirmationOut.expectNext()
    commitLog.calls should have size (1)
    confirmation.isCompleted should be (false)

    //emulate commit
    commitLog.calls.head match {
      case (offsets, callback) => callback.onComplete(offsets, null)
    }

    // check that the future is successfully completed
    confirmation.value should be (Some(Success(commit(record(1)))))
  }

  it should "fail future in case of commit fail" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (_, (_, commitIn), confirmationOut) = graph(mock.mock, testFlow[Record, CommitInfo], TestSink.probe)

    //send message to commit
    confirmationOut.request(100)
    commitIn.sendNext(commit(record(1)))

    //expect future and one call to commitAsync
    val confirmation = confirmationOut.expectNext()
    confirmation.isCompleted should be (false)

    //emulate commit
    val failure = new Exception()
    commitLog.calls.head match {
      case (offsets, callback) => callback.onComplete(null, failure)
    }

    // check that the future is failed
    confirmation.value should be (Some(Failure(failure)))
  }

  it should "call commitAsync for every commit message (no commit batching)" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (_, (_, commitIn), confirmationOut) = graph(mock.mock, testFlow[Record, CommitInfo], TestSink.probe)

    //send message to commit
    confirmationOut.request(100)
    (1 to 100).foreach(x => commitIn.sendNext(commit(record(x))))

    confirmationOut.expectNextN(100)
    commitLog.calls should have size (100)
  }

  it should "keep graph running until all futures completed" in {
    val commitLog = new ConsumerMock.LogHandler()
    val mock = new ConsumerMock[K, V](commitLog)
    val (control, (messages, commitIn), confirmationOut) = graph(mock.mock, testFlow[Record, CommitInfo], TestSink.probe)

    //setup sinks
    messages.request(100)
    confirmationOut.request(100)

    //send a message
    commitIn.sendNext(commit(record(1)))

    //complete the graph
    commitIn.sendComplete()
    control.stop()

    messages.expectComplete()
    confirmationOut.expectNext()
    confirmationOut.expectNoMsg(200 millis)

    //emulate commit
    commitLog.calls.head match {
      case (offsets, callback) => callback.onComplete(offsets, null)
    }

    confirmationOut.expectComplete()
    mock.verifyClosed()
    ()
  }
}

object ConsumerMock {
  type CommitHandler = (Map[TopicPartition, OffsetAndMetadata], OffsetCommitCallback) => Unit

  def notImplementedHandler: CommitHandler = (_, _) => ???

  class LogHandler extends CommitHandler {
    var calls: Seq[(Map[TopicPartition, OffsetAndMetadata], OffsetCommitCallback)] = Seq.empty
    def apply(offsets: Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback) = {
      calls :+= ((offsets, callback))
    }
  }
}

class ConsumerMock[K, V](handler: ConsumerMock.CommitHandler = ConsumerMock.notImplementedHandler) { self =>
  var actions = collection.immutable.Queue.empty[Seq[ConsumerRecord[K, V]]]

  val mock = {
    val result = Mockito.mock(classOf[KafkaConsumer[K, V]])
    Mockito.when(result.poll(mockito.Matchers.any[Long])).thenAnswer(new Answer[ConsumerRecords[K, V]] {
      override def answer(invocation: InvocationOnMock) = self.synchronized {
        import scala.collection.JavaConversions._
        val records = actions.dequeueOption.map {
          case (element, remains) =>
            actions = remains
            element
              .groupBy(x => new TopicPartition(x.topic(), x.partition()))
              .map {
                case (topicPart, messages) => (topicPart, seqAsJavaList(messages))
              }
        }.getOrElse(Map.empty)
        new ConsumerRecords[K, V](records)
      }
    })
    Mockito.when(result.commitAsync(mockito.Matchers.any[JMap[TopicPartition, OffsetAndMetadata]], mockito.Matchers.any[OffsetCommitCallback])).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock) = {
        import scala.collection.JavaConversions._
        val offsets = invocation.getArgumentAt(0, classOf[JMap[TopicPartition, OffsetAndMetadata]])
        val callback = invocation.getArgumentAt(1, classOf[OffsetCommitCallback])
        handler(mapAsScalaMap(offsets).toMap, callback)
        ()
      }
    })
    result
  }

  def enqueue(records: Seq[ConsumerRecord[K, V]]) = {
    self.synchronized {
      actions :+= records
    }
    ()
  }

  def verifyClosed(mode: VerificationMode = Mockito.times(1)) = {
    verify(mock, mode).close()
  }

  def verifyPoll(mode: VerificationMode = Mockito.atLeastOnce()) = {
    verify(mock, mode).poll(mockito.Matchers.any[Long])
  }
}

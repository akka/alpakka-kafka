package com.softwaremill.react.kafka2

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.verification.VerificationMode
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.Future
import scala.language.postfixOps

/**
 * @author Alexey Romanchuk
 */
object ConsumerTest {
  def noopFlow[K, V] = Flow[ConsumerRecord[K, V]].map { r =>
    Map(new TopicPartition(r.topic(), r.partition()) -> new OffsetAndMetadata(r.offset()))
  }
  def testFlow[I, O](implicit as: ActorSystem) = Flow.fromSinkAndSourceMat(TestSink.probe[I], TestSource.probe[O])(Keep.both)

  def graph[Key, Value, FlowMat, SinkMat](
    provider: ConsumerMock[Key, Value],
    processing: Flow[ConsumerRecord[Key, Value], Map[TopicPartition, OffsetAndMetadata], FlowMat],
    confirmation: Sink[Future[Map[TopicPartition, OffsetAndMetadata]], SinkMat]
  )(implicit m: Materializer) = {
    val graph = GraphDSL.create(
      new ManualCommitConsumer[Key, Value](() => provider.mock),
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

  import scala.concurrent.duration._

  it should "complete graph when stream control.stop called" in {
    val mock = new ConsumerMock[K, V]()
    val (control, (in, out), sink) = graph(mock, testFlow[Record, CommitInfo], TestSink.probe)

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
    val (_, (in, out), sink) = graph(mock, testFlow[Record, CommitInfo], TestSink.probe)

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
    val (_, (in, out), sink) = graph(mock, testFlow[Record, CommitInfo], TestSink.probe)

    sink.request(100)

    sink.cancel()
    out.expectCancellation()
    mock.verifyClosed(never())

    in.cancel()
    Thread.sleep(100) //we probably need some kind of Future related to lifecycle of graph shape instance
    mock.verifyClosed()
    ()
  }
}

class ConsumerMock[K, V]() {
  var actions = collection.immutable.Queue.empty[Seq[ConsumerRecord[K, V]]]

  val mock = {
    val result = Mockito.mock(classOf[KafkaConsumer[K, V]])
    Mockito.when(result.poll(mockito.Matchers.any[Long])).thenAnswer(new Answer[ConsumerRecords[K, V]] {
      override def answer(invocation: InvocationOnMock) = this.synchronized {
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
    result
  }

  def enqueue(records: Seq[ConsumerRecord[K, V]]) = {
    this.synchronized {
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

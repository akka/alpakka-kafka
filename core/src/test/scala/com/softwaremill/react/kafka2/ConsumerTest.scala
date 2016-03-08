package com.softwaremill.react.kafka2

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito
import org.scalatest.mock.MockitoSugar
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
    with Matchers
    with MockitoSugar {
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
    mock.verifyNotClosed()

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
    mock.verifyNotClosed()

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
    mock.verifyNotClosed()

    in.cancel()
    Thread.sleep(100) //we probably need some kind of Future related to lifecycle of graph shape instance
    mock.verifyClosed()
    ()
  }

}

class ConsumerMock[K, V]() {
  val mock = {
    val result = Mockito.mock(classOf[KafkaConsumer[K, V]])
    result
  }

  def verifyClosed() = {
    Mockito.verify(mock).close()
  }

  def verifyNotClosed() = {
    Mockito.verify(mock, Mockito.never()).close()
  }
}
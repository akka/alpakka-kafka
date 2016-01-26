package com.softwaremill.react.kafka.benchmarks

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.stream.{ClosedShape, Materializer}
import akka.stream.scaladsl._
import com.softwaremill.react.kafka.ReactiveKafkaConsumer
import com.softwaremill.react.kafka.benchmarks.ReactiveKafkaBenchmark._
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.language.{existentials, postfixOps}
import scala.util.{Success, Failure, Try}

/**
 * Establishes a stream with source reading from Kafka (up to given number of elements). Also tests a sink that
 * receives commits.
 */
class TestFetchCommitTotal(
    f: Fixture,
    val elemCount: Long,
    val name: String,
    provideSource: Fixture => (SourceType, ReactiveKafkaConsumer[_, _], Sink[ConsumerRecord[String, String], Unit])
)(implicit m: Materializer) extends ReactiveKafkaPerfTest with QueuePreparations {

  var sourceOpt: Option[Source[ConsumerRecord[String, String], Unit]] = None
  var commitSinkOpt: Option[Sink[ConsumerRecord[String, String], Unit]] = None
  var consumerOpt: Option[ReactiveKafkaConsumer[_, _]] = None
  val bufferCheckTickMs = 100L
  val testTimeoutMs = 60000L

  override def warmup(): Unit = {
    val msgs = List.fill(elemCount.toInt)("message")
    givenQueueWithElements(msgs, f)
    val (src, consumer, commitSink) = provideSource(f)
    sourceOpt = Some(src)
    consumerOpt = Some(consumer)
    commitSinkOpt = Some(commitSink)
  }

  override def run(): Try[String] = {
    val resultOpt = for {
      source <- sourceOpt
      sink <- commitSinkOpt
    } yield {
      val buffer: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
      val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[Unit] =>
        import GraphDSL.Implicits._

        val in = source
        val out1 = sink
        val out2 = Sink.foreach((r: ConsumerRecord[String, String]) => {
          buffer.add(r.value())
          ()
        })

        val bcast = builder.add(Broadcast[ConsumerRecord[String, String]](2))
        in ~> bcast ~> out1
        bcast ~> out2
        ClosedShape
      })

      g.run()

      var timeoutMs = testTimeoutMs

      while (buffer.size() < elemCount && timeoutMs > 0) {
        timeoutMs = timeoutMs - bufferCheckTickMs
        Thread.sleep(bufferCheckTickMs)
      }
      if (buffer.size() < elemCount) {
        val errMsg = s"Timing out after $testTimeoutMs, collected ${buffer.size()} of $elemCount elements"
        println(errMsg)
        Failure(new Exception(errMsg))
      }
      else
        Success("Done")
    }
    val result = resultOpt.getOrElse(Failure(new IllegalStateException("Source not initialized")))
    consumerOpt.foreach(_.close())
    result
  }
}

object TestFetchCommitTotal extends SourceProviders {

  def prepare(host: String, elemCounts: List[Long])(implicit system: ActorSystem, m: Materializer) = {
    val pairs = elemCounts.map(count => {
      val fixtureForActorBased = new Fixture(host)
      val fixtureForGraphBased = new Fixture(host)
      (new TestFetchCommitTotal(fixtureForActorBased, count, s"Fetching $count elements with actor-based provider (+commit)", actorSourceProviderWithCommit(system)),
        new TestFetchCommitTotal(fixtureForGraphBased, count, s"Fetching $count elements with graphStage-based provider", graphSourceProviderWithCommit))
    })
    pairs.map(_._1) ++ pairs.map(_._2)
  }
}
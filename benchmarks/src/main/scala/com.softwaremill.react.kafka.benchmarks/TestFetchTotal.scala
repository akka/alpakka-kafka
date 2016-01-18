package com.softwaremill.react.kafka.benchmarks

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.ReactiveKafkaConsumer
import com.softwaremill.react.kafka.benchmarks.ReactiveKafkaBenchmark.SourceType
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.language.{existentials, postfixOps}
import scala.util.{Failure, Success, Try}

/**
 * Establishes a stream with source reading from Kafka (up to given number of elements).
 */
class TestFetchTotal(
    f: Fixture,
    val elemCount: Long,
    val name: String,
    provideSource: Fixture => (SourceType, ReactiveKafkaConsumer[_, _])
)(implicit m: Materializer) extends ReactiveKafkaPerfTest {

  val bufferCheckTickMs = 100L
  val testTimeoutMs = 60000L
  var sourceOpt: Option[Source[ConsumerRecord[String, String], Unit]] = None
  var consumerOpt: Option[ReactiveKafkaConsumer[_, _]] = None

  override def warmup(): Unit = {
    val (src, consumer) = provideSource(f)
    sourceOpt = Some(src)
    consumerOpt = Some(consumer)
  }

  override def run(): Try[String] = {
    val buffer: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
    val result = sourceOpt.map({ source =>
      source
        .map(_.value())
        .to(Sink.foreach(str => {
          buffer.add(str)
          ()
        }))
        .run()

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

    }).getOrElse(Failure(new IllegalStateException("Source not initialized")))
    consumerOpt.foreach(_.close())
    result
  }
}

object TestFetchTotal extends SourceProviders {

  def prepare(elemCounts: List[Long], f: Fixture)(implicit system: ActorSystem, m: Materializer) = {
    val pairs = elemCounts.map(count => {
      (new TestFetchTotal(f, count, s"Fetching $count elements with actor-based provider", actorSourceProviderNoCommit(system)),
        new TestFetchTotal(f, count, s"Fetching $count elements with graphStage-based provider", graphSourceProviderNoCommit))
    })
    pairs.map(_._1) ++ pairs.map(_._2)
  }
}
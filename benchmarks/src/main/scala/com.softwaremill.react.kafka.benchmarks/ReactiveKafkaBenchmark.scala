package com.softwaremill.react.kafka.benchmarks

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.language.{existentials, postfixOps}

case class Fixture(host: String, topic: String = ReactiveKafkaBenchmark.uuid(), group: String = ReactiveKafkaBenchmark.uuid())

object ReactiveKafkaBenchmark extends App with QueuePreparations {

  def uuid() = UUID.randomUUID().toString

  implicit lazy val system = ActorSystem("ReactiveKafkaBenchmark")
  implicit lazy val materializer = ActorMaterializer()
  type SourceType = Source[ConsumerRecord[String, String], Unit]
  val kafkaHost = "localhost:9092"

  val f = new Fixture(kafkaHost)
  val sizes = List(250000L, 500000L, 1000000L, 2000000L)
  val fetchTotalTests = TestFetchTotal.prepare(sizes, f)
  private val fetchTotalCommitTests = TestFetchCommitTotal.prepare(f.host, sizes)

  val warmup = {
    tests: List[ReactiveKafkaPerfTest] =>
      val maxElements = tests.last.elemCount
      println(s"Filling Kafka queue with $maxElements elements")
      val msgs = List.fill(maxElements.toInt)("message")
      givenQueueWithElements(msgs, f)
      println(s"Queue filled, reading all elements")
      ()
  }

  Timed.runTests(fetchTotalCommitTests, 30, warmup)
  system.shutdown()

}
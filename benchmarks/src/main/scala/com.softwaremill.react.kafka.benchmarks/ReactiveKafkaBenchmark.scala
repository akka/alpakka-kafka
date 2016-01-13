package com.softwaremill.react.kafka.benchmarks

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.softwaremill.react.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scala.language.{existentials, postfixOps}

case class Fixture(host: String, topic: String = ReactiveKafkaBenchmark.uuid(), group: String = ReactiveKafkaBenchmark.uuid())

object ReactiveKafkaBenchmark extends App {

  def uuid() = UUID.randomUUID().toString

  implicit val system = ActorSystem("ReactiveKafkaBenchmark")
  implicit val materializer = ActorMaterializer()
  type SourceType = Source[ConsumerRecord[String, String], Unit]
  val serializer = new StringSerializer()
  val kafkaHost = "localhost:9092"

  def createProducerProperties(f: Fixture): ProducerProperties[String, String] = {
    ProducerProperties(kafkaHost, f.topic, serializer, serializer)
  }

  def givenQueueWithElements(msgs: Seq[String], f: Fixture) = {
    val prodProps: ProducerProperties[String, String] = createProducerProperties(f)
    val producer = new KafkaProducer(prodProps.rawProperties, prodProps.keySerializer, prodProps.valueSerializer)
    msgs.foreach { msg =>
      producer.send(new ProducerRecord(f.topic, msg))
    }
    producer.close()
  }

  val f = new Fixture(kafkaHost)
  val testList = TestFetchTotal.prepare(List(450000L, 650000L, 900000L, 1500000L, 2000000L), f)
  val warmup = {
    tests: List[ReactiveKafkaPerfTest] =>
      val maxElements = tests.last.elemCount
      println(s"Filling Kafka queue with $maxElements elements")
      val msgs = List.fill(maxElements.toInt)("message")
      givenQueueWithElements(msgs, f)
      println(s"Queue filled, reading all elements")
      ()
  }
  Timed.runTests(testList, 50, warmup)
  system.shutdown()

}
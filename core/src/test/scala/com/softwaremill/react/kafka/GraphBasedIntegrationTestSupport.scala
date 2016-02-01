package com.softwaremill.react.kafka

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.StringEncoder
import org.scalatest.fixture
import scala.collection.JavaConverters._

trait GraphBasedIntegrationTestSupport extends fixture.Suite with KafkaTest {

  this: TestKit =>
  val InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it"

  def givenQueueWithElements(msgs: Seq[String])(implicit f: FixtureParam) = {
    val prodProps: ProducerProperties[String] = createProducerProperties(f)

    val producer = new Producer[Any, Any](prodProps.toProducerConfig)
    msgs.foreach { msg =>
      producer.send(new KeyedMessage(f.topic, msg.getBytes))
    }
    producer.close()
  }

  def verifyQueueHas(msgs: Seq[String])(implicit f: FixtureParam) =
    awaitCond {
      val source = createSource(f, consumerProperties(f)
        .kafkaOffsetsStorage()
        .noAutoCommit())
      val buffer: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
      source
        .map(_.message())
        .take(msgs.length.toLong)
        .runWith(Sink.foreach(str => { buffer.add(str); () }))
      Thread.sleep(3000) // read messages into buffer
      buffer.asScala.toSeq.sorted == msgs.sorted
    }

  def stringGraphSink(f: FixtureParam) = {
    f.kafka.graphStageSink(ProducerProperties(kafkaHost, f.topic, new StringEncoder()))
  }

  def givenInitializedTopic()(implicit f: FixtureParam) = {
    val props = createProducerProperties(f)
    val producer = new Producer[Any, Any](props.toProducerConfig)
    producer.send(new KeyedMessage(f.topic, InitialMsg.getBytes))
    producer.close()
  }

  def withFixture(test: OneArgTest) = {
    val topic = uuid()
    val group = uuid()
    val kafka = newKafka()
    val theFixture = FixtureParam(topic, group, kafka)
    withFixture(test.toNoArgTest(theFixture))
  }

  def uuid() = UUID.randomUUID().toString

}
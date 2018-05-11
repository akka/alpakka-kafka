/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, SinkQueueWithCancel}
import akka.testkit.TestKit
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest._

// Very dirty way of reproducing https://github.com/akka/reactive-kafka/issues/382
class MyTest extends TestKit(ActorSystem("IntegrationSpec"))
  with FlatSpecLike with MustMatchers with BeforeAndAfterAll
  with BeforeAndAfterEach with TypeCheckedTripleEquals {

  implicit val mat = ActorMaterializer()(system)
  implicit val ec = system.dispatcher
  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(9099, 2189, Map("offsets.topic.replication.factor" -> "1"))
  val bootstrapServers = s"localhost:${embeddedKafkaConfig.kafkaPort}"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    EmbeddedKafka.start()
    EmbeddedKafka.createCustomTopic("test1", partitions = 10)
    EmbeddedKafka.createCustomTopic("test2", partitions = 10)
  }

  override def afterAll(): Unit = {
    shutdown(system, 30.seconds)
    EmbeddedKafka.stop()
    super.afterAll()
  }

  private def extractAll(trie: scala.collection.concurrent.TrieMap[String, Unit], queue: SinkQueueWithCancel[String]): Future[Unit] = {
    queue.pull().flatMap { opt =>
      if (opt.isDefined) {
        trie.put(opt.get, ())
        extractAll(trie, queue)
      }
      else Future.successful(())
    }
  }

  ignore must "test that works" in {
    def getSinkQueue() = {
      Consumer.plainPartitionedSource(
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
          .withBootstrapServers(bootstrapServers)
          .withGroupId("mygroup")
          .withProperty("auto.offset.reset", "earliest"),
        Subscriptions.topics("test1")
      ).flatMapMerge(10, _._2)
        .map(_.value())
        .runWith(Sink.queue())
    }
    val trie = new scala.collection.concurrent.TrieMap[String, Unit]()
    val queue1 = getSinkQueue()
    val queue2 = getSinkQueue()
    extractAll(trie, queue1)
    extractAll(trie, queue2)
    Thread.sleep(10000)

    (1 to 2000).par.foreach { i =>
      EmbeddedKafka.publishStringMessageToKafka("test1", i.toString)
    }
    Thread.sleep(10000)
    println(s"Set size: ${trie.size}")
    println("Missing: " + ((1 to 2000).toSet -- trie.keys.map(_.toInt)))
    trie.size must be (2000)
    queue1.cancel()
    queue2.cancel()
  }

  it must "test that doesn't work" in {
    def getSinkQueue() = {
      Consumer.plainPartitionedSource(
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
          .withBootstrapServers(bootstrapServers)
          .withGroupId("mygroup2")
          .withProperty("auto.offset.reset", "earliest"),
        Subscriptions.topics("test2")
      ).flatMapMerge(10, _._2)
        .map(_.value())
        .runWith(Sink.queue())
    }
    val trie = new scala.collection.concurrent.TrieMap[String, Unit]()
    val queue1 = getSinkQueue()
    extractAll(trie, queue1)

    (1 to 1000).foreach { i =>
      EmbeddedKafka.publishStringMessageToKafka("test2", i.toString)
    }
    val queue2 = getSinkQueue()
    extractAll(trie, queue2)
    (1001 to 2000).foreach { i =>
      EmbeddedKafka.publishStringMessageToKafka("test2", i.toString)
    }
    Thread.sleep(10000)
    println(s"Set size: ${trie.size}")
    println("Missing: " + ((1 to 2000).toSet -- trie.keys.map(_.toInt)))
    trie.size must be (2000)
    Thread.sleep(2000)
    queue1.cancel()
    queue2.cancel()
    Thread.sleep(2000)
  }
}

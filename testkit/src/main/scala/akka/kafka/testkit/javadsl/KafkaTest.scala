/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl

import java.util.concurrent.{CompletionStage, TimeUnit}

import akka.Done
import akka.japi.Pair
import akka.kafka.Subscriptions
import akka.kafka.javadsl.Consumer
import akka.kafka.scaladsl.Producer
import akka.kafka.testkit.internal.KafkaTestKit
import akka.stream.Materializer
import akka.stream.javadsl.{Keep, Sink}
import akka.stream.scaladsl.Source
import akka.stream.testkit.javadsl.StreamTestKit
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api._
import org.slf4j.{Logger, LoggerFactory}

import scala.compat.java8.FutureConverters._

/**
 * JUnit 5 aka Jupiter base-class with some convenience for accessing
 * a Kafka broker.
 * Extending classes must be annotated with `@TestInstance(Lifecycle.PER_CLASS)` to create
 * a single instance of the test class with `@BeforeAll` and `@AfterAll` annotated methods called
 * by the test framework.
 */
abstract class KafkaTest extends KafkaTestKit {

  val log: Logger = LoggerFactory.getLogger(getClass)

  final val partition0 = 0

  def materializer: Materializer

  /**
   * Sets up the Admin client. Override if you want custom initialization of the admin client.
   */
  @BeforeAll def setUpAdmin() = setUpAdminClient()

  /**
   * Cleans up the Admin client. Override if you want custom cleaning up of the admin client.
   */
  @AfterAll def cleanUpAdmin() = cleanUpAdminClient()

  @AfterEach def checkForStageLeaks() = StreamTestKit.assertAllStagesStopped(materializer)

  /**
   * Overwrite to set different default timeout for [[KafkaTest.resultOf]].
   */
  def resultOfTimeout: java.time.Duration = java.time.Duration.ofSeconds(5)

  def produceString(topic: String, messageCount: Int, partition: Int): CompletionStage[Done] =
    Source(1 to messageCount)
      .map(_.toString)
      // NOTE: If no partition is specified but a key is present a partition will be chosen
      // using a hash of the key. If neither key nor partition is present a partition
      // will be assigned in a round-robin fashion.
      .map(n => new ProducerRecord(topic, partition, DefaultKey, n))
      .runWith(Producer.plainSink(producerDefaults))(materializer)
      .toJava

  private final type Records = java.util.List[ConsumerRecord[String, String]]

  protected def consumeString(topic: String, take: Long): Consumer.DrainingControl[Records] =
    Consumer
      .plainSource(consumerDefaults.withGroupId(createGroupId(1)), Subscriptions.topics(topic))
      .take(take)
      .toMat(Sink.seq, Keep.both[Consumer.Control, CompletionStage[Records]])
      .mapMaterializedValue(
        new akka.japi.function.Function[Pair[Consumer.Control, CompletionStage[Records]], Consumer.DrainingControl[
          Records
        ]] {
          override def apply(
              p: Pair[Consumer.Control, CompletionStage[Records]]
          ): Consumer.DrainingControl[Records] = Consumer.createDrainingControl(p)
        }
      )
      .run(materializer)

  @throws[Exception]
  protected def resultOf[T](stage: CompletionStage[T]): T = resultOf(stage, resultOfTimeout)

  @throws[Exception]
  protected def resultOf[T](stage: CompletionStage[T], timeout: java.time.Duration): T =
    stage.toCompletableFuture.get(timeout.toMillis, TimeUnit.MILLISECONDS)

}

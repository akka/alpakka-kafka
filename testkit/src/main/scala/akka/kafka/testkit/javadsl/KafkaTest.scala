/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.javadsl

import java.util.concurrent.{CompletionStage, TimeUnit}

import akka.Done
import akka.kafka.Subscriptions
import akka.kafka.javadsl.Consumer
import akka.kafka.scaladsl.Producer
import akka.kafka.testkit.internal.KafkaTestKit
import akka.stream.Materializer
import akka.stream.javadsl.{Keep, Sink}
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.{Logger, LoggerFactory}

import scala.compat.java8.FutureConverters._

abstract class KafkaTest extends KafkaTestKit {

  val log: Logger = LoggerFactory.getLogger(getClass)

  final val partition0 = 0

  def materializer: Materializer

  /**
   * Overwrite to set different default timout for [[KafkaTest.resultOf]].
   */
  def defaultTimeoutSeconds: Int = 5

  def produceString(topic: String, messageCount: Int, partition: Int): CompletionStage[Done] =
    Source(1 to messageCount)
      .map(_.toString)
      // NOTE: If no partition is specified but a key is present a partition will be chosen
      // using a hash of the key. If neither key nor partition is present a partition
      // will be assigned in a round-robin fashion.
      .map(n => new ProducerRecord(topic, partition, DefaultKey, n))
      .runWith(Producer.plainSink(producerDefaults))(materializer)
      .toJava

  protected def consumeString(topic: String,
                              take: Long): Consumer.DrainingControl[java.util.List[ConsumerRecord[String, String]]] =
    Consumer
      .plainSource(consumerDefaults.withGroupId(createGroupId(1)), Subscriptions.topics(topic))
      .take(take)
      .toMat(Sink.seq, Keep.both[Consumer.Control, CompletionStage[java.util.List[ConsumerRecord[String, String]]]])
      .mapMaterializedValue(p => Consumer.createDrainingControl(p))
      .run(materializer)

  @throws[Exception]
  protected def resultOf[T](stage: CompletionStage[T]): T =
    stage.toCompletableFuture.get(defaultTimeoutSeconds.toLong, TimeUnit.SECONDS)

  @throws[Exception]
  protected def resultOf[T](stage: CompletionStage[T], seconds: Int): T =
    stage.toCompletableFuture.get(seconds.toLong, TimeUnit.SECONDS)

}

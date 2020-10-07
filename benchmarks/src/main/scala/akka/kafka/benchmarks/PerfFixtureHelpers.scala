/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit
import java.util.{Arrays, Properties, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.postfixOps

object PerfFixtureHelpers {
  def stringOfSize(size: Int) = new String(Array.fill(size)('0'))

  def randomId(): String = UUID.randomUUID().toString

  case class FilledTopic(
      msgCount: Int,
      msgSize: Int,
      numberOfPartitions: Int = 1,
      replicationFactor: Int = 1,
      topic: String = randomId()
  ) {
    def freshTopic: FilledTopic = copy(topic = randomId())
  }
}

private[benchmarks] trait PerfFixtureHelpers extends LazyLogging {
  import PerfFixtureHelpers._

  val producerTimeout = 6 minutes
  val logPercentStep = 25
  val adminClientCloseTimeout = Duration.ofSeconds(5)
  val producerCloseTimeout = adminClientCloseTimeout

  def randomId(): String = PerfFixtureHelpers.randomId()

  def fillTopic(ft: FilledTopic, kafkaHost: String): Unit =
    initTopicAndProducer(ft, kafkaHost)

  def createTopic(ft: FilledTopic, kafkaHost: String): KafkaProducer[Array[Byte], String] = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
    val admin = AdminClient.create(props)
    val producer = createTopicAndFill(ft, props, admin)
    admin.close(adminClientCloseTimeout)
    producer
  }

  private def initTopicAndProducer(ft: FilledTopic, kafkaHost: String): Unit = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
    //
    val admin = AdminClient.create(props)
    val existing = admin.listTopics().names().get(10, TimeUnit.SECONDS)
    if (existing.contains(ft.topic)) {
      logger.info(s"Reusing existing topic $ft")
    } else {
      val producer = createTopicAndFill(ft, props, admin)
      producer.close(producerCloseTimeout)
    }
    admin.close(adminClientCloseTimeout)
  }

  private def createTopicAndFill(ft: FilledTopic, props: Properties, admin: AdminClient) = {
    val result = admin.createTopics(
      Arrays.asList(
        new NewTopic(ft.topic, ft.numberOfPartitions, ft.replicationFactor.toShort)
          .configs(new util.HashMap[String, String]())
      )
    )
    result.all().get(10, TimeUnit.SECONDS)
    // fill topic with messages
    val producer =
      new KafkaProducer[Array[Byte], String](props, new ByteArraySerializer, new StringSerializer)
    val lastElementStoredPromise = Promise[Unit]()
    val loggedStep = if (ft.msgCount > logPercentStep) ft.msgCount / (100 / logPercentStep) else 1
    val msg = stringOfSize(ft.msgSize)
    for (i <- 0L to ft.msgCount.toLong) {
      if (!lastElementStoredPromise.isCompleted) {
        val partition: Int = (i % ft.numberOfPartitions).toInt
        producer.send(
          new ProducerRecord[Array[Byte], String](ft.topic, partition, null, msg),
          new Callback {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit =
              if (e == null) {
                if (i % loggedStep == 0)
                  logger.info(s"Written $i elements to Kafka (${100 * i / ft.msgCount}%)")
                if (i >= ft.msgCount - 1 && !lastElementStoredPromise.isCompleted)
                  lastElementStoredPromise.success(())
              } else {
                if (!lastElementStoredPromise.isCompleted) {
                  e.printStackTrace()
                  lastElementStoredPromise.failure(e)
                }
              }
          }
        )
      }
    }
    val lastElementStoredFuture = lastElementStoredPromise.future
    Await.result(lastElementStoredFuture, atMost = producerTimeout)
    producer
  }
}

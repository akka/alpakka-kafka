/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.benchmarks

import java.util
import java.util.concurrent.TimeUnit
import java.util.{Arrays, Properties, UUID}

import akka.kafka.benchmarks.app.RunTestCommand
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.postfixOps

object PerfFixtureHelpers {
  def stringOfSize(size: Int) = new String(Array.fill(size)('0'))
}

private[benchmarks] trait PerfFixtureHelpers extends LazyLogging {
  import PerfFixtureHelpers._

  val producerTimeout = 6 minutes
  val logPercentStep = 1

  def randomId() = UUID.randomUUID().toString

  def fillTopic(topic: String, cmd: RunTestCommand): Unit = {
    val producer = initTopicAndProducer(topic, cmd)
    producer.close()
  }

  def initTopicAndProducer(topic: String, cmd: RunTestCommand): KafkaProducer[Array[Byte], String] = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cmd.kafkaHost)
    createTopic(props, topic, cmd.numberOfPartitions, cmd.replicationFactor)
    val producer =
      new KafkaProducer[Array[Byte], String](props, new ByteArraySerializer, new StringSerializer)
    val lastElementStoredPromise = Promise[Unit]
    val loggedStep = if (cmd.msgCount > logPercentStep) cmd.msgCount / (100 / logPercentStep) else 1
    val msg = stringOfSize(cmd.msgSize)
    for (i <- 0L to cmd.msgCount.toLong) {
      if (!lastElementStoredPromise.isCompleted) {
        val partition: Int = (i % cmd.numberOfPartitions).toInt
        producer.send(
          new ProducerRecord[Array[Byte], String](topic, partition, null, msg),
          new Callback {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit =
              if (e == null) {
                if (i % loggedStep == 0)
                  logger.info(s"Written $i elements to Kafka (${100 * i / cmd.msgCount}%)")
                if (i >= cmd.msgCount - 1 && !lastElementStoredPromise.isCompleted)
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

  def createTopic(props: Properties, topicName: String, partitions: Int, replicationFactor: Int) = {
    val admin = AdminClient.create(props)
    val result = admin.createTopics(
      Arrays.asList(
        new NewTopic(topicName, partitions, replicationFactor.toShort).configs(new util.HashMap[String, String]())
      )
    )
    result.all().get(10, TimeUnit.SECONDS)
    admin.close()
  }
}

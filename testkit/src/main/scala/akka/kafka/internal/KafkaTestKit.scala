/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import java.util
import java.util.{Arrays, Properties}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings}
import kafka.admin.{AdminClient => OldAdminClient}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

trait KafkaTestKit {

  def log: Logger

  val DefaultKey = "key"

  val producerDefaults =
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

  val consumerDefaults = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withWakeupTimeout(10.seconds)
    .withMaxWakeups(10)

  def nextNumber(): Int = KafkaTestKit.topicCounter.incrementAndGet()

  def createTopicName(number: Int) = s"topic-$number-${nextNumber}"

  def createGroupId(number: Int = 0) = s"group-$number-${nextNumber}"

  def createTransactionalId(number: Int = 0) = s"transactionalId-$number-${nextNumber}"

  def system: ActorSystem
  def bootstrapServers: String

  private val adminDefaults = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config
  }

  def adminClient(): AdminClient =
    AdminClient.create(adminDefaults)

  /**
   * Get an old admin client which is deprecated. However only this client allows access
   * to consumer group summaries
   *
   */
  def oldAdminClient(): OldAdminClient =
    OldAdminClient.create(adminDefaults)

  /**
   * Create a topic with given partition number and replication factor.
   *
   * This method will block and return only when the topic has been successfully created.
   */
  def createTopic(number: Int = 0, partitions: Int = 1, replication: Int = 1): String = {
    val topicName = createTopicName(number)
    val configs = new util.HashMap[String, String]()
    val createResult = adminClient().createTopics(
      Arrays.asList(new NewTopic(topicName, partitions, replication.toShort).configs(configs))
    )
    createResult.all().get(10, TimeUnit.SECONDS)
    topicName
  }

  def sleepMillis(ms: Int, msg: String): Unit = {
    log.debug(s"sleeping $ms ms $msg")
    Thread.sleep(ms)
  }

  def sleepSeconds(s: Int, msg: String): Unit = {
    log.debug(s"sleeping $s s $msg")
    Thread.sleep(s * 1000L)
  }
}

object KafkaTestKit {
  val topicCounter = new AtomicInteger()
}

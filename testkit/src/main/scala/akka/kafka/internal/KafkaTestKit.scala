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

import scala.concurrent.duration._

trait KafkaTestKit {
  val DefaultKey = "key"

  private val topicCounter = new AtomicInteger()

  val producerDefaults =
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

  val consumerDefaults = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withWakeupTimeout(10.seconds)
    .withMaxWakeups(10)

  def createTopicName(number: Int) = s"topic-$number-${topicCounter.incrementAndGet()}"

  def createGroupId(number: Int = 0) = s"group-$number-${topicCounter.incrementAndGet()}"

  def createTransactionalId(number: Int = 0) = s"transactionalId-$number-${topicCounter.incrementAndGet()}"

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
   * Create a topic with given partinion number and replication factor.
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
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.internal

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.Arrays

import akka.actor.ActorSystem
import akka.kafka.testkit.KafkaTestkitSettings
import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serializer, StringDeserializer, StringSerializer}
import org.slf4j.Logger

import scala.collection.JavaConverters._

/**
 * Common functions for scaladsl and javadsl Testkit.
 *
 * Mixed-in in both, scaladsl and javadsl classes, therefore API should be usable from both - Scala and Java.
 */
trait KafkaTestKit {

  def log: Logger

  val DefaultKey = "key"

  val StringSerializer = new StringSerializer
  val StringDeserializer = new StringDeserializer

  def producerDefaults: ProducerSettings[String, String] = producerDefaults(StringSerializer, StringSerializer)

  def producerDefaults[K, V](keySerializer: Serializer[K], valueSerializer: Serializer[V]): ProducerSettings[K, V] =
    ProducerSettings(system, keySerializer, valueSerializer)
      .withBootstrapServers(bootstrapServers)

  def consumerDefaults: ConsumerSettings[String, String] = consumerDefaults(StringDeserializer, StringDeserializer)

  def consumerDefaults[K, V](keyDeserializer: Deserializer[K],
                             valueDeserializer: Deserializer[V]): ConsumerSettings[K, V] =
    ConsumerSettings(system, keyDeserializer, valueDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private lazy val committerDefaultsInstance = CommitterSettings(system)

  def committerDefaults: CommitterSettings = committerDefaultsInstance

  private def nextNumber(): Int = KafkaTestKitClass.topicCounter.incrementAndGet()

  /**
   * Return a unique topic name.
   */
  def createTopicName(suffix: Int): String = s"topic-$suffix-${nextNumber()}"

  /**
   * Return a unique group id with a default suffix.
   */
  def createGroupId(): String = createGroupId(0)

  /**
   * Return a unique group id with a given suffix.
   */
  def createGroupId(suffix: Int): String = s"group-$suffix-${nextNumber()}"

  /**
   * Return a unique transactional id with a default suffix.
   */
  def createTransactionalId(): String = createTransactionalId(0)

  /**
   * Return a unique transactional id with a given suffix.
   */
  def createTransactionalId(suffix: Int): String = s"transactionalId-$suffix-${nextNumber()}"

  def system: ActorSystem
  def bootstrapServers: String

  val settings = KafkaTestkitSettings(system)

  private lazy val adminDefaults: java.util.Map[String, AnyRef] = {
    val config = new java.util.HashMap[String, AnyRef]()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config
  }

  private var adminClientVar: Admin = _

  /**
   * Access to the Kafka Admin client
   */
  def adminClient: Admin = {
    assert(
      adminClientVar != null,
      "admin client not created, be sure to call setupAdminClient() and cleanupAdminClient()"
    )
    adminClientVar
  }

  /**
   * Create internal admin clients.
   * Gives access to `adminClient`,
   * be sure to call `cleanUpAdminClient` after the tests are done.
   */
  def setUpAdminClient(): Unit =
    if (adminClientVar == null) {
      adminClientVar = Admin.create(adminDefaults)
    }

  /**
   * Close internal admin client instances.
   */
  def cleanUpAdminClient(): Unit =
    if (adminClientVar != null) {
      adminClientVar.close(Duration.ofSeconds(60))
      adminClientVar = null
    }

  /**
   * Create a topic with a default suffix, single partition, a replication factor of one, and no topic configuration.
   *
   * This method will block and return only when the topic has been successfully created.
   */
  def createTopic(): String = createTopic(0, 1, 1, Map[String, String]())

  /**
   * Create a topic with a given suffix, single partitions, a replication factor of one, and no topic configuration.
   *
   * This method will block and return only when the topic has been successfully created.
   */
  def createTopic(suffix: Int): String = createTopic(suffix, 1, 1, Map[String, String]())

  /**
   * Create a topic with a given suffix, partition number, a replication factor of one, and no topic configuration.
   *
   * This method will block and return only when the topic has been successfully created.
   */
  def createTopic(suffix: Int, partitions: Int): String =
    createTopic(suffix, partitions, 1, Map[String, String]())

  /**
   * Create a topic with given suffix, partition number, replication factor, and no topic configuration.
   *
   * This method will block and return only when the topic has been successfully created.
   */
  def createTopic(suffix: Int, partitions: Int, replication: Int): String =
    createTopic(suffix, partitions, replication, Map[String, String]())

  /**
   * Create a topic with given suffix, partition number, replication factor, and topic configuration.
   *
   * This method will block and return only when the topic has been successfully created.
   */
  def createTopic(
      suffix: Int,
      partitions: Int,
      replication: Int,
      config: scala.collection.Map[String, String]
  ): String =
    createTopic(suffix, partitions, replication, config.asJava)

  /**
   * Java Api
   *
   * Create a topic with given suffix, partition number, replication factor, and topic configuration.
   *
   * This method will block and return only when the topic has been successfully created.
   */
  def createTopic(suffix: Int, partitions: Int, replication: Int, config: java.util.Map[String, String]): String = {
    val topicName = createTopicName(suffix)
    val createResult = adminClient.createTopics(
      Arrays.asList(new NewTopic(topicName, partitions, replication.toShort).configs(config))
    )
    createResult.all().get(10, TimeUnit.SECONDS)
    topicName
  }

  def sleepMillis(ms: Long, msg: String): Unit = {
    log.debug(s"sleeping $ms ms $msg")
    Thread.sleep(ms)
  }

  def sleepSeconds(s: Int, msg: String): Unit = {
    log.debug(s"sleeping $s s $msg")
    Thread.sleep(s * 1000L)
  }
}

abstract class KafkaTestKitClass(override val system: ActorSystem, override val bootstrapServers: String)
    extends KafkaTestKit

object KafkaTestKitClass {
  val topicCounter = new AtomicInteger()
  def createReplicationFactorBrokerProps(replicationFactor: Int): Map[String, String] = Map(
    "offsets.topic.replication.factor" -> s"$replicationFactor",
    "transaction.state.log.replication.factor" -> s"$replicationFactor",
    "transaction.state.log.min.isr" -> s"$replicationFactor"
  )
}

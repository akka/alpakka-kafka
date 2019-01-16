/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.internal

import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Arrays, Properties}

import akka.actor.ActorSystem
import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.Logger

trait KafkaTestKit {

  def log: Logger

  val DefaultKey = "key"

  private lazy val producerDefaultsInstance: ProducerSettings[String, String] =
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

  def producerDefaults: ProducerSettings[String, String] = producerDefaultsInstance

  private lazy val consumerDefaultsInstance: ConsumerSettings[String, String] =
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def consumerDefaults: ConsumerSettings[String, String] = consumerDefaultsInstance

  private lazy val committerDefaultsInstance = CommitterSettings(system)

  def committerDefaults: CommitterSettings = committerDefaultsInstance

  def nextNumber(): Int = KafkaTestKitClass.topicCounter.incrementAndGet()

  def createTopicName(number: Int) = s"topic-$number-${nextNumber}"

  def createGroupId(number: Int = 0) = s"group-$number-${nextNumber}"

  def createTransactionalId(number: Int = 0) = s"transactionalId-$number-${nextNumber}"

  def system: ActorSystem
  def bootstrapServers: String

  private lazy val adminDefaults = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config
  }

  private var adminClientVar: AdminClient = _

  def adminClient: AdminClient = {
    assert(adminClientVar != null,
           "admin client not created, be sure to call setupAdminClient() and cleanupAdminClient()")
    adminClientVar
  }

  /**
   * Create internal admin clients.
   * Gives access to `adminClient`,
   * be sure to call `cleanUpAdminClient` after the tests are done.
   */
  def setUpAdminClient(): Unit =
    if (adminClientVar == null) {
      adminClientVar = AdminClient.create(adminDefaults)
    }

  /**
   * Close internal admin client instances.
   */
  def cleanUpAdminClient(): Unit =
    if (adminClientVar != null) {
      adminClientVar.close(60, TimeUnit.SECONDS)
      adminClientVar = null
    }

  /**
   * Create a topic with given partition number and replication factor.
   *
   * This method will block and return only when the topic has been successfully created.
   */
  def createTopic(number: Int = 0, partitions: Int = 1, replication: Int = 1): String = {
    val topicName = createTopicName(number)
    val configs = new util.HashMap[String, String]()
    val createResult = adminClient.createTopics(
      Arrays.asList(new NewTopic(topicName, partitions, replication.toShort).configs(configs))
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
    "offsets.topic.replication.factor" -> s"$replicationFactor"
  )
}

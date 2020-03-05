/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.cluster.sharding

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.typed.scaladsl.adapter._
import akka.actor.{Actor, ActorRef, ActorSystem, Address, ExtendedActorSystem, Extension, ExtensionId, Props}
import akka.annotation.{ApiMayChange, InternalApi}
import akka.cluster.sharding.external.ExternalShardAllocation
import akka.cluster.sharding.typed.{ShardingEnvelope, ShardingMessageExtractor}
import akka.cluster.typed.Cluster
import akka.event.Logging
import akka.kafka.scaladsl.MetadataClient
import akka.kafka.{ConsumerSettings, KafkaConsumerActor, TopicPartitionsAssigned}
import akka.util.Timeout._
import org.apache.kafka.common.utils.Utils

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
 * API MAY CHANGE
 *
 * Akka Extension to enable Akka Cluster External Sharding with Alpakka Kafka.
 */
@ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1074")
final class KafkaClusterSharding(system: ExtendedActorSystem) extends Extension {
  import KafkaClusterSharding._

  /**
   * API MAY CHANGE
   *
   * Asynchronously return a [[akka.cluster.sharding.typed.ShardingMessageExtractor]] with a default hashing strategy
   * based on Apache Kafka's [[org.apache.kafka.clients.producer.internals.DefaultPartitioner]].
   *
   * The number of partitions to use with the hashing strategy will be automatically determined by querying the Kafka
   * cluster for the number of partitions of a user provided [[topic]]. Use the [[settings]] parameter to configure
   * the Kafka Consumer connection required to retrieve the number of partitions. Each call to this method will result
   * in a round trip to Kafka. This method should only be called once per entity type [[M]], per local actor system.
   *
   * All topics used in a Consumer [[akka.kafka.Subscription]] must contain the same number of partitions to ensure
   * that entities are routed to the same Entity type.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1074")
  def messageExtractor[M](topic: String,
                          timeout: FiniteDuration,
                          settings: ConsumerSettings[_, _]): Future[KafkaShardingMessageExtractor[M]] =
    getPartitionCount(topic, timeout, settings).map(new KafkaShardingMessageExtractor[M](_))(system.dispatcher)

  /**
   * API MAY CHANGE
   *
   * Asynchronously return a [[akka.cluster.sharding.typed.ShardingMessageExtractor]] with a default hashing strategy
   * based on Apache Kafka's [[org.apache.kafka.clients.producer.internals.DefaultPartitioner]].
   *
   * The number of partitions to use with the hashing strategy is provided explicitly with [[kafkaPartitions]].
   *
   * All topics used in a Consumer [[akka.kafka.Subscription]] must contain the same number of partitions to ensure
   * that entities are routed to the same Entity type.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1074")
  def messageExtractor[M](kafkaPartitions: Int): KafkaShardingMessageExtractor[M] =
    new KafkaShardingMessageExtractor[M](kafkaPartitions)

  /**
   * API MAY CHANGE
   *
   * Asynchronously return a [[akka.cluster.sharding.typed.ShardingMessageExtractor]] with a default hashing strategy
   * based on Apache Kafka's [[org.apache.kafka.clients.producer.internals.DefaultPartitioner]].
   *
   * The number of partitions to use with the hashing strategy will be automatically determined by querying the Kafka
   * cluster for the number of partitions of a user provided [[topic]]. Use the [[settings]] parameter to configure
   * the Kafka Consumer connection required to retrieve the number of partitions. Use the [[entityIdExtractor]] to pick
   * a field from the Entity to use as the entity id for the hashing strategy. Each call to this method will result
   * in a round trip to Kafka. This method should only be called once per entity type [[M]], per local actor system.
   *
   * All topics used in a Consumer [[akka.kafka.Subscription]] must contain the same number of partitions to ensure
   * that entities are routed to the same Entity type.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1074")
  def messageExtractorNoEnvelope[M](topic: String,
                                    timeout: FiniteDuration,
                                    entityIdExtractor: M => String,
                                    settings: ConsumerSettings[_, _]): Future[KafkaShardingNoEnvelopeExtractor[M]] =
    getPartitionCount(topic, timeout, settings)
      .map(partitions => new KafkaShardingNoEnvelopeExtractor[M](partitions, entityIdExtractor))(system.dispatcher)

  /**
   * API MAY CHANGE
   *
   * Asynchronously return a [[akka.cluster.sharding.typed.ShardingMessageExtractor]] with a default hashing strategy
   * based on Apache Kafka's [[org.apache.kafka.clients.producer.internals.DefaultPartitioner]].
   *
   * The number of partitions to use with the hashing strategy is provided explicitly with [[kafkaPartitions]].
   *
   * All topics used in a Consumer [[akka.kafka.Subscription]] must contain the same number of partitions to ensure
   * that entities are routed to the same Entity type.
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1074")
  def messageExtractorNoEnvelope[M](kafkaPartitions: Int,
                                    entityIdExtractor: M => String): KafkaShardingNoEnvelopeExtractor[M] =
    new KafkaShardingNoEnvelopeExtractor[M](kafkaPartitions, entityIdExtractor)

  private val metadataConsumerActorNum = new AtomicInteger
  private def getPartitionCount[M](topic: String,
                                   timeout: FiniteDuration,
                                   settings: ConsumerSettings[_, _]): Future[Int] = {
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    val num = metadataConsumerActorNum.getAndIncrement()
    val consumerActor = system
      .systemActorOf(KafkaConsumerActor.props(settings), s"metadata-consumer-actor-$num")
    val metadataClient = MetadataClient.create(consumerActor, timeout)
    val numPartitions = metadataClient.getPartitionsFor(topic).map(_.length)
    numPartitions.onComplete(_ => system.stop(consumerActor))
    numPartitions.map { count =>
      system.log.info("Retrieved {} partitions for topic '{}'", count, topic)
      count
    }
  }

  private val shardingRebalanceListenerActorNum = new AtomicInteger

  /**
   * API MAY CHANGE
   *
   * Create an Alpakka Kafka rebalance listener that handles [[TopicPartitionsAssigned]] events. The [[typeKeyName]] is
   * used to create the [[ExternalShardAllocation]] client. When partitions are assigned to this consumer group member
   * the rebalance listener will use the [[ExternalShardAllocation]] client to update the External Sharding strategy
   * accordingly so that entities are (eventually) routed to the local Akka cluster member.
   *
   * Returns an Akka classic [[akka.actor.ActorRef]] that can be passed to an Alpakka Kafka [[ConsumerSettings]].
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1074")
  def rebalanceListener(typeKeyName: String): ActorRef = {
    val num = shardingRebalanceListenerActorNum.getAndIncrement()
    system.systemActorOf(Props(new RebalanceListener(typeKeyName)), s"kafka-cluster-sharding-rebalance-listener-$num")
  }

  /**
   * API MAY CHANGE
   *
   * Create an Alpakka Kafka rebalance listener that handles [[TopicPartitionsAssigned]] events. The [[typeKeyName]] is
   * used to create the [[ExternalShardAllocation]] client. When partitions are assigned to this consumer group member
   * the rebalance listener will use the [[ExternalShardAllocation]] client to update the External Sharding strategy
   * accordingly so that entities are (eventually) routed to the local Akka cluster member.
   *
   * Returns an Akka classic [[akka.actor.ActorRef]] that can be passed to an Alpakka Kafka [[ConsumerSettings]].
   */
  @ApiMayChange(issue = "https://github.com/akka/alpakka-kafka/issues/1074")
  def rebalanceListener(otherSystem: ActorSystem, typeKeyName: String): ActorRef = {
    val num = shardingRebalanceListenerActorNum.getAndIncrement()
    otherSystem
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(Props(new RebalanceListener(typeKeyName)), s"kafka-cluster-sharding-rebalance-listener-$num")
  }
}

object KafkaClusterSharding extends ExtensionId[KafkaClusterSharding] {
  @InternalApi
  sealed trait KafkaClusterShardingContract {
    def kafkaPartitions: Int
    def shardId(entityId: String): String = {
      // simplified version of Kafka's `DefaultPartitioner` implementation
      val partition = org.apache.kafka.common.utils.Utils
          .toPositive(Utils.murmur2(entityId.getBytes())) % kafkaPartitions
      partition.toString
    }
  }

  @InternalApi
  final class KafkaShardingMessageExtractor[M] private[kafka] (val kafkaPartitions: Int)
      extends ShardingMessageExtractor[ShardingEnvelope[M], M]
      with KafkaClusterShardingContract {
    override def entityId(envelope: ShardingEnvelope[M]): String = envelope.entityId
    override def unwrapMessage(envelope: ShardingEnvelope[M]): M = envelope.message
  }

  @InternalApi
  final class KafkaShardingNoEnvelopeExtractor[M] private[kafka] (val kafkaPartitions: Int,
                                                                  entityIdExtractor: M => String)
      extends ShardingMessageExtractor[M, M]
      with KafkaClusterShardingContract {
    override def entityId(message: M): String = entityIdExtractor(message)
    override def unwrapMessage(message: M): M = message
  }

  @InternalApi
  private[kafka] final class RebalanceListener(typeKeyName: String) extends Actor {
    private val log = Logging(context.system, this)
    private val shardAllocationClient = ExternalShardAllocation(context.system).clientFor(typeKeyName)
    private val address: Address = Cluster(context.system.toTyped).selfMember.address

    override def receive: Receive = {
      case TopicPartitionsAssigned(_, partitions) =>
        implicit val ec: ExecutionContextExecutor = context.classicActorContext.dispatcher
        val partitionsList = partitions.mkString(",")
        log.info("Consumer group '{}' is assigning topic partitions to cluster member '{}': [{}]",
                 typeKeyName,
                 address,
                 partitionsList)
        val updates = partitions.map { tp =>
          val shardId = tp.partition().toString
          // the Kafka partition number becomes the akka shard id
          // TODO: Should the shard allocation client support assigning more than 1 shard id at once?
          shardAllocationClient.updateShardLocation(shardId, address)
        }
        Future
          .sequence(updates)
          // Each Future returns successfully once a majority of cluster nodes receive the update. There's no point
          // blocking here because the rebalance listener is triggered asynchronously. If we want to block during
          // rebalance then we should provide an implementation using the `PartitionAssignmentHandler` instead
          .onComplete {
            case Success(_) =>
              log.info("Completed consumer group '{}' assignment of topic partitions to cluster member '{}': [{}]",
                       typeKeyName,
                       address,
                       partitionsList)
            case Failure(ex) =>
              log.error("A failure occurred while updating cluster shards", ex)
          }
    }
  }

  override def createExtension(system: ExtendedActorSystem): KafkaClusterSharding =
    new KafkaClusterSharding(system)
}

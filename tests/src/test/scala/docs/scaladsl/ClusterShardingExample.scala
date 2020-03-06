/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import akka.actor.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.sharding.external.ExternalShardAllocationStrategy
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.kafka.cluster.sharding.KafkaClusterSharding
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerRebalanceEvent, ConsumerSettings, Subscriptions}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object ClusterShardingExample {
  val typedSystem: ActorSystem[Nothing] = ActorSystem(Behaviors.empty[Nothing], "ClusterShardingExample")
  val classicSystem = typedSystem.toClassic
  val kafkaBootstrapServers = "localhost:9092"

  implicit val ec = typedSystem.executionContext

  def userBehaviour(): Behavior[User] = Behaviors.empty[User]

  // #user-entity
  final case class User(id: String, name: String)
  // #user-entity

  // #message-extractor
  // automatically retrieving the number of partitions requires a round trip to a Kafka broker
  val messageExtractor: Future[KafkaClusterSharding.KafkaShardingNoEnvelopeExtractor[User]] =
    KafkaClusterSharding(typedSystem.toClassic).messageExtractorNoEnvelope(
      timeout = 10.seconds,
      topic = "user-topic",
      entityIdExtractor = (msg: User) => msg.id,
      settings = ConsumerSettings(classicSystem, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(kafkaBootstrapServers)
    )
  // #message-extractor

  // #setup-cluster-sharding
  // create an Akka Cluster Sharding `EntityTypeKey` for `User` for this Kafka Consumer Group
  val groupId = "user-topic-group-id"
  val typeKey = EntityTypeKey[User](groupId)

  messageExtractor.onComplete {
    case Success(extractor) =>
      ClusterSharding(typedSystem).init(
        Entity(typeKey)(createBehavior = _ => userBehaviour())
          .withAllocationStrategy(new ExternalShardAllocationStrategy(typedSystem, typeKey.name))
          .withMessageExtractor(extractor)
          .withSettings(ClusterShardingSettings(typedSystem))
      )
    case Failure(ex) => typedSystem.log.error("An error occurred while obtaining the message extractor", ex)
  }
  // #setup-cluster-sharding

  // #rebalance-listener
  // obtain an Akka classic ActorRef that will handle consumer group rebalance events
  val rebalanceListener: akka.actor.typed.ActorRef[ConsumerRebalanceEvent] =
    KafkaClusterSharding(classicSystem).rebalanceListener(typeKey)

  // convert the rebalance listener to a classic ActorRef
  import akka.actor.typed.scaladsl.adapter._
  val rebalanceListenerClassic: akka.actor.ActorRef = rebalanceListener.toClassic

  val consumerSettings =
    ConsumerSettings(classicSystem, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(kafkaBootstrapServers)
      .withGroupId(typeKey.name) // use the same group id as we used in the `EntityTypeKey` for `User`

  // pass the rebalance listener to the topic subscription
  val subscription = Subscriptions
    .topics("user-topic")
    .withRebalanceListener(rebalanceListenerClassic)

  val consumer = Consumer.plainSource(consumerSettings, subscription)
  // #rebalance-listener
}

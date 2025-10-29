/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2025 Lightbend Inc. <https://akka.io>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.kafka.{scaladsl, ConsumerSettings, ProducerSettings}
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.jdk.FunctionConverters._
import scala.jdk.FutureConverters._

/**
 * Scala API.
 *
 * Reads Kafka bootstrap servers from configured sources via [[akka.discovery.Discovery]] configuration.
 */
object DiscoverySupport {

  /**
   * Expects a `service` section in the given Config and reads the given service name's address
   * to be used as `bootstrapServers`.
   */
  def consumerBootstrapServers[K, V](
      config: Config,
      system: ClassicActorSystemProvider
  ): java.util.function.Function[ConsumerSettings[K, V], CompletionStage[ConsumerSettings[K, V]]] = {
    implicit val sys: ClassicActorSystemProvider = system
    val function: ConsumerSettings[K, V] => Future[ConsumerSettings[K, V]] =
      scaladsl.DiscoverySupport.consumerBootstrapServers(config)
    function.andThen(_.asJava).asJava
  }

  // kept for bin-compatibility
  def consumerBootstrapServers[K, V](
      config: Config,
      system: ActorSystem
  ): java.util.function.Function[ConsumerSettings[K, V], CompletionStage[ConsumerSettings[K, V]]] = {
    val sys: ClassicActorSystemProvider = system
    consumerBootstrapServers(config, sys)
  }

  /**
   * Expects a `service` section in the given Config and reads the given service name's address
   * to be used as `bootstrapServers`.
   */
  def producerBootstrapServers[K, V](
      config: Config,
      system: ClassicActorSystemProvider
  ): java.util.function.Function[ProducerSettings[K, V], CompletionStage[ProducerSettings[K, V]]] = {
    implicit val sys: ClassicActorSystemProvider = system
    val function: ProducerSettings[K, V] => Future[ProducerSettings[K, V]] =
      scaladsl.DiscoverySupport.producerBootstrapServers(config)
    function.andThen(_.asJava).asJava
  }

  // kept for bin-compatibility
  def producerBootstrapServers[K, V](
      config: Config,
      system: ActorSystem
  ): java.util.function.Function[ProducerSettings[K, V], CompletionStage[ProducerSettings[K, V]]] = {
    val sys: ClassicActorSystemProvider = system
    producerBootstrapServers(config, sys)
  }
}

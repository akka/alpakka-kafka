/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import akka.annotation.InternalApi
import akka.kafka.{scaladsl, ConsumerSettings, ProducerSettings}
import com.typesafe.config.Config

import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters
import scala.concurrent.Future

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
    function.andThen(FutureConverters.toJava).asJava
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
    function.andThen(FutureConverters.toJava).asJava
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

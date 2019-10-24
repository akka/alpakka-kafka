/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage

import akka.actor.ActorSystem
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
      system: ActorSystem
  ): java.util.function.Function[ConsumerSettings[K, V], CompletionStage[ConsumerSettings[K, V]]] = {
    val function: ConsumerSettings[K, V] => Future[ConsumerSettings[K, V]] =
      scaladsl.DiscoverySupport.consumerBootstrapServers(config)(system)
    function.andThen(FutureConverters.toJava).asJava
  }

  /**
   * Expects a `service` section in the given Config and reads the given service name's address
   * to be used as `bootstrapServers`.
   */
  def producerBootstrapServers[K, V](
      config: Config,
      system: ActorSystem
  ): java.util.function.Function[ProducerSettings[K, V], CompletionStage[ProducerSettings[K, V]]] = {
    val function: ProducerSettings[K, V] => Future[ProducerSettings[K, V]] =
      scaladsl.DiscoverySupport.producerBootstrapServers(config)(system)
    function.andThen(FutureConverters.toJava).asJava
  }

}

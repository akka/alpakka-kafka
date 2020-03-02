/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.{ActorSystem, ActorSystemImpl}
import akka.annotation.InternalApi
import akka.discovery.{Discovery, ServiceDiscovery}
import akka.kafka.{ConsumerSettings, ProducerSettings}
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure

/**
 * Scala API.
 *
 * Reads Kafka bootstrap servers from configured sources via [[akka.discovery.Discovery]] configuration.
 */
object DiscoverySupport {

  // used for initial discovery of contact points
  private def discovery(config: Config)(implicit system: ActorSystem): ServiceDiscovery =
    config.getString("discovery-method") match {
      case "akka.discovery" =>
        Discovery(system).discovery

      case otherDiscoveryMechanism =>
        Discovery(system).loadServiceDiscovery(otherDiscoveryMechanism)
    }

  /**
   * Use Akka Discovery to read the addresses for `serviceName` within `lookupTimeout`.
   */
  private def bootstrapServers(
      discovery: ServiceDiscovery,
      serviceName: String,
      lookupTimeout: FiniteDuration
  )(implicit system: ActorSystem): Future[String] = {
    import system.dispatcher
    discovery.lookup(serviceName, lookupTimeout).map { resolved =>
      resolved.addresses
        .map { target =>
          val port = target.port
            .getOrElse(throw new IllegalArgumentException(s"port missing for $serviceName ${target.host}"))
          s"${target.host}:$port"
        }
        .mkString(",")
    }
  }

  /**
   * Internal API.
   *
   * Expect a `service` section in Config and use Akka Discovery to read the addresses for `name` within `lookup-timeout`.
   */
  @InternalApi
  private[kafka] def bootstrapServers(config: Config)(implicit system: ActorSystem): Future[String] = {
    checkClassOrThrow(system.asInstanceOf[ActorSystemImpl])
    val serviceName = config.getString("service-name")
    if (serviceName.nonEmpty) {
      val lookupTimeout = config.getDuration("resolve-timeout").asScala
      bootstrapServers(discovery(config), serviceName, lookupTimeout)
    } else throw new IllegalArgumentException(s"value for `service-name` in $config is empty")
  }

  /**
   * Expects a `service` section in the given Config and reads the given service name's address
   * to be used as `bootstrapServers`.
   */
  def consumerBootstrapServers[K, V](
      config: Config
  )(implicit system: ActorSystem): ConsumerSettings[K, V] => Future[ConsumerSettings[K, V]] = {
    import system.dispatcher
    settings =>
      bootstrapServers(config)
        .map { bootstrapServers =>
          settings.withBootstrapServers(bootstrapServers)
        }
  }

  /**
   * Expects a `service` section in the given Config and reads the given service name's address
   * to be used as `bootstrapServers`.
   */
  def producerBootstrapServers[K, V](
      config: Config
  )(implicit system: ActorSystem): ProducerSettings[K, V] => Future[ProducerSettings[K, V]] = {
    import system.dispatcher
    settings =>
      bootstrapServers(config)
        .map { bootstrapServers =>
          settings.withBootstrapServers(bootstrapServers)
        }
  }

  private def checkClassOrThrow(system: ActorSystemImpl): Unit =
    system.dynamicAccess.getClassFor("akka.discovery.Discovery$") match {
      case Failure(_: ClassNotFoundException | _: NoClassDefFoundError) =>
        throw new IllegalStateException(
          s"Akka Discovery is being used but the `akka-discovery` library is not on the classpath, it must be added explicitly. See https://doc.akka.io/docs/alpakka-kafka/current/discovery.html"
        )
      case _ =>
    }

}

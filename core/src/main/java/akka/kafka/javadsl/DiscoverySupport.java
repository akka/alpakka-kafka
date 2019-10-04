/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerSettings;
import akka.kafka.scaladsl.DiscoverySupport$;
import com.typesafe.config.Config;

import java.util.concurrent.CompletionStage;

/**
 * Java API.
 *
 * <p>Reads Kafka bootstrap servers from configured sources via [[akka.discovery.Discovery]]
 * configuration.
 */
public final class DiscoverySupport {

  private DiscoverySupport() {}

  /**
   * Expects a `service` section in the given Config and reads the given service name's address to
   * be used as `bootstrapServers`.
   */
  public static <K, V>
      java.util.function.Function<ConsumerSettings<K, V>, CompletionStage<ConsumerSettings<K, V>>>
          consumerBootstrapServers(Config config, ActorSystem system) {
    return DiscoverySupport$.MODULE$.consumerBootstrapServersCompletionStage(config, system);
  }

  /**
   * Expects a `service` section in the given Config and reads the given service name's address to
   * be used as `bootstrapServers`.
   */
  public static <K, V>
      java.util.function.Function<ProducerSettings<K, V>, CompletionStage<ProducerSettings<K, V>>>
          producerBootstrapServers(Config config, ActorSystem system) {
    return DiscoverySupport$.MODULE$.producerBootstrapServersCompletionStage(config, system);
  }
}

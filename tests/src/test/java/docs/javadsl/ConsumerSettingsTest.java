/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.ConsumerSettingsSpec$;
// #discovery-settings
import akka.kafka.javadsl.DiscoverySupport;
// #discovery-settings
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ConsumerSettingsTest {

  @Test
  public void discoverySetup() throws Exception {
    Config config =
        ConfigFactory.parseString(ConsumerSettingsSpec$.MODULE$.DiscoveryConfigSection())
            .withFallback(ConfigFactory.load())
            .resolve();
    ActorSystem system = ActorSystem.create("ConsumerSettingsTest", config);

    // #discovery-settings

    Config consumerConfig = system.settings().config().getConfig("discovery-consumer");
    ConsumerSettings<String, String> settings =
        ConsumerSettings.create(consumerConfig, new StringDeserializer(), new StringDeserializer())
            .withEnrichCompletionStage(
                DiscoverySupport.consumerBootstrapServers(consumerConfig, system));
    // #discovery-settings
    TestKit.shutdownActorSystem(system);
  }

  @Test
  public void setAssignor() throws Exception {
    ActorSystem system = ActorSystem.create("ConsumerSettingsTest");
    ConsumerSettings<String, String> settings = ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
            .withPartitionAssignmentStrategies(new String[] {
                    org.apache.kafka.clients.consumer.CooperativeStickyAssignor.class.getName(),
                    org.apache.kafka.clients.consumer.StickyAssignor.class.getName()
            });
    assertEquals(
            settings.getProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor,org.apache.kafka.clients.consumer.StickyAssignor");
  }
}

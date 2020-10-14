/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

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
    // #discovery-settings
    @SuppressWarnings("deprecation")
    // #discovery-settings
    ConsumerSettings<String, String> settings =
        ConsumerSettings.create(consumerConfig, new StringDeserializer(), new StringDeserializer())
            .withEnrichCompletionStage(
                DiscoverySupport.consumerBootstrapServers(consumerConfig, system));
    // #discovery-settings
    TestKit.shutdownActorSystem(system);
  }
}

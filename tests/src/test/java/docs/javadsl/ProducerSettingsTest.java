/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.ProducerSettingsSpec$;
// #discovery-settings
import akka.kafka.javadsl.DiscoverySupport;
// #discovery-settings
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

public class ProducerSettingsTest {

  @Test
  public void discoverySetup() throws Exception {
    Config config =
        ConfigFactory.parseString(ProducerSettingsSpec$.MODULE$.DiscoveryConfigSection())
            .withFallback(ConfigFactory.load())
            .resolve();
    ActorSystem system = ActorSystem.create("ProducerSettingsTest", config);

    // #discovery-settings

    Config producerConfig = system.settings().config().getConfig("discovery-producer");
    ProducerSettings<String, String> settings =
        ProducerSettings.create(producerConfig, new StringSerializer(), new StringSerializer())
            .withEnrichCompletionStage(
                DiscoverySupport.producerBootstrapServers(producerConfig, system));
    // #discovery-settings
    TestKit.shutdownActorSystem(system);
  }
}

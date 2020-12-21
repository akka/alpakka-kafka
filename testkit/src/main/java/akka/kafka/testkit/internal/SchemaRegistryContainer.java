/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.internal;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
  // Align these confluent platform constants with testkit/src/main/resources/reference.conf
  public static final DockerImageName DEFAULT_SCHEMA_REGISTRY_IMAGE_NAME =
      DockerImageName.parse("confluentinc/cp-schema-registry")
          .withTag(AlpakkaKafkaContainer.DEFAULT_CONFLUENT_PLATFORM_VERSION);

  public static int SCHEMA_REGISTRY_PORT = 8081;

  public SchemaRegistryContainer() {
    this(DEFAULT_SCHEMA_REGISTRY_IMAGE_NAME);
  }

  public SchemaRegistryContainer(final DockerImageName schemaRegistryImage) {
    super(schemaRegistryImage);

    withNetwork(Network.SHARED);
    withExposedPorts(SCHEMA_REGISTRY_PORT);
    withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
    withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT);
  }

  public SchemaRegistryContainer withCluster(KafkaContainerCluster cluster) {
    withNetwork(cluster.getNetwork());
    withEnv(
        "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
        "PLAINTEXT://" + cluster.getInternalNetworkBootstrapServers());
    return self();
  }

  public String getSchemaRegistryUrl() {
    return String.format(
        "http://%s:%s", getContainerIpAddress(), getMappedPort(SCHEMA_REGISTRY_PORT));
  }
}

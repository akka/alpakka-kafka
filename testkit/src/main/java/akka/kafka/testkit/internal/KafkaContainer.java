/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.internal;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.Base58;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.util.concurrent.TimeUnit;

/**
 * This container wraps Confluent Kafka and Zookeeper (optionally)
 *
 * <p>NOTE: This may be deleted when/if the upstream testcontainers-java PR is merged:
 * https://github.com/testcontainers/testcontainers-java/pull/1984
 */
public class KafkaContainer extends GenericContainer<KafkaContainer> {

  public static final int DEFAULT_KAFKA_PORT = 9093;

  public static final String DEFAULT_KAFKA_BROKER_ID = "1";

  public static final int DEFAULT_INTERNAL_TOPIC_RF = 1;

  public static final int ZOOKEEPER_PORT = 2181;

  public static final String CONFLUENT_PLATFORM_VERSION = "5.2.1";

  private static final int PORT_NOT_ASSIGNED = -1;

  protected String externalZookeeperConnect = null;

  private int port = PORT_NOT_ASSIGNED;
  private int exposedPort;

  public KafkaContainer() {
    this(CONFLUENT_PLATFORM_VERSION);
  }

  public KafkaContainer(String confluentPlatformVersion) {
    this(
        confluentPlatformVersion,
        DEFAULT_KAFKA_PORT,
        DEFAULT_KAFKA_BROKER_ID,
        DEFAULT_INTERNAL_TOPIC_RF);
  }

  public KafkaContainer(
      String confluentPlatformVersion, int exposedPort, String brokerId, int internalTopicRf) {
    super(
        TestcontainersConfiguration.getInstance().getKafkaImage() + ":" + confluentPlatformVersion);

    this.exposedPort = exposedPort;
    // TODO Only for backward compatibility
    withNetwork(Network.newNetwork());
    withNetworkAliases("kafka-" + Base58.randomString(6));
    withExposedPorts(exposedPort);

    // Use two listeners with different names, it will force Kafka to communicate with itself via
    // internal
    // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set, otherwise Kafka will try to use the
    // advertised listener
    withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + exposedPort + ",BROKER://0.0.0.0:9092");
    withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
    withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

    withEnv("KAFKA_BROKER_ID", brokerId);
    withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicRf + "");
    withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicRf + "");
    withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicRf + "");
    withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicRf + "");
    withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
    withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
  }

  public KafkaContainer withEmbeddedZookeeper() {
    externalZookeeperConnect = null;
    return self();
  }

  public KafkaContainer withExternalZookeeper(String connectString) {
    externalZookeeperConnect = connectString;
    return self();
  }

  public String getBootstrapServers() {
    if (port == PORT_NOT_ASSIGNED) {
      throw new IllegalStateException("You should start Kafka container first");
    }
    return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), port);
  }

  @Override
  protected void doStart() {
    withCommand("sleep infinity");

    if (externalZookeeperConnect == null) {
      addExposedPort(ZOOKEEPER_PORT);
    }

    super.doStart();
  }

  @Override
  protected void containerIsStarting(InspectContainerResponse containerInfo) {
    super.containerIsStarting(containerInfo);

    port = getMappedPort(this.exposedPort);

    final String zookeeperConnect;
    if (externalZookeeperConnect != null) {
      zookeeperConnect = externalZookeeperConnect;
    } else {
      zookeeperConnect = startZookeeper();
    }

    String internalIp = containerInfo.getNetworkSettings().getIpAddress();

    ExecCreateCmdResponse execCreateCmdResponse =
        dockerClient
            .execCreateCmd(getContainerId())
            .withCmd(
                "sh",
                "-c",
                ""
                    + "export KAFKA_ZOOKEEPER_CONNECT="
                    + zookeeperConnect
                    + "\n"
                    + "export KAFKA_ADVERTISED_LISTENERS="
                    + getBootstrapServers()
                    + ","
                    + String.format("BROKER://%s:9092", internalIp)
                    + "\n"
                    + "/etc/confluent/docker/run")
            .exec();

    try {
      dockerClient
          .execStartCmd(execCreateCmdResponse.getId())
          .exec(new ExecStartResultCallback())
          .awaitStarted(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private String startZookeeper() {
    ExecCreateCmdResponse execCreateCmdResponse =
        dockerClient
            .execCreateCmd(getContainerId())
            .withCmd(
                "sh",
                "-c",
                ""
                    + "printf 'clientPort="
                    + ZOOKEEPER_PORT
                    + "\ndataDir=/var/lib/zookeeper/data\ndataLogDir=/var/lib/zookeeper/log' > /zookeeper.properties\n"
                    + "zookeeper-server-start /zookeeper.properties\n")
            .exec();

    try {
      dockerClient
          .execStartCmd(execCreateCmdResponse.getId())
          .exec(new ExecStartResultCallback())
          .awaitStarted(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return "localhost:" + ZOOKEEPER_PORT;
  }
}

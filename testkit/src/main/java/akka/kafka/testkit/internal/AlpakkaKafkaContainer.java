/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.internal;

import akka.annotation.InternalApi;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This container wraps Confluent Kafka and Zookeeper (optionally)
 *
 * <p>This is a copy of KafkaContainer from testcontainers/testcontainers-java that we can tweak as
 * needed
 */
@InternalApi
public class AlpakkaKafkaContainer extends GenericContainer<AlpakkaKafkaContainer> {

  private static final String START_STOP_SCRIPT = "/testcontainers_start_stop_wrapper.sh";

  private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

  // Align these confluent platform constants with testkit/src/main/resources/reference.conf
  public static final String DEFAULT_CONFLUENT_PLATFORM_VERSION = "6.0.1";

  public static final DockerImageName DEFAULT_ZOOKEEPER_IMAGE_NAME =
      DockerImageName.parse("confluentinc/cp-zookeeper")
          .withTag(DEFAULT_CONFLUENT_PLATFORM_VERSION);

  public static final DockerImageName DEFAULT_KAFKA_IMAGE_NAME =
      DockerImageName.parse("confluentinc/cp-kafka").withTag(DEFAULT_CONFLUENT_PLATFORM_VERSION);

  public static final int KAFKA_PORT = 9093;

  public static final int KAFKA_JMX_PORT = 49999;

  public static final int ZOOKEEPER_PORT = 2181;

  private static final int PORT_NOT_ASSIGNED = -1;

  protected String externalZookeeperConnect = null;

  private int brokerNum = 1;

  private int port = PORT_NOT_ASSIGNED;
  private int jmxPort = PORT_NOT_ASSIGNED;

  private boolean useImplicitNetwork = true;

  private boolean enableRemoteJmxService = false;

  public AlpakkaKafkaContainer() {
    this(DEFAULT_KAFKA_IMAGE_NAME);
  }

  public AlpakkaKafkaContainer(String confluentPlatformVersion) {
    this(DEFAULT_KAFKA_IMAGE_NAME.withTag(confluentPlatformVersion));
  }

  public AlpakkaKafkaContainer(final DockerImageName dockerImageName) {
    super(dockerImageName);

    super.withNetwork(Network.SHARED);

    withExposedPorts(KAFKA_PORT);

    withBrokerNum(this.brokerNum);

    // Use two listeners with different names, it will force Kafka to communicate with itself via
    // internal
    // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set, otherwise Kafka will try to use the
    // advertised listener
    withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:9092");
    withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
    withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

    withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
    withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
    withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
    withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
  }

  @Override
  public AlpakkaKafkaContainer withNetwork(Network network) {
    useImplicitNetwork = false;
    return super.withNetwork(network);
  }

  public AlpakkaKafkaContainer withBrokerNum(int brokerNum) {
    if (brokerNum != this.brokerNum) {
      this.brokerNum = brokerNum;
      return super.withNetworkAliases("broker-" + this.brokerNum)
          .withEnv("KAFKA_BROKER_ID", "" + this.brokerNum);
    }
    return this;
  }

  @Override
  public Network getNetwork() {
    if (useImplicitNetwork) {
      // TODO Only for backward compatibility, to be removed soon
      logger()
          .warn(
              "Deprecation warning! "
                  + "KafkaContainer#getNetwork without an explicitly set network. "
                  + "Consider using KafkaContainer#withNetwork",
              new Exception("Deprecated method"));
    }
    return super.getNetwork();
  }

  public int getBrokerNum() {
    return brokerNum;
  }

  public void stopKafka() {
    try {
      ExecResult execResult = execInContainer("sh", "-c", "touch /tmp/stop");
      if (execResult.getExitCode() != 0) throw new Exception(execResult.getStderr());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public void startKafka() {
    try {
      ExecResult execResult = execInContainer("sh", "-c", "touch /tmp/start");
      if (execResult.getExitCode() != 0) throw new Exception(execResult.getStderr());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public AlpakkaKafkaContainer withEmbeddedZookeeper() {
    externalZookeeperConnect = null;
    return self();
  }

  public AlpakkaKafkaContainer withExternalZookeeper(String connectString) {
    externalZookeeperConnect = connectString;
    return self();
  }

  public AlpakkaKafkaContainer withRemoteJmxService() {
    enableRemoteJmxService = true;
    return self();
  }

  public String getBootstrapServers() {
    if (port == PORT_NOT_ASSIGNED) {
      throw new IllegalStateException("You should start Kafka container first");
    }
    return String.format("PLAINTEXT://%s:%s", getContainerIpAddress(), port);
  }

  public String getJmxServiceUrl() {
    if (jmxPort == PORT_NOT_ASSIGNED) {
      throw new IllegalStateException("You should start Kafka container first");
    }

    return String.format(
        "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi", getContainerIpAddress(), jmxPort);
  }

  @Override
  protected void doStart() {
    withCommand(
        "sh",
        "-c",
        "while [ ! -f " + START_STOP_SCRIPT + " ]; do sleep 0.1; done; " + START_STOP_SCRIPT);

    if (externalZookeeperConnect == null) {
      addExposedPort(ZOOKEEPER_PORT);
    }
    if (enableRemoteJmxService) {
      addExposedPort(KAFKA_JMX_PORT);
    }

    super.doStart();
  }

  @Override
  protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
    try {
      super.containerIsStarting(containerInfo, reused);

      port = getMappedPort(KAFKA_PORT);
      if (enableRemoteJmxService) {
        jmxPort = getMappedPort(KAFKA_JMX_PORT);
      }

      if (reused) {
        return;
      }

      String command = "#!/bin/bash\n";
      final String zookeeperConnect;
      if (externalZookeeperConnect != null) {
        zookeeperConnect = externalZookeeperConnect;
      } else {
        zookeeperConnect = "localhost:" + ZOOKEEPER_PORT;
        command += "echo 'clientPort=" + ZOOKEEPER_PORT + "' > zookeeper.properties\n";
        command += "echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties\n";
        command += "echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties\n";
        command += "zookeeper-server-start zookeeper.properties &\n";
      }

      List<String> internalIps =
          containerInfo.getNetworkSettings().getNetworks().values().stream()
              .map(ContainerNetwork::getIpAddress)
              .collect(Collectors.toList());

      command += "export KAFKA_ZOOKEEPER_CONNECT='" + zookeeperConnect + "'\n";
      command +=
          "export KAFKA_ADVERTISED_LISTENERS='"
              + Stream.concat(
                      Stream.of(getBootstrapServers()),
                      internalIps.stream().map(ip -> "BROKER://" + ip + ":9092"))
                  .collect(Collectors.joining(","))
              + "'\n";

      if (enableRemoteJmxService) {
        String jmxIp =
            internalIps.stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Could not find IP address for JMX"));
        command += "export KAFKA_JMX_PORT='" + KAFKA_JMX_PORT + "' \n";
        command += "export KAFKA_JMX_HOSTNAME='" + jmxIp + "' \n";
      }

      command += ". /etc/confluent/docker/bash-config \n";
      command += "/etc/confluent/docker/configure \n";
      command += "/etc/confluent/docker/launch \n";

      copyFileToContainer(
          Transferable.of(command.getBytes(StandardCharsets.UTF_8), 0777), STARTER_SCRIPT);

      // start and stop the Kafka broker process without stopping the container
      String startStopWrapper =
          "#!/bin/bash\n"
              + "STARTER_SCRIPT='"
              + STARTER_SCRIPT
              + "'\n"
              + "touch /tmp/start\n"
              + "while :; do\n"
              + "\tif [ -f $STARTER_SCRIPT ]; then\n"
              + "\t\tif [ -f /tmp/stop ]; then rm /tmp/stop; /usr/bin/kafka-server-stop;\n"
              + "\t\telif [ -f /tmp/start ]; then rm /tmp/start; bash -c \"$STARTER_SCRIPT &\";fi\n"
              + "\tfi\n"
              + "\tsleep 0.1\n"
              + "done\n";

      copyFileToContainer(
          Transferable.of(startStopWrapper.getBytes(StandardCharsets.UTF_8), 0777),
          START_STOP_SCRIPT);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}

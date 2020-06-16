/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.internal;

import akka.annotation.InternalApi;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;

/** Provides an easy way to launch a Kafka cluster with multiple brokers. */
@InternalApi
public class KafkaContainerCluster implements Startable {

  public static final String CONFLUENT_PLATFORM_VERSION =
      AlpakkaKafkaContainer.DEFAULT_CP_PLATFORM_VERSION;
  public static final int START_TIMEOUT_SECONDS = 1200;

  private static final String TEST_SCRIPT = "/testcontainers_test.sh";
  private static final String TEST_TOPIC = "test-kafka-container-cluster";

  private final int brokersNum;
  private final Network network;
  private final GenericContainer zookeeper;
  private final Collection<AlpakkaKafkaContainer> brokers;
  private final DockerClient dockerClient = DockerClientFactory.instance().client();

  public KafkaContainerCluster(int brokersNum, int internalTopicsRf) {
    this(CONFLUENT_PLATFORM_VERSION, brokersNum, internalTopicsRf);
  }

  public KafkaContainerCluster(
      String confluentPlatformVersion, int brokersNum, int internalTopicsRf) {
    if (brokersNum < 0) {
      throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
    }
    if (internalTopicsRf < 0 || internalTopicsRf > brokersNum) {
      throw new IllegalArgumentException(
          "internalTopicsRf '"
              + internalTopicsRf
              + "' must be less than brokersNum and greater than 0");
    }

    this.brokersNum = brokersNum;
    this.network = Network.newNetwork();

    this.zookeeper =
        new GenericContainer("confluentinc/cp-zookeeper:" + confluentPlatformVersion)
            .withNetwork(network)
            .withNetworkAliases("zookeeper")
            .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(AlpakkaKafkaContainer.ZOOKEEPER_PORT));

    this.brokers =
        IntStream.range(0, this.brokersNum)
            .mapToObj(
                brokerNum ->
                    new AlpakkaKafkaContainer(confluentPlatformVersion)
                        .withNetwork(this.network)
                        .withNetworkAliases("broker-" + brokerNum)
                        .withRemoteJmxService()
                        .dependsOn(this.zookeeper)
                        .withExternalZookeeper("zookeeper:" + AlpakkaKafkaContainer.ZOOKEEPER_PORT)
                        .withEnv("KAFKA_BROKER_ID", brokerNum + "")
                        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf + "")
                        .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf + "")
                        .withEnv(
                            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf + "")
                        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicsRf + ""))
            .collect(Collectors.toList());
  }

  public Network getNetwork() {
    return this.network;
  }

  public GenericContainer getZooKeeper() {
    return this.zookeeper;
  }

  public Collection<AlpakkaKafkaContainer> getBrokers() {
    return this.brokers;
  }

  public String getBootstrapServers() {
    return brokers.stream()
        .map(AlpakkaKafkaContainer::getBootstrapServers)
        .collect(Collectors.joining(","));
  }

  private Stream<GenericContainer> allContainers() {
    Stream<GenericContainer> genericBrokers = this.brokers.stream().map(b -> b);
    Stream<GenericContainer> zookeeper = Stream.of(this.zookeeper);
    return Stream.concat(genericBrokers, zookeeper);
  }

  @Override
  public void start() {
    try {
      Stream<Startable> startables = this.brokers.stream().map(b -> b);
      Startables.deepStart(startables).get(START_TIMEOUT_SECONDS, SECONDS);

      // assert that cluster has formed
      Unreliables.retryUntilTrue(
          START_TIMEOUT_SECONDS,
          TimeUnit.SECONDS,
          () ->
              Stream.of(this.zookeeper)
                  .map(this::clusterBrokers)
                  .anyMatch(brokers -> brokers.split(",").length == this.brokersNum));

      this.brokers.stream()
          .findFirst()
          .ifPresent(
              broker -> {
                broker.copyFileToContainer(
                    Transferable.of(testScript().getBytes(StandardCharsets.UTF_8), 700),
                    TEST_SCRIPT);
              });

      // test produce & consume message with full cluster involvement
      Unreliables.retryUntilTrue(
          START_TIMEOUT_SECONDS,
          TimeUnit.SECONDS,
          () -> this.brokers.stream().findFirst().map(this::runTest).orElse(false));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private String clusterBrokers(GenericContainer c) {
    try {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      dockerClient
          .execStartCmd(
              dockerClient
                  .execCreateCmd(c.getContainerId())
                  .withAttachStdout(true)
                  .withCmd(
                      "sh",
                      "-c",
                      "zookeeper-shell zookeeper:"
                          + AlpakkaKafkaContainer.ZOOKEEPER_PORT
                          + " ls /brokers/ids | tail -n 1")
                  .exec()
                  .getId())
          .exec(new ExecStartResultCallback(outputStream, null))
          .awaitCompletion();
      return outputStream.toString();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Adapted from Kafka test template in incubator/kafka from Helm hub
   * (https://github.com/helm/charts)
   * https://github.com/helm/charts/blob/master/incubator/kafka/templates/tests/test_topic_create_consume_produce.yaml
   */
  private String testScript() {
    String command = "#!/bin/bash \n";
    command += "set -e \n";
    command +=
        "kafka-topics --bootstrap-server localhost:9092 --delete --topic "
            + TEST_TOPIC
            + " || echo \"topic does not exist\" \n";
    command +=
        "kafka-topics --bootstrap-server localhost:9092 --topic "
            + TEST_TOPIC
            + " --create --partitions "
            + this.brokersNum
            + " --replication-factor "
            + this.brokersNum
            + " --config min.insync.replicas="
            + this.brokersNum
            + " \n";
    command += "MESSAGE=\"`date -u`\" \n";
    command +=
        "echo \"$MESSAGE\" | kafka-console-producer --broker-list localhost:9092 --topic "
            + TEST_TOPIC
            + " --producer-property acks=all \n";
    command +=
        "kafka-console-consumer --bootstrap-server localhost:9092 --topic "
            + TEST_TOPIC
            + " --from-beginning --timeout-ms 2000 --max-messages 1 | grep \"$MESSAGE\" \n";
    command +=
        "kafka-topics --bootstrap-server localhost:9092 --delete --topic " + TEST_TOPIC + " \n";
    command += "echo \"test succeeded\" \n";
    return command;
  }

  private Boolean runTest(GenericContainer c) {
    try {
      ByteArrayOutputStream stdoutStream = new ByteArrayOutputStream();
      dockerClient
          .execStartCmd(
              dockerClient
                  .execCreateCmd(c.getContainerId())
                  .withAttachStdout(true)
                  .withCmd("sh", "-c", TEST_SCRIPT)
                  .exec()
                  .getId())
          .exec(new ExecStartResultCallback(stdoutStream, null))
          .awaitCompletion();
      return stdoutStream.toString().contains("test succeeded");
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void stop() {
    allContainers().parallel().forEach(GenericContainer::stop);
  }
}

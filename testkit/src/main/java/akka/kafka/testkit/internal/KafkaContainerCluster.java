/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.internal;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers.
 *
 * <p>TODO: This may be deleted when/if the upstream testcontainers-java PR is merged:
 * https://github.com/testcontainers/testcontainers-java/pull/1984
 */
public class KafkaContainerCluster implements Startable {

  public static final String CONFLUENT_PLATFORM_VERSION = "5.2.1";

  private final int brokersNum;
  private final Network network;
  private final GenericContainer zookeeper;
  private final Collection<KafkaContainer> brokers;
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
            .withEnv(
                "ZOOKEEPER_CLIENT_PORT",
                String.valueOf(org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT));

    this.brokers =
        IntStream.range(0, this.brokersNum)
            .mapToObj(
                brokerNum ->
                    new org.testcontainers.containers.KafkaContainer(confluentPlatformVersion)
                        .withNetwork(this.network)
                        .withNetworkAliases("broker-" + brokerNum)
                        .dependsOn(this.zookeeper)
                        .withExternalZookeeper(
                            "zookeeper:"
                                + org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT)
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

  public Collection<KafkaContainer> getBrokers() {
    return this.brokers;
  }

  public String getBootstrapServers() {
    return brokers.stream()
        .map(KafkaContainer::getBootstrapServers)
        .collect(Collectors.joining(","));
  }

  private Stream<GenericContainer> allContainers() {
    Stream<GenericContainer> genericBrokers = this.brokers.stream().map(b -> (GenericContainer) b);
    Stream<GenericContainer> zookeeper = Stream.of(this.zookeeper);
    return Stream.concat(genericBrokers, zookeeper);
  }

  @Override
  public void start() {
    Stream<Startable> startables = this.brokers.stream().map(b -> (Startable) b);
    try {
      Startables.deepStart(startables).get(60, SECONDS);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    Unreliables.retryUntilTrue(
        30,
        TimeUnit.SECONDS,
        () ->
            this.brokers.stream()
                .findFirst()
                .map(this::clusterBrokers)
                .filter(brokers -> brokers.split(",").length == this.brokersNum)
                .isPresent());
  }

  private String clusterBrokers(org.testcontainers.containers.KafkaContainer c) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      dockerClient
          .execStartCmd(
              dockerClient
                  .execCreateCmd(c.getContainerId())
                  .withAttachStdout(true)
                  .withCmd(
                      "sh",
                      "-c",
                      "zookeeper-shell zookeeper:"
                          + org.testcontainers.containers.KafkaContainer.ZOOKEEPER_PORT
                          + " ls /brokers/ids | tail -n 1")
                  .exec()
                  .getId())
          .exec(new ExecStartResultCallback(outputStream, null))
          .awaitCompletion();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return outputStream.toString();
  }

  @Override
  public void stop() {
    // TODO: is there an inverse of `deepStart` to stop all dependant containers?  stopping
    // zookeeper didn't seem to stop brokers alone.
    allContainers().parallel().forEach(GenericContainer::stop);
  }

  @Override
  public void close() {
    this.stop();
  }
}

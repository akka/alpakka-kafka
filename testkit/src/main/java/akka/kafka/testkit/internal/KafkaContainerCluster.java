/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.internal;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static akka.kafka.testkit.internal.KafkaContainer.CONFLUENT_PLATFORM_VERSION;

/**
 * Provides an easy way to launch a Kafka cluster with multiple brokers.
 *
 * <p>NOTE: This may be deleted when/if the upstream testcontainers-java PR is merged:
 * https://github.com/testcontainers/testcontainers-java/pull/1984
 */
public class KafkaContainerCluster implements AutoCloseable {

  private final int startPort;
  private final Network network;
  private final GenericContainer zookeeper;
  private final Collection<KafkaContainer> brokers;

  public KafkaContainerCluster(int brokersNum, int internalTopicsRf, int startPort) {
    this(CONFLUENT_PLATFORM_VERSION, brokersNum, internalTopicsRf, startPort);
  }

  public KafkaContainerCluster(
      String confluentPlatformVersion, int brokersNum, int internalTopicsRf, int startPort) {
    if (brokersNum < 0) {
      throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
    }
    if (internalTopicsRf < 0 || internalTopicsRf > brokersNum) {
      throw new IllegalArgumentException(
          "internalTopicsRf '"
              + internalTopicsRf
              + "' must be less than brokersNum and greater than 0");
    }

    this.startPort = startPort;

    this.network = Network.newNetwork();

    this.zookeeper =
        new GenericContainer("confluentinc/cp-zookeeper:" + confluentPlatformVersion)
            .withNetwork(network)
            .withNetworkAliases("zookeeper")
            .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(KafkaContainer.ZOOKEEPER_PORT));

    this.brokers =
        IntStream.range(0, brokersNum)
            .mapToObj(
                brokerNum ->
                    new KafkaContainer(
                            confluentPlatformVersion,
                            this.startPort + brokerNum,
                            String.valueOf(brokerNum),
                            internalTopicsRf)
                        .withNetwork(this.network)
                        .withExternalZookeeper("zookeeper:" + KafkaContainer.ZOOKEEPER_PORT))
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

  public void startAll() {
    allContainers().parallel().forEach(GenericContainer::start);
  }

  public void stopAll() {
    allContainers().parallel().forEach(GenericContainer::stop);
  }

  @Override
  public void close() throws Exception {
    this.stopAll();
  }
}

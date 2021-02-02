/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.testkit.internal;

import akka.annotation.InternalApi;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;

/** Provides an easy way to launch a Kafka cluster with multiple brokers. */
@InternalApi
public class KafkaContainerCluster implements Startable {

  public static final DockerImageName DEFAULT_ZOOKEEPER_IMAGE_NAME =
      AlpakkaKafkaContainer.DEFAULT_ZOOKEEPER_IMAGE_NAME;
  public static final DockerImageName DEFAULT_KAFKA_IMAGE_NAME =
      AlpakkaKafkaContainer.DEFAULT_KAFKA_IMAGE_NAME;
  public static final DockerImageName DEFAULT_SCHEMA_REGISTRY_IMAGE_NAME =
      SchemaRegistryContainer.DEFAULT_SCHEMA_REGISTRY_IMAGE_NAME;
  public static final Duration DEFAULT_CLUSTER_START_TIMEOUT = Duration.ofSeconds(360);
  public static final Duration DEFAULT_READINESS_CHECK_TIMEOUT = DEFAULT_CLUSTER_START_TIMEOUT;

  private static final String LOGGING_NAMESPACE_PREFIX = "akka.kafka.testkit.testcontainers.logs";
  private static final String READINESS_CHECK_SCRIPT = "/testcontainers_readiness_check.sh";
  private static final String READINESS_CHECK_TOPIC = "ready-kafka-container-cluster";
  private static final Version BOOTSTRAP_PARAM_MIN_VERSION = new Version("5.2.0");

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final Version kafkaImageTag;
  private final int brokersNum;
  private final Boolean useSchemaRegistry;
  private final Boolean containerLogging;
  private final Duration clusterStartTimeout;
  private final Duration readinessCheckTimeout;
  private final Network network;
  private final GenericContainer zookeeper;
  private final Collection<AlpakkaKafkaContainer> brokers;
  private DockerImageName schemaRegistryImage;
  private Optional<SchemaRegistryContainer> schemaRegistry = Optional.empty();

  public KafkaContainerCluster(int brokersNum, int internalTopicsRf) {
    this(
        DEFAULT_ZOOKEEPER_IMAGE_NAME,
        DEFAULT_KAFKA_IMAGE_NAME,
        DEFAULT_SCHEMA_REGISTRY_IMAGE_NAME,
        brokersNum,
        internalTopicsRf,
        false,
        false,
        DEFAULT_CLUSTER_START_TIMEOUT,
        DEFAULT_READINESS_CHECK_TIMEOUT);
  }

  public KafkaContainerCluster(
      DockerImageName zooKeeperImage,
      DockerImageName kafkaImage,
      DockerImageName schemaRegistryImage,
      int brokersNum,
      int internalTopicsRf,
      boolean useSchemaRegistry,
      boolean containerLogging,
      Duration clusterStartTimeout,
      Duration readinessCheckTimeout) {
    if (brokersNum < 0) {
      throw new IllegalArgumentException("brokersNum '" + brokersNum + "' must be greater than 0");
    }
    if (internalTopicsRf < 0 || internalTopicsRf > brokersNum) {
      throw new IllegalArgumentException(
          "internalTopicsRf '"
              + internalTopicsRf
              + "' must be less than brokersNum and greater than 0");
    }

    this.kafkaImageTag = new Version(kafkaImage.getVersionPart());
    this.brokersNum = brokersNum;
    this.useSchemaRegistry = useSchemaRegistry;
    this.containerLogging = containerLogging;
    this.clusterStartTimeout = clusterStartTimeout;
    this.readinessCheckTimeout = readinessCheckTimeout;
    this.network = Network.newNetwork();
    this.schemaRegistryImage = schemaRegistryImage;

    this.zookeeper =
        new GenericContainer(zooKeeperImage)
            .withNetwork(network)
            .withNetworkAliases("zookeeper")
            .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(AlpakkaKafkaContainer.ZOOKEEPER_PORT));

    this.brokers =
        IntStream.range(0, this.brokersNum)
            .mapToObj(
                brokerNum ->
                    new AlpakkaKafkaContainer(kafkaImage)
                        .withNetwork(this.network)
                        .withBrokerNum(brokerNum)
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

  public Optional<SchemaRegistryContainer> getSchemaRegistry() {
    return this.schemaRegistry;
  }

  public Collection<AlpakkaKafkaContainer> getBrokers() {
    return this.brokers;
  }

  public String getBootstrapServers() {
    return brokers.stream()
        .map(AlpakkaKafkaContainer::getBootstrapServers)
        .collect(Collectors.joining(","));
  }

  public String getInternalNetworkBootstrapServers() {
    return IntStream.range(0, this.brokersNum)
        .mapToObj(brokerNum -> String.format("broker-%s:%s", brokerNum, "9092"))
        .collect(Collectors.joining(","));
  }

  /** for backwards compatibility with Java 8 */
  private <T> Stream<T> optionalStream(Optional<T> option) {
    if (option.isPresent()) return Stream.of(option.get());
    else return Stream.empty();
  }

  private Stream<GenericContainer> allContainers() {
    return Stream.concat(
        Stream.concat(this.brokers.stream(), Stream.of(this.zookeeper)),
        optionalStream(this.schemaRegistry));
  }

  @Override
  public void start() {
    try {
      configureContainerLogging();
      Stream<Startable> startables = this.brokers.stream().map(Startable.class::cast);
      Startables.deepStart(startables).get(clusterStartTimeout.getSeconds(), SECONDS);

      this.brokers.stream()
          .findFirst()
          .ifPresent(
              broker -> {
                broker.copyFileToContainer(
                    Transferable.of(readinessCheckScript().getBytes(StandardCharsets.UTF_8), 0777),
                    READINESS_CHECK_SCRIPT);
              });

      waitForClusterFormation();

      if (useSchemaRegistry) {
        this.schemaRegistry =
            Optional.of(
                new SchemaRegistryContainer(this.schemaRegistryImage)
                    .withNetworkAliases("schema-registry")
                    .withCluster(this));
      } else {
        this.schemaRegistry = Optional.empty();
      }

      // start schema registry if the container is initialized
      Startables.deepStart(optionalStream(this.schemaRegistry))
          .get(clusterStartTimeout.getSeconds(), SECONDS);

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void configureContainerLogging() {
    if (containerLogging) {
      log.debug("Testcontainer logging enabled");
      this.brokers.forEach(
          broker ->
              setContainerLogger(
                  LOGGING_NAMESPACE_PREFIX + ".broker.broker-" + broker.getBrokerNum(), broker));
      setContainerLogger(LOGGING_NAMESPACE_PREFIX + ".zookeeper", this.zookeeper);
      this.schemaRegistry.ifPresent(
          container -> setContainerLogger(LOGGING_NAMESPACE_PREFIX + ".schemaregistry", container));
    }
  }

  private void setContainerLogger(String loggerName, GenericContainer<?> container) {
    Logger logger = LoggerFactory.getLogger(loggerName);
    Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
    container.withLogConsumer(logConsumer);
  }

  private void waitForClusterFormation() {
    // assert that cluster has formed
    runReadinessCheck(
        "Readiness check (1/2). ZooKeeper state updated.",
        () -> {
          Container.ExecResult result =
              this.zookeeper.execInContainer(
                  "sh",
                  "-c",
                  "zookeeper-shell zookeeper:"
                      + AlpakkaKafkaContainer.ZOOKEEPER_PORT
                      + " ls /brokers/ids | tail -n 1");
          String brokers = result.getStdout();
          return brokers != null && brokers.split(",").length == this.brokersNum;
        });

    runReadinessCheck(
        "Readiness check (2/2). Run producer consumer with acks=all.",
        () -> this.brokers.stream().findFirst().map(this::runReadinessCheck).orElse(false));
  }

  public void stopKafka() {
    this.brokers.forEach(AlpakkaKafkaContainer::stopKafka);
  }

  public void startKafka() {
    this.brokers.forEach(AlpakkaKafkaContainer::startKafka);
    waitForClusterFormation();
  }

  private void runReadinessCheck(String logLine, Callable<Boolean> fn) {
    try {
      log.debug("Start: {}", logLine);
      Unreliables.retryUntilTrue((int) readinessCheckTimeout.getSeconds(), TimeUnit.SECONDS, fn);
    } catch (Throwable t) {
      log.error("Failed: {}", logLine);
      throw t;
    }
    log.debug("Passed: {}", logLine);
  }

  private String readinessCheckScript() {
    String connect = kafkaTopicConnectParam();
    String command = "#!/bin/bash \n";
    command += "set -e \n";
    command +=
        "[[ $(kafka-topics "
            + connect
            + " --describe --topic "
            + READINESS_CHECK_TOPIC
            + " | wc -l) > 1 ]] && "
            + "kafka-topics "
            + connect
            + " --delete --topic "
            + READINESS_CHECK_TOPIC
            + " \n";
    command +=
        "kafka-topics "
            + connect
            + " --topic "
            + READINESS_CHECK_TOPIC
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
            + READINESS_CHECK_TOPIC
            + " --producer-property acks=all \n";
    command +=
        "kafka-console-consumer --bootstrap-server localhost:9092 --topic "
            + READINESS_CHECK_TOPIC
            + " --from-beginning --timeout-ms 2000 --max-messages 1 | grep \"$MESSAGE\" \n";
    command += "kafka-topics " + connect + " --delete --topic " + READINESS_CHECK_TOPIC + " \n";
    command += "echo \"test succeeded\" \n";
    return command;
  }

  private String kafkaTopicConnectParam() {
    if (this.kafkaImageTag.compareTo(BOOTSTRAP_PARAM_MIN_VERSION) >= 0) {
      return "--bootstrap-server localhost:9092";
    } else {
      return "--zookeeper zookeeper:" + AlpakkaKafkaContainer.ZOOKEEPER_PORT;
    }
  }

  private Boolean runReadinessCheck(GenericContainer c) {
    try {
      Container.ExecResult result = c.execInContainer("sh", "-c", READINESS_CHECK_SCRIPT);

      if (result.getExitCode() != 0 || !result.getStdout().contains("test succeeded")) {
        log.debug(
            "Readiness check returned errors:\nSTDOUT:\n{}\nSTDERR\n{}",
            result.getStdout(),
            result.getStderr());
        return false;
      }
      return true;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void stop() {
    allContainers().parallel().forEach(GenericContainer::stop);
  }
}

@InternalApi
class Version implements Comparable<Version> {

  private String version;

  public final String get() {
    return this.version;
  }

  public Version(String version) {
    if (version == null) throw new IllegalArgumentException("Version can not be null");
    if (!version.matches("[0-9]+(\\.[0-9]+)*"))
      throw new IllegalArgumentException("Invalid version format");
    this.version = version;
  }

  @Override
  public int compareTo(Version that) {
    if (that == null) return 1;
    String[] thisParts = this.get().split("\\.");
    String[] thatParts = that.get().split("\\.");
    int length = Math.max(thisParts.length, thatParts.length);
    for (int i = 0; i < length; i++) {
      int thisPart = i < thisParts.length ? Integer.parseInt(thisParts[i]) : 0;
      int thatPart = i < thatParts.length ? Integer.parseInt(thatParts[i]) : 0;
      if (thisPart < thatPart) return -1;
      if (thisPart > thatPart) return 1;
    }
    return 0;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) return true;
    if (that == null) return false;
    if (this.getClass() != that.getClass()) return false;
    return this.compareTo((Version) that) == 0;
  }
}

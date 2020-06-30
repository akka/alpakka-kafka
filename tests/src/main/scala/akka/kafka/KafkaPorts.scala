/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

/**
 * Ports to use for Kafka and Zookeeper throughout integration tests.
 * Zookeeper is Kafka port + 1 if nothing else specified.
 */
object KafkaPorts {

  val RetentionPeriodSpec = 9012
  val ReconnectSpec = 9032
  val ReconnectSpecProxy = 9034
  val MultiConsumerSpec = 9042
  val ScalaPartitionExamples = 9052
  val ScalaAvroSerialization = 9072
  val AssignmentTest = 9082
  val ProducerExamplesTest = 9112
  val KafkaConnectionCheckerTest = 9122
  val PartitionAssignmentHandlerSpec = 9132
}

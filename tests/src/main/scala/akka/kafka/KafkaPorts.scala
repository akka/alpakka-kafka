/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

/**
 * Ports to use for Kafka and Zookeeper throughout integration tests.
 * Zookeeper is Kafka port + 1 if nothing else specified.
 */
object KafkaPorts {

  val IntegrationSpec = 9002
  val RetentionPeriodSpec = 9012
  val TransactionsSpec = 9022
  val ReconnectSpec = 9032
  val ReconnectSpecProxy = 9034
  val MultiConsumerSpec = 9042
  val ScalaPartitionExamples = 9052
  val ScalaTransactionsExamples = 9062
  val ScalaAvroSerialization = 9072
  val AssignmentTest = 9082
  val SerializationTest = 9092
  val JavaTransactionsExamples = 9102
  val ProducerExamplesTest = 9112
  val KafkaConnectionCheckerTest = 9122
}

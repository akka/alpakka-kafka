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

  val DockerKafkaPort = -1

  val IntegrationSpec = 9002
  val RetentionPeriodSpec = 9012
  val TransactionsSpec = 9022
  val ReconnectSpec = 9032
  val ReconnectSpecProxy = 9034
  val TimestampSpec = 9042
  val MultiConsumerSpec = 9052
  val ScalaConsumerExamples = 9062
  val ScalaPartitionExamples = 9072
  val ScalaAtLeastOnceExamples = 9082
  val ScalaFetchMetadataExamples = 9092
  val ScalaTransactionsExamples = 9102
  val ScalaProducerExamples = 9112
  val PartitionedSourcesSpec = 9122
  val AssignmentSpec = 9132
  val AssignmentTest = 9142
  val ScalaAvroSerialization = 9152
  val SerializationTest = 9162
  val NoBrokerSpec = 9172
  val AtLeastOnceToManyTest = 9182
  val FetchMetadataTest = 9192
  val JavaProducerExamples = 9202
  val JavaTransactionsExamples = 9212
  val ConsumerExamplesTest = 9222
  val CommittingSpec = 9232

}

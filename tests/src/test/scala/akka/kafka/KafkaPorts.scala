/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
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
  val TimestampSpec = 9042
  val MultiConsumerSpec = 9052

}

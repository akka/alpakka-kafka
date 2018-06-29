/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package sample

/**
 * Ports to use for Kafka and Zookeeper throughout integration tests.
 * Zookeeper is Kafka port + 1 if nothing else specified.
 */
object DocKafkaPorts {

  val ConsumerExamples = 8002
  val PartitionExamples = 8004

}

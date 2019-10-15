/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import akka.kafka.testkit.internal.TestcontainersKafka

/**
 * Uses [[https://www.testcontainers.org/ Testcontainers]] to start a Kafka broker in a Docker container once per class.
 * The Testcontainers dependency has to be added explicitly.
 */
trait TestcontainersKafkaPerClassLike extends TestcontainersKafka.Spec {
  override def setUp(): Unit = {
    startKafka(testcontainersSettings)
    super.setUp()
  }

  override def cleanUp(): Unit = {
    super.cleanUp()
    stopKafka()
  }
}

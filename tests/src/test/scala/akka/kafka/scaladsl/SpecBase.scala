/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

// #testkit
import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}

abstract class SpecBase(kafkaPort: Int)
    extends ScalatestKafkaSpec(kafkaPort)
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Eventually {

  protected def this() = this(kafkaPort = -1)
}

// #testkit

// #testcontainers
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike

class TestcontainersSampleSpec extends SpecBase with TestcontainersKafkaLike {
  // ...
}
// #testcontainers

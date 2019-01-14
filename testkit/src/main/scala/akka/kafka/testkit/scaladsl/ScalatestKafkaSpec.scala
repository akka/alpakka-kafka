/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import akka.kafka.testkit.internal.TestFrameworkInterface
import org.scalatest.Suite

abstract class ScalatestKafkaSpec(override val kafkaPort: Int)
    extends KafkaSpec(kafkaPort)
    with Suite
    with TestFrameworkInterface.Scalatest { this: Suite ⇒
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.internal.TestFrameworkInterface
import org.scalatest.{Suite, WordSpecLike}

abstract class ScalatestKafkaSpec(override val kafkaPort: Int)
    extends KafkaSpec(kafkaPort)
    with WordSpecLike
    with TestFrameworkInterface.Scalatest { this: Suite ⇒
}

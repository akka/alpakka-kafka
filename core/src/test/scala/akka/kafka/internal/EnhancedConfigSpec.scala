/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class EnhancedConfigSpec extends WordSpecLike with Matchers {

  "EnhancedConfig" must {

    "parse infinite durations" in {
      val conf = ConfigFactory.parseString("foo-interval = infinite")
      val interval = conf.getPotentiallyInfiniteDuration("foo-interval")
      interval should ===(Duration.Inf)
    }

    "parse finite durations" in {
      val conf = ConfigFactory.parseString("foo-interval = 1m")
      val interval = conf.getPotentiallyInfiniteDuration("foo-interval")
      interval should ===(1.minute)
    }

  }

}

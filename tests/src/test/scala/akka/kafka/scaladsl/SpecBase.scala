/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.test.Utils.StageStoppingTimeout
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.Matchers

import scala.concurrent.duration._

abstract class SpecBase(kafkaPort: Int)
    extends ScalatestKafkaSpec(kafkaPort)
    with EmbeddedKafkaLike
    with Matchers
    with ScalaFutures
    with Eventually {

  implicit val stageStoppingTimeout: StageStoppingTimeout = StageStoppingTimeout(15.seconds)

}

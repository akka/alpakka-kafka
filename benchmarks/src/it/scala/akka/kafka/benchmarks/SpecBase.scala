package akka.kafka.benchmarks

import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{FlatSpecLike, Matchers}

abstract class SpecBase(kafkaPort: Int)
  extends ScalatestKafkaSpec(kafkaPort)
    with FlatSpecLike
    with Matchers
    with ScalaFutures
    with Eventually {

  protected def this() = this(kafkaPort = -1)
}
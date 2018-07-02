package akka.kafka.scaladsl

import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class SpecBase() extends {} with WordSpecLike
with Matchers
with BeforeAndAfterAll
with ScalaFutures
with Eventually {

}

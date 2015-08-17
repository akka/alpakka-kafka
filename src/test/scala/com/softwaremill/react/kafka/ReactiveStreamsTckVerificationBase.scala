package com.softwaremill.react.kafka

import akka.actor.ActorSystem
import org.scalatest.Suite

trait ReactiveStreamsTckVerificationBase extends Suite with KafkaTest {

  override implicit val system: ActorSystem = ActorSystem()
  val message = "foo"
}


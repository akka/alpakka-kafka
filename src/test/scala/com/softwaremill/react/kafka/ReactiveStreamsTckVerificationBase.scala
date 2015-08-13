package com.softwaremill.react.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

trait ReactiveStreamsTckVerificationBase extends KafkaTest {

  override implicit val system: ActorSystem = ActorSystem()
  val message = "foo"
}


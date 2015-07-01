package com.softwaremill.react.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

trait ReactiveStreamsTckVerificationBase {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val kafka = new ReactiveKafka("localhost:9092", "localhost:2181")

  val message = "foo"
}


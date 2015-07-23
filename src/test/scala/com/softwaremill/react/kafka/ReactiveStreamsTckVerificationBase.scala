package com.softwaremill.react.kafka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

trait ReactiveStreamsTckVerificationBase {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val kafkaHost = "localhost:9092"
  val zkHost = "localhost:2181"

  val kafka = new ReactiveKafka()

  val message = "foo"
}


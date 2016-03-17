/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.softwaremill.react.kafka

import akka.actor.ActorSystem
import org.scalatest.Suite

trait ReactiveStreamsTckVerificationBase extends Suite with KafkaTest {

  override implicit val system: ActorSystem = ActorSystem()
  val message = "foo"
}

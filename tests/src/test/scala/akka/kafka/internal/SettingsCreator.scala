/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import akka.kafka.ConsumerSettings
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration.Duration
import scala.concurrent.duration._

trait SettingsCreator {

  def closeTimeout = 500.millis

  def consumerSettings(mock: Consumer[String, String], groupId: String = "group1"): ConsumerSettings[String, String] =
    new ConsumerSettings(
      Map(ConsumerConfig.GROUP_ID_CONFIG -> groupId),
      Some(new StringDeserializer),
      Some(new StringDeserializer),
      pollInterval = 10.millis,
      pollTimeout = 10.millis,
      1.second,
      closeTimeout,
      1.second,
      5.seconds,
      3,
      Duration.Inf,
      "akka.kafka.default-dispatcher",
      1.second,
      true,
      100.millis
    ) {
      override def createKafkaConsumer(): Consumer[String, String] = mock
    }
}

object SettingsCreator extends SettingsCreator

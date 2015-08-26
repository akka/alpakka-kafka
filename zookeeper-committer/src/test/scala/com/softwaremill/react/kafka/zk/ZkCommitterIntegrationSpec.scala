package com.softwaremill.react.kafka.zk

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.pattern.ask
import akka.stream.actor._
import akka.stream.scaladsl.Source
import akka.testkit.{ImplicitSender, EventFilter, TestKit}
import akka.util.Timeout
import com.softwaremill.react.kafka.KafkaMessages._
import com.softwaremill.react.kafka.commit.CommitSink
import com.typesafe.config.ConfigFactory
import kafka.producer.ProducerClosedException
import org.scalatest.{Matchers, fixture}
import com.softwaremill.react.kafka.ReactiveKafkaIntegrationTestSupport

class ZkCommitterIntegrationSpec extends TestKit(ActorSystem("ZkCommitterIntegrationSpec"))
    with ImplicitSender with fixture.WordSpecLike with Matchers with ReactiveKafkaIntegrationTestSupport {

  "Reactive kafka streams" should {
    "manually commit offsets with zookeeper" in { implicit f =>
      shouldCommitOffsets("zookeeper")
    }
  }
}

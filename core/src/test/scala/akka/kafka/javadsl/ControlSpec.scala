/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import org.apache.kafka.common.{Metric, MetricName}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

class ControlSpec extends WordSpecLike with ScalaFutures with Matchers {

  class ControlImpl(stopFuture: Future[Done] = Future.successful(Done), shutdownFuture: Future[Done] = Future.successful(Done)) extends Consumer.Control {
    val shutdownCalled = new AtomicBoolean(false)

    override def stop(): CompletionStage[Done] = stopFuture.toJava
    override def shutdown(): CompletionStage[Done] = {
      shutdownCalled.set(true)
      shutdownFuture.toJava
    }
    override def isShutdown: CompletionStage[Done] = ???
    override def getMetrics: CompletionStage[java.util.Map[MetricName, Metric]] = ???
  }

  "Control" should {
    "drain to stream result" in {
      val control = new ControlImpl
      val drainingControl = Consumer.createDrainingControl(akka.japi.Pair(control, Future.successful("expected").toJava))
      drainingControl.drainAndShutdown().toScala.futureValue should be ("expected")
      control.shutdownCalled.get() should be (true)
    }

    "drain to stream failure" in {
      val control = new ControlImpl

      val drainingControl = Consumer.createDrainingControl(akka.japi.Pair(control, Future.failed[String](new RuntimeException("expected")).toJava))
      val value = drainingControl.drainAndShutdown().toScala.failed.futureValue
      value shouldBe a[RuntimeException]
      value.getMessage should be("expected")
      control.shutdownCalled.get() should be (true)
    }

    "drain to stream failure even if shutdown fails" in {
      val control = new ControlImpl(shutdownFuture = Future.failed(new RuntimeException("not this")))

      val drainingControl = Consumer.createDrainingControl(akka.japi.Pair(control, Future.failed[String](new RuntimeException("expected")).toJava))
      val value = drainingControl.drainAndShutdown().toScala.failed.futureValue
      value shouldBe a[RuntimeException]
      value.getMessage should be("expected")
      control.shutdownCalled.get() should be (true)
    }

    "drain to shutdown failure when stream succeeds" in {
      val control = new ControlImpl(shutdownFuture = Future.failed(new RuntimeException("expected")))

      val drainingControl = Consumer.createDrainingControl(akka.japi.Pair(control, Future.successful(Done).toJava))
      val value = drainingControl.drainAndShutdown().toScala.failed.futureValue
      value shouldBe a[RuntimeException]
      value.getMessage should be("expected")
      control.shutdownCalled.get() should be (true)
    }

  }
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.kafka.scaladsl.Consumer.DrainingControl
import org.apache.kafka.common.{Metric, MetricName}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ControlSpec extends WordSpecLike with ScalaFutures with Matchers {

  class ControlImpl(stopFuture: Future[Done] = Future.successful(Done), shutdownFuture: Future[Done] = Future.successful(Done)) extends Consumer.Control {
    val shutdownCalled = new AtomicBoolean(false)

    override def stop(): Future[Done] = stopFuture
    override def shutdown(): Future[Done] = {
      shutdownCalled.set(true)
      shutdownFuture
    }
    override def isShutdown: Future[Done] = ???
    override def metrics: Future[Map[MetricName, Metric]] = ???
  }

  "Control" should {
    "drain to stream result" in {
      val control = new ControlImpl
      val drainingControl = DrainingControl.apply((control, Future.successful("expected")))
      drainingControl.drainAndShutdown().futureValue should be ("expected")
      control.shutdownCalled.get() should be (true)
    }

    "drain to stream failure" in {
      val control = new ControlImpl

      val drainingControl = DrainingControl.apply((control, Future.failed(new RuntimeException("expected"))))
      val value = drainingControl.drainAndShutdown().failed.futureValue
      value shouldBe a[RuntimeException]
      value.getMessage should be("expected")
      control.shutdownCalled.get() should be (true)
    }

    "drain to stream failure even if shutdown fails" in {
      val control = new ControlImpl(shutdownFuture = Future.failed(new RuntimeException("not this")))

      val drainingControl = DrainingControl.apply((control, Future.failed(new RuntimeException("expected"))))
      val value = drainingControl.drainAndShutdown().failed.futureValue
      value shouldBe a[RuntimeException]
      value.getMessage should be("expected")
      control.shutdownCalled.get() should be (true)
    }

    "drain to shutdown failure when stream succeeds" in {
      val control = new ControlImpl(shutdownFuture = Future.failed(new RuntimeException("expected")))

      val drainingControl = DrainingControl.apply((control, Future.successful(Done)))
      val value = drainingControl.drainAndShutdown().failed.futureValue
      value shouldBe a[RuntimeException]
      value.getMessage should be("expected")
      control.shutdownCalled.get() should be (true)
    }
  }
}

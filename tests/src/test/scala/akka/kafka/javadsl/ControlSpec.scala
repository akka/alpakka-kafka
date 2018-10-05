/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.javadsl

import java.util
import java.util.concurrent.{CompletionStage, Executor, Executors}
import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.kafka.internal.ConsumerControlAsJava
import org.apache.kafka.common.{Metric, MetricName}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future
import scala.language.reflectiveCalls

object ControlSpec {
  def createControl(stopFuture: Future[Done] = Future.successful(Done),
                    shutdownFuture: Future[Done] = Future.successful(Done)) = {
    val control = new akka.kafka.scaladsl.ControlSpec.ControlImpl(stopFuture, shutdownFuture)
    val wrapped = new ConsumerControlAsJava(control)
    new Consumer.Control {
      def shutdownCalled: AtomicBoolean = control.shutdownCalled
      override def stop(): CompletionStage[Done] = wrapped.stop()
      override def shutdown(): CompletionStage[Done] = wrapped.shutdown()
      override def drainAndShutdown[T](streamCompletion: CompletionStage[T], ec: Executor): CompletionStage[T] =
        wrapped.drainAndShutdown(streamCompletion, ec)
      override def isShutdown: CompletionStage[Done] = wrapped.isShutdown
      override def getMetrics: CompletionStage[util.Map[MetricName, Metric]] = wrapped.getMetrics
    }
  }
}

class ControlSpec extends WordSpecLike with ScalaFutures with Matchers {
  import ControlSpec._

  val ec = Executors.newCachedThreadPool()

  "Control" should {
    "drain to stream result" in {
      val control = createControl()
      val drainingControl =
        Consumer.createDrainingControl(akka.japi.Pair(control, Future.successful("expected").toJava))
      drainingControl.drainAndShutdown(ec).toScala.futureValue should be("expected")
      control.shutdownCalled.get() should be(true)
    }

    "drain to stream failure" in {
      val control = createControl()

      val drainingControl = Consumer.createDrainingControl(
        akka.japi.Pair(control, Future.failed[String](new RuntimeException("expected")).toJava)
      )
      val value = drainingControl.drainAndShutdown(ec).toScala.failed.futureValue
      value shouldBe a[RuntimeException]
      // endWith to accustom Scala 2.11 and 2.12
      value.getMessage should endWith("expected")
      control.shutdownCalled.get() should be(true)
    }

    "drain to stream failure even if shutdown fails" in {
      val control = createControl(shutdownFuture = Future.failed(new RuntimeException("not this")))

      val drainingControl = Consumer.createDrainingControl(
        akka.japi.Pair(control, Future.failed[String](new RuntimeException("expected")).toJava)
      )
      val value = drainingControl.drainAndShutdown(ec).toScala.failed.futureValue
      value shouldBe a[RuntimeException]
      // endWith to accustom Scala 2.11 and 2.12
      value.getMessage should endWith("expected")
      control.shutdownCalled.get() should be(true)
    }

    "drain to shutdown failure when stream succeeds" in {
      val control = createControl(shutdownFuture = Future.failed(new RuntimeException("expected")))

      val drainingControl = Consumer.createDrainingControl(akka.japi.Pair(control, Future.successful(Done).toJava))
      val value = drainingControl.drainAndShutdown(ec).toScala.failed.futureValue
      value shouldBe a[RuntimeException]
      // endWith to accustom Scala 2.11 and 2.12
      value.getMessage should endWith("expected")
      control.shutdownCalled.get() should be(true)
    }

  }
}

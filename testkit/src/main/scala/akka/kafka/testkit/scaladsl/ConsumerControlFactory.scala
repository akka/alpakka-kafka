/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.testkit.scaladsl

import akka.Done
import akka.annotation.ApiMayChange
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.{Flow, Keep, Source}
import akka.stream.{KillSwitch, KillSwitches}
import org.apache.kafka.common.{Metric, MetricName}

import scala.concurrent.{Future, Promise}

/**
 * Helper factory to create [[akka.kafka.scaladsl.Consumer.Control]] instances when
 * testing without a Kafka broker.
 */
@ApiMayChange
object ConsumerControlFactory {

  def attachControl[A, B](source: Source[A, B]): Source[A, Consumer.Control] =
    source
      .viaMat(controlFlow())(Keep.right)

  def controlFlow[A](): Flow[A, A, Consumer.Control] =
    Flow[A]
      .viaMat(KillSwitches.single[A])(Keep.right)
      .mapMaterializedValue(killSwitch => control(killSwitch))

  def control(killSwitch: KillSwitch): Consumer.Control = new FakeControl(killSwitch)

  class FakeControl(val killSwitch: KillSwitch) extends Consumer.Control {

    val shutdownPromise = Promise[Done]

    override def stop(): Future[Done] = {
      killSwitch.shutdown()
      shutdownPromise.trySuccess(Done)
      shutdownPromise.future
    }

    override def shutdown(): Future[Done] = {
      killSwitch.shutdown()
      shutdownPromise.trySuccess(Done)
      shutdownPromise.future
    }

    override def isShutdown: Future[Done] = shutdownPromise.future

    override def metrics: Future[Map[MetricName, Metric]] = ???
  }

}

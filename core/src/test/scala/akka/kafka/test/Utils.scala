/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.test

import akka.actor.{ActorRef, ActorRefWithCell, ActorSystem}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.impl.StreamSupervisor
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace
import scala.language.reflectiveCalls

object Utils {

  /** Sets the default-mailbox to the usual [[akka.dispatch.UnboundedMailbox]] instead of [[StreamTestDefaultMailbox]]. */
  val UnboundedMailboxConfig = ConfigFactory.parseString("""akka.actor.default-mailbox.mailbox-type = "akka.dispatch.UnboundedMailbox"""")

  case class TE(message: String) extends RuntimeException(message) with NoStackTrace
  final case class StageStoppingTimeout(time: FiniteDuration)

  def assertAllStagesStopped[T](block: ⇒ T)(implicit timeout: StageStoppingTimeout, materializer: Materializer): T = {
    val impl = materializer.asInstanceOf[ActorMaterializer] // refined type, will never fail
    val probe = TestProbe()(impl.system)
    probe.send(impl.supervisor, StreamSupervisor.StopChildren)
    probe.expectMsg(StreamSupervisor.StoppedChildren)
    val result = block
    probe.within(timeout.time) {
      var children = Set.empty[ActorRef]
      try probe.awaitAssert {
        impl.supervisor.tell(StreamSupervisor.GetChildren, probe.ref)
        children = probe.expectMsgType[StreamSupervisor.Children].children
        assert(
          children.isEmpty,
          s"expected no StreamSupervisor children, but got [${children.mkString(", ")}]"
        )
      }
      catch {
        case ex: Throwable ⇒
          children.foreach(_ ! StreamSupervisor.PrintDebugDump)
          throw ex
      }
    }
    result
  }

  def assertDispatcher(ref: ActorRef, dispatcher: String): Unit = ref match {
    case r: ActorRefWithCell ⇒
      if (r.underlying.props.dispatcher != dispatcher)
        throw new AssertionError(s"Expected $ref to use dispatcher [$dispatcher], yet used: [${r.underlying.props.dispatcher}]")
    case _ ⇒
      throw new Exception(s"Unable to determine dispatcher of $ref")
  }
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.internal

private[akka] object CommitTrigger {
  sealed trait TriggerdBy
  case object BatchSize extends TriggerdBy {
    override def toString: String = "batch size"
  }
  case object Interval extends TriggerdBy {
    override def toString: String = "interval"
  }
  case object UpstreamClosed extends TriggerdBy {
    override def toString: String = "upstream closed"
  }
  case object UpstreamFinish extends TriggerdBy {
    override def toString: String = "upstream finish"
  }
  case object UpstreamFailure extends TriggerdBy {
    override def toString: String = "upstream failure"
  }
}

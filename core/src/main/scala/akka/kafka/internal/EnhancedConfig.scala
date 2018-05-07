/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import com.typesafe.config.Config

import scala.concurrent.duration._

private[kafka] class EnhancedConfig(val underlying: Config) extends AnyVal {

  def getPotentiallyInfiniteDuration(path: String): Duration = underlying.getString(path) match {
    case "infinite" => Duration.Inf
    case _ => underlying.getDuration(path).toMillis.millis
  }

}

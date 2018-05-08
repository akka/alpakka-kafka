/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka

import com.typesafe.config.Config

import scala.language.implicitConversions

package object internal {

  private[kafka] implicit def enhanceConfig(config: Config): EnhancedConfig = new EnhancedConfig(config)

}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks.app

import com.typesafe.config.ConfigFactory

trait Config {
  protected case class HttpConfig(interface: String, port: Int)

  private val config = ConfigFactory.load()
  protected val httpConfig = HttpConfig(config.getString("akka.kafka.benchmarks.app.interface"), config.getInt("akka.kafka.benchmarks.app.port"))
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import com.typesafe.config.Config
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigValue

/**
 * INTERNAL API
 */
private[kafka] object ConfigSettings {

  def parseKafkaClientsProperties(config: Config): Map[String, String] = {
    def collectKeys(c: ConfigObject, prefix: String, keys: Set[String]): Set[String] = {
      var result = keys
      val iter = c.entrySet.iterator
      while (iter.hasNext()) {
        val entry = iter.next()
        entry.getValue match {
          case o: ConfigObject =>
            result ++= collectKeys(o, prefix + entry.getKey + ".", Set.empty)
          case s: ConfigValue =>
            result += prefix + entry.getKey
          case _ =>
          // in case there would be something else
        }
      }
      result
    }

    val keys = collectKeys(config.root, "", Set.empty)
    keys.map(key => key -> config.getString(key)).toMap
  }

}

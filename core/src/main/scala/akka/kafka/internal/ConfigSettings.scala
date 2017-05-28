/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.internal

import java.util

import com.typesafe.config.{Config, ConfigObject}

import scala.collection.JavaConverters.asScalaSetConverter

/**
 * INTERNAL API
 */
private[kafka] object ConfigSettings {

  def parseKafkaClientsProperties(config: Config): Map[String, String] = {
    def collectKeys(c: ConfigObject, processedKeys: Set[String], unprocessedKeys: List[String]): Set[String] = {
      if (unprocessedKeys.isEmpty) processedKeys
      else {
        c.toConfig.getAnyRef(unprocessedKeys.head) match {
          case o: util.Map[_, _] =>
            collectKeys(c, processedKeys, unprocessedKeys.tail ::: o.keySet().asScala.toList.map(unprocessedKeys.head + "." + _))
          case _ =>
            collectKeys(c, processedKeys union Set(unprocessedKeys.head), unprocessedKeys.tail)
        }
      }
    }

    val keys = collectKeys(config.root, Set.empty[String], config.root().keySet().asScala.toList)
    keys.map(key => key -> config.getString(key)).toMap
  }

}

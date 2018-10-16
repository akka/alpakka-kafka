/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import spray.json._

final case class SampleData(name: String, value: Int)

object SprayProtocol extends DefaultJsonProtocol {
  implicit val sampleDataProtocol: RootJsonFormat[SampleData] = jsonFormat2(SampleData)
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestFrameworkInterface {
  def setUp(): Unit
  def cleanUp(): Unit
}

object TestFrameworkInterface {

  trait Scalatest extends TestFrameworkInterface with BeforeAndAfterAll {
    this: Suite ⇒

    abstract override protected def beforeAll(): Unit = {
      println("will call setup")
      setUp()
      println("after setup")
      super.beforeAll()
    }

    abstract override protected def afterAll(): Unit = {
      cleanUp()
      super.afterAll()
    }
  }
}

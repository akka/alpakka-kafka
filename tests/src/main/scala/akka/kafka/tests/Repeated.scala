/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka

import org.scalatest._

/**
 * Repeat test suite n times.  Default: 1.
 * Define number of times to repeat by overriding `timesToRepeat` or passing `-DtimesToRepeat=n`
 *
 * Ex) To run a single test 10 times from the terminal
 *
 * {{{
 * sbt "tests/testOnly *.TransactionsSpec -- -z \"must support copy stream with merging and multi message\" -DtimesToRepeat=2"
 * }}}
 */
trait Repeated extends TestSuiteMixin { this: TestSuite =>
  def timesToRepeat: Int = 1

  protected abstract override def runTest(testName: String, args: Args): Status = {
    def run0(times: Int): Status = {
      val status = super.runTest(testName, args)
      if (times <= 1) status else status.thenRun(run0(times - 1))
    }

    run0(args.configMap.getWithDefault("timesToRepeat", timesToRepeat.toString).toInt)
  }

  /**
   * Retry a code block n times or until Success
   */
  @annotation.tailrec
  final def retry[T](n: Int)(fn: Int => T): T =
    util.Try { fn(n + 1) } match {
      case util.Success(x) => x
      case _ if n > 1 => retry(n - 1)(fn)
      case util.Failure(e) => throw e
    }
}

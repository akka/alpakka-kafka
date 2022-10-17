/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.tests.javadsl

import akka.kafka.tests.CapturingAppender

import scala.util.control.NonFatal
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.slf4j.LoggerFactory

/**
 * See https://doc.akka.io/docs/akka/current/typed/testing-async.html#silence-logging-output-from-tests
 *
 * JUnit `TestRule` to make log lines appear only when the test failed.
 *
 * Use this in test by adding a public field annotated with `@TestRule`:
 * {{{
 *   @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();
 * }}}
 *
 * Requires Logback and configuration like the following the logback-test.xml:
 *
 * {{{
 *     <appender name="CapturingAppender" class="akka.actor.testkit.typed.internal.CapturingAppender" />
 *
 *     <logger name="akka.actor.testkit.typed.internal.CapturingAppenderDelegate" >
 *       <appender-ref ref="STDOUT"/>
 *     </logger>
 *
 *     <root level="DEBUG">
 *         <appender-ref ref="CapturingAppender"/>
 *     </root>
 * }}}
 */
final class LogCapturingJunit4 extends TestRule {
  // eager access of CapturingAppender to fail fast if misconfigured
  private val capturingAppender = CapturingAppender.get("")

  private val myLogger = LoggerFactory.getLogger(classOf[LogCapturingJunit4])

  override def apply(base: Statement, description: Description): Statement = {
    new Statement {
      override def evaluate(): Unit = {
        try {
          myLogger.info(s"Logging started for test [${description.getClassName}: ${description.getMethodName}]")
          base.evaluate()
          myLogger.info(
            s"Logging finished for test [${description.getClassName}: ${description.getMethodName}] that was successful"
          )
        } catch {
          case NonFatal(e) =>
            val method = s"[${Console.BLUE}${description.getClassName}: ${description.getMethodName}${Console.RESET}]"
            val error = e.toString
            System.out.println(s"--> $method Start of log messages of test that failed with $error")
            capturingAppender.flush()
            System.out.println(s"<-- $method End of log messages of test that failed with $error")
            throw e
        } finally {
          capturingAppender.clear()
        }
      }
    }
  }
}

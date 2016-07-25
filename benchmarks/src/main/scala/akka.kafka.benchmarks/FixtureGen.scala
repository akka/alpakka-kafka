/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.kafka.benchmarks.app.RunTestCommand

case class FixtureGen[F](command: RunTestCommand, singleFixture: Int => F) {

  def generate(msgCount: Int): F = singleFixture(msgCount)

  def dataset: Iterator[Int] =
    if (command.hop == 0)
      Iterator.single(command.from)
    else
      (command.from to command.upto by command.hop).iterator
}

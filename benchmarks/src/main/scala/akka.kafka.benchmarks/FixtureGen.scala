/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.kafka.benchmarks.app.RunTestCommand

case class FixtureGen[F](command: RunTestCommand, singleFixture: Int => F) {

  def warmupset: Iterator[F] = Iterator.single(singleFixture(command.upto))

  def generate(msgCount: Int): F = singleFixture(msgCount)

  def dataset: Iterator[Int] = (command.from to command.upto by command.hop).iterator
}

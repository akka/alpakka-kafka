package akka.kafka.benchmarks

case class FixtureGen[F](from: Int, upto: Int, hop: Int, singleFixture: Int => F) {

  def warmupset: Iterator[F] = Iterator.single(singleFixture(upto))

  def generate(msgCount: Int): F = singleFixture(msgCount)

  def dataset: Iterator[Int] = (from to upto by hop).iterator
}
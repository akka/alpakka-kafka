package akka.kafka.benchmarks

object Timed {
  val singleTestRepetitions = 4

  def runPerfTest[F](name: String, fixtureGen: FixtureGen[F], testBody: F => Unit): Unit = {
    fixtureGen.warmupset.foreach { fixture =>
      testBody(fixture)
    }
    val results = fixtureGen.dataset.map { msgCount =>
      val times = (1 to singleTestRepetitions).map { _ =>
        val fixture = fixtureGen.generate(msgCount)
        val start = System.currentTimeMillis()
        testBody(fixture)
        System.currentTimeMillis() - start
      }
      val count = times.length
      val mean = times.sum.toDouble / count
      val dev = times.map(t => (t - mean) * (t - mean))
      val stddev = Math.sqrt(dev.sum / count)
      PerfTestResult(name, msgCount, mean, stddev, times.min, times.max)
    }
    results.foreach(println)
  }
}

case class PerfTestResult(name: String, msgCount: Int, meanDuration: Double, durationStDev: Double, minDuration: Long, maxDuration: Long) {
  override def toString: String = {
    f"$name, $msgCount msgs: mean=${meanDuration / 1000.0d}%4.2fs stdev=$durationStDev%4.2fms, min=$minDuration, max=$maxDuration, mps=${msgCount / (meanDuration / 1000.0d)}%4.2f"
  }
}

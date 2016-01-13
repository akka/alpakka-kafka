package com.softwaremill.react.kafka.benchmarks

import scala.util.Random

object Timed {

  def timed[T](b: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val r = b
    (r, System.currentTimeMillis() - start)
  }

  private def defaultWarmup(tests: List[ReactiveKafkaPerfTest]): Unit = {
    println("Warmup")
    for (test <- tests) {
      val (result, time) = timed { test.run() }
      println(f"${test.name}%-25s $result%-25s ${time / 1000.0d}%4.2fs")
    }

    println("---")
  }

  def runTests(
    tests: List[ReactiveKafkaPerfTest],
    repetitions: Int,
    warmup: List[ReactiveKafkaPerfTest] => Unit = defaultWarmup
  ): Unit = {
    val allTests = Random.shuffle(List.fill(repetitions)(tests).flatten)
    warmup(tests)
    println(s"Running ${allTests.size} tests")

    val rawResults = for (test <- allTests) yield {
      test.warmup()
      val name = test.name
      val (result, time) = timed {
        test.run()
      }
      result.foreach {
        rStr => println(f"$name%-25s $rStr%-25s ${time / 1000.0d}%4.2fs")
      }
      result.map(r => name -> time)
    }
    val successfulRawResults = rawResults.filter(_.isSuccess).map(_.get)

    val results: Map[String, (Double, Double)] = successfulRawResults.groupBy(_._1)
      .mapValues(_.map(_._2))
      .mapValues { times =>
        val count = times.size
        val mean = times.sum.toDouble / count
        val dev = times.map(t => (t - mean) * (t - mean))
        val stddev = Math.sqrt(dev.sum / count)
        (mean, stddev)
      }

    println("---")
    println("Averages (name,  mean, stddev)")
    results.toList.sortBy(_._2._1).foreach {
      case (name, (mean, stddev)) =>
        println(f"$name%-25s ${mean / 1000.0d}%4.2fs $stddev%4.2fms")
    }
  }
}
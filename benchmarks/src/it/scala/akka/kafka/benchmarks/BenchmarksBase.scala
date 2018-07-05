/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.kafka.benchmarks

import akka.kafka.benchmarks.app.RunTestCommand
import akka.kafka.scaladsl.ScalatestKafkaSpec
import org.scalatest.FlatSpecLike

abstract class BenchmarksBase(name: String) extends ScalatestKafkaSpec(0) with FlatSpecLike {
  override def bootstrapServers: String = (1 to 3).map(i => sys.props(s"kafka_${i}_9094")).mkString(",")

  override def setUp(): Unit = {
    println("waiting for the sun")
    waitUntilCluster() {
      _.nodes().get().size == 3
    }
    super.setUp()
  }
}

class ApacheKafkaConsumerNokafka extends BenchmarksBase("ApacheKafkaConsumerNokafka") {
  it should "bench" in Benchmarks.run(RunTestCommand("apache-kafka-consumer-nokafka", bootstrapServers, 2000000))
}

class AlpakkaKafkaConsumerNokafka extends BenchmarksBase("AlpakkaKafkaConsumerNokafka") {
  it should "bench" in Benchmarks.run(RunTestCommand("alpakka-kafka-consumer-nokafka", bootstrapServers, 2000000))
}

class ApacheKafkaPlainConsumer extends BenchmarksBase("ApacheKafkaPlainConsumer") {
  it should "bench" in Benchmarks.run(RunTestCommand("apache-kafka-plain-consumer", bootstrapServers, 2000000))
}

class AlpakkaKafkaPlainConsumer extends BenchmarksBase("AlpakkaKafkaPlainConsumer") {
  it should "bench" in Benchmarks.run(RunTestCommand("alpakka-kafka-plain-consumer", bootstrapServers, 2000000))
}

class ApacheKafkaBatchedConsumer extends BenchmarksBase("ApacheKafkaBatchedConsumer") {
  it should "bench" in Benchmarks.run(RunTestCommand("apache-kafka-batched-consumer", bootstrapServers, 20000))
}

class AlpakkaKafkaBatchedConsumer extends BenchmarksBase("AlpakkaKafkaBatchedConsumer") {
  it should "bench" in Benchmarks.run(RunTestCommand("alpakka-kafka-batched-consumer", bootstrapServers, 20000))
}

class ApacheKafkaAtMostOnceConsumer extends BenchmarksBase("ApacheKafkaAtMostOnceConsumer") {
  it should "bench" in Benchmarks.run(RunTestCommand("apache-kafka-at-most-once-consumer", bootstrapServers, 50000))
}

class AlpakkaKafkaAtMostOnceConsumer extends BenchmarksBase("AlpakkaKafkaAtMostOnceConsumer") {
  it should "bench" in Benchmarks.run(RunTestCommand("alpakka-kafka-at-most-once-consumer", bootstrapServers, 50000))
}

class ApacheKafkaPlainProducer extends BenchmarksBase("ApacheKafkaPlainProducer") {
  it should "bench" in Benchmarks.run(RunTestCommand("apache-kafka-plain-producer", bootstrapServers, 2000000))
}

class AlpakkaKafkaPlainProducer extends BenchmarksBase("AlpakkaKafkaPlainProducer") {
  it should "bench" in Benchmarks.run(RunTestCommand("alpakka-kafka-plain-producer", bootstrapServers, 2000000))
}

class ApacheKafkaTransactions extends BenchmarksBase("ApacheKafkaTransactions") {
  it should "bench" in Benchmarks.run(RunTestCommand("apache-kafka-transactions", bootstrapServers, 100000))
}

class AlpakkaKafkaTransactions extends BenchmarksBase("AlpakkaKafkaTransactions") {
  it should "bench" in Benchmarks.run(RunTestCommand("alpakka-kafka-transactions", bootstrapServers, 100000))
}

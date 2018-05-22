/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

///*
// * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
// * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
// */
//
//package akka.kafka.benchmarks
//
//import akka.dispatch.ExecutionContexts
//import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffsetBatch}
//import akka.stream.Materializer
//import akka.stream.scaladsl.{Keep, Sink, Source}
//import com.codahale.metrics.Meter
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.kafka.clients.consumer.ConsumerRecord
//
//import scala.concurrent.duration._
//import scala.concurrent.{Await, Promise}
//import scala.language.postfixOps
//import scala.util.Success
//
//object ReactiveKafkaTransactionBenchmarks extends LazyLogging {
//  val streamingTimeout = 30 minutes
//  type NonCommitableFixture = ReactiveKafkaConsumerTestFixture[ConsumerRecord[Array[Byte], String]]
//  type CommitableFixture = ReactiveKafkaConsumerTestFixture[CommittableMessage[Array[Byte], String]]
//
//  /**
//   *
//   */
//  def transactionConsumeTransformProduce(fixture: NonCommitableFixture, meter: Meter)(implicit mat: Materializer): Unit = {
//    logger.debug("Creating and starting a stream")
//    meter.mark()
//    val future = Source.repeat("dummy")
//      .take(fixture.msgCount.toLong)
//      .map {
//        msg => meter.mark(); msg
//      }
//      .runWith(Sink.ignore)
//    Await.result(future, atMost = streamingTimeout)
//    logger.debug("Stream finished")
//  }
//}

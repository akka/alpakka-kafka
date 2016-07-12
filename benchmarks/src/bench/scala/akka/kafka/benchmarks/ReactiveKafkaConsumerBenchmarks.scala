package akka.kafka.benchmarks

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object ReactiveKafkaConsumerBenchmarks extends LazyLogging {
  val streamingTimeout = 3 minutes

  def reactiveConsumePlain(fixture: ReactiveKafkaConsumerTestFixture)(implicit mat: Materializer): Unit = {
    logger.debug("Creating and starting a stream")
    val future = fixture.source.take(fixture.msgCount.toLong).runWith(Sink.ignore)
    Await.result(future, atMost = streamingTimeout)
    logger.debug("Stream finished")
  }

}

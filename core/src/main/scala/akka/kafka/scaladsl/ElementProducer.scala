package akka.kafka.scaladsl

import akka.kafka.ProducerMessage.Message
import akka.kafka.ProducerSettings
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

import scala.concurrent.{ExecutionContext, Future, Promise}

class ElementProducer[K, V](val settings: ProducerSettings[K, V])(implicit executionContext: ExecutionContext) {
  val producerFuture = settings.createKafkaProducerAsync()(executionContext)

  def producer[PT](msg: Message[K, V, PT]): Future[PT] = {
    val result = Promise[PT]
    producerFuture.foreach { producer =>
      producer.send(
        msg.record,
        new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (exception == null)
              result.success(msg.passThrough)
            else
              result.failure(exception)
          }
        }
      )
    }
    result.future
  }
}

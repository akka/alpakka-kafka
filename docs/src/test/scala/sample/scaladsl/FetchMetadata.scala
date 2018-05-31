package sample.scaladsl

// #metadata
import akka.actor.ActorRef
import akka.kafka.KafkaConsumerActor
import akka.kafka.Metadata
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._

// #metadata

// Connect a Consumer to Producer, mapping messages one-to-many, and commit in batches
object FetchMetadata extends ConsumerExample {

  def main(args: Array[String]): Unit = {
    // #metadata
    implicit val timeout = Timeout(5.seconds)

    val consumer: ActorRef = system.actorOf(KafkaConsumerActor.props(consumerSettings))

    // ... create source ...

    val topicsFuture: Future[Metadata.Topics] = (consumer ? Metadata.ListTopics).mapTo[Metadata.Topics]

    topicsFuture.map(_.response.foreach { map =>
      map.foreach {
        case (topic, partitionInfo) =>
          partitionInfo.foreach { info =>
            println(s"$topic: $info")
          }
      }
    })
    // #metadata

  }
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package examples

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

object ByPartitionExample extends App {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  implicit val m = ActorMaterializer(ActorMaterializerSettings(as).withInputBuffer(1, 1))

  val settings = ConsumerSettings
    .create(as, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers("kafka.host")
    .withClientId(System.currentTimeMillis().toString)
    .withGroupId("test1")

  val (control, f) = Consumer.committablePartitionedSource(settings, Subscriptions.topics("topic1"))
    .map {
      case (tp, s) =>
        println(s"Starting - $tp")
        s.map { msg =>
          val tp = msg.committableOffset.partitionOffset.key
          println(s"Got message - ${tp.topic}, ${tp.partition}, ${msg.record.value}")
          Thread.sleep(100)
          msg
        }
          .mapAsync(1)(_.committableOffset.commitScaladsl())
          .toMat(Sink.ignore)(Keep.right)
          .mapMaterializedValue((tp, _))
          .run()
    }
    .map { case (tp, completion) => completion.onComplete(result => println(s"$tp finished with $result")); tp }
    .toMat(Sink.ignore)(Keep.both)
    .run()

  f.onComplete(x => println(x))
  Thread.sleep(1000000)
  control.shutdown()

}

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
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import scala.util.{Failure, Success}

object UsualSourceExample extends App {
  implicit val as = ActorSystem()
  implicit val ec = as.dispatcher
  implicit val m = ActorMaterializer(ActorMaterializerSettings(as).withInputBuffer(1, 1))

  val settings = ConsumerSettings
    .create(as, new LongDeserializer, new StringDeserializer)
    .withBootstrapServers("k1.c.test:9092")
    .withClientId(System.currentTimeMillis().toString)
    .withGroupId("test1")

  val (control, f) = Consumer.committableSource[java.lang.Long, String](settings, Subscriptions.topics("topic1"))
    .map { x => println(x.committableOffset.partitionOffset.offset); Thread.sleep(1000); x }
    .batch(max = 5, first => CommittableOffsetBatch.empty.updated(first.committableOffset)) { (batch, elem) =>
      batch.updated(elem.committableOffset)
    }
    .mapAsync(1)(x => x.commitScaladsl())
    .toMat(Sink.ignore)(Keep.both)
    .run()

  f.onComplete({
    case Success(x) => println(x)
    case Failure(ex) => ex.printStackTrace()
  })
  Thread.sleep(10000)
  control.stop()
  Thread.sleep(5000)
  control.shutdown()

}

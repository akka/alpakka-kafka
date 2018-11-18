/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

// #oneToMany
import akka.Done;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerMessage.CommittableOffset;
import akka.kafka.ConsumerMessage.CommittableOffsetBatch;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerMessage.Envelope;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Committer;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.Sink;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;

// #oneToMany

public class AtLeastOnceOneToMany extends ConsumerExample {

  public static void main(String[] args) {
    new AtLeastOnceOneToMany().demo();
  }

  void demo() {
    CompletionStage<Done> done =
        // #oneToMany
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
            .map(
                msg -> {
                  Envelope<String, byte[], CommittableOffset> multiMsg =
                      ProducerMessage.multi(
                          Arrays.asList(
                              new ProducerRecord<>("topic2", msg.record().value()),
                              new ProducerRecord<>("topic3", msg.record().value())),
                          msg.committableOffset());
                  return multiMsg;
                })
            .via(Producer.flexiFlow(producerSettings))
            .map(m -> m.passThrough())
            .runWith(Committer.sink(committerSettings), materializer);
    // #oneToMany

    done.thenAccept(m -> system.terminate());
  }
}

class AtLeastOnceOneToConditional extends ConsumerExample {

  public static void main(String[] args) {
    new AtLeastOnceOneToMany().demo();
  }

  boolean duplicate(byte[] s) {
    return true;
  }

  boolean ignore(byte[] s) {
    return true;
  }

  void demo() {
    CompletionStage<Done> done =
        // #oneToConditional
        Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
            .map(
                msg -> {
                  final Envelope<String, byte[], CommittableOffset> produce;
                  if (duplicate(msg.record().value())) {
                    produce =
                        ProducerMessage.multi(
                            Arrays.asList(
                                new ProducerRecord<>("topic2", msg.record().value()),
                                new ProducerRecord<>("topic3", msg.record().value())),
                            msg.committableOffset());
                  } else if (ignore(msg.record().value())) {
                    produce = ProducerMessage.passThrough(msg.committableOffset());
                  } else {
                    produce =
                        ProducerMessage.single(
                            new ProducerRecord<>("topic2", msg.record().value()),
                            msg.committableOffset());
                  }
                  return produce;
                })
            .via(Producer.flexiFlow(producerSettings))
            .map(m -> m.passThrough())
            .runWith(Committer.sink(committerSettings), materializer);
    // #oneToConditional

    done.thenAccept(m -> system.terminate());
  }
}

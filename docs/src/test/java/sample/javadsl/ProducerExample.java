/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

abstract class ProducerExample {
  protected final ActorSystem system = ActorSystem.create("example");

  protected final Materializer materializer = ActorMaterializer.create(system);

  // #producer
  // #settings
  protected final ProducerSettings<byte[], String> producerSettings = ProducerSettings
    .create(system, new ByteArraySerializer(), new StringSerializer())
    .withBootstrapServers("localhost:9092");
  // #settings
  protected final KafkaProducer<byte[], String> kafkaProducer = producerSettings.createKafkaProducer();
  // #producer

  protected void terminateWhenDone(CompletionStage<Done> result) {
    result
      .exceptionally(e -> {
        system.log().error(e, e.getMessage());
        return Done.getInstance();
      })
      .thenAccept(d -> system.terminate());
  }
}

class PlainSinkExample extends ProducerExample {
  public static void main(String[] args) {
    new PlainSinkExample().demo();
  }

  public void demo() {
    // #plainSink
    CompletionStage<Done> done =
      Source.range(1, 100)
        .map(n -> n.toString()).map(elem -> new ProducerRecord<byte[], String>("topic1", elem))
        .runWith(Producer.plainSink(producerSettings), materializer);
    // #plainSink

    terminateWhenDone(done);
  }
}

class PlainSinkWithProducerExample extends ProducerExample {
    public static void main(String[] args) {
        new PlainSinkExample().demo();
    }

    public void demo() {
        // #plainSinkWithProducer
        CompletionStage<Done> done =
                Source.range(1, 100)
                        .map(n -> n.toString()).map(elem -> new ProducerRecord<byte[], String>("topic1", elem))
                        .runWith(Producer.plainSink(producerSettings, kafkaProducer), materializer);
        // #plainSinkWithProducer

        terminateWhenDone(done);
    }
}

class ObserveMetricsExample extends ProducerExample {
    public static void main(String[] args) {
        new PlainSinkExample().demo();
    }

    public void demo() {
        // #producerMetrics
        Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics =
                kafkaProducer.metrics();// observe metrics
        metrics.get()
// #producerMetrics
    }
}

class ProducerFlowExample extends ProducerExample {
  public static void main(String[] args) {
    new ProducerFlowExample().demo();
  }

  public void demo() {
    // #flow
    CompletionStage<Done> done =
      Source.range(1, 100)
        .map(n -> {
          //int partition = Math.abs(n) % 2;
          int partition = 0;
          String elem = String.valueOf(n);
          return new ProducerMessage.Message<byte[], String, Integer>(
            new ProducerRecord<>("topic1", partition, null, elem), n);
        })
        .via(Producer.flow(producerSettings))
        .map(result -> {
          ProducerRecord<byte[], String> record = result.message().record();
          System.out.println(record);
          return result;
        })
        .runWith(Sink.ignore(), materializer);
    // #flow

    terminateWhenDone(done);
  }
}

/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RestartFlow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.duration.Duration;

abstract class ProducerExample {
  protected final ActorSystem system = ActorSystem.create("example");

  protected final Materializer materializer = ActorMaterializer.create(system);

  // #producer
  // #settings
  final Config config = system.settings().config();
  final ProducerSettings<String, String> producerSettings =
      ProducerSettings
          .create(config, new StringSerializer(), new StringSerializer())
          .withBootstrapServers("localhost:9092");
  // #settings
  final KafkaProducer<String, String> kafkaProducer =
      producerSettings.createKafkaProducer();
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
        .map(number -> number.toString())
        .map(value -> new ProducerRecord<String, String>("topic1", value))
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
                .map(number -> number.toString())
                .map(value -> new ProducerRecord<String, String>("topic1", value))
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
        .map(number -> {
          int partition = 0;
          String value = String.valueOf(number);
          return new ProducerMessage.Message<String, String, Integer>(
            new ProducerRecord<>("topic1", partition, "key", value),
            number
          );
        })
        .via(Producer.flow(producerSettings))
        .map(result -> {
          ProducerRecord<String, String> record = result.message().record();
          RecordMetadata meta = result.metadata();
          return meta.topic() + "/" + meta.partition() + " " + result.offset() + ": " + record.value();
        })
        .runWith(Sink.foreach(System.out::println), materializer);
    // #flow

    terminateWhenDone(done);
  }
}
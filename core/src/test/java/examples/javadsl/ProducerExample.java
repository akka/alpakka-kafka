/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package examples.javadsl;

import akka.actor.ActorSystem;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.scaladsl.Producer;

import akka.stream.javadsl.Source;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

abstract class ProducerExample {
  protected final ActorSystem system = ActorSystem.create("example");

  protected final ProducerSettings<byte[], String> producerSettings = ProducerSettings
    .create(system, new ByteArraySerializer(), new StringSerializer())
    .withBootstrapServers("localhost:9092");
}

class PlainSinkExample extends ProducerExample {
  public void demo() {
    Source.range(1, 10000)
      .map(n -> n.toString()).map(elem -> new ProducerRecord<byte[], String>("topic1", elem))
      .to(Producer.plainSink(producerSettings));
  }
}

class ProducerFlowExample extends ProducerExample {
  public void demo() {
    Source.range(1, 10000)
      .map(n -> new ProducerMessage.Message<byte[], String, Integer>(
        new ProducerRecord<>("topic1", n.toString()), n))
      .via(Producer.flow(producerSettings))
      .map(result -> {
        ProducerRecord record = result.message().record();
        System.out.println(record);
        return result;
      });
  }
}

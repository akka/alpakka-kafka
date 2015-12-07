package com.softwaremill.react.kafka;


import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import static junit.framework.Assert.assertNotNull;

public class JavaConstructorTest {

    @Test
    public void javaCanConstructReactiveKafkaWithoutDefaultArgs() {
        final ReactiveKafka reactiveKafka = new ReactiveKafka();
        assertNotNull(reactiveKafka);
    }

    @Test
    @Ignore("Disabled as we need a kafka endpoint - this example is displayed on the README")
    public void javaCanConstructKafkaConsumerAndProducerInJava() {

        String brokerList = "localhost:9092";

        ReactiveKafka kafka = new ReactiveKafka();
        ActorSystem system = ActorSystem.create("ReactiveKafka");
        ActorMaterializer materializer = ActorMaterializer.create(system);

        ConsumerProperties<String, String> cp =
                new PropertiesBuilder.Consumer(brokerList, "topic", "groupId", new StringDeserializer(), new StringDeserializer())
                        .build();

        Publisher<ConsumerRecord<String, String>> publisher = kafka.consume(cp, system);

        ProducerProperties<String, String> pp = new PropertiesBuilder.Producer(
                brokerList,
                "topic",
                new StringSerializer(),
                new StringSerializer()).build();
        Subscriber<ProducerMessage<String, String>> subscriber = kafka.publish(pp, system);

        Source.from(publisher).map(ProducerMessage::apply).to(Sink.create(subscriber)).run(materializer);
    }

}

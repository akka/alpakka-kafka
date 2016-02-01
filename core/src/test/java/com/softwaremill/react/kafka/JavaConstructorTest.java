package com.softwaremill.react.kafka;


import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

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

        StringDeserializer deserializer = new StringDeserializer();
        ConsumerProperties<String, String> cp =
                new PropertiesBuilder.Consumer(brokerList, "topic", "groupId", deserializer, deserializer)
                        .build();

        Publisher<ConsumerRecord<String, String>> publisher = kafka.consume(cp, system);

        ProducerProperties<String, String> pp = producerProperties(brokerList);
        Subscriber<ProducerMessage<String, String>> subscriber = kafka.publish(pp, system);
        Source.fromPublisher(publisher).map(this::toProdMessage).to(Sink.fromSubscriber(subscriber)).run(materializer);
    }

    @Test
    public void javaCanConstructGraphStageSource() {

        String brokerList = "localhost:9092";

        ReactiveKafka kafka = new ReactiveKafka();
        ActorSystem system = ActorSystem.create("ReactiveKafka");
        ActorMaterializer materializer = ActorMaterializer.create(system);

        StringDeserializer deserializer = new StringDeserializer();
        ConsumerProperties<String, String> cp =
            new PropertiesBuilder.Consumer(brokerList, "topic", "groupId", deserializer, deserializer)
                .build();

        ProducerProperties<String, String> pp = producerProperties(brokerList);

        Source.fromGraph(kafka.graphStageJavaSource(cp))
            .map(this::toProdMessage)
            .to(Sink.fromSubscriber(kafka.publish(pp, system)))
            .run(materializer);

        Source.fromGraph(kafka.graphStageJavaSource(cp, new HashSet<>()))
            .map(this::toProdMessage)
            .to(Sink.fromSubscriber(kafka.publish(pp, system)))
            .run(materializer);

        Source.fromGraph(kafka.graphStageJavaSource(cp, new HashMap<>()))
            .map(this::toProdMessage)
            .to(Sink.fromSubscriber(kafka.publish(pp, system)))
            .run(materializer);
    }

    @Test
    public void javaCanConstructPublisherWithCommitSink() {

        String brokerList = "localhost:9092";

        ReactiveKafka kafka = new ReactiveKafka();

        StringDeserializer deserializer = new StringDeserializer();
        ConsumerProperties<String, String> cp =
            new PropertiesBuilder.Consumer(brokerList, "topic", "groupId", deserializer, deserializer)
                .build();

        ActorSystem system = ActorSystem.create();
        ActorMaterializer materializer = ActorMaterializer.create(system);

        PublisherWithCommitSink consumerWithOffsetSink1 = kafka.consumeWithOffsetSink(cp, system);
        Source.fromPublisher(consumerWithOffsetSink1.publisher())
            .map(record -> toProdMessage((ConsumerRecord<String, String>) record))
            .to(consumerWithOffsetSink1.offsetCommitSink())
            .run(materializer);

        PublisherWithCommitSink consumerWithOffsetSink2 = kafka.consumeWithOffsetSink(cp, new HashSet<>(), system);
        Source.fromPublisher(consumerWithOffsetSink1.publisher())
            .map(record -> toProdMessage((ConsumerRecord<String, String>) record))
            .to(consumerWithOffsetSink2.offsetCommitSink())
            .run(materializer);

        PublisherWithCommitSink consumerWithOffsetSink3 = kafka.consumeWithOffsetSink(cp, new HashMap<>(), system);
        Source.fromPublisher(consumerWithOffsetSink1.publisher())
            .map(record -> toProdMessage((ConsumerRecord<String, String>) record))
            .to(consumerWithOffsetSink3.offsetCommitSink())
            .run(materializer);
    }

    private ProducerProperties<String, String> producerProperties(final String brokerList) {
        StringSerializer serializer = new StringSerializer();
        return new PropertiesBuilder.Producer(
            brokerList,
            "topic",
            serializer,
            serializer).build();
    }

    private ProducerMessage<String, String> toProdMessage(ConsumerRecord<String, String> record) {
        return KeyValueProducerMessage.apply(record.key(), record.value());
    }

}

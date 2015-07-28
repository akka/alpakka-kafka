package com.softwaremill.react.kafka;


import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import kafka.serializer.StringDecoder;
import kafka.serializer.StringEncoder;
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

        String zooKeeperHost = "localhost:2181";
        String brokerList = "localhost:9092";

        ReactiveKafka kafka = new ReactiveKafka();
        ActorSystem system = ActorSystem.create("ReactiveKafka");
        ActorMaterializer materializer = ActorMaterializer.create(system);

        ConsumerProperties<String> cp =
                new PropertiesBuilder.Consumer(zooKeeperHost, brokerList, "topic", "groupId", new StringDecoder(null))
                        .build();

        Publisher<String> publisher = kafka.consume(cp, system);

        ProducerProperties<String> pp = new PropertiesBuilder.Producer(zooKeeperHost, brokerList, "topic", "clientId", new StringEncoder(null))
                .build();

        Subscriber<String> subscriber = kafka.publish(pp, system);

        Source.from(publisher).map(String::toUpperCase).to(Sink.create(subscriber)).run(materializer);
    }

}

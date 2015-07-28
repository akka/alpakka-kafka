package com.softwaremill.react.kafka;


import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.testng.annotations.Test;

import java.util.UUID;

import static junit.framework.Assert.assertEquals;

public class JavaProducerPropertiesTest {


    private final String uuid = UUID.randomUUID().toString();
    private final String brokerList = "localhost:9092";
    private final String zooKeepHost = "localhost:2181";
    private final String topic = uuid;
    private final String groupId = uuid;

    @Test
    public void javaHandleBaseCase() {

        final ProducerProperties producerProperties =
                new PropertiesBuilder.Producer(zooKeepHost, brokerList, topic, groupId, new StringEncoder(null)).build();

        final ProducerConfig producerConfig = producerProperties.toProducerConfig();

        assertEquals(producerProperties.topic(), topic);
        assertEquals(producerProperties.encoder().getClass().getSimpleName(), StringEncoder.class.getSimpleName());
        assertEquals(producerConfig.clientId(), groupId);
        assertEquals(producerConfig.messageSendMaxRetries(), 3);
        assertEquals(producerConfig.requestRequiredAcks(), -1);
        assertEquals(producerConfig.batchNumMessages(), 200); // kafka defaults
        assertEquals(producerConfig.queueBufferingMaxMs(), 5000); // kafka defaults
    }

    @Test
    public void javaHandleAsyncSnappyCase() {

        final ProducerProperties producerProperties =
                new PropertiesBuilder.Producer(zooKeepHost, brokerList, topic, groupId, new StringEncoder(null))
                        .build()
                        .asynchronous(123, 456)
                        .useSnappyCompression();

        final ProducerConfig producerConfig = producerProperties.toProducerConfig();

        assertEquals(producerProperties.topic(), topic);
        assertEquals(producerProperties.clientId(), groupId);
        assertEquals(producerProperties.encoder().getClass().getSimpleName(), StringEncoder.class.getSimpleName());
        assertEquals(producerConfig.clientId(), groupId);
        assertEquals(producerConfig.messageSendMaxRetries(), 3);
        assertEquals(producerConfig.requestRequiredAcks(), -1);
        assertEquals(producerConfig.batchNumMessages(), 123);
        assertEquals(producerConfig.queueBufferingMaxMs(), 456);
    }

}

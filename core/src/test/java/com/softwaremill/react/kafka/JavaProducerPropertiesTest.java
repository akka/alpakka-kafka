package com.softwaremill.react.kafka;


import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;


public class JavaProducerPropertiesTest {


    private final String uuid = UUID.randomUUID().toString();
    private final String brokerList = "localhost:9092";
    private final String topic = uuid;
    private final String groupId = uuid;
    private final StringSerializer serializer = new StringSerializer();

    @Test
    public void javaHandleBaseCase() {

        final PropertiesBuilder.Producer propsBuilder = new PropertiesBuilder.Producer(brokerList, topic, serializer, serializer);
        assertEquals(propsBuilder.getBrokerList(), brokerList);

        final ProducerProperties producerProperties = propsBuilder.build();

        Properties rawProperties = producerProperties.rawProperties();
        assertEquals(rawProperties.getProperty("bootstrap.servers"), brokerList);
    }

    @Test
    public void javaHandleAsyncSnappyCase() {

        final ProducerProperties producerProperties =
                new PropertiesBuilder.Producer(brokerList, topic, groupId, serializer, serializer)
                        .build()
                        .useSnappyCompression()
                        .clientId("client-id2")
                        .requestRequiredAcks(5);

        Properties rawProperties = producerProperties.rawProperties();
        assertEquals(rawProperties.getProperty("bootstrap.servers"), brokerList);
        assertEquals(rawProperties.getProperty("compression.type"), "snappy");
        assertEquals(rawProperties.getProperty("client.id"), "client-id2");
        assertEquals(rawProperties.getProperty("acks"), "5");
    }

}

package com.softwaremill.react.kafka;


import kafka.consumer.ConsumerConfig;
import kafka.serializer.StringDecoder;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.UUID;

import static junit.framework.Assert.assertEquals;

public class JavaConsumerPropertiesTest {

    private final String uuid = UUID.randomUUID().toString();
    private final String topic = uuid;
    private final String groupId = uuid;

    @Test
    public void javaHandleBaseCase() {

        String brokerList = "localhost:9092";
        final PropertiesBuilder.Consumer propsBuilder = new PropertiesBuilder.Consumer(brokerList, topic, groupId, new ByteArrayDeserializer(), new StringDeserializer());
        assertEquals(propsBuilder.getBrokerList(), brokerList);

        final ConsumerProperties consumerProperties = propsBuilder.build();

        final Properties props = consumerProperties.toProps();

        assertEquals(consumerProperties.topic(), topic);
        assertEquals(consumerProperties.groupId(), groupId);
        assertEquals(consumerProperties.keyDeserializer().getClass().getSimpleName(), ByteArrayDeserializer.class.getSimpleName());
        assertEquals(consumerProperties.valueDeserializer().getClass().getSimpleName(), StringDeserializer.class.getSimpleName());
// TODO
//        assertEquals(consumerConfig.clientId(), groupId);
//        assertEquals(consumerConfig.autoOffsetReset(), "smallest");
//        assertEquals(consumerConfig.offsetsStorage(), "zookeeper");
//        assertEquals(consumerConfig.consumerTimeoutMs(), 1500);
//        assertEquals(consumerConfig.dualCommitEnabled(), false);
    }


}

package com.softwaremill.react.kafka;


import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testng.annotations.Test;
import scala.concurrent.duration.FiniteDuration;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class JavaConsumerPropertiesTest {

    private final String uuid = UUID.randomUUID().toString();
    private final String topic = uuid;
    private final String groupId = uuid;

    @Test
    public void javaHandleBaseCase() {

        String brokerList = "localhost:9092";
        final PropertiesBuilder.Consumer propsBuilder = new PropertiesBuilder.Consumer(
                brokerList,
                topic,
                groupId,
                new ByteArrayDeserializer(),
                new StringDeserializer(),
                new FiniteDuration(2, TimeUnit.SECONDS),
                new FiniteDuration(3, TimeUnit.SECONDS));
        assertEquals(propsBuilder.getBrokerList(), brokerList);

        final ConsumerProperties consumerProperties = propsBuilder.build();

        assertEquals(consumerProperties.topic(), topic);
        assertEquals(consumerProperties.groupId(), groupId);
        assertEquals(consumerProperties.keyDeserializer().getClass().getSimpleName(), ByteArrayDeserializer.class.getSimpleName());
        assertEquals(consumerProperties.valueDeserializer().getClass().getSimpleName(), StringDeserializer.class.getSimpleName());
        assertEquals(consumerProperties.pollTimeout(), new FiniteDuration(2, TimeUnit.SECONDS));
        assertEquals(consumerProperties.pollRetryDelay(), new FiniteDuration(3, TimeUnit.SECONDS));
    }
}

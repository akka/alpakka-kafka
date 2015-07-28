package com.softwaremill.react.kafka;


import kafka.consumer.ConsumerConfig;
import kafka.serializer.StringDecoder;
import org.testng.annotations.Test;

import java.util.UUID;

import static junit.framework.Assert.assertEquals;

public class JavaConsumerPropertiesTest {

    private final String uuid = UUID.randomUUID().toString();
    private final String brokerList = "localhost:9092";
    private final String zooKeepHost = "localhost:2181";
    private final String topic = uuid;
    private final String groupId = uuid;

    @Test
    public void javaHandleBaseCase() {

        final ConsumerProperties<String> consumerProperties =
                new PropertiesBuilder.Consumer<>(zooKeepHost, brokerList, topic, groupId, new StringDecoder(null))
                        .build();

        final ConsumerConfig consumerConfig = consumerProperties.toConsumerConfig();

        assertEquals(consumerProperties.topic(), topic);
        assertEquals(consumerProperties.groupId(), groupId);
        assertEquals(consumerProperties.decoder().getClass().getSimpleName(), StringDecoder.class.getSimpleName());
        assertEquals(consumerConfig.clientId(), groupId);
        assertEquals(consumerConfig.autoOffsetReset(), "smallest");
        assertEquals(consumerConfig.offsetsStorage(), "zookeeper");
        assertEquals(consumerConfig.consumerTimeoutMs(), 1500);
        assertEquals(consumerConfig.dualCommitEnabled(), false);
    }

    @Test
    public void javaHandleKafkaStorage() {

        final ConsumerProperties<String> consumerProperties =
                new PropertiesBuilder.Consumer<>(zooKeepHost, brokerList, topic, groupId, new StringDecoder(null))
                        .build()
                        .readFromEndOfStream()
                        .consumerTimeoutMs(1234)
                        .kafkaOffsetsStorage(true);

        final ConsumerConfig consumerConfig = consumerProperties.toConsumerConfig();

        assertEquals(consumerProperties.topic(), topic);
        assertEquals(consumerProperties.groupId(), groupId);
        assertEquals(consumerProperties.decoder().getClass().getSimpleName(), StringDecoder.class.getSimpleName());
        assertEquals(consumerConfig.clientId(), groupId);
        assertEquals(consumerConfig.autoOffsetReset(), "largest");
        assertEquals(consumerConfig.offsetsStorage(), "kafka");
        assertEquals(consumerConfig.consumerTimeoutMs(), 1234);
        assertEquals(consumerConfig.dualCommitEnabled(), true);
    }

}

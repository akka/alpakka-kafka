package com.softwaremill.react.kafka;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import scala.collection.JavaConverters;
import scala.collection.immutable.HashMap;
import scala.concurrent.duration.FiniteDuration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Builder class wrapping Consumer & Producer properties creation in Java API.
 */
public class PropertiesBuilder {

    /**
     * Base properties required by consumers and producers
     */
    private static class BaseProperties {

        private String brokerList;
        private String topic;

        public BaseProperties(String brokerList, String topic) {
            this.brokerList = brokerList;
            this.topic = topic;
        }

        /**
         * Determines if a broker list and zookeeper host are both not null
         *
         * @return boolean if both are not null
         */
        private boolean hasConnectionPropertiesSet() {
            return this.brokerList != null;
        }

        public String getBrokerList() {
            return brokerList;
        }

        public String getTopic() {
            return topic;
        }
    }


    /**
     * The Consumer Builder
     */
    public static class Consumer extends BaseProperties {

        private Deserializer keyDeserializer;
        private Deserializer valueDeserializer;
        private String groupId;
        private FiniteDuration pollTimeout;

        private scala.collection.immutable.Map<String, String> consumerParams = new HashMap<>();

        public Consumer(String brokerList, String topic, String groupId, Deserializer keyDeserializer, Deserializer valueDeserializer) {
            this(brokerList, topic, groupId, keyDeserializer, valueDeserializer, new FiniteDuration(1, TimeUnit.SECONDS));
        }

        public Consumer(String brokerList, String topic, String groupId, Deserializer keyDeserializer, Deserializer valueDeserializer, FiniteDuration pollTimeout) {
            super(brokerList, topic);
            this.keyDeserializer = keyDeserializer;
            this.valueDeserializer = valueDeserializer;
            this.groupId = groupId;
            this.pollTimeout = pollTimeout;
        }

        public Consumer withParams(Map<String, String> params) {
            this.consumerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        /**
         * Create a ConsumerProperties object
         *
         * @param <K> type of partition key
         * @param <V> type of message
         * @return a fully constructed ConsumerProperties
         */
        public <K, V> ConsumerProperties<K, V> build() {
            if (super.hasConnectionPropertiesSet()) {
                return ConsumerProperties.<K, V>apply(getBrokerList(), getTopic(), groupId, keyDeserializer, valueDeserializer);
            }
            return new ConsumerProperties(consumerParams, getTopic(), groupId, keyDeserializer, valueDeserializer, pollTimeout);
        }

    }

    /**
     * The Producer Builder
     */
    public static class Producer extends BaseProperties {

        private String clientId;
        private Serializer keySerializer;
        private Serializer valueSerializer;

        private scala.collection.immutable.Map<String, String> producerParams = new HashMap<>();

        public Producer(String brokerList, String topic, String clientId, Serializer keySerializer, Serializer valueSerializer) {
            super(brokerList, topic);
            this.clientId = clientId;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        public Producer(String brokerList, String topic, String clientId, Serializer valueSerializer) {
            super(brokerList, topic);
            this.clientId = clientId;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        public Producer(String brokerList, String topic, Serializer keySerializer, Serializer valueSerializer) {
            super(brokerList, topic);
            this.clientId = "";
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        public Producer withParams(Map<String, String> params) {
            this.producerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        /**
         * Create a ProducerProperties object
         *
         * @param <K> key type of produced messages
         * @param <V> value type of produced messages
         * @return a fully constructed ProducerProperties
         */
        public <K ,V> ProducerProperties<K, V> build() {
            if (super.hasConnectionPropertiesSet()) {
                return ProducerProperties.<K, V>apply(getBrokerList(), getTopic(), keySerializer, valueSerializer);
            }
            return ProducerProperties.<K, V>apply(producerParams, getTopic(), keySerializer, valueSerializer);

        }

    }

}

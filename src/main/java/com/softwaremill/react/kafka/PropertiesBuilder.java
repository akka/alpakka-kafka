package com.softwaremill.react.kafka;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import scala.collection.JavaConverters;
import scala.collection.immutable.HashMap;

import java.util.Map;

/**
 * Builder class wrapping Consumer & Producer properties creation in Java API.
 */
public class PropertiesBuilder {

    /**
     * Base properties required by consumers and producers
     */
    private static class BaseProperties {

        private String brokerList;
        private String zooKeeperHost;
        private String topic;

        public BaseProperties(String brokerList, String zooKeeperHost, String topic) {
            this.brokerList = brokerList;
            this.zooKeeperHost = zooKeeperHost;
            this.topic = topic;
        }

        /**
         * Determines if a broker list and zookeeper host are both not null
         *
         * @return boolean if both are not null
         */
        private boolean hasConnectionPropertiesSet() {
            return this.brokerList != null && this.zooKeeperHost != null;
        }

        public String getBrokerList() {
            return brokerList;
        }

        public String getZooKeeperHost() {
            return zooKeeperHost;
        }

        public String getTopic() {
            return topic;
        }
    }


    /**
     * The Consumer Builder
     */
    public static class Consumer extends BaseProperties {

        private Decoder decoder;
        private String groupId;
        private scala.collection.immutable.Map<String, String> consumerParams = new HashMap<>();

        public Consumer(String zooKeeperHost, String brokerList, String topic, String groupId, Decoder decoder) {
            super(zooKeeperHost, brokerList, topic);
            this.decoder = decoder;
            this.groupId = groupId;
        }

        public Consumer withParams(Map<String, String> params) {
            this.consumerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        /**
         * Create a ConsumerProperties object
         *
         * @param <T> type of message
         * @return a fully constructed ConsumerProperties
         */
        public <T> ConsumerProperties<T> build() {
            if (super.hasConnectionPropertiesSet()) {
                return ConsumerProperties.<T>apply(getBrokerList(), getZooKeeperHost(), getTopic(), groupId, decoder);
            }
            return new ConsumerProperties(consumerParams, getTopic(), groupId, decoder);
        }

    }

    /**
     * The Producer Builder
     */
    public static class Producer extends BaseProperties {

        private String clientId;
        private Encoder encoder;
        private scala.collection.immutable.Map<String, String> producerParams = new HashMap<>();

        public Producer(String brokerList, String zooKeeperHost, String topic, String clientId, Encoder encoder) {
            super(brokerList, zooKeeperHost, topic);
            this.clientId = clientId;
            this.encoder = encoder;
        }

        public Producer(String brokerList, String zooKeeperHost, String topic, Encoder encoder) {
            super(brokerList, zooKeeperHost, topic);
            this.clientId = "";
            this.encoder = encoder;
        }

        public Producer withParams(Map<String, String> params) {
            this.producerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        /**
         * Create a ProducerProperties object
         *
         * @param <P> the type of Producer to construct
         * @return a fully constructed ProducerProperties
         */
        public <P> ProducerProperties<P> build() {
            if (super.hasConnectionPropertiesSet()) {
                return ProducerProperties.<P>apply(getBrokerList(), getTopic(), clientId, encoder);
            }
            return new ProducerProperties(producerParams, getTopic(), encoder, null);

        }

    }

}

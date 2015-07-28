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
     *
     * @param <D> the type of Decoder to construct
     */
    public static class Consumer<D> extends BaseProperties {

        private Decoder<D> decoder;
        private String groupId;
        private scala.collection.immutable.Map<String, String> consumerParams = new HashMap<>();

        public Consumer(String zooKeeperHost, String brokerList, String topic, String groupId, Decoder<D> decoder) {
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
         * @return a fully constructed ConsumerProperties
         */
        public ConsumerProperties<D> build() {
            if (super.hasConnectionPropertiesSet()) {
                return ConsumerProperties.apply(getBrokerList(), getZooKeeperHost(), getTopic(), groupId, decoder);
            }
            return ConsumerProperties.apply(consumerParams, getTopic(), groupId, decoder);
        }

    }

    /**
     * The Producer Builder
     * @param <E> the type of Encoder to construct
     */
    public static class Producer<E> extends BaseProperties {

        private String clientId;
        private Encoder<E> encoder;
        private scala.collection.immutable.Map<String, String> producerParams = new HashMap<>();

        public Producer(String brokerList, String zooKeeperHost, String topic, String clientId, Encoder<E> encoder) {
            super(brokerList, zooKeeperHost, topic);
            this.clientId = clientId;
            this.encoder = encoder;
        }

        public Producer withParams(Map<String, String> params) {
            this.producerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        /**
         * Create a ProducerProperties object
         *
         * @return a fully constructed ProducerProperties
         */
        public  ProducerProperties<E> build() {
            if (super.hasConnectionPropertiesSet()) {
                return ProducerProperties.apply(getBrokerList(), getTopic(), clientId, encoder);
            }
            return ProducerProperties.apply(producerParams, getTopic(), clientId, encoder, null);

        }

    }

}

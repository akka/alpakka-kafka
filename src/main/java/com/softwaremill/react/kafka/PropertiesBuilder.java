package com.softwaremill.react.kafka;

import kafka.serializer.*;
import kafka.utils.VerifiableProperties;
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

        public Consumer(String zooKeeperHost, String brokerList, String topic, String groupId) {
            super(zooKeeperHost, brokerList, topic);
            this.decoder = new DefaultDecoder(null);
            this.groupId = groupId;
        }

        public Consumer withStringDecoder(VerifiableProperties props) {
            this.decoder = new StringDecoder(props);
            return this;
        }

        public Consumer withStringDecoder() {
            this.decoder = new StringDecoder(null);
            return this;
        }

        public Consumer withDefaultDecoder(VerifiableProperties props) {
            this.decoder = new DefaultDecoder(props);
            return this;
        }

        public Consumer withParams(Map<String, String> params) {
            this.consumerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        /**
         * Create a ConsumerProperties object
         *
         * @param <C> the type of Consumer to construct
         * @return a fully constructed ConsumerProperties
         */
        public <C> ConsumerProperties<C> build() {
            if (super.hasConnectionPropertiesSet()) {
                return ConsumerProperties.<C>apply(getBrokerList(), getZooKeeperHost(), getTopic(), groupId, decoder);
            }
            return ConsumerProperties.<C>apply(consumerParams, getTopic(), groupId, decoder);
        }

    }

    /**
     * The Producer Builder
     */
    public static class Producer extends BaseProperties {

        private String clientId;
        private Encoder encoder;
        private scala.collection.immutable.Map<String, String> producerParams = new HashMap<>();

        public Producer(String brokerList, String zooKeeperHost, String topic, String clientId) {
            super(brokerList, zooKeeperHost, topic);
            this.clientId = clientId;
            this.encoder = new DefaultEncoder(null);
        }

        public Producer withStringEncoder(VerifiableProperties props) {
            this.encoder = new StringEncoder(props);
            return this;
        }

        public Producer withStringEncoder() {
            this.encoder = new StringEncoder(null);
            return this;
        }

        public Producer withDefaultEncoder(VerifiableProperties props) {
            this.encoder = new DefaultEncoder(props);
            return this;
        }

        public Producer withNullEncoder() {
            this.encoder = new NullEncoder(null);
            return this;
        }

        public Producer withNullEncoder(VerifiableProperties props) {
            this.encoder = new NullEncoder(props);
            return this;
        }

        public Producer withParams(Map<String, String> params) {
            this.producerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        /**
         * Create a ProducerProperties object
         *
         * @param <C> the type of Producer to construct
         * @return a fully constructed ProducerProperties
         */
        public <P> ProducerProperties<P> build() {
            if (super.hasConnectionPropertiesSet()) {
                return ProducerProperties.<P>apply(getBrokerList(), getTopic(), clientId, encoder);
            }
            return ProducerProperties.<P>apply(producerParams, getTopic(), clientId, encoder, null);

        }

    }

}

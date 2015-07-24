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
     * The Consumer Builder
     */
    public static class Consumer {

        private String topic;
        private String groupId;
        private Decoder decoder;
        private String brokerList;
        private String zooKeeperHost;
        private scala.collection.immutable.Map<String, String> consumerParams = new HashMap<>();

        public Consumer withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Consumer withGroupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Consumer withBrokerList(String brokerList) {
            this.brokerList = brokerList;
            return this;
        }

        public Consumer withZooKeeperHost(String zooKeeperHost) {
            this.zooKeeperHost = zooKeeperHost;
            return this;
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

        public Consumer withDefaultDecoder() {
            this.decoder = new DefaultDecoder(null);
            return this;
        }

        public Consumer withParams(Map<String, String> params) {
            this.consumerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        public <C> ConsumerProperties<C> build() {
            if (brokerList != null && zooKeeperHost != null) {
                return ConsumerProperties.<C>apply(brokerList, zooKeeperHost, topic, groupId, decoder);
            }
            return ConsumerProperties.<C>apply(consumerParams, topic, groupId, decoder);
        }

    }

    /**
     * The Producer Builder
     */
    public static class Producer {

        private String topic;
        private String clientId;
        private Encoder encoder;
        private String brokerList;
        private String zooKeeperHost;
        private scala.collection.immutable.Map<String, String> producerParams = new HashMap<>();

        public Producer withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Producer withClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Producer withBrokerList(String brokerList) {
            this.brokerList = brokerList;
            return this;
        }

        public Producer withZooKeeperHost(String zooKeeperHost) {
            this.zooKeeperHost = zooKeeperHost;
            return this;
        }

        public Producer withStringEncoder(VerifiableProperties props) {
            this.encoder = new StringEncoder(props);
            return this;
        }

        public Producer withStringEncoder() {
            this.encoder = new StringEncoder(null);
            return this;
        }

        public Producer withDefaultEncoder() {
            this.encoder = new DefaultEncoder(null);
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

        public <P> ProducerProperties<P> build() {
            if (brokerList != null && zooKeeperHost != null) {
                return ProducerProperties.<P>apply(brokerList, topic, clientId, encoder);
            }
            return ProducerProperties.<P>apply(producerParams, topic, clientId, encoder, null);

        }

    }

}

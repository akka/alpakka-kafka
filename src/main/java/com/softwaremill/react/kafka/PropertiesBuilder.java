package com.softwaremill.react.kafka;

import kafka.serializer.*;
import kafka.utils.VerifiableProperties;
import scala.collection.JavaConverters;
import scala.collection.immutable.HashMap;

import java.util.Map;
import java.util.Optional;

/**
 * Builder class wrapping Consumer & Prooducer  properties creation in Java API.
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

        public Consumer withStringDecoder(Optional<VerifiableProperties> props) {
            this.decoder = props.isPresent()
                    ? new StringDecoder(props.get())
                    : new StringDecoder(null);
            return this;
        }

        public Consumer withDefaultDecoder(Optional<VerifiableProperties> props) {
            this.decoder = props.isPresent()
                    ? new DefaultDecoder(props.get())
                    : new DefaultDecoder(null);
            return this;
        }

        public Consumer withParams(Map<String, String> params) {
            this.consumerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        public <C> ConsumerProperties build() {
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

        public Producer withStringEncoder(Optional<VerifiableProperties> props) {
            this.encoder = props.isPresent()
                    ? new StringEncoder(props.get())
                    : new StringEncoder(null);
            return this;
        }

        public Producer withDefaultEncoder(Optional<VerifiableProperties> props) {
            this.encoder = props.isPresent()
                    ? new DefaultEncoder(props.get())
                    : new DefaultEncoder(null);
            return this;
        }

        public Producer withNullEncoder(Optional<VerifiableProperties> props) {
            this.encoder = props.isPresent()
                    ? new NullEncoder(props.get())
                    : new NullEncoder(null);
            return this;
        }

        public Producer withParams(Map<String, String> params) {
            this.producerParams = new HashMap<String, String>().$plus$plus(JavaConverters.mapAsScalaMapConverter(params).asScala());
            return this;
        }

        public <P> ProducerProperties build() {
            if (brokerList != null && zooKeeperHost != null) {
                return ProducerProperties.<P>apply(brokerList, topic, clientId, encoder);
            }
            return ProducerProperties.<P>apply(producerParams, topic, clientId, encoder, null);

        }

    }

}

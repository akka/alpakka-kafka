---
project.description: Consider these areas when using Alpakka Kafka in production.
---
# Production considerations


## Alpakka Kafka API

1. Do not use `Consumer.atMostOnceSource` in production as it internally commits the offset after every element.
1. If you create `Producer` sinks in "inner flows", be sure to @ref:[share the `Producer` instance](producer.md#sharing-the-kafkaproducer-instance). This avoids the expensive creation of `KafkaProducer` instances.

@@@ note

This is just a start, please add your experiences to this list by [opening a Pull Request](https://github.com/akka/alpakka-kafka/pulls).

@@@


## Monitoring

For performance monitoring consider [Lightbend Telemetry](https://developer.lightbend.com/docs/telemetry/current/) which gives insights into Akka and Akka Streams.


## Security setup

The different security setups offered by Kafka brokers are described in the @extref[Apache Kafka documentation](kafka:/documentation.html#security).


### SSL

The properties described in Kafka's @extref[Configuring Kafka Clients for SSL](kafka:/documentation.html#security_configclients) go in the
`akka.kafka.consumer.kafka-clients` and `akka.kafka.producer.kafka-clients` sections of the configuration, or can be added programmatically via
`ProducerSettings.withProperties` and `ConsumerSettings.withProperties`.

```hocon
akka.kafka.producer { # and akka.kafka.consumer respectively
  kafka-clients {
    security.protocol=SSL
    ssl.truststore.location=/var/private/ssl/kafka.client.truststore.jks
    ssl.truststore.password=test1234
    ssl.keystore.location=/var/private/ssl/kafka.client.keystore.jks
    ssl.keystore.password=test1234
    ssl.key.password=test1234
  }
}
```

The truststore and keystore locations may specify URLs, absolute paths or relative paths (starting with `./`).

You have the option to pass the passwords as command line parameters or environment values via the support in [Config](https://github.com/lightbend/config#optional-system-or-env-variable-overrides).


### Kerberos

The properties described in Kafka's @extref[Configuring Kafka Clients for Kerberos](kafka:/documentation.html#security_sasl_kerberos_clientconfig) go in the
`akka.kafka.consumer.kafka-clients` and `akka.kafka.producer.kafka-clients` sections of the configuration, or can be added programmatically via
`ProducerSettings.withProperties` and `ConsumerSettings.withProperties`.

```hocon
akka.kafka.producer { # and akka.kafka.consumer respectively
  kafka-clients {
    security.protocol=SASL_PLAINTEXT # (or SASL_SSL)
    sasl.mechanism=GSSAPI
    sasl.kerberos.service.name=kafka
  }
}
```

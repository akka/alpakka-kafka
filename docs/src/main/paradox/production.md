# Production considerations


## Alpakka Kafka API

1. Do not use `Consumer.atMostOnceSource` in production
1. If you create `Producer` sinks in "inner flows", be sure to [share the `Producer` instance](https://doc.akka.io/docs/akka-stream-kafka/current/producer.html#sharing-the-kafkaproducer-instance)

@@@ note

This is just a start, please add your experiences to this list by [opening a Pull Request](https://github.com/akka/alpakka-kafka/pulls).

@@@


## Monitoring

For performance monitoring consider [Lightbend Telemetry](https://developer.lightbend.com/docs/telemetry/current/) which gives insights into Akka and Akka Streams.


## Security setup

Configure the Kafka brokers as described in [Confluent's article 
“Configuring Kafka Clients”](https://www.confluent.io/blog/apache-kafka-security-authorization-authentication-encryption/).

For Alpakka Kafka the client configuration parameters go in the `akka.kafka.consumer.kafka-clients` and `akka.kafka.producer.kafka-clients` sections of the configuration.

```hocon
akka.kafka.producer {
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

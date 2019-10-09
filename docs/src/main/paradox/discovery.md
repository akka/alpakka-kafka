---
project.description: Akka Discovery can be used to achieve Kafka broker discovery from the operations environment.
---
# Service discovery

By using @extref:[Akka Discovery](akka-docs:discovery/index.html) Alpakka Kafka may read the Kafka bootstrap server addresses from any Akka Discovery-compatible service discovery mechanism.

Akka Discovery supports Configuration (HOCON), DNS (SRV records), and aggregation of multiple discovery methods out-of-the-box. Kubernetes API, AWS API: EC2 Tag-Based Discovery, AWS API: ECS Discovery and Consul implementations for Akka Discovery are available in @extref:[Akka Management](akka-management:).

## Dependency

The Akka Discovery version must match the Akka version used in your build. To use the implementations provided by Akka Management, you need to add the desired dependency.

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-discovery_$scala.binary.version$
  version=$akka.version$
}

## Configure consumer settings

To use Akka Discovery with Alpakka Kafka consumers, configure a section for your consumer settings which inherits the default settings (by using `${akka.kafka.consumer}`) and add a service name and a timeout for the service lookup. Setting the `service-name` in the `akka.kafka.consumer` config will work, if all your consumers connect to the same Kafka broker.

The service name must match the one configured with the discovery technology you use. Overwrite the `resolve-timeout` depending on the discovery technology used, and your environment.

Note that consumers and producers may share a service (as shown in the examples on this page).

```hocon
discovery-consumer: ${akka.kafka.consumer} {
  service-name = "kafkaService1"
}
```

Scala
: @@snip [snip](/tests/src/test/scala/akka/kafka/ConsumerSettingsSpec.scala) { #discovery-settings }

Java
: @@snip [conf](/tests/src/test/java/docs/javadsl/ConsumerSettingsTest.java) { #discovery-settings }


## Configure producer settings

To use Akka Discovery with Alpakka Kafka producers, configure a section for your producer settings which inherits the default settings (by using `${akka.kafka.producer}`) and add a service name and a timeout for the service lookup. Setting the `service-name` in the `akka.kafka.producer` config will work, if all your producers connect to the same Kafka broker.

The service name must match the one configured with the discovery technology you use. Overwrite the `resolve-timeout` depending on the discovery technology used, and your environment.

Note that consumers and producers may share a service (as shown in the examples on this page).

```hocon
discovery-producer: ${akka.kafka.producer} {
  service-name = "kafkaService1"
}
```

Scala
: @@snip [conf](/tests/src/test/scala/akka/kafka/ProducerSettingsSpec.scala) { #discovery-settings }

Java
: @@snip [conf](/tests/src/test/java/docs/javadsl/ProducerSettingsTest.java) { #discovery-settings }


## Use Config (HOCON) to describe the bootstrap servers

The setup below uses the built-in Akka Discovery implementation reading from Config (HOCON) files. That might be a good choice for development and testing. You may use the @extref:[Aggregate implementation](akka-docs:discovery/index.html#discovery-method-aggregate-multiple-discovery-methods) to first use another discovery technology, before falling back to the config file.

@@snip [conf](/tests/src/test/scala/akka/kafka/ConsumerSettingsSpec.scala) { #discovery-with-config }

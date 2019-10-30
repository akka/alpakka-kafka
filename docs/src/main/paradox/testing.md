---
project.description: Alpakka Kafka provides a Testkit with support for running local Kafka brokers for integration tests.
---
# Testing

To simplify testing of streaming integrations with Alpakka Kafka, it provides the **Alpakka Kafka testkit**. It provides help for

* @ref:[Using an embedded Kafka broker](#testing-with-an-embedded-kafka-server)
* @ref:[Using Docker to launch a local Kafka cluster with testcontainers](testing-testcontainers.md)
* @ref:[Mocking the Alpakka Kafka Consumers and Producers](#mocking-the-consumer-or-producer)

@@project-info{ projectId="testkit" }

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-stream-kafka-testkit_$scala.binary.version$
  version=$project.version$
  scope=test
}

Note that Akka testkits do not promise binary compatibility. The API might be changed even between patch releases.

The table below shows Alpakka Kafka testkit's direct dependencies and the second tab shows all libraries it depends on transitively. 
We've overriden the `commons-compress` library to use a version with [fewer known security vulnerabilities](https://commons.apache.org/proper/commons-compress/security-reports.html).

@@dependencies { projectId="testkit" }

## Running Kafka with your tests

The Testkit provides a variety of ways to test your application against a real Kafka broker or cluster. There are two main options:

1. @ref:[Embedded Kafka](#testing-with-an-embedded-kafka-server)
2. @ref:[Testcontainers (Docker)](testing-testcontainers.md)

The table below helps guide you to the right Testkit implementation depending on your programming language, testing framework, and use (or not) of Docker containers.
You must mix in or implement these types into your test classes to use them.
See the documentation for each for more details.

| Type                                                                                                                                                | Test Framework | Runtime Mode    | Cluster | Schema Registry | Lang     | Lifetime             |
|-----------------------------------------------------------------------------------------------------------------------------------------------------|----------------|-----------------|---------|-----------------|----------|----------------------|
| @ref:[`akka.kafka.testkit.javadsl.EmbeddedKafkaTest`](#testing-with-avro-and-schema-registry-from-java-code)                                        | JUnit 5        | Embedded Kafka  | No      | Yes             | Java     | All tests, Per class |
| @ref:[`akka.kafka.testkit.javadsl.EmbeddedKafkaJunit4Test`](#testing-with-avro-and-schema-registry-from-java-code)                                  | JUnit 4        | Embedded Kafka  | No      | Yes             | Java     | All tests, Per class |
| @ref:[`akka.kafka.testkit.scaladsl.EmbeddedKafkaLike`](#testing-with-avro-and-schema-registry-from-scala-code)                                      | ScalaTest      | Embedded Kafka  | No      | Yes             | Scala    | Per class            |
| @ref:[`akka.kafka.testkit.javadsl.TestcontainersKafkaJunit4Test`](testing-testcontainers.md#testing-with-a-docker-kafka-cluster-from-java-code)     | JUnit 5        | Testcontainers  | Yes     | No              | Java     | All tests, Per class |
| @ref:[`akka.kafka.testkit.javadsl.TestcontainersKafkaTest`](testing-testcontainers.md#testing-with-a-docker-kafka-cluster-from-java-code)           | JUnit 4        | Testcontainers  | Yes     | No              | Java     | All tests, Per class |
| @ref:[`akka.kafka.testkit.scaladsl.TestcontainersKafkaLike`](testing-testcontainers.md#testing-with-a-docker-kafka-cluster-from-scala-code)         | ScalaTest      | Testcontainers  | Yes     | No              | Scala    | All tests            |
| @ref:[`akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike`](testing-testcontainers.md#testing-with-a-docker-kafka-cluster-from-scala-code) | ScalaTest      | Testcontainers  | Yes     | No              | Scala    | Per class            |

## Testing with an embedded Kafka server

To test the Alpakka Kafka connector the [Embedded Kafka library](https://github.com/embeddedkafka/embedded-kafka) is an important tool as it helps to easily start and stop Kafka brokers from test cases.

Add the Embedded Kafka to your test dependencies:

@@dependency [Maven,sbt,Gradle] {
  group=io.github.embeddedkafka
  artifact=embedded-kafka_2.12
  version=$embeddedKafka.version$
  scope=test
}

@@@ note

As Kafka uses Scala internally, only the Scala versions supported by Kafka can be used together with Embedded Kafka. To be independent of Kafka's supported Scala versions, run @ref:[Kafka in a Docker container](testing-testcontainers.md).

The helpers for running Embedded Kafka are available for **Scala 2.11 and 2.12**.

@@@

The testkit contains helper classes used by the tests in the Alpakka Kafka connector and may be used for other testing, as well.


### Testing with Avro and Schema Registry

If you need to run tests using [Confluent's Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html), you might include [embedded-kafka-schema-registry](https://github.com/embeddedkafka/embedded-kafka-schema-registry) instead.


### Testing with Avro and Schema Registry from Java code

Test classes may extend `akka.kafka.testkit.javadsl.EmbeddedKafkaTest` (JUnit 5) or `akka.kafka.testkit.javadsl.EmbeddedKafkaJunit4Test` (JUnit 4) to automatically start and stop an embedded Kafka broker.

Furthermore it provides

* preconfigured consumer settings (`ConsumerSettings<String, String> consumerDefaults`),
* preconfigured producer settings (`ProducerSettings<String, String> producerDefaults`),
* unique topic creation (`createTopic(int number, int partitions, int replication)`), and
* `CompletionStage` value extraction helper (`<T> T resultOf(CompletionStage<T> stage, java.time.Duration timeout)`).

The example below shows skeleton test classes for JUnit 4 and JUnit 5.

Java JUnit 4
: @@snip [snip](/tests/src/test/java/docs/javadsl/AssignmentTest.java) { #testkit }

Java JUnit 5
: @@snip [snip](/tests/src/test/java/docs/javadsl/ProducerExampleTest.java) { #testkit }

The JUnit test base classes run the [`assertAllStagesStopped`](https://doc.akka.io/api/akka/current/akka/stream/testkit/javadsl/StreamTestKit$.html#assertAllStagesStopped) check from Akka Stream testkit to ensure all stages are shut down properly within each test. This may interfere with the `stop-timeout` which delays shutdown for Alpakka Kafka consumers. You might need to configure a shorter timeout in your `application.conf` for tests.


### Testing with Avro and Schema Registry from Scala code

The `KafkaSpec` class offers access to 

* preconfigured consumer settings (`consumerDefaults: ConsumerSettings[String, String]`),
* preconfigured producer settings (`producerDefaults: ProducerSettings[String, String]`),
* unique topic creation (`createTopic(number: Int = 0, partitions: Int = 1, replication: Int = 1)`),
* an implicit `LoggingAdapter` for use with the `log()` operator, and
* other goodies.

`EmbeddedKafkaLike` extends `KafkaSpec` to add automatic starting and stopping of the embedded Kafka broker.

Some Alpakka Kafka tests implemented in Scala use [Scalatest](http://www.scalatest.org/) with the mix-ins shown below. You need to add Scalatest explicitly in your test dependencies (this release of Alpakka Kafka uses Scalatest $scalatest.version$.)

@@dependency [Maven,sbt,Gradle] {
  group=org.scalatest
  artifact=scalatest
  version=$scalatest.version$
  scope=test
}

Scala
: @@snip [snip](/tests/src/test/scala/akka/kafka/scaladsl/SpecBase.scala) { #testkit }

By mixing in `EmbeddedKafkaLike` an embedded Kafka instance will be started before the tests in this test class execute shut down after all tests in this test class are finished.

Scala
: @@snip [snip](/tests/src/test/scala/akka/kafka/scaladsl/EmbeddedKafkaSampleSpec.scala) { #embeddedkafka }

With this `EmbeddedKafkaSpecBase` class test classes can extend it to automatically start and stop a Kafka broker to test with. To configure the Kafka broker non-default, override the `createKafkaConfig` as shown above.

To ensure proper shutdown of all stages in every test, wrap your test code in [`assertAllStagesStopped`](https://doc.akka.io/api/akka/current/akka/stream/testkit/scaladsl/StreamTestKit$.html#assertAllStagesStopped). This may interfere with the `stop-timeout` which delays shutdown for Alpakka Kafka consumers. You might need to configure a shorter timeout in your `application.conf` for tests.

## Alternative testing libraries

If using Maven and Java, an alternative library that provides running Kafka broker instance during testing is [kafka-unit by salesforce](https://github.com/salesforce/kafka-junit). It has support for Junit 4 and 5 and supports many different versions of Kafka.

## Mocking the Consumer or Producer

The testkit contains factories to create the messages emitted by Consumer sources in `akka.kafka.testkit.ConsumerResultFactory` and Producer flows in `akka.kafka.testkit.ProducerResultFactory`.

To create the materialized value of Consumer sources, @scala[`akka.kafka.testkit.scaladsl.ConsumerControlFactory`]@java[`akka.kafka.testkit.javadsl.ConsumerControlFactory`] offers a wrapped `KillSwitch`.

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/TestkitSamplesSpec.scala) { #factories }

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/TestkitSamplesTest.java) { #factories }

@@@ index

* [testcontainers](testing-testcontainers.md)

@@@

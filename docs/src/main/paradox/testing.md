# Testing

To simplify testing of streaming integrations with Alpakka Kafka, it provides the **Alpakka Kafka testkit**. It provides help for

* @ref:[mocking the Alpakka Kafka Consumers and Producers](#mocking-the-consumer-or-producer)
* @ref:[using an embedded Kafka broker](#testing-with-an-embedded-kafka-server)
* @ref:[starting and stopping Kafka in Docker](#testing-with-kafka-in-docker)

@@project-info{ projectId="testkit" }

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-stream-kafka-testkit_$scala.binary.version$
  version=$project.version$
}

Note that Akka testkits do not promise binary compatibility. The API might be changed even between patch releases.

The table below shows Alpakka Kafka testkit's direct dependencies and the second tab shows all libraries it depends on transitively. We've overriden the `commons-compress` library to use a version with [fewer known security vulnerabilities](https://commons.apache.org/proper/commons-compress/security-reports.html).

@@dependencies { projectId="testkit" }


## Mocking the Consumer or Producer

The testkit contains factories to create the messages emitted by Consumer sources in `akka.kafka.testkit.ConsumerResultFactory` and Producer flows in `akka.kafka.testkit.ProducerResultFactory`.

To create the materialized value of Consumer sources, @scala[`akka.kafka.testkit.scaladsl.ConsumerControlFactory`]@java[`akka.kafka.testkit.javadsl.ConsumerControlFactory`] offers a wrapped `KillSwitch`.

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/TestkitSamplesSpec.scala) { #factories }

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/TestkitSamplesTest.java) { #factories }


## Testing with an embedded Kafka server

To test the Alpakka Kafka connector the [Embedded Kafka library](https://github.com/embeddedkafka/embedded-kafka) is an important tool as it helps to easily start and stop Kafka brokers from test cases.

@@@ note

As Kafka uses Scala internally, only the Scala versions supported by Kafka can be used together with Embedded Kafka. To be independent of Kafka's supported Scala versions, run [Kafka in a Docker container](#testing-with-kafka-in-docker).

The helpers for running Embedded Kafka are available for **Scala 2.11 and 2.12**.

@@@

The testkit contains helper classes used by the tests in the Alpakka Kafka connector and may be used for other testing, as well.


### Testing with Avro and Schema Registry

If you need to run tests using [Confluent's Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html), you might include [embedded-kafka-schema-registry](https://github.com/embeddedkafka/embedded-kafka-schema-registry) instead.


### Testing from Java code

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


### Testing from Scala code

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


## Testing with Kafka in Docker

The [Testcontainers](https://www.testcontainers.org/) project contains a nice API to start and stop Apache Kafka in Docker containers. This becomes very relevant when your application code uses a Scala version which Apache Kafka doesn't support so that *EmbeddedKafka* can't be used.

@@@note

The Testcontainers support is new to Alpakka Kafka since 1.0.2 and may evolve a bit more.

@@@

### Testing from Java code

The Alpakka Kafka testkit contains helper classes to start Kafka via Testcontainers. Alternatively, you may use just Testcontainers, as it is designed to be used with JUnit and you can follow [their documentation](https://www.testcontainers.org/modules/kafka/) to start and stop Kafka. To start a single instance for many tests see [Singleton containers](https://www.testcontainers.org/test_framework_integration/manual_lifecycle_control/).

The Testcontainers dependency must be added to your project explicitly.

@@dependency [Maven,sbt,Gradle] {
  group=org.testcontainers
  artifact=kafka
  version=$testcontainers.version$
  scope=test
}

The example below shows skeleton test classes for JUnit 4 and JUnit 5. The Kafka broker will start before the first test and be stopped after all test classes are finished.

Java JUnit 4
: @@snip [snip](/tests/src/test/java/docs/javadsl/AssignmentWithTestcontainersTest.java) { #testkit }

Java JUnit 5
: @@snip [snip](/tests/src/test/java/docs/javadsl/ProducerWithTestcontainersTest.java) { #testkit }


### Testing from Scala code

The Testcontainers dependency must be added to your project explicitly.

@@dependency [Maven,sbt,Gradle] {
  group=org.testcontainers
  artifact=kafka
  version=$testcontainers.version$
  scope=test
}

By mixing in `TestcontainersKafkaLike` the Kafka Docker container will be started before the first test and shut down after all tests are finished.

Scala
: @@snip [snip](/tests/src/test/scala/akka/kafka/scaladsl/SpecBase.scala) { #testkit #testcontainers}


With this `TestcontainersSampleSpec` class test classes can extend it to automatically start and stop a Kafka broker to test with.

To ensure proper shutdown of all stages in every test, wrap your test code in [`assertAllStagesStopped`](https://doc.akka.io/api/akka/current/akka/stream/testkit/scaladsl/StreamTestKit$.html#assertAllStagesStopped). This may interfere with the `stop-timeout` which delays shutdown for Alpakka Kafka consumers. You might need to configure a shorter timeout in your `application.conf` for tests.


## Alternative testing libraries

If using Maven and Java, an alternative library that provides running Kafka broker instance during testing is [kafka-unit by salesforce](https://github.com/salesforce/kafka-junit). It has support for Junit 4 and 5 and supports many different versions of Kafka.

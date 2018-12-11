# Testing

To simplify testing of streaming integrations with Alpakka Kafka, it provides the **Alpakka Kafka testkit**.

@@project-info{ projectId="testkit" }

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-stream-kafka-testkit_$scala.binary.version$
  version=$project.version$
}

Note that Akka testkits do not promise binary compatibility. The API might be changed even between minor versions.

The table below shows Alpakka Kafka testkits's direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="testkit" }


## Mocking the Consumer or Producer

The testkit contains factories to create the messages emitted by Consumer sources in `akka.kafka.testkit.ConsumerResultFactory` and Producer flows in `akka.kafka.testkit.ProducerResultFactory`.

To create the materialized value of Consumer sources, @scala[`akka.kafka.testkit.scaladsl.ConsumerControlFactory`]@java[`akka.kafka.testkit.javadsl.ConsumerControlFactory`] offers a wrapped `KillSwitch`.

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/TestkitSamplesSpec.scala) { #factories }

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/TestkitSamplesTest.java) { #factories }


## Testing with an embedded Kafka server

To test the Alpakka Kafka connector the [Embedded Kafka library](https://github.com/manub/scalatest-embedded-kafka) is an important tool as it helps to easily start and stop Kafka brokers from test cases.

The testkit contains helper classes used by the tests in the Alpakka Kafka connector and may be used for other testing, as well.


### Testing from Java code

Test classes may extend `akka.kafka.testkit.javadsl.EmbeddedKafkaJunit4Test` to automatically start and stop an embedded Kafka broker.

Furthermore it provides

* preconfigured consumer settings (`ConsumerSettings<String, String> consumerDefaults`),
* preconfigured producer settings (`ProducerSettings<String, String> producerDefaults`),
* unique topic creation (`createTopic(int number, int partitions, int replication)`), and
* `CompletionStage` value extraction helper (`<T> T resultOf(CompletionStage<T> stage, java.time.Duration timeout)`).

The example below shows a skeleton test class for use with JUnit 4.

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/AssignmentTest.java) { #testkit }


### Testing from Scala code

The `KafkaSpec` class offers access to 
* preconfigured consumer settings (`ConsumerSettings<String, String> consumerDefaults`),
* preconfigured producer settings (`ProducerSettings<String, String> producerDefaults`),
* unique topic creation (`createTopic(number: Int = 0, partitions: Int = 1, replication: Int = 1)`),
* an implicit `LoggingAdapter` for use with the `log()` operator, and
* other goodies.

`EmbeddedKafkaLike` extends `KafkaSpec` to add automatic starting and stopping of the embedded Kafka broker.

Most Alpakka Kafka tests implemented in Scala use [Scalatest](http://www.scalatest.org/) with the mix-ins shown below. You need to add Scalatest explicitly in your test dependencies (this release of Alpakka Kafka uses Scalatest $scalatest.version$.)

Scala
: @@snip [snip](/tests/src/test/scala/akka/kafka/scaladsl/SpecBase.scala) { #testkit }


With this `SpecBase` class test classes can extend it to automatically start and stop a Kafka broker to test with.

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/AssignmentSpec.scala) { #testkit }

## Alternative testing libraries

If using Maven and Java, an alternative library that provides running Kafka broker instance during testing is [kafka-unit by salesforce](https://github.com/salesforce/kafka-junit). It has support for Junit 4 and 5 and supports many different versions of Kafka.

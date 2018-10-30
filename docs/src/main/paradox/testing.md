# Testing

To simplify testing of streaming integrations with Alpakka Kafka, it provides the Alpakka Kafka testkit.

@@dependency [Maven,sbt,Gradle] {
  group=com.typesafe.akka
  artifact=akka-stream-kafka-testkit_$scala.binary.version$
  version=$version$
}

## Mocking the Consumer or Producer

The testkit contains factories to create the messages emitted by Consumer sources in `akka.kafka.testkit.ConsumerResultFactory` and Producer flows in `akka.kafka.testkit.ProducerResultFactory`.

To create the materialized value of Consumer sources, `akka.kafka.testkit.scaladsl.ConsumerControlFactory` offers a wrapped `KillSwitch`.

Scala
: @@snip [snip](/tests/src/test/scala/docs/scaladsl/TestkitSamplesSpec.scala) { #factories }

Java
: @@snip [snip](/tests/src/test/java/docs/javadsl/TestkitSamplesTest.java) { #factories }

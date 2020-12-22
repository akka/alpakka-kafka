---
project.description: Handle errors from the Kafka API in Alpakka Kafka.
---
# Error handling

## Failing consumer

Errors from the Kafka consumer will be forwarded to the Alpakka sources that use it, the sources will fail their streams.

### Lost connection to the Kafka broker

To fail a Alpakka Kafka consumer in case the Kafka broker is not available, configure a **Connection Checker** via @apidoc[ConsumerSettings]. If not **Connection Checker** is configured, Alpakka will continue to poll the broker indefinitely.


## Failing producer

Retry handling for producers is built-in into Kafka. In case of failure when sending a message, an exception will be thrown, which should fail the stream. 

## Restarting the stream with a backoff stage

Akka streams @extref[provides graph stages](akka:stream/stream-error.html#delayed-restarts-with-a-backoff-stage)
to gracefully restart a stream on failure, with a configurable backoff. This can be taken advantage of to restart a failing stream and its consumer with an exponential backoff, by wrapping it in a `RestartSource`.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #restartSource }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExampleTest.java) { #restartSource }

When a stream fails, library internals will handle all underlying resources.

@@@note { title=(de)serialization }

If reading from Kafka failure is caused by other reasons, like **deserialization problems**, then the stage will fail immediately. If you expect such cases, consider
consuming raw byte arrays and deserializing in a subsequent `map` stage where you can use supervision to skip failed elements. See also @ref:[Serialization](serialization.md) and @ref:["At least once"](atleastonce.md) pages for more suggestions.

@@@

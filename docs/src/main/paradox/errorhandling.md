# Error handling

## Failing consumer

When a consumer fails to read from Kafka due to connection problems, it throws a @javadoc[WakeupException](org.apache.kafka.common.errors.WakeupException) which is handled internally with retries. Refer to consumer configuration [settings](consumer.html#settings) for details on `wakeup-timeout` and `max-wakeups` if you're interested in tweaking the retry handling parameters.
When the currently configured number of `max-wakeups` is reached, the source stage will fail with an exception and stop.

## Failing producer

Retry handling for producers is built-in into Kafka. In case of failure when sending a message, an exception will be thrown, which should fail the stream. 

## Restarting the stream with a backoff stage

Akka streams @extref[provides graph stages](akka-docs:stream/stream-error.html#delayed-restarts-with-a-backoff-stage)
to gracefully restart a stream on failure, with a configurable backoff. This can be taken advantage of to restart a failing consumer with an exponential backoff, by wrapping it in a `RestartSource`:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/ConsumerExample.scala) { #restartSource }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/ConsumerExample.java) { #restartSource }

When a stream fails, library internals will handle all underlying resources.

@@@note { title=(de)serialization }

If reading from Kafka failure is caused by other reasons, like **deserialization problems**, then the stage will fail immediately. If you expect such cases, consider
consuming raw byte arrays and deserializing in a subsequent `map` stage where you can use supervision to skip failed elements. See also the ["At least once"](atleastonce.html) page for more suggestions.

@@@

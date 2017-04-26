# Error handling

## Failing consumer

When a consumer fails to read from Kafka due to connection problems, it throws a WakeupException which is handled internally with retries. Refer to consumer configuration [settings](consumer.html#settings) for details on `wakeup-timeout` and `max-wakeups` if you're interested in tweaking the retry handling parameters.
When the last retry fails, source stage will be failed with an exception.

## Failing producer

Retry handling in case of producer is built-in into Kafka. In case of failure when sending a message, an exception will be thrown, which should fail the stream. 

## Restarting the stream

Typical approach is to run the stream inside an actor. When there's an exception, this actor should be stopped and a new one should be created.
Stopping the actor on stream failure:

Scala
: @@ snip [stopping](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #errorHandlingStop }

Java
: @@ snip [stopping](../../test/java/sample/javadsl/StreamWrapperActor.java) { #errorHandlingStop }

In order to ensure that stopped actor gets re-created, it can be wrapped with a [BackoffSupervisor](http://doc.akka.io/docs/akka/current/general/supervision.html#Delayed_restarts_with_the_BackoffSupervisor_pattern)

Scala
: @@ snip [restart](../../test/scala/sample/scaladsl/ConsumerExample.scala) { #errorHandlingSupervisor}

Java
: @@ snip [stopping](../../test/java/sample/javadsl/StreamWrapperActor.java) { #errorHandlingSupervisor }

When a stream fails, library internals will handle all underlying resources.

@@@note { title=(de)serialization }

If reading from Kafka failure is caused by other reasons, like **deserialization problems**, then the stage will fail immediately. If you expect such cases, consider
consuming raw byte arrays and deserializing in a subsequent `map` stage where you can use supervision to skip failed elements. See also the ["At least once"](atleastonce.html) page for more suggestions.

@@@

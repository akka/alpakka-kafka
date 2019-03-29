# At-Least-Once Delivery

At-least-once delivery semantics, the requirement to process every message, is a basic requirement of most applications. 

When using committable sources (@ref:[Offset Storage in Kafka](consumer.md#offset-storage-in-kafka-committing)), care is needed to ensure at-least-once delivery semantics are not lost inadvertently by committing an offset too early.

Below are some scenarios where this risk is present. These risks can easily be overlooked. Problems can also go undetected during tests since they depend on abruptly interrupting
the flow in a particular state, and that state could be unlikely to occur. 

## Multiple Effects per Commit

### Multiple Messages

When connecting a committable source to a producer flow, some applications may require each consumed message to produce more than one message. In that case, in order to preserve at-least-once semantics, the message offset should only be committed after all associated messages have been produced.

To achieve this, use the `ProducerMessage.MultiMessage` implementation of `Envelope`:

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/AtLeastOnce.scala) { #oneToMany }  

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/AtLeastOnceTest.java) { #oneToMany }


### Batches

If committable messages are processed in batches (using `batch` or `grouped`), it is also important to commit the resulting `CommittableOffsetBatch` only after all messages in the batch are fully processed.

Should the batch need to be split up again, using mapConcat, care should be taken to associate the `CommittableOffsetBatch` only with the last message. This scenario could occur if we created batches to more efficiently update a database and then needed to split up the batches to send individual messages to a Kafka producer flow.

### Multiple Destinations

In the Conditional Message Processing section below we discuss how to handle producing to multiple Kafka topics, but here we consider writing to other types of persistent storage, or performing other side-effects, perhaps in addition to producing to Kafka topics.

To commit an offset or an offset batch only after the multiple effects have been performed, we will usually want to asssemble our side-effecting flows in series, one after the other. This still allows the side effects to be performed concurrently on distinct messages, using `mapAsync` for example.

Alternatively, we could split-off the flow using `alsoTo` to perform the effects in distinct parallel flows. We would then use `zip` to bring the two flows back together and re-associate the matching committable offsets. This step is important to ensure that we only commit an offset once the effects from both flows are complete. This constrains the two flows to output the exact same sequence of committable offsets. So this approach may not be significantly more flexible then a serial arrangement.

## Non-Sequential Processing

Messages from committable sources should be processed in order, otherwise a larger offset for a partition could be committed before the work associated to a smaller offset has been completed.

Reordering would be acceptable if the original order was reconstituted before committing the offsets, but that is a fairly complex and possibly brittle process that we will not consider here.

Using `mapAsync` is safe since it preserves the order of messages. That is in constrast to `mapAsyncUnordered` which would not be safe to use here. As indicated in the @extref[Akka Streams documentation](akka-docs:/scala/stream/stream-flows-and-basics.html#Stream_ordering) almost all stages will preserve input ordering.

### Using groupBy

Using `groupBy` followed at some point by a `mergeSubstreams` can reorder messages, so in general it is not safe with respect to the at-least-once guarantee.

However it can only lead to reordering between messages sent to different substreams, so it is possible to use `groupBy` and preserve at-least-once semantics as long as all messages from the same partition are sent to the same substream.

If a particular substream expects to see all messages regarding some entity, it then requires that writers to the source topic become responsible for placing messages about various entities in the appropriate partitions. If your application already has a requirement to preserve the order of messages about a particular entity within a Kafka topic, you will already need to ensure those messages go to the same partition since Kafka only preserves order information within a partition.

## Conditional Message Processing
 
Most flows will require some messages to be handled differently from others. Unfortunately this is difficult to do while preserving the at-least-once guarantee because the order of messages must be maintained.

We cannot safely send off the messages to be handled differently to a distinct flow: this other flow cannot commit on its own, and even if we merge it back, downstream, with the main flow, ordering will not be preserved. The reason this separate flow cannot commit on its own is that it will only be seeing a subset of the committable messages. If it commits an offset, it cannot know that all prior offsets have been processed in the main flow.

This is a significant challenge. Below we suggest a few strategies to deal with some special cases of this general problem.
 
### Publishing to Message-Dependent Topics

Since `ProducerRecord` contains the destination topic, it is possible to use a single producer flow to write to any number of topics. This preserves the ordering of messages coming from the committable source. Since the destination topics likely admit different types of messages, it will be necessary to serialize the messages to the appropriate input type for the common producer flow, which could be a byte array or a string.

In case a committable message should lead to the production of multiple messages, the `ProducerMessage.MultiMessage` is available. If no messages should be produced, the `ProducerMessage.PassThroughMessage` can be used.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/AtLeastOnce.scala) { #oneToConditional }  

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/AtLeastOnceTest.java) { #oneToConditional }


### Excluding Messages

Failure to deserialize a message is a particular case of conditional message processing. It is also likely that we would have no message to produce to Kafka when we encounter messages that fail to deserialize. As described above, the producer flow will not let us pass through the corresponding committable offset without producing a message. 

Why can't we commit the offsets of bad messages as soon as we encounter them, instead of passing them downstream? Because the previous offsets, for messages that have deserialized successfully, may not have been committed yet. That's possible if the downstream flow includes a buffer, an asynchronous boundary or performs batching. It is then likely that some previous messages would concurrently be making their way downstream to a final committing stage.

Note that here we assume that we take the full control over the handling of messages that fail to deserialize. To do this, we should not ask for the deserialization to be performed by the committable source. We can instead create a `ConsumerSettings` parametrized by byte arrays. A subsequent `map` can deserialize and use `ProducerMessage.PassThroughMessage` to skip bad messages.

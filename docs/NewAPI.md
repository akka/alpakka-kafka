Reactive Streams for Kafka. New API
====

# Consumer #

## Core ##

Consumer in new API represents two processes:
 - message emmit
 - offset commit

The message emmit represented as a `message` `Out` and the offset commit represented as a `commit` `In` and a `confirmation` `Out`.

![Consumer shape](./Consumer.png)

You can create such consumer via `Consumer.apply` method.

Here it is an example consumer usage:

![Consumer example](./Consumer-example.png)

```scala
  val graph = GraphDSL.create(Consumer[Array[Byte], String](provider)) { implicit b => kafka =>
    import GraphDSL.Implicits._
    type In = ConsumerRecord[Array[Byte], String]
    val processing = Flow[In].map{ x => ??? /* your logic here */ ; x }
    val shutdownHandler: Sink[Any, Unit] = ??? // your logic here

    kafka.messages ~> processing ~> Consumer.record2commit ~> kafka.commit
    kafka.confirmation ~> shutdownHandler
    ClosedShape
  }

```

## Consumer control ##

To control consumer you should use `Control` object given after meterialization:

```scala
val control = RunnableGraph.fromGraph(graph).run()
```

`Control` provides one method `stop` which issues stopping of the consumer. After you call `stop` the consumer asynchronously completes a `message` `Out` and will wait for a `commit` `In` completion. After the `commit` completes the consumer will wait for all confirmations to be completed and next will complete itself.

## Simple shapes ##

You can use simpler shapes based on a consumer shape.

If you do not care about offset confirmations then the consumer may be represented as `Flow` and created via `Consumer.flow`:

![Consumer flow shape](./Consumer-flow.png)

If you do not care about offset commit at all you may represent consumer as a message `Source` and create it via `Consumer.source`:

![Consumer source shape](./Consumer-source.png)


# Producer #

Producer represents a process of message publishing to kafka and getting confirmation of this publication.

Producer is represented as a `Flow` shape and may be created with `Producer.apply` method.

![Producer shape](./Producer.png)

If you do not care about confirmation you can use producer as `Sink` and create it with `Producer.sink`.

![Producer shape](./Producer-sink.png)

To complete producer just complete it `In`.



---
name: "\U0001F41B Bug report"
about: Create a report to improve Alpakka Kafka
---

### Are you looking for help?

If you have a [Lightbend Subscription](https://www.lightbend.com/lightbend-platform-subscription), please reach out via the [Lightbend Portal](https://portal.lightbend.com/).

This is an issue tracker used to manage and track the development of **Alpakka Kafka**.

Please report issues regarding specific projects in their respective issue trackers, e.g.:
 - Akka:      https://github.com/akka/akka/issues 
 - Akka HTTP: https://github.com/akka/akka-http/issues 
 - Alpakka:   https://github.com/akka/alpakka/issues 

This is not a support system, please ask questions or discuss ideas in the [Lightbend discuss forum](https://discuss.lightbend.com/c/akka/streams-and-alpakka).

We've collected a few tips for [debugging Alpakka Kafka](https://doc.akka.io/docs/akka-stream-kafka/current/debugging.html).


## Please add the following sections to your bug report

### Versions used 
Alpakka Kafka version: ____ (1.0-M1 / 1.0-RC1 / etc)

Akka version: ____ (2.5.19 / etc)

Kafka version: ____ (2.0 / etc)


### Expected Behavior

Please describe the expected behavior of the issue, starting from the first action.


### Actual Behavior

Please provide a description of what actually happens, working from the same starting point.


### Used Alpakka Kafka configuration

If deemed relevant, please add the configuration of Alpakka Kafka. With debug logging enabled the Alpakka Kafka consumers log their configuration at startup, it looks something like
```
akka.kafka.internal.KafkaConsumerActor  Creating Kafka consumer with akka.kafka.ConsumerSettings(properties=enable.auto.commit -> false,group.id -> group1,ke...
```


### Relevant logs

If you identified a section in your logs that explains the bug or might be important to understand it, please add it.


### Reproducible Test Case

Please provide a PR with a failing test.

If the issue is more complex or requires configuration, please provide a link to a project on Github that reproduces the issue.

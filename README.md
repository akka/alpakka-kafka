Reactive Streams for Kafka
====
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.typesafe.akka/akka-stream-kafka_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.typesafe.akka/akka-stream-kafka_2.11)
If you have questions or are working on a pull request or just curious, please feel welcome to join the chat room: [![Join the chat at https://gitter.im/akka/reactive-kafka](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/akka/reactive-kafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


[Akka Streams](http://doc.akka.io/docs/akka/current/scala/stream/index.html) connector for [Apache Kafka](https://kafka.apache.org/).

Created and maintained by
[<img src="https://softwaremill.com/images/header-main-logo.3449d6a3.svg" alt="SoftwareMill logo" height="25">](https://softwaremill.com)

## Documentation for version 0.11 or later

[Documentation](http://doc.akka.io/docs/akka-stream-kafka/current/) and [API](http://doc.akka.io/api/akka-stream-kafka/current/)

Supports Kafka 0.10.x

## Documentation for version 0.10 or earlier

The documentation for the old API can be found in [OLD_README.md](https://github.com/akka/reactive-kafka/blob/master/OLD_README.md)

Supports Kafka 0.9.0.x

## Akka versions compatibility

Please note that while the library depends on Akka 2.4.x is is fully compatible to be used with Akka 2.5.x.
This is because of Akka's strict [backwards compatibility guarantees](http://doc.akka.io/docs/akka/2.5.3/scala/common/binary-compatibility-rules.html). If you want to use reactive-kafka with Akka 2.5 simply include Akka **and** Akka Streams dependencies using the latest 2.5 version, for example like this:

```
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.x" // pick the latest version here
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.x"
```

Note that it is important that all `akka-*` dependencies are in the same version, so it is recommended to depend on them explicitly to avoid problems with transient dependencies causing an unlucky mix of versions.

## Contributions

Contributions are welcome, see [CONTRIBUTING.md](https://github.com/akka/reactive-kafka/blob/master/CONTRIBUTING.md)

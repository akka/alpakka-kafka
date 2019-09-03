---
project.description: Snapshot builds of Alpakka Kafka are provided via a Bintray repository.
---
# Snapshots

[snapshots-badge]:  https://api.bintray.com/packages/akka/snapshots/alpakka-kafka/images/download.svg
[snapshots]:        https://bintray.com/akka/snapshots/alpakka-kafka/_latestVersion

Snapshots are published to a repository in Bintray after every successful build on master.
Add the following to your project build definition to resolve Alpakka Kafka connector snapshots:

## Configure repository

Maven
:   ```xml
    <project>
    ...
      <repositories>
        <repository>
          <id>alpakka-kafka-snapshots</id>
          <name>Alpakka Kafka Snapshots</name>
          <url>https://dl.bintray.com/akka/snapshots</url>
        </repository>
      </repositories>
    ...
    </project>
    ```

sbt
:   ```scala
    resolvers += Resolver.bintrayRepo("akka", "snapshots")
    ```

Gradle
:   ```gradle
    repositories {
      maven {
        url  "https://dl.bintray.com/akka/snapshots"
      }
    }
    ```

## Documentation

The [snapshot documentation](https://doc.akka.io/docs/alpakka-kafka/snapshot/) is updated with every snapshot build.

## Versions

Latest published snapshot version is [![snapshots-badge][]][snapshots]

The snapshot repository is cleaned from time to time with no further notice. Check [Bintray Alpakka Kafka files](https://bintray.com/akka/snapshots/alpakka-kafka#files/com/typesafe/akka) to see what versions are currently available.

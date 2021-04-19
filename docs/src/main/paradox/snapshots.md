---
project.description: Snapshot builds of Alpakka Kafka are provided via the Sonatype snapshot repository.
---
# Snapshots

[snapshots-badge]:  https://img.shields.io/nexus/s/com.typesafe.akka/akka-stream-kafka_2.13?server=https%3A%2F%2Foss.sonatype.org
[snapshots]:        https://oss.sonatype.org/content/repositories/snapshots/com/typesafe/akka/akka-stream-kafka_2.13/

Snapshots are published to the Sonatype Snapshot repository after every successful build on master.
Add the following to your project build definition to resolve Alpakka Kafka connector snapshots:

## Configure repository

Maven
:   ```xml
    <project>
    ...
      <repositories>
        <repository>
            <id>snapshots-repo</id>
            <name>Sonatype snapshots</name>
            <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        </repository>
      </repositories>
    ...
    </project>
    ```

sbt
:   ```scala
    resolvers += Resolver.sonatypeRepo("snapshots")
    ```

Gradle
:   ```gradle
    repositories {
      maven {
        url  "https://oss.sonatype.org/content/repositories/snapshots"
      }
    }
    ```

## Documentation

The [snapshot documentation](https://doc.akka.io/docs/alpakka-kafka/snapshot/) is updated with every snapshot build.

## Versions

Latest published snapshot version is [![snapshots-badge][]][snapshots]

The snapshot repository is cleaned from time to time with no further notice. Check [Sonatype snapshots Alpakka Kafka files](https://oss.sonatype.org/content/repositories/snapshots/com/typesafe/akka/akka-stream-kafka_2.13/) to see what versions are currently available.

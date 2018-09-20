# Snapshots

[snapshots-badge]:  https://api.bintray.com/packages/akka/snapshots/alpakka-kafka/images/download.svg
[snapshots]:        https://bintray.com/akka/snapshots/alpakka-kafka/_latestVersion

Snapshots are published after every merged PR to a repository in bintray. Add the following to your project build definition to resolve Alpakka Kafka connector snapshots:

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

Latest published snapshot version is [![snapshots-badge][]][snapshots]

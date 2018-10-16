# Serialization

The general recommendation for de-/serialization of messages is to use byte arrays as value and do the de-/serialization in a `map` operation in the Akka Stream instead of implementing it directly in Kafka de-/serializers.


## Jackson JSON

Serializing data to JSON text with Jackson in a `map` operator will turn the object instance into a String which is used as value in the `ProducerRecord`.

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/SerializationTest.java) { #jackson-imports #jackson-serializer }


To de-serialize a JSON String with Jackson in a `map` operator, extract the String and apply the Jackson object reader in a `map` operator. Amend the `map` operator with the extracted type as the object reader is not generic.

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/SerializationTest.java) { #jackson-imports #jackson-deserializer }



## Avro with Schema Registry

If you want to use [Confluent's Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html), you need to include the dependency on `kafka-avro-serializer` as shown below. It is not available from Maven Central, that's why Confluent's repository has to be specified. These examples use `kafka-avro-seriazlizer` version $confluent.version$.

Maven
:   ```xml
    <project>
    ...
      <dependencies>
        ...
        <dependency>
          <groupId>io.confluent</groupId>
          <artifactId>kafka-avro-serializer</artifactId>
          <version>confluent.version (eg. 5.0.0)</version>
        </dependency>
        ...
      </dependencies>
      ...
      <repositories>
        <repository>
          <id>confluent-maven-repo</id>
          <name>Confluent Maven Repository</name>
          <url>https://packages.confluent.io/maven/</url>
        </repository>
      </repositories>
    ...
    </project>
    ```

sbt
:   ```scala
    libraryDependencies += "io.confluent" % "kafka-avro-serializer" % confluentAvroVersion, //  eg. 5.0.0
    resolvers += "Confluent Maven Repository" at "https://packages.confluent.io/maven/",
    ```

Gradle
:   ```gradle
    dependencies {
      compile group: 'io.confluent', name: 'kafka-avro-serializer', version: confluentAvroVersion // eg. 5.0.0
    }
    repositories {
      maven {
        url  "https://packages.confluent.io/maven/"
      }
    }
    ```


## Producer

To create serializers that use the Schema Registry, its URL needs to be provided as configuration `AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG` to the serializer and that serializer is used in the `ProducerSettings`.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/SerializationSpec.scala) { #imports #serializer }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/SerializationTest.java) { #imports #serializer }



## Consumer

To create deserializers that use the Schema Registry, its URL needs to be provided as configuration  `AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG` to the deserializer and that deserializer is used in the `ConsumerSettings`.

Scala
: @@ snip [snip](/tests/src/test/scala/docs/scaladsl/SerializationSpec.scala) { #imports #de-serializer }

Java
: @@ snip [snip](/tests/src/test/java/docs/javadsl/SerializationTest.java) { #imports #de-serializer }


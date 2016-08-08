akka-streams-kafka benchmarks
====

### Running in sbt
Adjust `akka.kafka.benchmarks.BenchmarksSpec` to define and parameterize which tests should be run.
Execute the `benchmarks/bench:test` task
Assumed default kafka host and port is "localhost:9092"

### Running with docker
Typical scenarios are:

1. All containers on a single host.
2. Kafka + Zookeeper on one host, app on another.

Assuming that you have docker and eventually docker-machine to switch between VMs:

1. Switch to the machine where you want Kafka/ZK to be deployed
2. Run:
`docker run -d --name zookeeper -p 2181:2181 jplock/zookeeper:3.4.6` for ZK
`docker run -d --name kafka --link zookeeper:zookeeper -p 9092:9092 ches/kafka:0.10.0.0` for Kafka
3. Optionally switch to the machine where you want your app to be deployed
4. Run `docker run -d --name benchmarks -e KAFKA_IP=172.17.0.3 -e TEST_NAME=akka-plain-consumer -e MSG_COUNT=500000000 com.typesafe.akka/akka-stream-kafka-benchmarks`
Options:
* `-e KAFKA_IP` to point to a specific host if you're testing remote Kafka connection.
* `-e TEST_NAME` to set which test to run. See `akka.kafka.benchmarks.Benchmarks` for all.
* `-e MSG_COUNT` to set number of messages.
5. When tests are finished, results should be available in `docker logs benchmarks`
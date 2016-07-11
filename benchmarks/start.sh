#!/bin/bash
echo Starting ZK and Kafka
nohup bash -c "cd /kafka && bin/zookeeper-server-start.sh config/zookeeper.properties &"
nohup bash -c "cd /kafka && bin/kafka-server-start.sh config/server.properties &"
echo Waiting for Kafka to be available
while ! nc -z localhost 9092; do
  sleep 0.1
done
echo Kafka is up and running, starting benchmarks
java -jar $1
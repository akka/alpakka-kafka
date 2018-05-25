/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package sample.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.kafka.*;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RestartFlow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.duration.Duration;

abstract class ProducerExample {
  protected final ActorSystem system = ActorSystem.create("example");

  protected final Materializer materializer = ActorMaterializer.create(system);

  // #producer
  // #settings
  final Config config = system.settings().config();
  final ProducerSettings<String, String> producerSettings =
      ProducerSettings
          .create(config, new StringSerializer(), new StringSerializer())
          .withBootstrapServers("localhost:9092");
  // #settings
  final KafkaProducer<String, String> kafkaProducer =
      producerSettings.createKafkaProducer();
  // #producer

  protected void terminateWhenDone(CompletionStage<Done> result) {
    result
      .exceptionally(e -> {
        system.log().error(e, e.getMessage());
        return Done.getInstance();
      })
      .thenAccept(d -> system.terminate());
  }
}

class PlainSinkExample extends ProducerExample {
  public static void main(String[] args) {
    new PlainSinkExample().demo();
  }

  public void demo() {
    // #plainSink
    CompletionStage<Done> done =
      Source.range(1, 100)
        .map(number -> number.toString())
        .map(value -> new ProducerRecord<String, String>("topic1", value))
        .runWith(Producer.plainSink(producerSettings), materializer);
    // #plainSink

    terminateWhenDone(done);
  }
}

class PlainSinkWithProducerExample extends ProducerExample {
    public static void main(String[] args) {
        new PlainSinkExample().demo();
    }

    public void demo() {
        // #plainSinkWithProducer
        CompletionStage<Done> done =
            Source.range(1, 100)
                .map(number -> number.toString())
                .map(value -> new ProducerRecord<String, String>("topic1", value))
                .runWith(Producer.plainSink(producerSettings, kafkaProducer), materializer);
        // #plainSinkWithProducer

        terminateWhenDone(done);
    }
}

class ObserveMetricsExample extends ProducerExample {
    public static void main(String[] args) {
        new PlainSinkExample().demo();
    }

    public void demo() {
        // #producerMetrics
        Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> metrics =
                kafkaProducer.metrics();// observe metrics
// #producerMetrics
    }
}

class ProducerFlowExample extends ProducerExample {
  public static void main(String[] args) {
    new ProducerFlowExample().demo();
  }

  public void demo() {
    // #flow
    CompletionStage<Done> done =
      Source.range(1, 100)
        .map(number -> {
          int partition = 0;
          String value = String.valueOf(number);
          return new ProducerMessage.Message<String, String, Integer>(
            new ProducerRecord<>("topic1", partition, "key", value),
            number
          );
        })
        .via(Producer.flow(producerSettings))
        .map(result -> {
          ProducerRecord<String, String> record = result.message().record();
          RecordMetadata meta = result.metadata();
          return meta.topic() + "/" + meta.partition() + " " + result.offset() + ": " + record.value();
        })
        .runWith(Sink.foreach(System.out::println), materializer);
    // #flow

    terminateWhenDone(done);
  }
}

class ProducerFlowExample2 extends ProducerExample {
  public static void main(String[] args) {
    new ProducerFlowExample().demo();
  }

  class ProcConfig {

      public String getKafkaBootstrapServers() {
          return  null;
      }

      public Map<String, String> getKafkaProperties() {
          return null;
      }

      public String getKafkaTopic() {
          return null;
      }

      public int getNumberMessageParsers() {
          return 2;

      }
  }

  /*
  class InternalMonitoringMessage {}

  public void demo() {

      ProcConfig procConfig = null;

      ConsumerSettings<byte[], String> consumerSettings = ConsumerSettings.create(system, new ByteArrayDeserializer(), new StringDeserializer())
              .withBootstrapServers(procConfig.getKafkaBootstrapServers()).withProperties(procConfig.getKafkaProperties()).withGroupId("mmp")
              .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

// OCP-Code
      ProducerSettings<byte[], String> producerSettings = ProducerSettings.create(system, new ByteArraySerializer(), new StringSerializer())
              .withBootstrapServers(procConfig.getKafkaBootstrapServers()).withProperties(procConfig.getKafkaProperties());


// Event Source
      Source<List<InternalMonitoringMessage>, Consumer.Control> kafkaSource = Consumer.committableSource(consumerSettings, Subscriptions.topics(procConfig.getKafkaTopic()))
              .buffer(30, OverflowStrategy.backpressure())
              .mapAsyncUnordered(procConfig.getNumberMessageParsers(),
                      msg -> CompletableFuture.supplyAsync(() -> MonitoringMessageParser.parseMonitoringEvent(msg, configurableMessageSelectorsHolder)))
              .async().groupedWithin(procConfig.getBatchSize(), Duration.create(procConfig.getBatchTimeoutSeconds(), TimeUnit.SECONDS));

// Event Flows
// Flow sending to ElasticSearch
      Flow<List, List<ConsumerMessage.CommittableOffset>, NotUsed> eventWriterFlow = RestartFlow.withBackoff(Duration.apply(30, TimeUnit.SECONDS), Duration.apply(300, TimeUnit.SECONDS), 0.2, 288,
              () -> Flow.fromGraph(new EventIndexWriter(eventIndexDAO)));

// Flow sending to OCP
      Flow<List, List<ConsumerMessage.CommittableOffset>, NotUsed> ocpWriterFlow =
          Flow.of(List.class)
              .flatMapConcat(msgList -> (Source<InternalMonitoringMessage, NotUsed>) Source.from(msgList))
              .filter(msgParam -> {
                  InternalMonitoringMessage msg = (InternalMonitoringMessage) msgParam;
                  if (procConfig.getOcpWhiteList() != null || procConfig.getOcpWhiteList().length > 0) {
                      for (String whitelistElement : procConfig.getOcpWhiteList()) {
                          if (StringUtils.equals(msg.getMonitoringMessage().getMsgType(), whitelistElement)) {
                              return true;
                          }
                      }
                      return false;
                  } else {
                      return false;
                  }
              }).map(msgParam -> {
                  InternalMonitoringMessage msg = (InternalMonitoringMessage) msgParam;
                  return new ProducerMessage.Message<byte[], String, String>(
                          new ProducerRecord<>(procConfig.getKafkaOCPTopic(), msg.getMonitoringMessage().getMsgId().getBytes(), msg.getMonitoringMessage().getPayload()), msg.getMonitoringMessage().getPayload());
              }).via(Producer.flow(producerSettings))
              .map(msg -> {
                  return new LinkedList<CommittableOffset>();
              });

// partial Flow commiting read kafka messages
      Flow<List, Done, NotUsed> commitFlow = Flow.of(List.class).map(offsetList -> foldLeft(offsetList)).mapAsync(3, cParam -> {
          CommittableOffset c = (CommittableOffset) cParam;
          return c.commitJavadsl();
      });

// Graph glue
      RunnableGraph<CompletionStage<Done>> runnableGraph = RunnableGraph.fromGraph(GraphDSL.create(Sink.ignore(), (builder, out) -> {
          final UniformFanOutShape<List, List> bcast = builder.add(Broadcast.create(2));
          final UniformFanInShape<List, List> merge = builder.add(Merge.create(2));

          Outlet<List<InternalMonitoringMessage>> source = builder.add(kafkaSource).out();
          builder.from(source).viaFanOut(bcast).via(builder.add(eventWriterFlow)).viaFanIn(merge).via(builder.add(commitFlow)).to(out);
          builder.from(bcast).via(builder.add(ocpWriterFlow)).toFanIn(merge);

          return ClosedShape.getInstance();
      }));
      runnableGraph.run(materializer);

    terminateWhenDone(done);
  }
  */
}

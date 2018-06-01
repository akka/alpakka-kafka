package sample.javadsl;

import akka.NotUsed;
import akka.kafka.ConsumerMessage;
import akka.kafka.ProducerMessage;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.stream.javadsl.RestartSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

class TransactionsSink extends ConsumerExample {
    public static void main(String[] args) {
        new TransactionsFailureRetryExample().demo();
    }

    public void demo() {
        // #transactionalSink
        Consumer.Control control =
            Consumer
                .transactionalSource(consumerSettings, Subscriptions.topics("source-topic"))
                .via(business())
                .map(msg ->
                        new ProducerMessage.Message<String, byte[], ConsumerMessage.PartitionOffset>(
                                new ProducerRecord<>("sink-topic", msg.record().value()), msg.partitionOffset()))
                .to(Producer.transactionalSink(producerSettings, "transactional-id"))
                .run(materializer);

        // ...

        control.shutdown();
        // #transactionalSink
    }
}

class TransactionsFailureRetryExample extends ConsumerExample {
    public static void main(String[] args) {
        new TransactionsFailureRetryExample().demo();
    }

    public void demo() {
        // #transactionalFailureRetry
        AtomicReference<Consumer.Control> innerControl = null;

        Source<ProducerMessage.Results<String, byte[], ConsumerMessage.PartitionOffset>,NotUsed> stream =
            RestartSource.onFailuresWithBackoff(
                java.time.Duration.of(3, ChronoUnit.SECONDS), // min backoff
                java.time.Duration.of(30, ChronoUnit.SECONDS), // max backoff
                0.2, // adds 20% "noise" to vary the intervals slightly
                () -> Consumer.transactionalSource(consumerSettings, Subscriptions.topics("source-topic"))
                    .via(business())
                    .map(msg ->
                        new ProducerMessage.Message<String, byte[], ConsumerMessage.PartitionOffset>(
                            new ProducerRecord<>("sink-topic", msg.record().value()), msg.partitionOffset()))
                    // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
                    .mapMaterializedValue(control -> {
                        innerControl.set(control);
                        return control;
                    })
                    .via(Producer.transactionalFlow(producerSettings, "transactional-id")));

        stream.runWith(Sink.ignore(), materializer);

        // Add shutdown hook to respond to SIGTERM and gracefully shutdown stream
        Runtime.getRuntime().addShutdownHook(new Thread(() -> innerControl.get().shutdown()));
        // #transactionalFailureRetry
    }
}
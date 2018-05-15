package sample.javadsl;

// #oneToMany
            import akka.Done;
            import akka.japi.function.Function;
            import akka.japi.function.Function2;
            import akka.kafka.ConsumerMessage;
            import akka.kafka.ConsumerMessage.CommittableOffset;
            import akka.kafka.ConsumerMessage.CommittableOffsetBatch;
            import akka.kafka.ProducerMessage;
            import akka.kafka.Subscriptions;
            import akka.kafka.javadsl.Consumer;
            import akka.kafka.javadsl.Producer;
            import akka.stream.javadsl.Sink;
            import org.apache.kafka.clients.producer.ProducerRecord;

            import java.util.Arrays;
            import java.util.Optional;
            import java.util.concurrent.CompletionStage;

// #oneToMany


public class AtLeastOnceOneToMany extends ConsumerExample {

    public static void main(String[] args) {
        new AtLeastOnceOneToMany().demo();
    }

    void demo() {
        CompletionStage<Done> done =
            // #oneToMany
            Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
                .mapConcat(msg ->
                     Arrays.asList(
                        new ProducerMessage.Message<byte[], String, Optional<CommittableOffset>>(
                            new ProducerRecord<>("topic2", msg.record().value()),
                            Optional.empty()
                        ),
                        new ProducerMessage.Message<byte[], String, Optional<CommittableOffset>>(
                            new ProducerRecord<>("topic2", msg.record().value()),
                            Optional.ofNullable(msg.committableOffset())
                        )
                    )
                )
                .via(Producer.flow(producerSettings))
                .map(m -> m.message().passThrough())
                .filterNot(Optional::isPresent)
                .map(Optional::get)
                .batch(
                    20,
                    (Function<CommittableOffset, CommittableOffsetBatch>) m ->
                        ConsumerMessage.emptyCommittableOffsetBatch().updated(m),
                    (Function2<CommittableOffsetBatch, CommittableOffset, CommittableOffsetBatch>) (batch, offset) ->
                        batch.updated(offset)
                )
                .mapAsync(3, m -> m.commitJavadsl())
                .runWith(Sink.<Done>ignore(), materializer);
        // #oneToMany

        done.thenAccept(m -> system.terminate());
    }
}

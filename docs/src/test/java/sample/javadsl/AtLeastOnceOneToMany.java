package sample.javadsl;

// #oneToMany
            import akka.Done;
            import akka.japi.JavaPartialFunction;
            import akka.japi.function.Function;
            import akka.japi.function.Function2;
            import akka.japi.Option;
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
                        new ProducerMessage.Message<byte[], String, Option<CommittableOffset>>(
                            new ProducerRecord<>("topic2", msg.record().value()),
                            Option.none()
                        ),
                        new ProducerMessage.Message<byte[], String, Option<CommittableOffset>>(
                            new ProducerRecord<>("topic2", msg.record().value()),
                            Option.some(msg.committableOffset())
                        )
                    )
                )
                .via(Producer.flow(producerSettings))
                .map(m -> m.message().passThrough())
                .collect(new JavaPartialFunction<Option<CommittableOffset>, CommittableOffset>() {
                    @Override
                    public CommittableOffset apply(Option<CommittableOffset> offset, boolean isCheck) {
                        if (offset.isDefined()) {
                            return offset.get();
                        } else {
                            throw noMatch();
                        }
                    }
                })
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

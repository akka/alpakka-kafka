package sample.javadsl;

// #metadata
        import akka.actor.ActorRef;
        import akka.kafka.KafkaConsumerActor;
        import akka.kafka.Metadata;
        import akka.pattern.PatternsCS;
        import akka.util.Timeout;
        import org.apache.kafka.common.TopicPartition;

        import java.util.concurrent.CompletionStage;
        import java.util.concurrent.TimeUnit;

// #metadata


public class FetchMetadata extends ConsumerExample {

    public static void main(String[] args) {
        new FetchMetadata().demo();
    }

    void demo() {
        // #metadata
        // Create kafka consumer actor to be used with Consumer.plainExternalSource or committableExternalSource
        ActorRef consumer = system.actorOf((KafkaConsumerActor.props(consumerSettings)));

        // ... create source ...

        CompletionStage<Metadata.Topics> topicsStage = PatternsCS.ask(consumer, new Metadata.GetCommittedOffset(new TopicPartition("f", 0)), new Timeout(2, TimeUnit.SECONDS))
                .thenApply(reply -> ((Metadata.Topics) reply));

        // print response
        topicsStage
                .thenApply(Metadata.Topics::getResponse)
                .thenAccept(responseOption -> responseOption.ifPresent(map -> map.forEach((topic, partitionInfo) ->
                        partitionInfo.forEach(info ->
                                System.out.println(topic + ": " + info.toString())
                        ))));

        // #metadata
    }
}

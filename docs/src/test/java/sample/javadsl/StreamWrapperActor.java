package sample.javadsl;

import akka.Done;
import akka.actor.*;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import static akka.pattern.PatternsCS.ask;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.util.Timeout;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import akka.pattern.Backoff;
import akka.pattern.BackoffSupervisor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.concurrent.duration.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class StreamWrapperActor extends UntypedActor {

    final ActorSystem system = getContext().system();
        
    protected final Materializer materializer = ActorMaterializer.create(getContext());

    protected final ConsumerSettings<byte[], String> consumerSettings =
            ConsumerSettings.create(system, new ByteArrayDeserializer(), new StringDeserializer())
                    .withBootstrapServers("localhost:9092")
                    .withGroupId("group1")
                    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    public void onReceive(Object messageObj) {
        if (messageObj instanceof ConsumerRecord) {
            ConsumerRecord<byte[], String> record = (ConsumerRecord<byte[], String>) messageObj;
            // ... process record
            ConsumerRecord<byte[], String> reply = record;
            // reply to the ask
            getSender().tell(reply, getSelf());
        } else {
            unhandled(messageObj);
        }
    }

    CompletionStage<String> process(ConsumerRecord<byte[], String> msg) {
        return CompletableFuture.completedFuture("processed");
    }

    public void createStream() {

        ActorRef processingActor = self();
        Timeout timeout = new Timeout(3, TimeUnit.SECONDS);

        //#errorHandlingStop
        CompletionStage<Done> done = Consumer.plainSource(
                consumerSettings,
                Subscriptions.topics("topic1"))
                .mapAsync(1, msg -> ask(processingActor, msg, timeout)) // akka.pattern.PatternsCS.ask
                .map(elem -> (ConsumerRecord<byte[], String>) elem)
                .runWith(Sink.ignore(), materializer);

        done.exceptionally(e -> {
            system.log().error(e, e.getMessage());
            getSelf().tell(PoisonPill.getInstance(), getSelf());
            return Done.getInstance();
        });

        return;
        //#errorHandlingStop
    }

    public static final void createSupervisor(ActorSystem system) {
        //#errorHandlingSupervisor
        Props childProps = Props.create(StreamWrapperActor.class);

        final Props supervisorProps = BackoffSupervisor.props(
                Backoff.onStop(
                        childProps,
                        "streamActor",
                        Duration.create(3, TimeUnit.SECONDS),
                        Duration.create(30, TimeUnit.SECONDS),
                        0.2));

        system.actorOf(supervisorProps, "streamActorSupervisor");
        //#errorHandlingSupervisor
    }
}

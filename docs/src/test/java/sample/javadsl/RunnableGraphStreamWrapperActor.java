package sample.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.function.Function;
import akka.kafka.ConsumerMessage;
import akka.kafka.ConsumerSettings;
import akka.kafka.ProducerMessage;
import akka.kafka.ProducerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.kafka.javadsl.Producer;
import akka.pattern.Backoff;
import akka.pattern.BackoffSupervisor;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.ClosedShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.Outlet;
import akka.stream.Supervision;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Partition;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * This is reference example to re start the stream on Kafka consumer fails. This is specific stream example using {@link RunnableGraph}.
 * The trick is in the below example is using two parameter GraphDSL.create by passing source, as it is materialized value and on it we can implement 'whenComplete', this is where we kill our stream wrapper actor and start the pipeline again!!
 * Though it is closed graph, as we materialized source so we can get back Consumer.Control on running the graph.
 * Consumer.Control is from reactive kafka library and on which we can implement callback when the underlying KafkaConsumer has been closed. That is what we do, we listen for isShutdown event and we kill our actor to restart the stream.
 */
public class RunnableGraphStreamWrapperActor extends UntypedActor {

    private final ActorSystem system;
    private final Materializer materializer = ActorMaterializer.create(getContext());
    private final ConsumerSettings<String, String> consumerSettings;
    private final ProducerSettings<String, String> producerSettings;

    private static class EventMessage {
        final String message;
        // retain this through the flow so that we can use it to commit the offset
        final ConsumerMessage.CommittableMessage<String, String> committableMessage;

        public EventMessage(ConsumerMessage.CommittableMessage<String, String> committableMessage, String message) {
            this.committableMessage = committableMessage;
            this.message = message;
        }
    }

    public RunnableGraphStreamWrapperActor() {
        system = getContext().system();

        consumerSettings = ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                .withBootstrapServers("localhost:9092")
                .withGroupId("group1")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        producerSettings = ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
                .withBootstrapServers("localhost:9092")
                .withProperty("compression.type", "lz4")
                .withProperty("acks", "all");
    }

    public void onReceive(Object messageObj) {
        // Trigger the pipeline on receive a message
        pipeline();
    }

    /**
     * Reactive Kafka stream.
     */
    private void pipeline() {
        ActorMaterializer materializer = ActorMaterializer.create(ActorMaterializerSettings
                .create(system)
                .withDebugLogging(true)
                .withSupervisionStrategy(stopOnSpecificExceptionDecider(getSelf())), system);

        // Source: Receive messages and transform to a EventMessage
        Source<EventMessage, Consumer.Control> sourceStream = Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
                .map(committableMessage -> new EventMessage(committableMessage, committableMessage.record().value()));

        // Flow to process the message
        Flow<EventMessage, EventMessage, NotUsed> firstPartitionFlow = Flow.of(EventMessage.class).map(this::processMessage); // Save and Transform
        Flow<EventMessage, EventMessage, NotUsed> secondPartitionFlow = Flow.of(EventMessage.class).map(this::processMessage); // Delete and Transform

        // Flow to create Kafka ProducerMessage
        Flow<EventMessage, ProducerMessage.Message<String, String, ConsumerMessage.Committable>, NotUsed> produceMessageFlow = Flow.of(EventMessage.class)
                .map(eventTuple -> produceCommittableEvent(eventTuple, "producerTopic"));

        // Sink: Committable producer sink
        Sink<ProducerMessage.Message<String, String, ConsumerMessage.Committable>, CompletionStage<Done>> producerCommitSink = Producer.commitableSink(producerSettings);
        // Simple commit the offset!
        Sink<EventMessage, CompletionStage<Done>> commitOnlySink = Sink.foreach(eventTuple -> eventTuple.committableMessage.committableOffset().commitJavadsl());

        Graph<UniformFanOutShape<EventMessage, EventMessage>, NotUsed> eventTypePartition = Partition.create(3, (Function<EventMessage, Object>) tuple -> {
            switch (tuple.message) {
                case "EVENT_TYPE_1":
                    return 1;
                case "EVENT_TYPE_2":
                    return 2;
                default:
                    return 0;
            }
        });

        // Build the Partition Graph
        Graph<ClosedShape, Consumer.Control> completionStageGraph = GraphDSL.create(sourceStream, (builder, sourceShape) -> {

            UniformFanOutShape<EventMessage, EventMessage> eventTypeFanOut = builder.add(eventTypePartition);
            Outlet<EventMessage> unknownEventOutlet = eventTypeFanOut.out(0);
            Outlet<EventMessage> firstPartitionOutlet = eventTypeFanOut.out(1);
            Outlet<EventMessage> secondPartitionOutlet = eventTypeFanOut.out(2);

            builder.from(sourceShape)
                    .toFanOut(eventTypeFanOut)
                        .from(unknownEventOutlet)
                            .to(builder.add(commitOnlySink))
                        .from(firstPartitionOutlet)
                            .via(builder.add(firstPartitionFlow))
                            .via(builder.add(produceMessageFlow))
                            .to(builder.add((producerCommitSink)))
                        .from(secondPartitionOutlet)
                            .via(builder.add(secondPartitionFlow))
                            .to(builder.add(commitOnlySink));

            return ClosedShape.getInstance();
        });

        //#errorHandlingClosedRunnableGraph
        // Self kill this actor so that BackoffSupervisor starts this actor(pipeline/stream) again!
        RunnableGraph.fromGraph(completionStageGraph)
                .run(materializer)
                .isShutdown()
                .whenComplete((done, throwable) -> {
                    getSelf().tell(PoisonPill.getInstance(), getSelf());
                });
        //#errorHandlingClosedRunnableGraph

    }

    private EventMessage processMessage(EventMessage tuple2) {
        // main business logic to process message
        return tuple2;
    }

    private ProducerMessage.Message<String, String, ConsumerMessage.Committable> produceCommittableEvent(EventMessage eventTuple, String topic) {
        return new ProducerMessage.Message<>(new ProducerRecord<String, String>(topic, "Produce New Message"), eventTuple.committableMessage.committableOffset());
    }

    /**
     * Custom decider to decide the Supervision strategy.
     */
    private Function<Throwable, Supervision.Directive> stopOnSpecificExceptionDecider(ActorRef actorRef) {
        return exception -> {

            if (exception instanceof IllegalStateException) { // Any specific exception
                return Supervision.stop(); // BackoffSupervisor will start again!
            }
            return Supervision.resume();
        };
    }

    // Call it from application bootstrap when you want to kick off the pipeline
    public static final void createSupervisor(ActorSystem system) {
        Props childProps = Props.create(RunnableGraphStreamWrapperActor.class);

        final Props supervisorProps = BackoffSupervisor.props(
                Backoff.onStop(
                        childProps,
                        "streamActor",
                        Duration.create(3, TimeUnit.SECONDS),
                        Duration.create(30, TimeUnit.SECONDS),
                        0.2));

        system.actorOf(supervisorProps, "streamActorSupervisor");
    }
}

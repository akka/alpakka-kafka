package examples;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.softwaremill.react.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class JavaKafkaExamples {

    public void run() {
        String brokerList = "localhost:9092";

        ReactiveKafka kafka = new ReactiveKafka();
        ActorSystem system = ActorSystem.create("ReactiveKafka");
        ActorMaterializer materializer = ActorMaterializer.create(system);

        StringDeserializer deserializer = new StringDeserializer();
        ConsumerProperties<String, String> cp =
                new PropertiesBuilder.Consumer(brokerList, "topic", "groupId", deserializer, deserializer)
                        .build();

        Publisher<ConsumerRecord<String, String>> publisher = kafka.consume(cp, system);

        StringSerializer serializer = new StringSerializer();
        ProducerProperties<String, String> pp = new PropertiesBuilder.Producer(
                brokerList,
                "topic",
                serializer,
                serializer).build();

        Subscriber<ProducerMessage<String, String>> subscriber = kafka.publish(pp, system);
        Source.fromPublisher(publisher).map(this::toProdMessage)
                .to(Sink.fromSubscriber(subscriber)).run(materializer);
    }

    private ProducerMessage<String, String> toProdMessage(ConsumerRecord<String, String> record) {
        return KeyValueProducerMessage.apply(record.key(), record.value());
    }
}

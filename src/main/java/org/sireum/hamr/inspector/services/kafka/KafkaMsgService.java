package org.sireum.hamr.inspector.services.kafka;

import art.Bridge;
import art.DataContent;
import art.UPort;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.sireum.hamr.inspector.common.ArtUtils;
import org.sireum.hamr.inspector.common.InspectionBlueprint;
import org.sireum.hamr.inspector.common.Msg;
import org.sireum.hamr.inspector.services.MsgService;
import org.sireum.hamr.inspector.services.Session;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.function.Tuple3;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

@Slf4j
@Service
public class KafkaMsgService implements MsgService {

//    final KafkaReceiver<String, String> receiver;
//
//    private final ReceiverOptions<String, String> receiverOptions;

    private final InspectionBlueprint inspectionBlueprint;
    private final ArtUtils artUtils;

//    private final ConsumerFactory consumerFactory; // yes (NOT WITH THIS IMPORT)
//    private final ConsumerFactory<String, String> consumerFactory; // yes (NOT WITH THIS IMPORT)

//    final KafkaTemplate<String, String> kafkaTemplate; // yes

//    final KafkaClient kafkaClient;

    public KafkaMsgService(InspectionBlueprint inspectionBlueprint, ArtUtils artUtils) {
        this.inspectionBlueprint = inspectionBlueprint;
        this.artUtils = artUtils;
    }

    @Override
    public Flux<Msg> replayThenLive(Session session) {
//        ReceiverOptions.create
//        KafkaReceiver.create()

        final String topic = "topic-1";
//        final Consumer<String, String> consumer = consumerFactory.createConsumer(topic, "inspector-client-1");

        final var p1 = new TopicPartition(topic, 0);
        final var p2 = new TopicPartition(topic, 1);
        final var p3 = new TopicPartition(topic, 2);
//        consumer.assign(List.of(p1, p2, p3));

        final var receiverOptions = ReceiverOptions.<String, String>create(createProperties())
                .assignment(List.of(p1, p2, p3))
                .subscription(List.of(topic));

        Flux<GroupedFlux<String, ReceiverRecord<String, String>>> x = KafkaReceiver.create(receiverOptions)
//                .doOnConsumer(c -> {
//                    c.seekToEnd(List.of(p1, p2, p3));
//                    return c;
//                })
                .receive()
                .filter(r -> r.timestampType() == TimestampType.CREATE_TIME)
                .groupBy(ConsumerRecord::key);
//                .flatMap(gf -> gf.map(r -> r.))
//                .map(Flux::from);

//                .transform(gf -> Flux.zip())

        return Flux.<Tuple3<ReceiverRecord<String, String>, ReceiverRecord<String, String>, ReceiverRecord<String, String>>, Msg>zip(x, tuple3 -> {

//            String src = null;
//            String dst = null;
//            String data = null;

//            tuple3.forEach((ReceiverRecord<String, String>) value -> {
//                if (value )
//            });

            final int src =
                    Integer.parseInt(tuple3.getT1().key().equals("src") ? tuple3.getT1().key()
                            : (tuple3.getT2().key().equals("src") ? tuple3.getT2().key() : tuple3.getT3().key()));

            final int dst =
                    Integer.parseInt(tuple3.getT1().key().equals("dst") ? tuple3.getT1().key()
                            : (tuple3.getT2().key().equals("dst") ? tuple3.getT2().key() : tuple3.getT3().key()));

            final DataContent data =
                    inspectionBlueprint.deserializer().apply(tuple3.getT1().key().equals("data") ? tuple3.getT1().key()
                            : (tuple3.getT2().key().equals("data") ? tuple3.getT2().key() : tuple3.getT3().key()));

            final long time = tuple3.getT1().timestamp();
            if (tuple3.getT2().timestamp() != time || tuple3.getT3().timestamp() != time) {
                throw new IllegalStateException("src, dst, and data must be marked with the same timestamps");
            }

            final long uid = tuple3.getT1().receiverOffset().offset();
            if (tuple3.getT2().receiverOffset().offset() != uid || tuple3.getT3().receiverOffset().offset() != uid) {
                throw new IllegalStateException("src, dst, and data must be marked with the same timestamps");
            }

            final UPort msgSrc = artUtils.getPort(src);
            final UPort msgDst = artUtils.getPort(dst);
            final Bridge msgSrcBridge = artUtils.getBridge(msgSrc);
            final Bridge msgDstBridge = artUtils.getBridge(msgDst);

            return new Msg(msgSrc, msgDst, msgSrcBridge, msgDstBridge, data, time, uid);
        });

//                .transform((GroupedFlux<String, ReceiverRecord<String, String>> it) ->
//                        Flux.zip((Flux<ReceiverRecord<String, String>>)it, ))
//                .
    }

    @Override
    public Flux<Msg> replay(Session session) {
        log.error("Using method with incorrect (but type-safe) dummy implementation! replay");
        return replayThenLive(session); // todo implement correctly
    }

    @Override
    public Flux<Msg> live(Session session) {
        log.error("Using method with incorrect (but type-safe) dummy implementation! live");
        return replayThenLive(session); // todo implement correctly
    }

    @Override
    public Mono<Long> count(Session session) {
        log.error("Using method with incorrect (but type-safe) dummy implementation! count");
        return replayThenLive(session).take(Duration.ofMillis(500)).count(); // todo implement correctly
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(Collection<TopicPartition> partitions) {
        final var consumer = new KafkaConsumer<String, String>(createProperties());
        consumer.assign(partitions);
        return consumer;
    }

    private static Properties createProperties() {
        final Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "inspector-consumer-group-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "10000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }
}

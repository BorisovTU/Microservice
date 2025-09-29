package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.stream;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.IncomingMessageTopology;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.globalTables.GlobalTableTopologyBuilder;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.globalTables.StreamStorable;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@Slf4j
@Component
@RequiredArgsConstructor
public class StreamsTopology implements ApplicationRunner {

    private final List<StreamStorable> storableStreams;
    private final KafkaStreamsConfiguration kafkaStreamsConfiguration;
    private final IncomingMessageTopology incomingMessageTopology;
    private final List<GlobalTableTopologyBuilder> globalTableTopologyBuilder;
    private KafkaStreams streams;

    /*
        Запуска Kafka Streams
        Ждём, пока не перейдёт в состояние running, чтобы потом можно было безопасно вызывать KafkaStreams.store()
     */
    private void startStreams(KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);

        // State listener to listen for a transition to RUNNING state
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                latch.countDown();
            }
        });

        streams.cleanUp();
        streams.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.warn("Error on latch waiting", e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void run(ApplicationArguments args) {
        StreamsBuilder builder = new StreamsBuilder();

        globalTableTopologyBuilder.forEach(topologyBuilder -> topologyBuilder.build(builder));

        incomingMessageTopology.buildTopology(builder);

        Topology topology = builder.build();
        streams = new KafkaStreams(topology, kafkaStreamsConfiguration.asProperties());

        log.info(topology.describe().toString());

        startStreams(streams);

        storableStreams.forEach(storableStream -> storableStream.saveStore(streams));
    }

    @PreDestroy
    public void cleanUp() {
        if (streams != null) {
            streams.close();
            streams.cleanUp();
        }
    }
}

package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.stream;

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
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.IncomingMessageTopology;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables.GlobalTableTopologyBuilder;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables.StreamStorable;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class StreamsTopology implements ApplicationRunner {

    private final KafkaStreamsConfiguration kafkaStreamsConfiguration;
    private final StreamsBuilder streamsBuilder;
    private final IncomingMessageTopology incomingMessageTopology;
    private final List<GlobalTableTopologyBuilder> globalTableTopologyBuilders;
    private final List<StreamStorable> storableStreams;

    private KafkaStreams streams;

    @Override
    public void run(ApplicationArguments args) {

        globalTableTopologyBuilders.forEach(builder -> builder.build(streamsBuilder));

        incomingMessageTopology.buildOrdersStream(streamsBuilder);
        incomingMessageTopology.buildTradesStream(streamsBuilder);

        Topology topology = streamsBuilder.build();
        log.info("Kafka Streams topology: {}", topology.describe());

        streams = new KafkaStreams(topology, kafkaStreamsConfiguration.asProperties());

        streams.cleanUp();

        streams.start();

        storableStreams.forEach(store -> store.saveStore(streams));
    }

    @PreDestroy
    public void shutdown() {
        if (streams != null) {
            try {
                log.info("Closing Kafka Streams...");
                streams.close(Duration.ofSeconds(30));
            } catch (Exception e) {
                log.error("Error closing Kafka Streams", e);
            } finally {
                streams.cleanUp();
            }
        }
    }
}

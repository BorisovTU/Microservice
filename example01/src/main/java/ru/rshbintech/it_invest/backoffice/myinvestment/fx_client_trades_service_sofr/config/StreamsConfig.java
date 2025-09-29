package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class StreamsConfig implements ApplicationRunner {

    private final StreamsBuilder streamsBuilder;
    private final KafkaStreamsConfiguration kafkaStreamsConfiguration;

    private KafkaStreams streams;

    @Override
    public void run(ApplicationArguments args) {
        Topology topology = streamsBuilder.build();

        log.info("KafkaStreams topology description:\n{}", topology.describe());

        streams = new KafkaStreams(topology, kafkaStreamsConfiguration.asProperties());

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping KafkaStreams");
            streams.close();
        }));
    }
}

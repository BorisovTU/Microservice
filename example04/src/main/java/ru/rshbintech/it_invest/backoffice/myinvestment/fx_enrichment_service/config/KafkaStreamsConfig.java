package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.exceptionHandlers.DlqDeserializationExceptionHandler;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.exceptionHandlers.DlqProcessingExceptionHandler;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables.GlobalTableTopologyBuilder;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables.StreamStorable;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.producers.MessageProducer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaStreamsConfig {

    private final KafkaConfig kafkaConfig;
    private final MessageProducer messageProducer;

    @Bean
    @Primary
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaConfig.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.getAutoOffsetReset());

        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, DlqProcessingExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, DlqDeserializationExceptionHandler.class);

        props.put("dlq-message-producer", messageProducer);
        props.put("kafka-config", kafkaConfig);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean
    public KafkaStreams kafkaStreams(StreamsBuilder streamsBuilder,
                                     KafkaStreamsConfiguration kafkaStreamsConfiguration,
                                     List<GlobalTableTopologyBuilder> globalTableTopologyBuilders,
                                     List<StreamStorable> storableStreams) {

        globalTableTopologyBuilders.forEach(builder -> builder.build(streamsBuilder));

        Topology topology = streamsBuilder.build();

        KafkaStreams streams = new KafkaStreams(topology, kafkaStreamsConfiguration.asProperties());
        streams.start();

        storableStreams.forEach(s -> s.saveStore(streams));

        log.info("Kafka Streams application started with topology: {}", topology.describe());

        return streams;
    }
}

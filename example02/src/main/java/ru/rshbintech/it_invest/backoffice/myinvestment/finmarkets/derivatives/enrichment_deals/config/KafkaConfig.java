package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
@Slf4j
@Data
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaConfig {
    private Topic topic = new Topic();
    private Dlq dlq = new Dlq();

    private String bootstrapServers;
    private String autoOffsetReset;

    @Value("${spring.kafka.streams.application-id}")
    private String applicationID;

    @Data
    public static class Topic {
        private String rawData;
        private String requests;
        private String clientRequests;
        private String deals;
        private String clientDeals;
        private String contractsMoexLnk;
        private String contracts;
        private String instrumentsMoexLnk;
        private String instruments;
        private String calendar;
        private String marketSchemeMoexLnk;
        private String commissionTypes;
        private String commissionPlans;
        private String bankMpCodes;
        private String dlq;
    }

    @Data
    public static class Dlq {
        private long interval;
        private long attempts;
    }
}

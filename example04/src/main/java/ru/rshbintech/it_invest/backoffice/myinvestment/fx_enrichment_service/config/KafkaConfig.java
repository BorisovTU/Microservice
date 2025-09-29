package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
@Slf4j
@Getter
@Setter
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaConfig {

    private Topic topic = new Topic();
    private Dlq dlq = new Dlq();

    private String bootstrapServers;
    private String autoOffsetReset;
    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;
    @Getter
    @Setter
    public static class Topic {
        private String rawOrders;
        private String rawTrades;
        private String ordersClient;
        private String tradesClient;
        private String ordersClientEnriched;
        private String tradesClientEnriched;
        private String subcontractActive;
        private String subcontractActiveMoexLnk;
        private String marketSchemesMoexLnk;
        private String commissionsTypes;
        private String commissionPlans;;
        private String instruments;
        private String instrumentsMoexLnk;
        private String dlq;
    }
    @Getter
    @Setter
    public static class Dlq {
        private long interval;
        private long attempts;

    }
}

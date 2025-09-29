package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.config;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@Configuration
@EnableKafka
@ConfigurationProperties(prefix = "app.kafka")
@Getter
@Setter
@Slf4j
public class KafkaConfig {

    private String bootstrapServers;
    private String groupId;
    private Topic topic = new Topic();
    private Dlq dlq = new Dlq();

    @Getter
    @Setter
    public static class Topic {
        private String ordersClientEnriched;
        private String tradesClientEnriched;
        private String dlq;
        private String OrdersClients;
        private String TradesClients;
    }

    @Getter
    @Setter
    public static class Dlq {
        private long interval;
        private long attempts;
    }
}

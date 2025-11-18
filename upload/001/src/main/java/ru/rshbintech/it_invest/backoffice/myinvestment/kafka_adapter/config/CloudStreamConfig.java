package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class CloudStreamConfig {

    @Bean
    public ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> customizer() {
        return (container, destination, group) -> {
            DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                    (record, exception) -> {
                        log.error("Failed to process message: {}", exception.getMessage(), exception);
                    },
                    new FixedBackOff(1000L, 3)
            );
            container.setCommonErrorHandler(errorHandler);
        };
    }
}

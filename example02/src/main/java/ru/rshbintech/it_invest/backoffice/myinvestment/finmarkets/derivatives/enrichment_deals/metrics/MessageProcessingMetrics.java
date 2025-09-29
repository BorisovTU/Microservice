package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class MessageProcessingMetrics {

    public final String REQUEST = "request";
    public final String DEAL = "deal";

    private final MeterRegistry registry;
    private final String receivedMessageName = "kafka.messages.received";
    private final String sendMessageName = "kafka.messages.sent";

    private final Timer processingTimer;

    public MessageProcessingMetrics(MeterRegistry registry) {
        processingTimer = Timer.builder("kafka.message.processing.time")
                .description("Time taken to process a message")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry);

        this.registry = registry;
    }

    public void incrementReceivedMessages() {
        registry.counter(receivedMessageName).increment();
    }

    public void incrementSendMessages(String requestType) {
        registry.counter(sendMessageName,
                "requestType", requestType)
                .increment();
    }

    public void saveProcessingTime(long duration) {
        processingTimer.record(duration, TimeUnit.NANOSECONDS);
    }
}
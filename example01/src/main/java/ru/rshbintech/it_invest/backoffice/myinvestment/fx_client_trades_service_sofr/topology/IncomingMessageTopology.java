package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.OrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.TradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service.OrderService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service.TradeService;

@Component
@RequiredArgsConstructor
@Slf4j
public class IncomingMessageTopology {

    private final KafkaConfig kafkaConfig;
    private final ObjectMapper objectMapper;
    private final OrderService orderService;
    private final TradeService tradeService;

    @Bean
    public KStream<String, String> ordersStream(StreamsBuilder builder) {
        String topicName = kafkaConfig.getTopic().getOrdersClient();
        log.info("Subscribing to orders topic: {}", topicName);

        KStream<String, String> stream = builder.stream(
                topicName,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        stream.foreach((key, value) -> {
            try {
                OrderDto dto = objectMapper.readValue(value, OrderDto.class);
                orderService.saveOrder(dto, topicName, key, value);
            } catch (Exception e) {
                log.error("Ошибка обработки заявки: key={}, value={}", key, value, e);
            }
        });

        return stream;
    }

    @Bean
    public KStream<String, String> tradesStream(StreamsBuilder builder) {
        String topicName = kafkaConfig.getTopic().getTradesClient();
        log.info("Subscribing to trades topic: {}", topicName);

        KStream<String, String> stream = builder.stream(
                topicName,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        stream.foreach((key, value) -> {
            try {
                TradeDto dto = objectMapper.readValue(value, TradeDto.class);
                tradeService.saveTradeWithCommissions(dto, topicName, key, value);
            } catch (Exception e) {
                log.error("Ошибка обработки трейда: key={}, value={}", key, value, e);
            }
        });

        return stream;
    }
}

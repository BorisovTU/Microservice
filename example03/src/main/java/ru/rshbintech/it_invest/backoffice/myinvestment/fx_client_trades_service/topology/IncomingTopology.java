package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.OrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.TradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.OrderEventEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEventEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.service.OrderService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.service.TradeService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.util.EventFactory;

@Component
@Slf4j
@RequiredArgsConstructor
public class IncomingTopology {

    private final KafkaConfig kafkaConfig;
    private final ObjectMapper objectMapper;
    private final OrderService orderService;
    private final TradeService tradeService;
    private final EventFactory eventFactory;

    @Bean
    public KStream<String, String> ordersStream(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream(
                kafkaConfig.getTopic().getOrdersClientEnriched(),
                Consumed.with(Serdes.String(), Serdes.String())
        );

        stream.foreach((key, value) -> {
            log.info("Получено сообщение из топика OrdersEnriched: key={}, value={}", key, value);

            try {
                OrderDto dto = objectMapper.readValue(value, OrderDto.class);
                log.debug("Десериализован OrderDto: {}", dto);

                OrderEventEntity event = eventFactory.createOrderEvent(
                        kafkaConfig.getTopic().getOrdersClientEnriched(), value
                );
                log.debug("Создан OrderEventEntity: {}", event);

                orderService.processOrder(dto, event);
                log.info("Order успешно обработан: externalCode={}, clientId={}", dto.externalCode(), dto.clientId());

            } catch (Exception e) {
                log.error("Ошибка обработки заявки key={}, value={}", key, value, e);
            }
        });

        return stream;
    }

    @Bean
    public KStream<String, String> tradesStream(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream(
                kafkaConfig.getTopic().getTradesClientEnriched(),
                Consumed.with(Serdes.String(), Serdes.String())
        );

        stream.foreach((key, value) -> {
            log.info("Получено сообщение из топика TradesEnriched: key={}, value={}", key, value);

            try {
                TradeDto dto = objectMapper.readValue(value, TradeDto.class);
                log.debug("Десериализован TradeDto: {}", dto);

                TradeEventEntity event = eventFactory.createTradeEvent(
                        kafkaConfig.getTopic().getTradesClientEnriched(), value
                );
                log.debug("Создан TradeEventEntity: {}", event);

                tradeService.processTrade(dto, event);
                log.info("Trade успешно обработан: externalCode={}, clientId={}", dto.externalCode(), dto.clientId());

            } catch (Exception e) {
                log.error("Ошибка обработки сделки key={}, value={}", key, value, e);
            }
        });

        return stream;
    }
}

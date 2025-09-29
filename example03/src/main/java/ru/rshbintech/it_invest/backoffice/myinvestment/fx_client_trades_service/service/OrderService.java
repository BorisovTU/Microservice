package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.OrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.OrderEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.OrderEventEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.mapper.OrderMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.OrderEventRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.OrderRepository;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventRepository orderEventRepository;
    private final OrderMapper orderMapper;
    private final KafkaConfig  kafkaConfig;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper  objectMapper;

    @Transactional
    public void processOrder(OrderDto dto, OrderEventEntity event) {
        log.info("Начало обработки заявки: code={}, externalCode={}, clientId={}",
                dto.code(), dto.externalCode(), dto.clientId());

        try {
            OrderEntity order = orderRepository
                    .findByExternalCodeAndExchangeId(dto.externalCode(), dto.exchangeId())
                    .map(existing -> {
                        log.info("Найдена существующая заявка: id={}, lastUpdateTime={}",
                                existing.getId(), existing.getLastUpdateTime());

                        if (dto.lastUpdateTime() != null && (
                                existing.getLastUpdateTime() == null ||
                                        dto.lastUpdateTime().isAfter(existing.getLastUpdateTime()))) {
                            log.info("Обновление заявки id={} из-за нового lastUpdateTime={}",
                                    existing.getId(), dto.lastUpdateTime());
                            orderMapper.updateEntityFromDto(dto, existing);
                        } else {
                            log.info("Обновление заявки id={} не требуется", existing.getId());
                        }
                        return existing;
                    })
                    .orElseGet(() -> {
                        log.info("Создание новой заявки для externalCode={}", dto.externalCode());
                        return orderMapper.toEntity(dto);
                    });

            orderRepository.save(order);
            log.info("Сохранена заявка в БД: id={}", order.getId());

            event.setLinkedFk(order.getId());
            event.setStatus("processed");
            orderEventRepository.save(event);
            log.info("Сохранено событие OrderEvent: linkedFk={}, status={}",
                    event.getLinkedFk(), event.getStatus());

            String payload = objectMapper.writeValueAsString(dto);
            kafkaTemplate.send(kafkaConfig.getTopic().getOrdersClients(), dto.clientId().toString(), payload);
            log.info("Отправлено событие в Kafka: topic={}, key={}, payload={}",
                    kafkaConfig.getTopic().getOrdersClients(), dto.clientId(), payload);

        } catch (Exception e) {
            log.error("Ошибка обработки заявки: {}", dto.externalCode(), e);
            event.setStatus("failed");
            orderEventRepository.save(event);
            log.info("Сохранено событие OrderEvent со статусом failed: linkedFk={}, status={}",
                    event.getLinkedFk(), event.getStatus());

            try {
                String payload = objectMapper.writeValueAsString(dto);
                kafkaTemplate.send(kafkaConfig.getTopic().getDlq(), dto.clientId().toString(), payload);
                log.info("Сообщение отправлено в DLQ: externalCode={}", dto.externalCode());
            } catch (JsonProcessingException ex) {
                log.error("Ошибка сериализации при отправке в DLQ:", ex);
            }
        }
    }
}

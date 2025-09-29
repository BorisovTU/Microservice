package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.TradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEventEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.mapper.TradeMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.TradeCommissionsRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.TradeEventRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.TradeRepository;

@Service
@Slf4j
@RequiredArgsConstructor
public class TradeService {

    private final TradeRepository tradeRepository;
    private final TradeCommissionsRepository tradeCommissionRepository;
    private final TradeEventRepository tradeEventRepository;
    private final TradeMapper tradeMapper;
    private final KafkaConfig kafkaConfig;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Transactional
    public void processTrade(TradeDto dto, TradeEventEntity event) {
        log.info("Начало обработки сделки: code={}, externalCode={}, clientId={}",
                dto.code(), dto.externalCode(), dto.clientId());

        try {
            var existingTradeOpt = tradeRepository.findByExternalCodeAndDirection(dto.externalCode(), dto.direction());

            TradeEntity trade;
            if (existingTradeOpt.isPresent()) {
                trade = existingTradeOpt.get();

                tradeMapper.updateEntityFromDto(dto, trade);
                log.info("Обновлена сущность сделки: id={}", trade.getId());

                log.info("Удаляем старые комиссии для сделки id={}", trade.getId());
                tradeCommissionRepository.deleteByTrade(trade);

                var commissions = tradeMapper.toCommissions(dto, trade);
                tradeCommissionRepository.saveAll(commissions);
                log.info("Сохранены комиссии для сделки id={}, count={}", trade.getId(), commissions.size());

            } else {
                log.info("Создание новой сделки для externalCode={}", dto.externalCode());
                trade = tradeMapper.toEntity(dto);
                tradeRepository.save(trade);
                log.info("Сохранена новая сделка в БД: id={}", trade.getId());

                var commissions = tradeMapper.toCommissions(dto, trade);
                tradeCommissionRepository.saveAll(commissions);
                log.info("Сохранены комиссии для новой сделки id={}, count={}", trade.getId(), commissions.size());
            }

            event.setLinkedFk(trade.getId());
            event.setStatus("processed");
            tradeEventRepository.save(event);
            log.info("Сохранено событие TradeEvent: linkedFk={}, status={}", event.getLinkedFk(), event.getStatus());

            String payload = objectMapper.writeValueAsString(dto);
            kafkaTemplate.send(
                    kafkaConfig.getTopic().getTradesClients(),
                    dto.clientId().toString(),
                    payload
            );
            log.info("Отправлено событие в Kafka: topic={}, key={}, payload={}",
                    kafkaConfig.getTopic().getTradesClients(), dto.clientId(), payload);

        } catch (Exception e) {
            log.error("Ошибка обработки сделки: {}", dto.externalCode(), e);
            event.setStatus("failed");
            tradeEventRepository.save(event);
            log.info("Сохранено событие TradeEvent со статусом failed: linkedFk={}, status={}",
                    event.getLinkedFk(), event.getStatus());

            try {

                String payload = objectMapper.writeValueAsString(dto);
                kafkaTemplate.send(kafkaConfig.getTopic().getDlq(), dto.clientId().toString(), payload);
                log.info("Сообщение отправлено в DLQ: externalCode={}", dto.externalCode());
            } catch (JsonProcessingException ex) {
                log.error("Ошибка сериализации при отправке в DLQ:", ex);
            }
        }

        log.info("Обработка сделки завершена: externalCode={}", dto.externalCode());
    }
}

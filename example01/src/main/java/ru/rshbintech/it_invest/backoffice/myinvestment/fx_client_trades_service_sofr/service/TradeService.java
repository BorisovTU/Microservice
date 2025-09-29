package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao.TradeDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.TradeDto;

@Service
@RequiredArgsConstructor
@Slf4j
public class TradeService {

    private final TradeDao tradeDao;
    private final DlqService dlqService;

    @Transactional
    public void saveTradeWithCommissions(TradeDto tradeDto, String topic, String key, String value) {
        try {
            tradeDao.saveTrade(tradeDto);
            log.info("Трейд успешно сохранен: externalCode={}", tradeDto.externalCode());
        } catch (Exception e) {
            log.error("Ошибка при сохранении трейда: externalCode={}", tradeDto.externalCode(), e);

            dlqService.sendToDlq(topic, key, value, e);

            throw e;
        }
    }
}

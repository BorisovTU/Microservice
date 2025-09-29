package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao.OrderDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.OrderDto;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderDao orderDao;
    private final DlqService dlqService;

    @Transactional
    public void saveOrder(OrderDto orderDto, String topic, String key, String value) {
        try {
            orderDao.saveOrder(orderDto);
            log.info("Заявка успешно сохранена: externalCode={}", orderDto.externalCode());
        } catch (Exception e) {
            log.error("Ошибка при сохранении заявки: externalCode={}", orderDto.externalCode(), e);
            dlqService.sendToDlq(topic, key, value, e);
            throw e;
        }
    }
}

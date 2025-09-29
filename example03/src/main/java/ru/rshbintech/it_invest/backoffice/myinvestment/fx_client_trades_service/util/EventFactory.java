package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.util;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.OrderEventEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEventEntity;

import java.time.LocalDateTime;
@Component
public class EventFactory {

    public OrderEventEntity createOrderEvent(String topic, String payload) {
        OrderEventEntity event = new OrderEventEntity();
        event.setKafkaTopic(topic);
        event.setPayload(payload);
        event.setReceivedAt(LocalDateTime.now());
        return event;
    }

    public TradeEventEntity createTradeEvent(String topic, String payload) {
        TradeEventEntity event = new TradeEventEntity();
        event.setKafkaTopic(topic);
        event.setPayload(payload);
        event.setReceivedAt(LocalDateTime.now());
        return event;
    }
}

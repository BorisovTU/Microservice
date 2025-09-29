package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.DirectionNotDeterminedException;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RequestEnriched {
    private String code; //Внутренний код заявки
    private String externalCode; //Внешний код заявки
    private String createdDate; //Дата и время подачи заявки
    private String contractId; //ИД субдоговора обслуживания
    private String clientId;
    private String direction; // "BUY", "SELL"
    private String fiId;
    private Long amount; //Количество
    private BigDecimal price;
    private String priceType; // "Пункты", "Валюта"
    private String priceFiId;
    private String status;
    private String exchangeId;
    private Boolean isAddress;
    private Boolean isMarginCall;
    private String lastUpdateTime;

    /*
        Формируется конкатенацией трёх строк:
          - Дата подачи заявки в формате "DDMMYYYY"
          - Символ "0"
          - Биржевой код заявки, начиная с пятого символа
     */
    public void setCodeFromTransactTimeAndOrderId(LocalDateTime transactTime, String orderId) {
        if (transactTime != null && orderId != null && orderId.length() >= 5) {
            String datePart = transactTime.format(DateTimeFormatter.ofPattern("ddMMyyyy"));
            String orderIdPart = orderId.substring(4);
            this.code = datePart + "0" + orderIdPart;
        }
    }

    public void setDirectionFromSide(String side) {
        if (side == null) {
            throw new DirectionNotDeterminedException();
        }

        if (side.equals("1")) {
            this.direction = "BUY";
        } else if (side.equals("2")) {
            this.direction = "SELL";
        } else {
            throw new DirectionNotDeterminedException();
        }
    }
}

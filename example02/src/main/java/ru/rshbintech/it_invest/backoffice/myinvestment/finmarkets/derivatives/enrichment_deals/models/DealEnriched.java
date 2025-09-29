package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.DirectionNotDeterminedException;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DealEnriched {

    private String dealKind; // "TRADE", "EXECUTION", "EXPIRATION"
    private String code; // Сформированный код сделки
    private String externalCode; // Внешний код сделки
    private String direction; // "BUY" или "SELL"
    private String createdDate; // Время заключения сделки в формате "YYYY-MM-DDTHH:mm:ss"
    private String contractId; // Идентификатор договора клиента
    private String clientId; // Идентификатор клиента
    private String fiId; // Идентификатор финансового инструмента
    private Double amount; // Количество
    private BigDecimal price; // Первичная цена
    private Integer point; // Точность первичной цены (значение "5")
    private BigDecimal price2; // Вторичная цена
    private BigDecimal priceRUB; // Цена в рублях
    private BigDecimal positionCost; // Стоимость
    private BigDecimal bonus; // Опционная премия
    private BigDecimal positionBonus; // Сумма, по которой учитывается премия по позиции
    private String clearingDate; // Дата клиринга в формате YYYY-MM-DD
    private String counterPartyId; // Идентификатор контрагента ("2")
    private String exchangeId; // Идентификатор биржи ("2")
    private String requestExternalCode; // Биржевой код заявки
    private Integer marketScheme; // Схема расчетов
    private Integer spreadLegNumber; // Номер сделки в рамках календарного спреда
    private List<Commission> commissions; // Комиссии по сделке
    private Boolean isMarginCall; // Является сделкой MarginCall
    private Boolean isPrognose; // Является неподтверждённой сделкой

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

    /*
    Формируется конкатенацией четырёх строк:
        - Направление сделки.
            Если значение Direction (см. поле 1.4) = "BUY", то строка "B/"
            Если значение Direction (см. поле 1.4) = "SELL", то строка "S/"
        - Дата сделки в формате "DDMMYYYY" на основе значения поля "TransactTime"
        - Символ "0"
        - Биржевой код сделки: значение поля "SecondaryExecID"
     */
    public void setCodeFromTimeAndIDAndDirection(String direction, String date, Long execID) {
        String directionPart = direction.equals("BUY") ? "B/" : "S/";

        LocalDateTime parsedDate = LocalDateTime.parse(date, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        String datePart = parsedDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"));

        this.code = directionPart + datePart + "0" + execID;
    }

    public void putCommission(Commission commission) {
        if (commission != null) {
            if (this.commissions == null) {
                this.commissions = new ArrayList<>();
            }
            this.commissions.add(commission);
        }
    }
}

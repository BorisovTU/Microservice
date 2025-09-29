package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.enums.OrderStatus;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OrderClientEnrichedDto {

    @JsonProperty("headers")
    private KafkaHeadersDto headers;

    @JsonProperty("code")
    private String code;

    @JsonProperty("externalCode")
    private String externalCode;

    @JsonProperty("createdDate")
    private String createdDate;

    @JsonProperty("contractId")
    private String contractId;

    @JsonProperty("clientId")
    private String clientId;

    @JsonProperty("direction")
    private String direction;

    @JsonProperty("fiId")
    private String fiId;

    @JsonProperty("amount")
    private BigDecimal amount;

    @JsonProperty("price")
    private BigDecimal price;

    @JsonProperty("priceType")
    private String priceType;

    @JsonProperty("priceFiId")
    private String priceFiId;

    @JsonProperty("status")
    private OrderStatus status;

    @JsonProperty("exchangeId")
    private String exchangeId;

    @JsonProperty("lastUpdateTime")
    private String lastUpdateTime;
}

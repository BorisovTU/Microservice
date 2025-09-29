package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TradeClientEnrichedDto {

    @JsonProperty("headers")
    private KafkaHeadersDto headers;

    @JsonProperty("tradeKind")
    private String tradeKind;

    @JsonProperty("code")
    private String code;

    @JsonProperty("externalCode")
    private String externalCode;

    @JsonProperty("direction")
    private String direction;

    @JsonProperty("createdDate")
    private String createdDate;

    @JsonProperty("contractId")
    private String contractId;

    @JsonProperty("clientId")
    private String clientId;

    @JsonProperty("fiId")
    private String fiId;

    @JsonProperty("amount")
    private BigDecimal amount;

    @JsonProperty("price")
    private BigDecimal price;

    @JsonProperty("clearingDate")
    private String clearingDate;

    @JsonProperty("counterPartyId")
    private String counterPartyId;

    @JsonProperty("exchangeId")
    private String exchangeId;

    @JsonProperty("externalOrderId")
    private String externalOrderId;

    @JsonProperty("marketSchemeId")
    private Integer marketSchemeId;

    @JsonProperty("commissions")
    private List<TradeCommissionDto> commissions;
}

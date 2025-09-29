package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RawTradeDto {

    @JsonProperty("orderId")
    private String orderId;

    @JsonProperty("secondaryClOrdId")
    private String secondaryClOrdId;

    @JsonProperty("clOrdId")
    private String clOrdId;

    @JsonProperty("origClOrdId")
    private String origClOrdId;

    @JsonProperty("parties")
    private List<PartyDto> parties;

    @JsonProperty("instrument")
    private List<InstrumentDto> instrument;

    @JsonProperty("orderQtyData")
    private List<QtyDto> qtyData;

    @JsonProperty("yieldData")
    private List<YieldDto> yieldData;
}

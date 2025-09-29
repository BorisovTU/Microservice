package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaHeadersDto {

    @JsonProperty("msgId")
    private String msgId;

    @JsonProperty("requestTime")
    private String requestTime;

    @JsonProperty("traceId")
    private String traceId;
}

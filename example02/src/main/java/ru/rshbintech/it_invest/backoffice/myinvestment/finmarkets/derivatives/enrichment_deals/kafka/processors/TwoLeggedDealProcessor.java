package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.processors;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.FixDerivativeDataRaw;

@Slf4j
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TwoLeggedDealProcessor {
    private FixDerivativeDataRaw rawData;

    private DealProcessor firstDealProcessor;
    private DealProcessor secondDealProcessor;

    public TwoLeggedDealProcessor(FixDerivativeDataRaw rawData) {
        DealProcessor firstDealProcessor = new DealProcessor(rawData);
        DealProcessor secondDealProcessor = new DealProcessor(rawData);

        FixDerivativeDataRaw.Leg firstLeg = rawData.getLegs().get(0);
        FixDerivativeDataRaw.Leg secondLeg = rawData.getLegs().get(1);

        firstDealProcessor.setLegData(firstLeg.getLegSide(), firstLeg.getLegLastPrice(), firstLeg.getLegRatioQty(), firstLeg.getLegSymbol());
        secondDealProcessor.setLegData(secondLeg.getLegSide(), secondLeg.getLegLastPrice(), secondLeg.getLegRatioQty(), secondLeg.getLegSymbol());

        this.rawData = rawData;
        this.firstDealProcessor = firstDealProcessor;
        this.secondDealProcessor = secondDealProcessor;
    }

    public TwoLeggedDealProcessor() {
    }
}

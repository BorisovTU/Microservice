package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.globalTables.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.processors.DealProcessor;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.processors.HeaderTransformProcessor;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.processors.RequestProcessor;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.processors.TwoLeggedDealProcessor;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.enums.ExecTypes;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service.DealService;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service.RequestService;

import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class IncomingMessageTopology {

    private final KafkaConfig kafkaConfig;
    private final RequestService requestService;
    private final DealService dealService;
    private static final Serde<String> STRING_SERDE = Serdes.String();

    //Комиссии "МскБиржПИ" и "КлиентПФИ". Был хардкод по названию, стал сразу по ИД. Избавляемся от одного джойна
    private static final int MOEX_COMMISSION_ID = 12534;
    private static final int CLIENT_COMMISSION_ID = 322;

    private final InstrumentMoexLinkTable instrumentMoexLinkTable;
    private final InstrumentsTable instrumentsTable;
    private final BankMarketPlaceCodesTable bankMarketPlaceCodesTable;
    private final ContractMoexLinkTable contractMoexLinkTable;
    private final ContractsTable contractsTable;
    private final MarketSchemeMoexLinkTable marketSchemeMoexLinkTable;
    private final CommissionPlansTable commissionPlansTable;
    private final CommissionTypesTable commissionTypesTable;

    /*  Полная топология:
        -- Входящий поток с "сырыми" данными
           разделяется на два:
           -- поток с заявками
              разделяется на два:
              -- клиентская
                 завершение обработки и отправка в топик
              -- собственная
                 завершение обработки и отправка в топик
           -- поток со сделками
              разделяется на два:
              -- сделка с одной ногой
                 предобработка с учётом одной ноги сделки
                 <-- слияние в один общий поток сделок
              -- сделка с двумя ногами
                 предобработка с учётом двух ног сделки
                 <-- слияние в один общий поток сделок
              новый общий поток со сделками
              разделяется на два:
              -- клиентская
                 завершение обработки и отправка в топик
              -- собственная
                 завершение обработки и отправка в топик
     */
    public void buildTopology(StreamsBuilder builder) {
        String incomingDataTopic = kafkaConfig.getTopic().getRawData();

        /*
            Поток входящих сообщений
            - настроить хедеры (для дальнейшей отправки)
            - сразу получить "внутреннюю" часть FixDerivativeDataRaw
         */
        KStream<String, FixDerivativeDataRaw> messageStream = builder
                .stream(incomingDataTopic, Consumed.with(STRING_SERDE, new JsonSerde<>(RawDataMessage.class)))
                .peek((key, value) -> log.info("Received message: {}", value))
                .processValues(HeaderTransformProcessor::new)
                .mapValues((key, value) -> value.getFixDerivativeDataRaw());

        //разделение потока на два: сделки и заявки
        KStream<String, FixDerivativeDataRaw> dealStream = messageStream.filter((key, value) -> ExecTypes.TRADE.getCode().equals(value.getExecType()));
        KStream<String, FixDerivativeDataRaw> requestStream = messageStream.filterNot((key, value) -> ExecTypes.TRADE.getCode().equals(value.getExecType()));

        //обработка сделок

        //поток сделок разделяется на два: сделка с одной ногой и с двумя ногами
        KStream<String, FixDerivativeDataRaw> twoLeggedDealStream = dealStream.filter((key, rawData) -> rawData.getNoLegs() != null && rawData.getNoLegs() == 2);
        KStream<String, FixDerivativeDataRaw> oneLeggedDealStream = dealStream.filterNot((key, rawData) -> rawData.getNoLegs() != null && rawData.getNoLegs() == 2);

        KStream<String, DealProcessor> preprocessedTwoLeggedDealStream = preprocessTwoLeggedDealStream(twoLeggedDealStream);
        KStream<String, DealProcessor> preprocessedOneLeggedDeal = preprocessOneLeggedDeal(oneLeggedDealStream);

        //сделки обратно сливаются в один поток
        KStream<String, DealProcessor> mergedDeal = preprocessedOneLeggedDeal.merge(preprocessedTwoLeggedDealStream);

        KStream<String, DealProcessor> processedDealStream = processDealStream(mergedDeal);

        //разделяем сделку на два вида: клиентская или собственная
        KStream<String, DealProcessor> oneLeggedDealClient = processedDealStream.filter((clientAccount, deal) -> deal.getIsClientDeal());
        KStream<String, DealProcessor> oneLeggedDealBank = processedDealStream.filterNot((clientAccount, deal) -> deal.getIsClientDeal());

        finalizeBankDealStream(oneLeggedDealBank);
        finalizeClientDealStream(oneLeggedDealClient);

        //обработка заявок
        KStream<String, RequestProcessor> preprocessedRequestStream = processRequestStream(requestStream);

        //разделяем заявку на два вида: клиентская или собственная
        KStream<String, RequestProcessor> requestClientStream = preprocessedRequestStream.filter((clientAccount, requestProcessor) -> requestProcessor.getIsClientRequest());
        KStream<String, RequestProcessor> requestBankStream = preprocessedRequestStream.filterNot((clientAccount, requestProcessor) -> requestProcessor.getIsClientRequest());

        finalizeRequestBankStream(requestBankStream);
        finalizeRequestClientStream(requestClientStream);
    }

    private KStream<String, RequestProcessor> processRequestStream(KStream<String, FixDerivativeDataRaw> requestStream) {
        KStream<String, RequestProcessor> processedRequestStream = requestStream
                .mapValues((key, rawData) -> new RequestProcessor(rawData))
                .leftJoin(instrumentMoexLinkTable.getTable(),
                        (key, requestProcessor) -> requestProcessor.getRawData().getSymbol(),
                        RequestProcessor::withFiInstrumentMoexLink)
                .leftJoin(instrumentsTable.getTable(),
                        (key, requestProcessor) -> String.valueOf(requestProcessor.getFinancialInstrumentMoexLink().fiId()),
                        RequestProcessor::withFiInstrument)
                .leftJoin(bankMarketPlaceCodesTable.getTable(),
                        (key, requestProcessor) -> requestProcessor.getRawData().getAccount(),
                        RequestProcessor::withBankMarketPlaceCode)
                .mapValues((key, requestProcessor) -> {
                    RequestEnriched requestEnriched = requestService.createBaseRequest(requestProcessor.getRawData());
                    requestService.setInstrumentSpecificFields(requestEnriched, requestProcessor.getFinancialInstrument());

                    requestProcessor.setRequestEnriched(requestEnriched);

                    return requestProcessor;
                });

        return processedRequestStream;
    }

    private void finalizeRequestBankStream(KStream<String, RequestProcessor> requestBankStream) {
        String requestTopic = kafkaConfig.getTopic().getRequests();

        requestBankStream
                .mapValues((key, requestProcessor) -> requestProcessor.getRequestEnriched())
                .peek((key, value) -> log.info("mapped to send bank request"))
                .to(requestTopic, Produced.with(STRING_SERDE, new JsonSerde<>(RequestEnriched.class)));
    }

    private void finalizeRequestClientStream(KStream<String, RequestProcessor> requestClientStream) {
        String clientRequestTopic = kafkaConfig.getTopic().getClientRequests();

        requestClientStream
                .leftJoin(contractMoexLinkTable.getTable(),
                        (key, requestProcessor) -> requestProcessor.getRawData().getAccount(),
                        RequestProcessor::withContractMoexLink)
                .leftJoin(contractsTable.getTable(),
                        (key, requestProcessor) -> String.valueOf(requestProcessor.getContractMoexLink().contractId()),
                        RequestProcessor::withContract)
                .map((key, requestProcessor) -> {
                    //формируем поток для отправки:
                    //  Делаем ключ requestEnriched.getClientID()
                    //  в value присваиваем обогащённую сделку requestEnriched
                    RequestEnriched requestEnriched = requestProcessor.getRequestEnriched();
                    requestService.setContractSpecificFields(requestEnriched, requestProcessor.getContract());

                    return KeyValue.pair(requestEnriched.getClientId(), requestEnriched);
                })
                .peek((key, value) -> log.info("mapped to send client request"))
                .to(clientRequestTopic, Produced.with(STRING_SERDE, new JsonSerde<>(RequestEnriched.class)));
    }

    /*
        Предобработка двуногой сделки:
          Джойнятся оба FinancialInstrument
          высчитываются spreadNumber
          трансформируется в поток сделок
     */
    private KStream<String, DealProcessor> preprocessTwoLeggedDealStream (KStream<String, FixDerivativeDataRaw> twoLeggedDealStream) {
        KStream<String, DealProcessor> preprocessedTwoLeggedDealStream = twoLeggedDealStream
                .peek((key, rawData) -> dealService.validateTwoLeggedDeal(rawData))
                .mapValues((key, rawData) -> new TwoLeggedDealProcessor(rawData))
                .leftJoin(instrumentMoexLinkTable.getTable(),
                        (key, dealProcessor) -> dealProcessor.getFirstDealProcessor().getLegData().getInstrumentCode(),
                        (dealProcessor, value) -> {
                            dealProcessor.getFirstDealProcessor().withFiInstrumentMoexLink(value);
                            return dealProcessor;
                        }
                )
                .leftJoin(instrumentsTable.getTable(),
                        (key, dealProcessor) -> {
                            FinancialInstrumentMoexLink fiInstr = dealProcessor.getFirstDealProcessor().getFinancialInstrumentMoexLink();
                            return String.valueOf(fiInstr.fiId());
                        },
                        (dealProcessor, value) -> {
                            dealProcessor.getFirstDealProcessor().withFiInstrument(value);
                            return dealProcessor;
                        }
                )
                .leftJoin(instrumentMoexLinkTable.getTable(),
                        (key, dealProcessor) -> dealProcessor.getSecondDealProcessor().getLegData().getInstrumentCode(),
                        (dealProcessor, value) -> {
                            dealProcessor.getSecondDealProcessor().withFiInstrumentMoexLink(value);
                            return dealProcessor;
                        })
                .leftJoin(instrumentsTable.getTable(),
                        (key, dealProcessor) -> {
                            FinancialInstrumentMoexLink fiInstr = dealProcessor.getSecondDealProcessor().getFinancialInstrumentMoexLink();
                            return String.valueOf(fiInstr.fiId());
                        },
                        (dealProcessor, value) -> {
                            dealProcessor.getSecondDealProcessor().withFiInstrument(value);
                            return dealProcessor;
                        })
                .flatMapValues((key, dealProcessor) -> {
                    DealProcessor firstDealProcessor = dealProcessor.getFirstDealProcessor();
                    DealProcessor secondDealProcessor = dealProcessor.getSecondDealProcessor();

                    LocalDateTime drawingDateFirst = dealService.parseDrawingDate(firstDealProcessor.getFinancialInstrument().drawingDate());
                    LocalDateTime drawingDateSecond = dealService.parseDrawingDate(secondDealProcessor.getFinancialInstrument().drawingDate());

                    if (drawingDateFirst.isBefore(drawingDateSecond)) {
                        firstDealProcessor.getLegData().setSpreadNumber(1);
                        secondDealProcessor.getLegData().setSpreadNumber(2);
                    } else {
                        firstDealProcessor.getLegData().setSpreadNumber(2);
                        secondDealProcessor.getLegData().setSpreadNumber(1);
                    }

                    return List.of(firstDealProcessor, secondDealProcessor);
                });

        return preprocessedTwoLeggedDealStream;
    }

    private KStream<String, DealProcessor> preprocessOneLeggedDeal (KStream<String, FixDerivativeDataRaw> oneLeggedDealStream) {
        KStream<String, DealProcessor> preprocessedOneLeggedDeal = oneLeggedDealStream
                .mapValues((key, rawData) -> {
                    DealProcessor dealProcessor = new DealProcessor(rawData);
                    dealProcessor.setLegData(rawData.getSide(), rawData.getLastPx(), 1L, rawData.getSymbol());
                    return dealProcessor;
                })
                .leftJoin(instrumentMoexLinkTable.getTable(),
                        (key, deal) -> deal.getRawData().getSymbol(),
                        DealProcessor::withFiInstrumentMoexLink)
                .leftJoin(instrumentsTable.getTable(),
                        (key, deal) -> String.valueOf(deal.getFinancialInstrumentMoexLink().fiId()),
                        DealProcessor::withFiInstrument);

        return preprocessedOneLeggedDeal;
    }

    private KStream<String, DealProcessor> processDealStream(KStream<String, DealProcessor> dealStream) {
        KStream<String, DealProcessor> processedDeal = dealStream
                .leftJoin(bankMarketPlaceCodesTable.getTable(),
                        (key, deal) -> deal.getRawData().getAccount(),
                        DealProcessor::withBankMarketPlaceCode)
                .mapValues((key, dealProcessor) -> {
                    FixDerivativeDataRaw rawData = dealProcessor.getRawData();
                    DealProcessor.LegData legData = dealProcessor.getLegData();
                    DealEnriched dealEnriched = dealService.createBaseDeal(rawData);

                    dealService.setLegSpecificFields(dealEnriched,
                            legData.getSide(),
                            (double) (rawData.getLastQty() * legData.getRatioQty()),
                            rawData.getSecondaryExecId());

                    dealService.setInstrumentSpecificFields(dealEnriched, dealProcessor.getFinancialInstrument(), legData.getLastPrice());
                    dealService.setPrices(dealEnriched, dealProcessor.getFinancialInstrument());
                    dealService.setSpreadLegNumber(dealEnriched, legData.getSpreadNumber());
                    dealService.putExchangeCommission(dealEnriched, MOEX_COMMISSION_ID, rawData.getMiscFeeAmt());

                    dealProcessor.setDealEnriched(dealEnriched);

                    return dealProcessor;
                });

        return processedDeal;
    }

    private void finalizeBankDealStream(KStream<String, DealProcessor> bankDealStream) {
        String dealTopic = kafkaConfig.getTopic().getDeals();

        bankDealStream
                .leftJoin(marketSchemeMoexLinkTable.getTable(),
                        (key, deal) -> deal.getRawData().getAccount().substring(0, 4),
                        DealProcessor::withMarketSchemeMoexLink)
                .peek((key, deal) -> dealService.setMarketScheme(deal.getDealEnriched(), deal.getMarketSchemeMoexLink()))
                //формируем поток для отправки:
                //  ключ остаётся null. Собственные сделки отправляются без ключа
                //  в value присваиваем обогащённую сделку deal.getDealEnriched()
                .mapValues((key, deal) -> deal.getDealEnriched())
                .peek((key, value) -> log.info("mapped to send bank deal"))
                .to(dealTopic, Produced.with(STRING_SERDE, new JsonSerde<>(DealEnriched.class)));
    }

    private void finalizeClientDealStream(KStream<String, DealProcessor> clientDealStream) {
        String clientDealTopic = kafkaConfig.getTopic().getClientDeals();

        clientDealStream
                .leftJoin(contractMoexLinkTable.getTable(),
                        (key, deal) -> deal.getRawData().getAccount(),
                        DealProcessor::withContractMoexLink)
                .leftJoin(contractsTable.getTable(),
                        (key, deal) -> String.valueOf(deal.getContractMoexLink().contractId()),
                        DealProcessor::withContract)
                .leftJoin(marketSchemeMoexLinkTable.getTable(),
                        (key, deal) -> deal.getContract().firmID(),
                        DealProcessor::withMarketSchemeMoexLink)
                .leftJoin(commissionPlansTable.getTable(),
                        (key, deal) -> commissionPlansTable.getKey(CLIENT_COMMISSION_ID, deal.getContract().planID()),
                        DealProcessor::withCommissionPlan)
                .leftJoin(commissionTypesTable.getTable(),
                        (key, deal) -> String.valueOf(deal.getCommissionPlan().commissionID()),
                        DealProcessor::withCommissionType)
                .map((commissionId, dealProcessor) -> {
                    //формируем поток для отправки:
                    //  Делаем ключ dealEnriched.getClientID()
                    //  в value присваиваем обогащённую сделку deal.getDealEnriched()
                    DealEnriched dealEnriched = dealProcessor.getDealEnriched();
                    dealService.setContractSpecificFields(dealEnriched, dealProcessor.getContract());
                    dealService.putClientCommission(dealEnriched, dealProcessor.getCommissionType(), dealProcessor.getCommissionPlan());
                    dealService.setMarketScheme(dealEnriched, dealProcessor.getMarketSchemeMoexLink());

                    return KeyValue.pair(dealEnriched.getClientId(), dealEnriched);
                })
                .peek((key, value) -> log.info("mapped to send client deal"))
                .to(clientDealTopic, Produced.with(STRING_SERDE, new JsonSerde<>(DealEnriched.class)));
    }
}

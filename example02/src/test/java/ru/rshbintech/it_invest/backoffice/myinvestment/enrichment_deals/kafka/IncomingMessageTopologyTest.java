package ru.rshbintech.it_invest.backoffice.myinvestment.enrichment_deals.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.IncomingMessageTopology;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.globalTables.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.enums.ExecTypes;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service.DealService;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service.RequestService;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IncomingMessageTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, RawDataMessage> inputTopic;
    private TestOutputTopic<String, RequestEnriched> bankRequestsOutputTopic;
    private TestOutputTopic<String, RequestEnriched> clientRequestsOutputTopic;
    private TestOutputTopic<String, DealEnriched> bankDealOutputTopic;
    private TestOutputTopic<String, DealEnriched> clientDealOutputTopic;

    private KeyValueStore<Object, Object> fiInstrumentMoexLinkStore;
    private KeyValueStore<Object, Object> fiInstrumentStore;
    private KeyValueStore<Object, Object> bankMarketPlaceCodeStore;
    private KeyValueStore<Object, Object> contractMoexLinkStore;
    private KeyValueStore<Object, Object> contractDataStore;
    private KeyValueStore<Object, Object> marketSchemeMoexLinkStore;
    private KeyValueStore<Object, Object> commissionPlanStore;
    private KeyValueStore<Object, Object> commissionTypeStore;

    @Mock
    private KafkaConfig kafkaConfig;
    @Mock
    private RequestService requestService;
    @Mock
    private DealService dealService;

    @InjectMocks
    private InstrumentMoexLinkTable instrumentMoexLinkTable;
    @InjectMocks
    private InstrumentsTable instrumentsTable;
    @InjectMocks
    private BankMarketPlaceCodesTable bankMarketPlaceCodesTable;
    @InjectMocks
    private ContractMoexLinkTable contractMoexLinkTable;
    @InjectMocks
    private ContractsTable contractsTable;
    @InjectMocks
    private MarketSchemeMoexLinkTable marketSchemeMoexLinkTable;
    @InjectMocks
    private CommissionPlansTable commissionPlansTable;
    @InjectMocks
    private CommissionTypesTable commissionTypesTable;

    @BeforeEach
    void setUp() {
        KafkaConfig.Topic topicConfig = Mockito.mock(KafkaConfig.Topic.class);
        when(kafkaConfig.getTopic()).thenReturn(topicConfig);
        when(topicConfig.getRawData()).thenReturn("sofr.derivatives.fix");
        when(topicConfig.getRequests()).thenReturn("derivatives.requests.bank.enriched");
        when(topicConfig.getClientRequests()).thenReturn("derivatives.requests.client.enriched");
        when(topicConfig.getDeals()).thenReturn("derivatives.deals.bank.enriched");
        when(topicConfig.getClientDeals()).thenReturn("derivatives.deals.client.enriched");
        when(topicConfig.getContractsMoexLnk()).thenReturn("subcontracts.derivatives.active.moex-lnk");
        when(topicConfig.getContracts()).thenReturn("subcontracts.derivatives.active");
        when(topicConfig.getInstrumentsMoexLnk()).thenReturn("instruments.derivatives.active.moex-lnk");
        when(topicConfig.getInstruments()).thenReturn("instruments.derivatives.active");
        when(topicConfig.getMarketSchemeMoexLnk()).thenReturn("deals.marketschemes.moex-lnk");
        when(topicConfig.getCommissionTypes()).thenReturn("commissions.types");
        when(topicConfig.getCommissionPlans()).thenReturn("commissions.parameters.plans");
        when(topicConfig.getBankMpCodes()).thenReturn("bank.marketplacecodes");

        // Create topology
        IncomingMessageTopology topology = new IncomingMessageTopology(
                kafkaConfig,
                requestService,
                dealService,
                instrumentMoexLinkTable,
                instrumentsTable,
                bankMarketPlaceCodesTable,
                contractMoexLinkTable,
                contractsTable,
                marketSchemeMoexLinkTable,
                commissionPlansTable,
                commissionTypesTable
        );

        StreamsBuilder builder = new StreamsBuilder();

        setupGlobalTables(builder);
        topology.buildTopology(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic(
                "sofr.derivatives.fix",
                new StringSerializer(),
                new JsonSerde<>(RawDataMessage.class).serializer()
        );

        fiInstrumentMoexLinkStore = testDriver.getKeyValueStore("dv-deals-enricher-instruments-moex-lnk-store");
        fiInstrumentStore = testDriver.getKeyValueStore("dv-deals-enricher-instruments-store");
        bankMarketPlaceCodeStore = testDriver.getKeyValueStore("dv-deals-enricher-bank-market-place-codes");
        contractMoexLinkStore = testDriver.getKeyValueStore("dv-deals-enricher-contracts-moex-lnk-store");
        contractDataStore = testDriver.getKeyValueStore("dv-deals-enricher-contracts-store");
        marketSchemeMoexLinkStore = testDriver.getKeyValueStore("dv-deals-enricher-market-scheme-store");
        commissionPlanStore = testDriver.getKeyValueStore("dv-deals-enricher-commission-plans-store");
        commissionTypeStore = testDriver.getKeyValueStore("dv-deals-enricher-commission-types-store");


        bankRequestsOutputTopic = testDriver.createOutputTopic(
                "derivatives.requests.bank.enriched",
                Serdes.String().deserializer(),
                new JsonSerde<>(RequestEnriched.class).deserializer()
        );

        clientRequestsOutputTopic = testDriver.createOutputTopic(
                "derivatives.requests.client.enriched",
                Serdes.String().deserializer(),
                new JsonSerde<>(RequestEnriched.class).deserializer()
        );

        bankDealOutputTopic = testDriver.createOutputTopic(
                "derivatives.deals.bank.enriched",
                Serdes.String().deserializer(),
                new JsonSerde<>(DealEnriched.class).deserializer()
        );

        clientDealOutputTopic = testDriver.createOutputTopic(
                "derivatives.deals.client.enriched",
                Serdes.String().deserializer(),
                new JsonSerde<>(DealEnriched.class).deserializer()
        );
    }

    private void setupGlobalTables(StreamsBuilder builder) {
        instrumentMoexLinkTable.build(builder);
        instrumentsTable.build(builder);
        bankMarketPlaceCodesTable.build(builder);
        contractMoexLinkTable.build(builder);
        contractsTable.build(builder);
        marketSchemeMoexLinkTable.build(builder);
        commissionPlansTable.build(builder);
        commissionTypesTable.build(builder);
    }

    private RawDataMessage createRawDataMessage(boolean isRequest, boolean isClient) {
        RawDataMessage message = new RawDataMessage();
        FixDerivativeDataRaw fixData = new FixDerivativeDataRaw();
        fixData.setExecType(isRequest ? ExecTypes.NEW.getCode() : ExecTypes.TRADE.getCode());
        fixData.setSymbol("INSTRUMENT_CODE");
        fixData.setAccount(isClient ? "CLIENT_ACCOUNT_CODE" : "BANK_ACCOUNT_CODE");
        fixData.setLastQty(7L);
        message.setFixDerivativeDataRaw(fixData);
        return message;
    }

    private FinancialInstrumentMoexLink createFinancialInstrumentMoexLink() {
        return new FinancialInstrumentMoexLink("INSTRUMENT_CODE", 10);
    }

    private FinancialInstrumentMoexLink createSecondFinancialInstrumentMoexLink() {
        return new FinancialInstrumentMoexLink("INSTRUMENT_CODE2", 11);
    }

    private FinancialInstrument createFinancialInstrument() {
        FinancialInstrument fiInstr = new FinancialInstrument(
                10L,
                "marketPlaceCode",
                "pfiKind",
                BigDecimal.valueOf(0.0),
                "faceValueFiKind",
                0L,
                1,
                BigDecimal.valueOf(1.0),
                0L,
                BigDecimal.valueOf(1.0),
                "DATE1",
                BigDecimal.valueOf(1.0)
        );

        return fiInstr;
    }

    private FinancialInstrument createSecondFinancialInstrument() {
        FinancialInstrument fiInstr = new FinancialInstrument(
                11L,
                "marketPlaceCode",
                "pfiKind",
                BigDecimal.valueOf(0.0),
                "faceValueFiKind",
                0L,
                1,
                BigDecimal.valueOf(1.0),
                0L,
                BigDecimal.valueOf(1.0),
                "DATE2",
                BigDecimal.valueOf(1.0)
        );

        return fiInstr;
    }

    private BankMarketPlaceCode createBankMarketPlaceCode() {
        return new BankMarketPlaceCode("BANK_ACCOUNT_CODE");
    }

    private ContractMoexLink createContractMoexLink() {
        return new ContractMoexLink("CLIENT_ACCOUNT_CODE", 11);
    }

    private Contract createContractData() {
        Contract contract = new Contract(11, "mpcode", 1L, "FIRM_ID", 18);

        return contract;
    }

    private MarketSchemeMoexLink createMarketSchemeMoexLink(String id) {
        return new MarketSchemeMoexLink(id, 1);
    }

    private CommissionPlan createCommissionPlan() {
        int CLIENT_COMMISSION_ID = 322;
        CommissionPlan commissionPlan = new CommissionPlan("CLIENT_COMMISSION_ID",
                CLIENT_COMMISSION_ID,
                0,
                0.0,
                0.0,
                false,
                false,
                "1",
                0.0);

        return commissionPlan;
    }

    private CommissionType createCommissionType() {
        return new CommissionType(1, "code", BigDecimal.valueOf(0.0), BigDecimal.valueOf(0.0));
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldProcessBankRequest() {
        // Given
        RawDataMessage message = createRawDataMessage(true, false);
        FinancialInstrumentMoexLink financialInstrumentMoexLink = createFinancialInstrumentMoexLink();
        fiInstrumentMoexLinkStore.put(financialInstrumentMoexLink.id(), financialInstrumentMoexLink);

        FinancialInstrument financialInstrument = createFinancialInstrument();
        fiInstrumentStore.put(String.valueOf(financialInstrument.id()), financialInstrument);

        BankMarketPlaceCode bankMarketPlaceCode = createBankMarketPlaceCode();
        bankMarketPlaceCodeStore.put(bankMarketPlaceCode.id(), bankMarketPlaceCode);

        // When
        when(requestService.createBaseRequest(any(FixDerivativeDataRaw.class))).thenReturn(new RequestEnriched());
        Mockito.doNothing().when(requestService).setInstrumentSpecificFields(any(RequestEnriched.class), any(FinancialInstrument.class));

        inputTopic.pipeInput("key", message);

        // Then
        assertFalse(bankRequestsOutputTopic.isEmpty());
        assertTrue(clientRequestsOutputTopic.isEmpty());
        assertTrue(bankDealOutputTopic.isEmpty());
        assertTrue(clientDealOutputTopic.isEmpty());

        TestRecord<String, RequestEnriched> outputRecord = bankRequestsOutputTopic.readRecord();
        assertNotNull(outputRecord.value());
    }

    @Test
    void shouldProcessClientRequest() {
        // Given
        RawDataMessage message = createRawDataMessage(true, true);
        FinancialInstrumentMoexLink financialInstrumentMoexLink = createFinancialInstrumentMoexLink();
        fiInstrumentMoexLinkStore.put(financialInstrumentMoexLink.id(), financialInstrumentMoexLink);

        FinancialInstrument financialInstrument = createFinancialInstrument();
        fiInstrumentStore.put(String.valueOf(financialInstrument.id()), financialInstrument);

        ContractMoexLink contractMoexLink = createContractMoexLink();
        contractMoexLinkStore.put(contractMoexLink.id(), contractMoexLink);

        Contract contract = createContractData();
        contractDataStore.put(String.valueOf(contract.id()), contract);

        // When
        when(requestService.createBaseRequest(any(FixDerivativeDataRaw.class))).thenReturn(new RequestEnriched());
        Mockito.doNothing().when(requestService).setInstrumentSpecificFields(any(RequestEnriched.class), any(FinancialInstrument.class));
        Mockito.doNothing().when(requestService).setContractSpecificFields(any(RequestEnriched.class), any(Contract.class));

        inputTopic.pipeInput("key", message);

        // Then
        assertTrue(bankRequestsOutputTopic.isEmpty());
        assertFalse(clientRequestsOutputTopic.isEmpty());
        assertTrue(bankDealOutputTopic.isEmpty());
        assertTrue(clientDealOutputTopic.isEmpty());

        TestRecord<String, RequestEnriched> outputRecord = clientRequestsOutputTopic.readRecord();
        assertNotNull(outputRecord.value());
    }

    @Test
    void shouldProcessBankDeal() {
        // Given
        RawDataMessage message = createRawDataMessage(false, false);
        FinancialInstrumentMoexLink financialInstrumentMoexLink = createFinancialInstrumentMoexLink();
        fiInstrumentMoexLinkStore.put(financialInstrumentMoexLink.id(), financialInstrumentMoexLink);

        FinancialInstrument financialInstrument = createFinancialInstrument();
        fiInstrumentStore.put(String.valueOf(financialInstrument.id()), financialInstrument);

        BankMarketPlaceCode bankMarketPlaceCode = createBankMarketPlaceCode();
        bankMarketPlaceCodeStore.put(bankMarketPlaceCode.id(), bankMarketPlaceCode);

        MarketSchemeMoexLink marketSchemeMoexLink = createMarketSchemeMoexLink(message.getFixDerivativeDataRaw().getAccount().substring(0, 4));
        marketSchemeMoexLinkStore.put(marketSchemeMoexLink.id(), marketSchemeMoexLink);

        // When
        when(dealService.createBaseDeal(any(FixDerivativeDataRaw.class))).thenReturn(new DealEnriched());
        Mockito.doNothing().when(dealService).setLegSpecificFields(any(), any(), anyDouble(), any());
        Mockito.doNothing().when(dealService).setInstrumentSpecificFields(any(), any(), any());
        Mockito.doNothing().when(dealService).setPrices(any(), any(FinancialInstrument.class));
        Mockito.doNothing().when(dealService).setSpreadLegNumber(any(), any());
        Mockito.doNothing().when(dealService).putExchangeCommission(any(), anyInt(), any());
        Mockito.doNothing().when(dealService).setMarketScheme(any(), any());

        inputTopic.pipeInput("key", message);

        // Then
        assertTrue(bankRequestsOutputTopic.isEmpty());
        assertTrue(clientRequestsOutputTopic.isEmpty());
        assertFalse(bankDealOutputTopic.isEmpty());
        assertTrue(clientDealOutputTopic.isEmpty());

        TestRecord<String, DealEnriched> outputRecord = bankDealOutputTopic.readRecord();
        assertNotNull(outputRecord.value());
    }

    @Test
    void shouldProcessClientDeal() {
        RawDataMessage message = createRawDataMessage(false, true);

        FinancialInstrumentMoexLink financialInstrumentMoexLink = createFinancialInstrumentMoexLink();
        fiInstrumentMoexLinkStore.put(financialInstrumentMoexLink.id(), financialInstrumentMoexLink);

        FinancialInstrument financialInstrument = createFinancialInstrument();
        fiInstrumentStore.put(String.valueOf(financialInstrument.id()), financialInstrument);

        BankMarketPlaceCode bankMarketPlaceCode = createBankMarketPlaceCode();
        bankMarketPlaceCodeStore.put("EMPTY_CODE", bankMarketPlaceCode);

        ContractMoexLink contractMoexLink = createContractMoexLink();
        contractMoexLinkStore.put(contractMoexLink.id(), contractMoexLink);

        Contract contract = createContractData();
        contractDataStore.put(String.valueOf(contract.id()), contract);

        MarketSchemeMoexLink marketSchemeMoexLink = createMarketSchemeMoexLink(contract.firmID());
        marketSchemeMoexLinkStore.put(marketSchemeMoexLink.id(), marketSchemeMoexLink);

        CommissionPlan commissionPlan = createCommissionPlan();
        commissionPlanStore.put(commissionPlansTable.getKey(commissionPlan.commissionID(), contract.planID()), commissionPlan);

        CommissionType commissionType = createCommissionType();
        commissionTypeStore.put(String.valueOf(commissionPlan.commissionID()), commissionType);

        // When
        when(dealService.createBaseDeal(any(FixDerivativeDataRaw.class))).thenReturn(new DealEnriched());
        Mockito.doNothing().when(dealService).setLegSpecificFields(any(), any(), anyDouble(), any());
        Mockito.doNothing().when(dealService).setInstrumentSpecificFields(any(), any(), any());
        Mockito.doNothing().when(dealService).setPrices(any(), any(FinancialInstrument.class));
        Mockito.doNothing().when(dealService).setSpreadLegNumber(any(), any());
        Mockito.doNothing().when(dealService).putExchangeCommission(any(), anyInt(), any());
        Mockito.doNothing().when(dealService).setContractSpecificFields(any(), any());
        Mockito.doNothing().when(dealService).putClientCommission(any(), any(), any());
        Mockito.doNothing().when(dealService).setMarketScheme(any(), any());

        inputTopic.pipeInput("key", message);

        // Then
        assertTrue(bankRequestsOutputTopic.isEmpty());
        assertTrue(clientRequestsOutputTopic.isEmpty());
        assertTrue(bankDealOutputTopic.isEmpty());
        assertFalse(clientDealOutputTopic.isEmpty());

        TestRecord<String, DealEnriched> outputRecord = clientDealOutputTopic.readRecord();
        assertNotNull(outputRecord.value());
    }

    @Test
    void shouldProcessTwoLeggedBankDeal() {
        // Given
        RawDataMessage message = createRawDataMessage(false, false);
        FixDerivativeDataRaw.Leg leg1 = new FixDerivativeDataRaw.Leg();
        FixDerivativeDataRaw.Leg leg2 = new FixDerivativeDataRaw.Leg();
        leg1.setLegSide("1");
        leg1.setLegSymbol("INSTRUMENT_CODE");
        leg1.setLegRatioQty(2L);

        leg2.setLegSide("2");
        leg2.setLegSymbol("INSTRUMENT_CODE2");
        leg2.setLegRatioQty(3L);

        message.getFixDerivativeDataRaw().setLegs(List.of(leg1, leg2));
        message.getFixDerivativeDataRaw().setNoLegs(2);

        FinancialInstrumentMoexLink financialInstrumentMoexLink = createFinancialInstrumentMoexLink();
        fiInstrumentMoexLinkStore.put(financialInstrumentMoexLink.id(), financialInstrumentMoexLink);

        FinancialInstrument financialInstrument = createFinancialInstrument();
        fiInstrumentStore.put(String.valueOf(financialInstrument.id()), financialInstrument);

        FinancialInstrumentMoexLink financialInstrumentMoexLink2 = createSecondFinancialInstrumentMoexLink();
        fiInstrumentMoexLinkStore.put(financialInstrumentMoexLink2.id(), financialInstrumentMoexLink2);

        FinancialInstrument financialInstrument2 = createSecondFinancialInstrument();
        fiInstrumentStore.put(String.valueOf(financialInstrument2.id()), financialInstrument2);

        BankMarketPlaceCode bankMarketPlaceCode = createBankMarketPlaceCode();
        bankMarketPlaceCodeStore.put(bankMarketPlaceCode.id(), bankMarketPlaceCode);

        MarketSchemeMoexLink marketSchemeMoexLink = createMarketSchemeMoexLink(message.getFixDerivativeDataRaw().getAccount().substring(0, 4));
        marketSchemeMoexLinkStore.put(marketSchemeMoexLink.id(), marketSchemeMoexLink);

        // When
        when(dealService.createBaseDeal(any(FixDerivativeDataRaw.class))).thenReturn(new DealEnriched());
        when(dealService.parseDrawingDate("DATE1")).thenReturn(LocalDateTime.now());
        when(dealService.parseDrawingDate("DATE2")).thenReturn(LocalDateTime.now().plusDays(1));
        Mockito.doNothing().when(dealService).setLegSpecificFields(any(), any(), anyDouble(), any());
        Mockito.doNothing().when(dealService).setInstrumentSpecificFields(any(), any(), any());
        Mockito.doNothing().when(dealService).setPrices(any(), any());
        Mockito.doNothing().when(dealService).setSpreadLegNumber(any(), any());
        Mockito.doNothing().when(dealService).putExchangeCommission(any(), anyInt(), any());
        Mockito.doNothing().when(dealService).setMarketScheme(any(), any());

        inputTopic.pipeInput("key", message);

        // Then
        assertTrue(bankRequestsOutputTopic.isEmpty());
        assertTrue(clientRequestsOutputTopic.isEmpty());
        assertFalse(bankDealOutputTopic.isEmpty());
        assertTrue(clientDealOutputTopic.isEmpty());

        List<DealEnriched> result = bankDealOutputTopic.readValuesToList();

        assertEquals(2, result.size());
        assertNotNull(result.getFirst());
        assertNotNull(result.getLast());
    }
}
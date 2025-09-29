package kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.OrderClientEnrichedDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.TradeClientEnrichedDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.RawOrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.RawTradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.IncomingMessageTopology;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.producers.MessageProducer;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service.OrderService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service.TradeService;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class IncomingMessageTopologyTest {

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private KafkaConfig.Topic topicMock;

    @Mock
    private OrderService orderService;

    @Mock
    private TradeService tradeService;

    @Mock
    private KafkaTemplate<String, OrderClientEnrichedDto> orderKafkaTemplate;

    @Mock
    private KafkaTemplate<String, TradeClientEnrichedDto> tradeKafkaTemplate;

    @Mock
    private MessageProducer messageProducer;

    @Mock
    private KStream<String, RawOrderDto> ordersStreamMock;

    @Mock
    private KStream<String, RawTradeDto> tradesStreamMock;

    @Mock
    private StreamsBuilder streamsBuilder;

    private IncomingMessageTopology incomingMessageTopology;

    @BeforeEach
    void setUp() {
        incomingMessageTopology = new IncomingMessageTopology(
                kafkaConfig,
                orderService,
                tradeService,
                messageProducer,
                orderKafkaTemplate,
                tradeKafkaTemplate
        );
        when(kafkaConfig.getTopic()).thenReturn(topicMock);
    }

    @Test
    void shouldProcessOrderAndSendToKafka() {
        when(topicMock.getRawOrders()).thenReturn("orders-raw-topic");
        when(topicMock.getOrdersClientEnriched()).thenReturn("orders-enriched-topic");

        RawOrderDto rawOrder = new RawOrderDto();
        OrderClientEnrichedDto enrichedOrder = new OrderClientEnrichedDto();
        enrichedOrder.setClientId("client-123");

        when(streamsBuilder.stream(eq("orders-raw-topic"), any(Consumed.class)))
                .thenReturn(ordersStreamMock);

        doAnswer(invocation -> {
            ForeachAction<String, RawOrderDto> action = invocation.getArgument(0);
            action.apply("orderKey", rawOrder);
            return null;
        }).when(ordersStreamMock).foreach(any());

        when(orderService.enrichOrder(rawOrder)).thenReturn(enrichedOrder);
        when(orderKafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        incomingMessageTopology.buildOrdersStream(streamsBuilder);

        verify(orderService).enrichOrder(rawOrder);
        verify(orderKafkaTemplate).send(
                ArgumentMatchers.<ProducerRecord<String, OrderClientEnrichedDto>>argThat(record ->
                        record.topic().equals("orders-enriched-topic")
                                && record.key().equals("client-123")
                                && record.value() == enrichedOrder
                )
        );
        verify(messageProducer, never()).sendToDlq(anyLong(), any(), any(), any(), any(),
                anyLong(), any(), anyInt());
    }

    @Test
    void shouldSendOrderToDlqOnException() {
        KafkaConfig.Topic topicMock = mock(KafkaConfig.Topic.class);
        when(kafkaConfig.getTopic()).thenReturn(topicMock);
        when(topicMock.getRawOrders()).thenReturn("orders-raw-topic");
        when(topicMock.getOrdersClientEnriched()).thenReturn("orders-enriched-topic");

        RawOrderDto rawOrder = new RawOrderDto();

        when(streamsBuilder.stream(eq("orders-raw-topic"), any(Consumed.class))).thenReturn(ordersStreamMock);

        doAnswer(invocation -> {
            ForeachAction<String, RawOrderDto> action = invocation.getArgument(0);
            action.apply("orderKey", rawOrder);
            return null;
        }).when(ordersStreamMock).foreach(any());

        when(orderService.enrichOrder(rawOrder)).thenThrow(new RuntimeException("Test exception"));

        incomingMessageTopology.buildOrdersStream(streamsBuilder);

        verify(orderKafkaTemplate, never()).send(any(ProducerRecord.class));
        verify(messageProducer).sendToDlq(anyLong(), eq("orderKey"), any(), any(), eq("orders-raw-topic"), eq(-1L), eq("Test exception"), eq(0));
    }

    @Test
    void shouldProcessTradeAndSendToKafka() {
        when(topicMock.getRawTrades()).thenReturn("trades-raw-topic");
        when(topicMock.getTradesClientEnriched()).thenReturn("trades-enriched-topic");

        RawTradeDto rawTrade = new RawTradeDto();
        TradeClientEnrichedDto enrichedTrade = new TradeClientEnrichedDto();
        enrichedTrade.setClientId("client-456");

        when(streamsBuilder.stream(eq("trades-raw-topic"), any(Consumed.class)))
                .thenReturn(tradesStreamMock);

        doAnswer(invocation -> {
            ForeachAction<String, RawTradeDto> action = invocation.getArgument(0);
            action.apply("tradeKey", rawTrade);
            return null;
        }).when(tradesStreamMock).foreach(any());

        when(tradeService.enrichTrade(rawTrade)).thenReturn(enrichedTrade);
        when(tradeKafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        incomingMessageTopology.buildTradesStream(streamsBuilder);

        verify(tradeService).enrichTrade(rawTrade);
        verify(tradeKafkaTemplate).send(
                ArgumentMatchers.<ProducerRecord<String, TradeClientEnrichedDto>>argThat(record ->
                        record.topic().equals("trades-enriched-topic")
                                && record.key().equals("client-456")
                                && record.value() == enrichedTrade
                )
        );
        verify(messageProducer, never()).sendToDlq(anyLong(), any(), any(), any(),
                any(), anyLong(), any(), anyInt());
    }

    @Test
    void shouldSendTradeToDlqOnException() {
        KafkaConfig.Topic topicMock = mock(KafkaConfig.Topic.class);
        when(kafkaConfig.getTopic()).thenReturn(topicMock);
        when(topicMock.getRawTrades()).thenReturn("trades-raw-topic");
        when(topicMock.getTradesClientEnriched()).thenReturn("trades-enriched-topic");

        RawTradeDto rawTrade = new RawTradeDto();

        when(streamsBuilder.stream(eq("trades-raw-topic"), any(Consumed.class))).thenReturn(tradesStreamMock);

        doAnswer(invocation -> {
            ForeachAction<String, RawTradeDto> action = invocation.getArgument(0);
            action.apply("tradeKey", rawTrade);
            return null;
        }).when(tradesStreamMock).foreach(any());

        when(tradeService.enrichTrade(rawTrade)).thenThrow(new RuntimeException("Test exception"));

        incomingMessageTopology.buildTradesStream(streamsBuilder);

        verify(tradeKafkaTemplate, never()).send(any(ProducerRecord.class));
        verify(messageProducer).sendToDlq(anyLong(), eq("tradeKey"), any(), any(), eq("trades-raw-topic"), eq(-1L), eq("Test exception"), eq(0));
    }
}

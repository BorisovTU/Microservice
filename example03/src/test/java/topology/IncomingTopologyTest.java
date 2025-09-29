package topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.OrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.TradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.OrderEventEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEventEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.service.OrderService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.service.TradeService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.topology.IncomingTopology;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.util.EventFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IncomingTopologyTest {

    @Mock
    private KafkaConfig kafkaConfig;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private OrderService orderService;
    @Mock
    private TradeService tradeService;
    @Mock
    private EventFactory eventFactory;

    private IncomingTopology topology;

    @BeforeEach
    void setUp() {
        topology = new IncomingTopology(kafkaConfig, objectMapper, orderService, tradeService, eventFactory);

        KafkaConfig.Topic topic = new KafkaConfig.Topic();
        topic.setOrdersClientEnriched("fx.orders.client.enriched");
        topic.setTradesClientEnriched("fx.trades.client.enriched");
        when(kafkaConfig.getTopic()).thenReturn(topic);
    }

    @Test
    void ordersStream_shouldCallOrderService() throws Exception {

        OrderDto dto = new OrderDto(
                "code1",
                "extCode1",
                LocalDateTime.now(),
                100L,
                200L,
                "BUY",
                300L,
                new BigDecimal("1000.50"),
                new BigDecimal("75.25"),
                "LIMIT",
                301L,
                1,
                2,
                10L,
                1,
                LocalDateTime.now()
        );

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        String jsonValue = mapper.writeValueAsString(dto);
        OrderEventEntity event = new OrderEventEntity();

        when(objectMapper.readValue(jsonValue, OrderDto.class)).thenReturn(dto);
        when(eventFactory.createOrderEvent(anyString(), eq(jsonValue))).thenReturn(event);

        StreamsBuilder builder = mock(StreamsBuilder.class);
        @SuppressWarnings("unchecked")
        KStream<String, String> stream = (KStream<String, String>) mock(KStream.class);
        when(builder.stream(eq("fx.orders.client.enriched"), any(Consumed.class))).thenReturn(stream);

        KStream<String, String> result = topology.ordersStream(builder);

        assertThat(result).isEqualTo(stream);

        ArgumentCaptor<ForeachAction<String, String>> captor = ArgumentCaptor.forClass(ForeachAction.class);
        verify(stream).foreach(captor.capture());

        captor.getValue().apply("key1", jsonValue);

        verify(orderService).processOrder(dto, event);
    }

    @Test
    void tradesStream_shouldCallTradeService() throws Exception {

        TradeDto dto = new TradeDto(
                1,
                "code1",
                "extCode1",
                "BUY",
                LocalDateTime.now(),
                100L,
                200L,
                300L,
                new BigDecimal("1000.50"),
                new BigDecimal("75.25"),
                LocalDateTime.now(),
                2,
                10L,
                1L,
                1,
                List.of()
        );
        String jsonValue = "test-json";
        TradeEventEntity event = new TradeEventEntity();

        when(objectMapper.readValue(jsonValue, TradeDto.class)).thenReturn(dto);
        when(eventFactory.createTradeEvent(anyString(), eq(jsonValue))).thenReturn(event);

        StreamsBuilder builder = mock(StreamsBuilder.class);
        @SuppressWarnings("unchecked")
        KStream<String, String> stream = mock(KStream.class);
        when(builder.stream(eq("fx.trades.client.enriched"), any(Consumed.class))).thenReturn(stream);

        KStream<String, String> result = topology.tradesStream(builder);

        assertThat(result).isEqualTo(stream);

        ArgumentCaptor<ForeachAction<String, String>> captor = ArgumentCaptor.forClass(ForeachAction.class);
        verify(stream).foreach(captor.capture());

        captor.getValue().apply("key1", jsonValue);

        verify(tradeService).processTrade(dto, event);
    }

    @Test
    void tradesStream_shouldHandleExceptionGracefully() throws Exception {

        String jsonValue = "test-json";
        StreamsBuilder builder = mock(StreamsBuilder.class);
        @SuppressWarnings("unchecked")
        KStream<String, String> stream = mock(KStream.class);
        when(builder.stream(eq("fx.trades.client.enriched"), any(Consumed.class))).thenReturn(stream);

        when(objectMapper.readValue(jsonValue, TradeDto.class))
                .thenThrow(new RuntimeException("Deserialization error"));

        KStream<String, String> result = topology.tradesStream(builder);

        assertThat(result).isEqualTo(stream);

        ArgumentCaptor<ForeachAction<String, String>> captor = ArgumentCaptor.forClass(ForeachAction.class);
        verify(stream).foreach(captor.capture());

        assertDoesNotThrow(() -> captor.getValue().apply("key1", jsonValue));

        verifyNoInteractions(tradeService);
    }

    @Test
    void ordersStream_shouldHandleExceptionGracefully() throws Exception {

        String jsonValue = "test-json";
        StreamsBuilder builder = mock(StreamsBuilder.class);
        @SuppressWarnings("unchecked")
        KStream<String, String> stream = mock(KStream.class);
        when(builder.stream(eq("fx.orders.client.enriched"), any(Consumed.class))).thenReturn(stream);

        when(objectMapper.readValue(jsonValue, OrderDto.class))
                .thenThrow(new RuntimeException("Deserialization error"));

        KStream<String, String> result = topology.ordersStream(builder);

        assertThat(result).isEqualTo(stream);

        ArgumentCaptor<ForeachAction<String, String>> captor = ArgumentCaptor.forClass(ForeachAction.class);
        verify(stream).foreach(captor.capture());

        assertDoesNotThrow(() -> captor.getValue().apply("key1", jsonValue));

        verifyNoInteractions(orderService);
    }


}

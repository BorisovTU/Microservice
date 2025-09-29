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
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.OrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.TradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service.OrderService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service.TradeService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.topology.IncomingMessageTopology;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IncomingMessageTopologyTest {

    @Mock
    private KafkaConfig kafkaConfig;
    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private OrderService orderService;
    @Mock
    private TradeService tradeService;

    private IncomingMessageTopology topology;

    @BeforeEach
    void setUp() {
        topology = new IncomingMessageTopology(kafkaConfig, objectMapper, orderService, tradeService);

        KafkaConfig.Topic topic = new KafkaConfig.Topic();
        topic.setOrdersClient("fx.orders.client.test");
        topic.setTradesClient("fx.trades.client.test");
        topic.setDlq("fx.dlq.test");
        when(kafkaConfig.getTopic()).thenReturn(topic);
    }

    @Test
    void ordersStream_shouldCallOrderService() throws Exception {

        OrderDto dto = new OrderDto(
                "code1",
                "ext1",
                LocalDateTime.now(),
                1L,
                1L,
                "BUY",
                1L,
                BigDecimal.valueOf(100),
                BigDecimal.valueOf(10.5),
                "LIMIT",
                2L,
                "NEW",
                1,
                1,
                LocalDateTime.now()
        );

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        String jsonValue = mapper.writeValueAsString(dto);

        when(objectMapper.readValue(jsonValue, OrderDto.class)).thenReturn(dto);

        StreamsBuilder builder = mock(StreamsBuilder.class);
        @SuppressWarnings("unchecked")
        KStream<String, String> stream = mock(KStream.class);
        when(builder.stream(eq("fx.orders.client.test"), any(Consumed.class))).thenReturn(stream);

        KStream<String, String> result = topology.ordersStream(builder);
        assertThat(result).isEqualTo(stream);

        ArgumentCaptor<ForeachAction<String, String>> captor = ArgumentCaptor.forClass(ForeachAction.class);
        verify(stream).foreach(captor.capture());

        String key = "key1";
        captor.getValue().apply(key, jsonValue);

        verify(orderService).saveOrder(dto, "fx.orders.client.test", key, jsonValue);
    }


    @Test
    void tradesStream_shouldCallTradeService() throws Exception {
        TradeDto dto = new TradeDto(
                1,
                "code1",
                "ext1",
                "BUY",
                LocalDateTime.now(),
                100L,
                200L,
                300L,
                BigDecimal.valueOf(1000.50),
                BigDecimal.valueOf(75.25),
                LocalDateTime.now(),
                2,
                10L,
                1L,
                1,
                List.of(),
                "NEW"
        );

        when(objectMapper.readValue(anyString(), eq(TradeDto.class))).thenReturn(dto);

        StreamsBuilder builder = mock(StreamsBuilder.class);
        @SuppressWarnings("unchecked")
        KStream<String, String> stream = mock(KStream.class);
        when(builder.stream(eq("fx.trades.client.test"), any(Consumed.class))).thenReturn(stream);

        KStream<String, String> result = topology.tradesStream(builder);
        assertThat(result).isEqualTo(stream);

        ArgumentCaptor<ForeachAction<String, String>> captor = ArgumentCaptor.forClass(ForeachAction.class);
        verify(stream).foreach(captor.capture());

        String key = "key1";
        String jsonValue = "{\"any\":\"value\"}";
        captor.getValue().apply(key, jsonValue);

        verify(tradeService).saveTradeWithCommissions(dto, "fx.trades.client.test", key, jsonValue);
    }

}

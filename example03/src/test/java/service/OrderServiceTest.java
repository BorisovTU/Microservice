package service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.OrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.OrderEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.OrderEventEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.mapper.OrderMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.OrderEventRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.OrderRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.service.OrderService;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderEventRepository orderEventRepository;

    @Mock
    private OrderMapper orderMapper;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private OrderService orderService;

    private OrderDto sampleDto;
    private OrderEventEntity sampleEvent;


    @BeforeEach
    void setUp() {
        sampleDto = new OrderDto(
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

        sampleEvent = new OrderEventEntity();
    }

    @Test
    void shouldInsertNewOrder() throws Exception {
        when(orderRepository.findByExternalCodeAndExchangeId(sampleDto.externalCode(), sampleDto.exchangeId()))
                .thenReturn(Optional.empty());

        OrderEntity mappedEntity = mock(OrderEntity.class);
        when(orderMapper.toEntity(sampleDto)).thenReturn(mappedEntity);
        when(orderRepository.save(mappedEntity)).thenReturn(mappedEntity);

        KafkaConfig.Topic topicConfig = new KafkaConfig.Topic();
        topicConfig.setOrdersClients("fx.orders.client");
        lenient().when(kafkaConfig.getTopic()).thenReturn(topicConfig);

        when(objectMapper.writeValueAsString(sampleDto)).thenReturn("{\"code\":\"extCode1\"}");

        orderService.processOrder(sampleDto, sampleEvent);

        verify(orderMapper).toEntity(sampleDto);
        verify(orderRepository).save(mappedEntity);
        verify(orderEventRepository).save(sampleEvent);
        verify(kafkaTemplate).send(eq("fx.orders.client"), eq(sampleDto.clientId().toString()), anyString());

        assertEquals("processed", sampleEvent.getStatus());
    }

    @Test
    void shouldUpdateExistingOrderIfLastUpdateTimeNewer() throws Exception {
        OrderEntity existingOrder = new OrderEntity();
        existingOrder.setLastUpdateTime(sampleDto.lastUpdateTime().minusMinutes(5));

        when(orderRepository.findByExternalCodeAndExchangeId(sampleDto.externalCode(), sampleDto.exchangeId()))
                .thenReturn(Optional.of(existingOrder));

        doAnswer(invocation -> {
            OrderDto dto = invocation.getArgument(0);
            OrderEntity entity = invocation.getArgument(1);
            entity.setCode(dto.code());
            return null;
        }).when(orderMapper).updateEntityFromDto(sampleDto, existingOrder);

        KafkaConfig.Topic topicConfig = new KafkaConfig.Topic();
        topicConfig.setOrdersClients("fx.orders.client");
        lenient().when(kafkaConfig.getTopic()).thenReturn(topicConfig);

        when(orderRepository.save(existingOrder)).thenReturn(existingOrder);
        when(objectMapper.writeValueAsString(sampleDto)).thenReturn("{\"code\":\"extCode1\"}");

        orderService.processOrder(sampleDto, sampleEvent);

        verify(orderMapper).updateEntityFromDto(sampleDto, existingOrder);
        verify(orderRepository).save(existingOrder);
        verify(orderEventRepository).save(sampleEvent);
        verify(kafkaTemplate).send(eq("fx.orders.client"), eq(sampleDto.clientId().toString()), anyString());

        assertEquals("processed", sampleEvent.getStatus());
    }

    @Test
    void shouldSendToDlqWhenProcessingFails() throws Exception {

        when(orderRepository.findByExternalCodeAndExchangeId(sampleDto.externalCode(), sampleDto.exchangeId()))
                .thenReturn(Optional.empty());

        when(orderRepository.save(any(OrderEntity.class)))
                .thenThrow(new DataAccessException("Error DB") {});

        KafkaConfig.Topic topicConfig = new KafkaConfig.Topic();
        topicConfig.setOrdersClients("fx.orders.client");
        topicConfig.setDlq("orders-dlq");
        lenient().when(kafkaConfig.getTopic()).thenReturn(topicConfig);

        when(objectMapper.writeValueAsString(sampleDto)).thenReturn("{\"code\": \"extCode1\", \"externalCode\": \"extCode1\"}");

        orderService.processOrder(sampleDto, sampleEvent);

        assertEquals("failed", sampleEvent.getStatus());
        verify(orderEventRepository).save(sampleEvent);

        verify(kafkaTemplate).send(eq("orders-dlq"), eq(sampleDto.clientId().toString()), anyString());
    }
}

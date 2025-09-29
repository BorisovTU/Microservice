package service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao.OrderDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.OrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service.DlqService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service.OrderService;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class OrderServiceTest {

    @Mock
    private OrderDao orderDao;
    @Mock
    private DlqService dlqService;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        orderService = new OrderService(orderDao, dlqService);
    }

    @Test
    void saveOrder_success() {
        OrderDto orderDto = new OrderDto(
                "code1",
                "extCode1",
                LocalDateTime.now(),
                100L,
                200L,
                "BUY",
                300L,
                BigDecimal.valueOf(50),
                BigDecimal.valueOf(500),
                "LIMIT",
                400L,
                "NEW",
                1,
                1,
                LocalDateTime.now()
        );

        orderService.saveOrder(orderDto, "fx.orders.client", "key1", "{\"json\":\"value\"}");

        ArgumentCaptor<OrderDto> captor = ArgumentCaptor.forClass(OrderDto.class);
        verify(orderDao, times(1)).saveOrder(captor.capture());
        assertEquals(orderDto.externalCode(), captor.getValue().externalCode());

        verify(dlqService, never()).sendToDlq(anyString(), anyString(), anyString(), any());
    }

    @Test
    void saveOrder_failure_sendsToDlq() {
        OrderDto orderDto = new OrderDto(
                "code2",
                "extCode2",
                LocalDateTime.now(),
                101L,
                201L,
                "SELL",
                301L,
                BigDecimal.valueOf(75),
                BigDecimal.valueOf(750),
                "MARKET",
                401L,
                "FAILED",
                2,
                2,
                LocalDateTime.now()
        );

        RuntimeException exception = new RuntimeException("DB error");

        doThrow(exception).when(orderDao).saveOrder(any(OrderDto.class));

        try {
            orderService.saveOrder(orderDto, "fx.orders.client", "key2", "{\"json\":\"value2\"}");
        } catch (RuntimeException ignored) {

        }

        verify(dlqService, times(1))
                .sendToDlq(eq("fx.orders.client"), eq("key2"), eq("{\"json\":\"value2\"}"), eq(exception));
    }

}

package service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dao.TradeDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto.TradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service.DlqService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service.TradeService;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TradeServiceTest {

    @Mock
    private TradeDao tradeDao;
    @Mock
    private DlqService dlqService;

    private TradeService tradeService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        tradeService = new TradeService(tradeDao, dlqService);
    }

    @Test
    void saveTradeWithCommissions_success() {
        TradeDto tradeDto = new TradeDto(
                1, "code1", "extCode1", "SELL", LocalDateTime.now(),
                100L, 200L, 300L, BigDecimal.valueOf(50), BigDecimal.valueOf(500),
                LocalDateTime.now(), 10, 20L, 30L, 1, List.of(), "TEST"
        );

        tradeService.saveTradeWithCommissions(tradeDto, "fx.trades.client", "key1", "{\"json\":\"value\"}");

        ArgumentCaptor<TradeDto> captor = ArgumentCaptor.forClass(TradeDto.class);
        verify(tradeDao, times(1)).saveTrade(captor.capture());

        assertEquals(tradeDto.externalCode(), captor.getValue().externalCode());
        verify(dlqService, never()).sendToDlq(anyString(), anyString(), anyString(), any());
    }

    @Test
    void saveTradeWithCommissions_failure_sendsToDlq() {
        TradeDto tradeDto = new TradeDto(
                1, "code2", "extCode2", "BUY", LocalDateTime.now(),
                101L, 201L, 301L, BigDecimal.valueOf(75), BigDecimal.valueOf(750),
                LocalDateTime.now(), 11, 21L, 31L, 1, List.of(), "TEST"
        );

        RuntimeException exception = new RuntimeException("DB error");

        doThrow(exception).when(tradeDao).saveTrade(any(TradeDto.class));

        try {
            tradeService.saveTradeWithCommissions(tradeDto, "fx.trades.client", "key2", "{\"json\":\"value2\"}");
        } catch (RuntimeException ignored) {

        }
        verify(dlqService, times(1))
                .sendToDlq(eq("fx.trades.client"), eq("key2"), eq("{\"json\":\"value2\"}"), eq(exception));
    }
}

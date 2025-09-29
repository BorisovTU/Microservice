package service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.TradeCommissionDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto.TradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeCommissionEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.entity.TradeEventEntity;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.mapper.TradeMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.TradeCommissionsRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.TradeEventRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.repository.TradeRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.service.TradeService;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TradeServiceTest {

    @Mock
    private TradeRepository tradeRepository;

    @Mock
    private TradeCommissionsRepository tradeCommissionRepository;

    @Mock
    private TradeEventRepository tradeEventRepository;

    @Mock
    private TradeMapper tradeMapper;

    @Mock
    private KafkaConfig kafkaConfig;

    @Mock
    private KafkaConfig.Topic topic;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    private TradeService tradeService;

    @BeforeEach
    public void setup() {
        tradeService = new TradeService(tradeRepository,
                tradeCommissionRepository,
                tradeEventRepository,
                tradeMapper,
                kafkaConfig,
                kafkaTemplate,
                objectMapper);
    }

    @Test
    public void testCreateNewTrade() throws Exception {

        TradeDto dto = new TradeDto(
                1,
                "CODE",
                "EXTERNAL_CODE",
                "BUY",
                LocalDateTime.now(),
                1L,
                1L,
                1L,
                new BigDecimal("100"),
                new BigDecimal("50"),
                LocalDateTime.now(),
                1,
                1L,
                1L,
                1,
                Arrays.asList(
                        new TradeCommissionDto(1, new BigDecimal(22), new BigDecimal("10"))
                )
        );

        TradeEventEntity event = new TradeEventEntity();
        event.setStatus("NEW");

        when(tradeRepository.findByExternalCodeAndDirection(anyString(), anyString()))
                .thenReturn(Optional.empty());

        TradeEntity entity = new TradeEntity();
        entity.setId(1L);
        when(tradeMapper.toEntity(dto)).thenReturn(entity);
        when(tradeRepository.save(entity)).thenReturn(entity);

        List<TradeCommissionEntity> commissions = Arrays.asList(new TradeCommissionEntity());
        when(tradeMapper.toCommissions(dto, entity)).thenReturn(commissions);
        when(tradeCommissionRepository.saveAll(commissions)).thenReturn(commissions);

        KafkaConfig.Topic topic = new KafkaConfig.Topic();
        topic.setTradesClients("trades-clients");

        when(kafkaConfig.getTopic()).thenReturn(topic);

        when(objectMapper.writeValueAsString(dto)).thenReturn("SerializedDTO");

        tradeService.processTrade(dto, event);

        verify(tradeRepository).findByExternalCodeAndDirection(eq("EXTERNAL_CODE"), eq("BUY"));
        verify(tradeRepository).save(entity);
        verify(tradeCommissionRepository).saveAll(commissions);
        assertEquals("processed", event.getStatus());
        verify(kafkaTemplate).send("trades-clients", "1", "SerializedDTO");
    }

    @Test
    public void testUpdateExistingTrade() throws Exception {

        TradeDto dto = new TradeDto(
                1,
                "CODE",
                "EXTERNAL_CODE",
                "SELL",
                LocalDateTime.now(),
                1L,
                1L,
                1L,
                new BigDecimal("100"),
                new BigDecimal("50"),
                LocalDateTime.now(),
                1,
                1L,
                1L,
                1,
                Arrays.asList(
                        new TradeCommissionDto(1, new BigDecimal(22), new BigDecimal("10"))
                )
        );

        TradeEventEntity event = new TradeEventEntity();
        event.setStatus("NEW");

        TradeEntity existingTrade = new TradeEntity();
        existingTrade.setId(1L);
        when(tradeRepository.findByExternalCodeAndDirection(anyString(), anyString()))
                .thenReturn(Optional.of(existingTrade));

        doNothing().when(tradeMapper).updateEntityFromDto(any(), any());

        List<TradeCommissionEntity> commissions = Arrays.asList(new TradeCommissionEntity());
        when(tradeMapper.toCommissions(dto, existingTrade)).thenReturn(commissions);
        when(tradeCommissionRepository.saveAll(commissions)).thenReturn(commissions);

        KafkaConfig.Topic topic = new KafkaConfig.Topic();
        topic.setTradesClients("trades-clients");
        when(kafkaConfig.getTopic()).thenReturn(topic);

        when(objectMapper.writeValueAsString(dto)).thenReturn("SerializedDTO");

        tradeService.processTrade(dto, event);

        verify(tradeRepository).findByExternalCodeAndDirection(eq("EXTERNAL_CODE"), eq("SELL"));
        verify(tradeMapper).updateEntityFromDto(dto, existingTrade);
        verify(tradeCommissionRepository).deleteByTrade(existingTrade);
        verify(tradeCommissionRepository).saveAll(commissions);
        assertEquals("processed", event.getStatus());
        verify(kafkaTemplate).send("trades-clients", "1", "SerializedDTO");
    }

    @Test
    public void testFailedProcessing() throws Exception {

        TradeDto dto = new TradeDto(
                1,
                "CODE",
                "EXTERNAL_CODE",
                "BUY",
                LocalDateTime.now(),
                1L,
                1L,
                1L,
                new BigDecimal("100"),
                new BigDecimal("50"),
                LocalDateTime.now(),
                1,
                1L,
                1L,
                1,
                null
        );

        TradeEventEntity event = new TradeEventEntity();
        event.setStatus("NEW");

        when(tradeRepository.findByExternalCodeAndDirection(anyString(), anyString()))
                .thenReturn(Optional.empty());

        TradeEntity entity = new TradeEntity();
        entity.setId(1L);
        when(tradeMapper.toEntity(dto)).thenReturn(entity);
        when(tradeRepository.save(entity)).thenThrow(new DataIntegrityViolationException("Error saving data"));

        KafkaConfig.Topic topic = new KafkaConfig.Topic();
        topic.setTradesClients("trades-clients-enriched");
        topic.setDlq("dlq-topic");
        when(kafkaConfig.getTopic()).thenReturn(topic);

        when(objectMapper.writeValueAsString(dto)).thenReturn("SerializedDTO");

        doAnswer((invocation) -> {
            String topicName = invocation.getArgument(0);
            String key = invocation.getArgument(1);
            String value = invocation.getArgument(2);
            System.out.println("Отправлено в DLQ: " + topicName + ", ключ=" + key + ", значение=" + value);
            return null;
        }).when(kafkaTemplate).send(anyString(), anyString(), anyString());

        tradeService.processTrade(dto, event);

        assertEquals("failed", event.getStatus());
        verify(tradeEventRepository).save(event);

        verify(kafkaTemplate).send("dlq-topic", "1", "SerializedDTO");
    }
}

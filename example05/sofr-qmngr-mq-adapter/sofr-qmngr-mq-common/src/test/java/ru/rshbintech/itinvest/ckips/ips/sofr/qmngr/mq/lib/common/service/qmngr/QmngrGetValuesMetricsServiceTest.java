package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao.QmngrDao;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrGetValuesMetricsCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.QmngrGetValuesMetricsAuditService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QmngrGetValuesMetricsServiceTest {

    @Mock
    private QmngrDao qmngrDao;

    @Mock
    private QmngrGetValuesMetricsAuditService auditService;

    @InjectMocks
    private QmngrGetValuesMetricsService metricsService;

    @Test
    void testGetValuesMetricsSuccess() {
        // Задаем поведение мока для метода
        doNothing().when(qmngrDao).callGetValuesMetrics(any(QmngrGetValuesMetricsCall.class));
        // Вызов тестируемого метода
        metricsService.getValuesMetrics();
        // Проверка, что методы взаимодействия с зависимостями были вызваны
        ArgumentCaptor<QmngrGetValuesMetricsCall> captor = ArgumentCaptor.forClass(QmngrGetValuesMetricsCall.class);
        verify(qmngrDao, times(1)).callGetValuesMetrics(captor.capture());
        // Проверка, что объект QmngrGetValuesMetricsCall был создан и передан в метод
        QmngrGetValuesMetricsCall capturedCall = captor.getValue();
        assertNotNull(capturedCall);

        // Проверка, что метод логирования был вызван
        verify(auditService, times(1)).logGetValuesMetricsCall(capturedCall);
        verify(auditService, times(1)).auditGetValuesMetricsCallSuccessCompletion(capturedCall);
    }

    @Test
    void testGetValuesMetricsFailure() {
        // Пример ситуации, когда метод кидает исключение
        doThrow(new RuntimeException("Database error")).when(qmngrDao).callGetValuesMetrics(any(QmngrGetValuesMetricsCall.class));
        // Вызов тестируемого метода, который должен подавить исключение
        metricsService.getValuesMetrics();
        // Проверка, что методы взаимодействия с зависимостями были вызваны
        verify(qmngrDao, times(1)).callGetValuesMetrics(any(QmngrGetValuesMetricsCall.class));

        // Проверка логирования ошибки
        ArgumentCaptor<QmngrGetValuesMetricsCall> captor = ArgumentCaptor.forClass(QmngrGetValuesMetricsCall.class);
        verify(auditService, times(1)).logGetValuesMetricsCall(captor.capture());
        verify(auditService, times(1)).
                auditGetValuesMetricsCallError(any(QmngrGetValuesMetricsCall.class), anyString());
    }




}
package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.configuration.properties.PlatformKafkaProducersProperties;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public final class PlatformDynamicKafkaTemplateServiceTest {


    @Mock
    private PlatformKafkaProducersProperties platformKafkaProducersProperties;

    @InjectMocks
    private PlatformDynamicKafkaTemplateService service;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        // Создаем mock DefaultKafkaProducerFactory c пустыми свойствами
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(new HashMap<>());

        // Создаем KafkaTemplate с мокированным producerFactory
        KafkaTemplate<String, String> expectedKafkaTemplate = new KafkaTemplate<>(producerFactory);

        // Инициализируем KafkaTemplateMap с тестовыми данными через рефлексию
        Map<String, KafkaTemplate<String, String>> kafkaTemplateMap = new ConcurrentHashMap<>();
        kafkaTemplateMap.put("testClusterId", expectedKafkaTemplate);

        // Получаем доступ к приватному полю kafkaTemplateMap и устанавливаем тестовые данные
        Field kafkaTemplateMapField = PlatformDynamicKafkaTemplateService.class.getDeclaredField("kafkaTemplateMap");
        kafkaTemplateMapField.setAccessible(true);
        kafkaTemplateMapField.set(service, kafkaTemplateMap);
    }

    /**
     * Тест корректности работы метода getKafkaTemplateByTopic.
     */
    @Test
    void testGetKafkaTemplateByTopic() {
        // Задаем тестовые данные
        String testTopic = "testTopic";
        String kafkaClusterId = "testClusterId";

        // Создаем объект PlatformQmngrReadMsgProducerProperties и задаем значения через сеттеры
        PlatformKafkaProducersProperties.PlatformQmngrReadMsgProducerProperties producerProperties =
                new PlatformKafkaProducersProperties.PlatformQmngrReadMsgProducerProperties();
        producerProperties.setTopic(testTopic);
        producerProperties.setKafkaClusterId(kafkaClusterId);

        // Мокаем поведение метода getPlatform() у platformKafkaProducersProperties
        when(platformKafkaProducersProperties.getPlatform()).thenReturn(List.of(producerProperties));

        // Выполняем тестируемый метод
        KafkaTemplate<String, String> actualKafkaTemplate = service.getKafkaTemplateByTopic(testTopic);

        // Проверяем результат
        assertEquals(service.getKafkaTemplateByTopic(testTopic), actualKafkaTemplate);
    }

}

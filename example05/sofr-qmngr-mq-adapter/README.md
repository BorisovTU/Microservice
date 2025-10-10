# Интеграционный сервис для взаимодействия между различными брокерами сообщений и SOFR QManager

[[_TOC_]]

## 1. Общая информация

Интеграционный сервис для взаимодействия между различными брокерами сообщений и SOFR QManager.
Основная функциональность: загрузка данных из брокеров сообщений в SOFR QManager/выгрузка данных из SOFR QManager в
брокеры сообщений.

Ссылки:

- [Требования](https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=237127378)

## 2. Описание модулей сервиса

На данный момент сервис имеет многомодульную структуру. Так реализовано для того, чтобы соблюсти требование к его
универсальности (на будущее), а именно, иметь возможность быстро добавить другие брокеры сообщений или изменить
текущую реализацию с целью использования другого брокера сообщений, при этом, избежать влияния на другие модули.

В дальнейшем, если логика разрастаться не будет, можно отказаться от многомодульности и использовать пакетную структуру.

Описание модулей:

| **Наименование модуля**   | **Описание**                                                            |
|---------------------------|-------------------------------------------------------------------------|
| **kafka-common**          | Модуль с библиотечным функционалом для Kafka                            |
| **kafka-sofr-qmngr**      | Модуль с функционалом для обмена в направлении из Kafka в SOFR QManager |
| **sofr-qmngr-kafka**      | Модуль с функционалом для обмена в направлении из SOFR QManager в Kafka |
| **sofr-qmngr-mq-common**  | Модуль с общим функционалом для обмена между MQ и SOFR QManager         |
| **sofr-qmngr-mq-service** | Модуль с микросервисом для обмена между MQ и SOFR QManager              |
| **sofr-metric**           | Модуль для получения метрик из SOFR по расписанию                       |

## 3. Конфигурационные параметры сервиса

### 3.1. Конфигурационные параметры логирования и аудита

Для конфигурации сервиса в части логирования и аудита в разрезе сред требуется использовать следующие параметры:

| **Наименование параметра**                          | **Брокер сообщений** | **Направление**      | **Описание**                                                                                                                                                              | **Обязательность** | **Значение по умолчанию** |
|-----------------------------------------------------|----------------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------------------|
| **app.monitoring.param.message.logging.enabled**    | все брокеры          | для всех направлений | Включено ли логирование тела сообщения                                                                                                                                    | -                  | true                      |
| **app.monitoring.param.message.audit.enabled**      | все брокеры          | для всех направлений | Включено ли тело сообщения в параметры сообщений аудита                                                                                                                   | -                  | false                     |
| **app.monitoring.param.message.logging.max-length** | все брокеры          | для всех направлений | Максимальная длина параметра **message** в логах микросервиса. Сообщения, длина которых превышает это значение, будут обрезаны до указанной максимальной длины            | -                  | 500                       |
| **app.monitoring.param.message.audit.max-length**   | все брокеры          | для всех направлений | Максимальная длина параметра **message** в событиях аудита. Сообщения, длина которых превышает это значение, будут обрезаны до указанной максимальной длины               | -                  | 500                       |
| **app.monitoring.param.headers.logging.enabled**    | все брокеры          | для всех направлений | Включено ли логирование заголовков сообщения                                                                                                                              | -                  | true                      |
| **app.monitoring.param.headers.audit.enabled**      | все брокеры          | для всех направлений | Включены ли заголовки сообщения в параметры сообщений аудита                                                                                                              | -                  | false                     |
| **app.monitoring.param.headers.logging.max-length** | все брокеры          | для всех направлений | Максимальная длина параметра **headers** в логах микросервиса. JSON'ы с заголовками, длина которых превышает это значение, будут обрезаны до указанной максимальной длины | -                  | 500                       |
| **app.monitoring.param.headers.audit.max-length**   | все брокеры          | для всех направлений | Максимальная длина параметра **headers** в событиях аудита. JSON'ы с заголовками, длина которых превышает это значение, будут обрезаны до указанной максимальной длины    | -                  | 500                       |
| **app.monitoring.db.connection.logging.enabled**    | все брокеры          | для всех направлений | Требуется ли логировать параметры подключения к БД (вводится для тестовых сред, так как там часто происходит переключение БД), на продуктиве включать не рекомендуется    | -                  | false                     |
| **audit.enabled**                                   | все брокеры          | для всех направлений | Включена ли отправка событий аудита                                                                                                                                       | -                  | true                      |

Пример:

```yaml
app:
  monitoring:
    db:
      connection:
        logging:
          enabled: true
    param:
      headers:
        logging:
          enabled: true
          max-length: 500
        audit:
          enabled: true
          max-length: 500
      message:
        logging:
          enabled: true
          max-length: 500
        audit:
          enabled: true
          max-length: 500
audit:
  enabled: true
```

Данные параметры настраиваются в файле **deploy/envs/<название_среды>/values.yaml** в параметре **env** отдельно
для каждой среды через
[config.yaml](https://platform-user-manual.rshbdev.ru/#/functionality/environment/application/config), например:

```yaml
config:
  app:
    monitoring:
      param:
        headers:
          logging:
            enabled: true
            max-length: 500
          audit:
            enabled: true
            max-length: 500
        message:
          logging:
            enabled: true
            max-length: 500
          audit:
            enabled: true
            max-length: 500
  audit:
    enabled: true
```

### 3.2. Конфигурационные параметры брокеров сообщений

При использовании библиотеки **spring-kafka** в контексте приложения автоматически поднимается bean 
_ConcurrentKafkaListenerContainerFactory_ который будет использован в качестве фабрики для создания потребителей
Kafka (consumer).
Для конфигурации _ConcurrentKafkaListenerContainerFactory_ используются стандартные настройки из блока настроек
**spring.kafka**, например, как в [application.yml](sofr-qmngr-mq-service/src/main/resources/application.yml):

```yaml
spring:
  kafka:
    bootstrap-servers: ${APP_KAFKA_BOOTSTRAP_SERVERS_IPS}
    security:
      protocol: ${KAFKA_SSL_PROTOCOL:SASL_SSL}
    properties:
      sasl:
        mechanism: SCRAM-SHA-512
        jaas:
          config: |
            org.apache.kafka.common.security.scram.ScramLoginModule
            required username='${APP_KAFKA_USER}' password='${APP_KAFKA_PASSWORD}';
    ssl:
      trust-store-location: ${KAFKA_TRUSTSTORE_LOCATION}
      trust-store-password: ${KAFKA_TRUSTSTORE_PASSWORD}
    consumer:
      group-id: ${APP_KAFKA_CONSUMER_GROUP_ID}
      auto-offset-reset: latest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    producer:
      retries: ${KAFKA_PRODUCER_RETRIES:3}
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: ${KAFKA_PRODUCER_ACKS:all}
      properties:
        max.block.ms: ${KAFKA_PRODUCER_TIMEOUTS_MS:3000}
```

Стандартный _ConcurrentKafkaListenerContainerFactory_ используется для конфигурации подключения к кластерам Kafka _IPS_,
см. [KafkaMessageListenersConfiguration.java](kafka-sofr-qmngr/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/to/qmngr/kafka/configuration/KafkaMessageListenersConfiguration.java)
метод _getContainerFactory_:

```java
case IPS -> kafkaListenerContainerFactory;
```

Помимо кластеров _IPS_ Kafka адаптер должен уметь работать с кластерами платформенной Kafka.
Для этого вводится новый блок настроек **app.kafka.platform** в виде карты (см.
[AppKafkaProperties.java](kafka-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/kafka/common/configuration/properties/AppKafkaProperties.java)
), где в качестве ключа указывается
идентификатор кластера Kafka, а в виде значения - настройки подключения к Kafka в стандартном виде 
_org.springframework.boot.autoconfigure.kafka.KafkaProperties_, например:

```yaml
app:
  kafka:
    platform:
      maks_maks-kafka-main:
        bootstrap-servers: ${APP_KAFKA_BOOTSTRAP_SERVERS_PLATFORM_MAKS_MAKS_KAFKA_MAIN}
        security:
          protocol: PLAINTEXT
        consumer:
          group-id: ${APP_KAFKA_CONSUMER_GROUP_ID}
          auto-offset-reset: latest
          enable-auto-commit: false
          key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
          value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
        producer:
          retries: ${KAFKA_PRODUCER_RETRIES:3}
          key-serializer: org.apache.kafka.common.serialization.StringSerializer
          value-serializer: org.apache.kafka.common.serialization.StringSerializer
          acks: ${KAFKA_PRODUCER_ACKS:all}
          properties:
            max.block.ms: ${KAFKA_PRODUCER_TIMEOUTS_MS:3000}
```

Здесь нужно обратить внимание на ключ **maks_maks-kafka-main**. Данный ключ будет являться уникальным идентификатором
кластера платформенной Kafka в рамках адаптера.

Рекомендуется задавать ключ в следующем формате: 
**<название_информационной_системы_в_App.Farm>_<kafka_cluster_id_в_App.Farm>**, например, если информационная система
называется **maks**, а кластер Kafka для данной информационной системы называется **maks-kafka-main**, то ключ будет
выглядеть следующим образом: **maks_maks-kafka-main**. Данный подход позволит создавать уникальные ключи для настроек
подключения к платформенной Kafka.

На основании карты данных настроек создается карта _ConcurrentKafkaListenerContainerFactory_ с помощью которой 
будут инициализироваться слушатели Kafka, см.
[KafkaMessageListenersConfiguration.java](kafka-sofr-qmngr/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/to/qmngr/kafka/configuration/KafkaMessageListenersConfiguration.java)
метод _getContainerFactory_:

```java
case PLATFORM -> {
    final String kafkaClusterId = loadMsgConsumerProperties.getKafkaClusterId();
    checkPlatformKafkaConfiguration(kafkaClusterId, loadMsgConsumerProperties);
    yield platformKafkaListenerContainerFactories.get(kafkaClusterId);
}
```
Также на основании карты данных настроек создается карта _ConcurrentHashMap_ с динамически созданными продюсерами Kafka (KafkaTemplate), см.
[PlatformDynamicKafkaTemplateService.java](sofr-qmngr-kafka/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/from/qmngr/kafka/service/PlatformDynamicKafkaTemplateService.java)

В итоге для топика, который должен читаться из платформенной Kafka, требуется установить настройки:

```yaml
app:
  broker-connectors:
    to:
      kafka:
        consumers:
          load-msg:
            - topic: maks.transfer-rates.universal-json
              use-key-as-msg-id: false
              kafka-type: PLATFORM
              kafka-cluster-id: maks_maks-kafka-main
```
Для топика платформенной Kafka, в который необходимо отправлять сообщение из SOFR QManager, требуется установить настройки:

```yaml
app:
  broker-connectors:
    from:
      kafka:
        producers:
          platform:
            - topic: maks.transfer-rates.universal-json
              kafka-cluster-id: maks_maks-kafka-main
```

Особое внимание требуется уделить настройкам:
- **app.broker-connectors.to.kafka.consumers.load-msg.kafka-type** - требуется указать _PLATFORM_, чтобы чтение топика
производилось именно из платформенной Kafka.
- **app.broker-connectors.to.kafka.consumers.load-msg.kafka-cluster-id** - требуется указать наименование кластера
Kafka из блока настроек **app.kafka.platform**.

Подробнее о данных параметрах можно узнать в п.3.3. документации.

### 3.3. Конфигурационные параметры коннекторов к брокерам сообщений

Для конфигурации коннекторов брокеров (в разрезе сред), требуется использовать следующие параметры:

| **Наименование параметра**                                                     | **Брокер сообщений** | **Направление**          | **Описание**                                                                                                                   | **Обязательность** | **Значение по умолчанию** |
|--------------------------------------------------------------------------------|----------------------|--------------------------|--------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------------------|
| **app.broker-connectors.to.kafka.consumers.load-msg.topic**                    | kafka                | Из Kafka в SOFR QManager | Топик Kafka для получения сообщений                                                                                            | +                  | Пустой List               |
| **app.broker-connectors.to.kafka.consumers.load-msg.use-key-as-msg-id**        | kafka                | Из Kafka в SOFR QManager | Использовать ли ключ сообщения Kafka в качестве msg id                                                                         | -                  | true                      |
| **app.broker-connectors.to.kafka.consumers.load-msg.msg-id-header-name**       | kafka                | Из Kafka в SOFR QManager | Название заголовка сообщения Kafka, значение которого можно использовать в качестве msg id                                     | -                  | null                      |
| **app.broker-connectors.to.kafka.consumers.load-msg.kafka-type**               | kafka                | Из Kafka в SOFR QManager | Типы кластеров Kafka. Может принимать значения: IPS (Кластеры Kafka IPS), PLATFORM (Кластеры платформенной Kafka).             | -                  | IPS                       |
| **app.broker-connectors.to.kafka.consumers.load-msg.kafka-cluster-id**         | kafka                | Из Kafka в SOFR QManager | Наименование кластера Kafka из настроек коннектора.                                                                            | -                  | null                      |
| **app.broker-connectors.from.kafka.consumers.read-msg-trigger.topic**          | kafka                | Из SOFR QManager в Kafka | Топик Kafka из которого должны потребляться сообщения триггера (периодическое задание) для выгрузки сообщений из SOFR Qmanager | +                  | null                      |
| **app.broker-connectors.from.kafka.consumers.read-msg-trigger.msg-read-limit** | kafka                | Из SOFR QManager в Kafka | Лимит сообщений, обрабатываемых за один запуск периодического задания                                                          | -                  | 100                       |

Пример:

```yaml
app:
  broker-connectors:
    from:
      kafka:
        consumers:
          read-msg-trigger:
            topic: ips.tss.sofr-qmngr-mq-adapter
    to:
      kafka:
        consumers:
          load-msg:
            - topic: ips.dias-sofr.ndfl
            - topic: quik.limits.NewInstMon
            - topic: diasoft.sofr.pko-info.req
            - topic: diasoft.sofr.pko-info-status-result.req
            - topic: maks.transfer-rates.universal-json
              use-key-as-msg-id: false
              kafka-type: PLATFORM
              kafka-cluster-id: maks_maks-kafka-main
            - topic: ips.efr-sofr.state-officer-sert
```

Данные параметры настраиваются в файле **deploy/envs/<название_среды>/values.yaml** в параметре **env** отдельно
для каждой среды через
[config.yaml](https://platform-user-manual.rshbdev.ru/#/functionality/environment/application/config), например:

```yaml
config:
  app:
    broker-connectors:
      from:
        kafka:
          consumers:
            read-msg-trigger:
              topic: ips.tss.sofr-qmngr-mq-adapter
      to:
        kafka:
          consumers:
            load-msg:
              - topic: ips.dias-sofr.ndfl
              - topic: quik.limits.NewInstMon
              - topic: diasoft.sofr.pko-info.req
              - topic: diasoft.sofr.pko-info-status-result.req
              - topic: maks.transfer-rates.universal-json
                use-key-as-msg-id: false
                kafka-type: PLATFORM
                kafka-cluster-id: maks_maks-kafka-main
              - topic: ips.efr-sofr.state-officer-sert
```

Следует обратить внимание на структуру параметров:

**app.broker-connectors.<направление обмена>.<название брокера сообщений>.<тип коннектора>.<наименование процесса>.<
название параметра>**

где:

- **направление обмена**: **from**/**to** (из SOFR Qmanager/в SOFR QManager);
- **название брокера сообщений**: например, **kafka**;
- **тип коннектора**: **consumers**/**producers**;
- **наименование процесса**: например, **load-msg** и т.д.;
- **название параметра**: наименование параметра конфигурации, например, **topic**.

При разработке новых интеграций (например, с другими брокерами сообщений) рекомендуется придерживаться данной структуры
параметров, например:

**app.broker-connectors.to.rabbit.consumers.load-msg.topic**

чтобы конфигурация была читабельной и наглядной.

## 4. Сохранение сообщений из брокеров в SOFR QManager

Сообщения, поступающие от брокеров, должны сохраняться в SOFR QManager посредством вызова хранимой процедуры
**IN_INTEGRATION.QMANAGER_LOAD_MSG** с параметрами:

| **Наименование параметра** | **Описание**                        | **Обязательность** | **Тип**   | **Направление** |
|----------------------------|-------------------------------------|--------------------|-----------|-----------------|
| **P_KAFKA_TOPIC**          | Наименование топика                 | +                  | VARCHAR   | IN              |
| **P_GUID**                 | Уникальный идентификатор сообщения  | -                  | VARCHAR   | IN              |
| **P_ESBDT**                | Дата поступления сообщения в брокер | -                  | TIMESTAMP | IN              |
| **PCL_HEADER**             | Заголовки сообщения                 | -                  | CLOB      | IN              |
| **PCL_MESSAGE**            | Тело сообщения                      | +                  | CLOB      | IN              |
| **O_ERRORCODE**            | Код ошибки                          | +                  | NUMBER    | OUT             |
| **O_ERRORDESC**            | Текст ошибки                        | -                  | VARCHAR   | OUT             |

Для этого реализован сервис
[QmngrLoadMsgService](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/service/qmngr/QmngrLoadMsgService.java).

Сервис является общим для всех брокеров сообщений. Для того чтобы подключить новый брокер сообщений,
нужно реализовать потребителя, а так же преобразовать полученное от него сообщение в
[QmngrLoadMsgDto](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/dto/QmngrLoadMsgDto.java).
Например,
[QmngrLoadMsgListener](kafka-sofr-qmngr/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/to/qmngr/kafka/listener/QmngrLoadMsgListener.java).

Далее требуется вызвать
[QmngrLoadMsgService](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/service/qmngr/QmngrLoadMsgService.java)
для сохранения сообщения в SOFR QManager.

Таким образом можно достаточно быстро подключить новых поставщиков сообщений для SOFR QManager.

Сервис
[QmngrLoadMsgService](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/service/qmngr/QmngrLoadMsgService.java)
реализует всю необходимую логику мониторинг процесса сохранения сообщения в SOFR QManager:

1. Логирование.
2. Аудит.
3. Сбор метрик (гистограмма времени обработки сообщения **rshbintech_sofr_qmngr_mq_load_msg_time_seconds_bucket**).

## 5. Выгрузка сообщений из SOFR QManager в брокеры

Сообщения, отправляемые в брокеры, должны загружаться из SOFR QManager посредством вызова хранимой процедуры
**IN_INTEGRATION.QMANAGER_READ_MSG** с параметрами:

| **Наименование параметра** | **Описание**                       | **Обязательность** | **Тип** | **Направление** |
|----------------------------|------------------------------------|--------------------|---------|-----------------|
| **P_WAIT_MSG**             | Время ожидания сообщения           | +                  | NUMBER  | IN              |
| **O_KAFKA_TOPIC**          | Наименование топика                | +                  | VARCHAR | OUT             |
| **O_MSGID**                | Уникальный идентификатор сообщения | +                  | VARCHAR | OUT             |
| **OCL_HEADER**             | Заголовки сообщения                | -                  | CLOB    | OUT             |
| **OCL_MESSAGE**            | Тело сообщения                     | +                  | CLOB    | OUT             |
| **O_ERRORCODE**            | Код ошибки                         | +                  | NUMBER  | OUT             |
| **O_ERRORDESC**            | Текст ошибки                       | -                  | VARCHAR | OUT             |

Хранимая процедура вызывается с параметром **P_WAIT_MSG** = 0, то есть без ожидания. Это связано с тем, что запуск
хранимой процедуры - это периодическое задание на уровне сервиса (см.
[QmngrReadMsgTriggerKafkaConsumer](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/trigger/QmngrReadMsgTriggerKafkaConsumer.java))

В качестве триггера периодического задания используется сообщение из **Kafka** вида:

```json
{
  "id": "827dd9ec-b1ef-4aee-9902-c02d53e123bf",
  "targetSystem": "sofr-qmngr-mq-adapter",
  "triggerSendTime": "2023-10-20T10:20:12+03:00"
}
```

которое поступает от
[task-scheduler-service](https://gitlab.rshbdev.ru/rshbintech/it-invest/ckips/ips/util/task-scheduler-service).

Периодическое задание запускает цикл, в котором из SOFR QManager загружается количество сообщений, указанное
в параметре конфигурации **app.qmngr.from.mq-connectors.kafka.consumers.read-msg-trigger.msg-read-limit** (либо, если
сообщений для выгрузки нет, цикл останавливается).

Для загрузки сообщений из SOFR QManager и отправки в брокеры реализован сервис
[QmngrReadMsgService](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/service/qmngr/QmngrReadMsgService.java).

Сервис является общим для всех брокеров сообщений. Для того чтобы подключить новый брокер,
нужно реализовать интерфейс поставщика сообщений:
[QmngrReadMsgToMqSendingService](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/service/QmngrReadMsgToMqSendingService.java),
например, [QmngrReadMsgToMqSendingService](sofr-qmngr-kafka/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/from/qmngr/kafka/service/QmngrReadMsgToKafkaSendingService.java)

На данный момент
[QmngrReadMsgService](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/service/qmngr/QmngrReadMsgService.java)
работает только с **Kafka** (зашито в коде). Поэтому в случае необходимости встраивания взаимодействия с другими
брокерами требуется его модификация, а именно: реализовать карту (Map) реализаций интерфейса
[QmngrReadMsgToMqSendingService](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/service/QmngrReadMsgToMqSendingService.java),
и правила выбора из этой карты соответствующей реализации для соответствующего брокера сообщений по данным,
загруженным из SOFR QManager.

Таким образом можно достаточно быстро подключить новых поставщиков сообщений для SOFR QManager.

Сервис
[QmngrReadMsgService](sofr-qmngr-mq-common/src/main/java/ru/rshbintech/itinvest/ckips/ips/sofr/qmngr/mq/lib/common/service/qmngr/QmngrReadMsgService.java)
реализует всю необходимую логику мониторинг процесса сохранения сообщения в SOFR QManager:

1. Логирование.
2. Аудит.
3. Сбор метрик (гистограмма времени обработки сообщения **rshbintech_sofr_qmngr_mq_read_msg_time_seconds_bucket**).
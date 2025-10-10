                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             # Журнал изменений

# Change Log
***

## Версия 1.0.23
- [BOSS-217] (https://sdlc.go.rshbank.ru/jira/browse/BOSS-217). Ко о погашениях ц/б
- [BOSS-3625] (https://sdlc.go.rshbank.ru/jira/browse/BOSS-3615). Справка НДФЛ

## Версия 1.0.22
- Изменения конфигурации
- [PEIN-42](https://sdlc.go.rshbank.ru/jira/browse/PEIN-42)
- [DEF-97101](https://sdlc.go.rshbank.ru/jira/browse/DEF-97101)
- [DEF-96843](https://sdlc.go.rshbank.ru/jira/browse/DEF-96843)

## Версия 1.0.21
- Перенесен топик **sofr.mq-adapter.client-info** в кластер PEIN

## Версия 1.0.20

- Исправлена обработка ошибок в QmngrReadMsgService DEF-70821
- Изменена логика получения хранимой процедуры в QmngrReadMsgService DEF-84522
- 
## Версия 1.0.19

- [BIQ-11266](https://sdlc.go.rshbank.ru/jira/browse/BIQ-12266). Загрузка НКД
- [BOSS-7693](https://sdlc.go.rshbank.ru/jira/browse/BOSS-7693). S3 адаптер СОФР

## Версия 1.0.18

- Изменено расположение топика sofr.send-nontrading-order-info-request. Теперь он находится в старой Кафке IPS (BOSS-7110)

## Версия 1.0.17

- Добавлен топик sofr.send-nontrading-order-info-request (BOSS-6888)

## Версия 1.0.16

- Добавлены топики для producer: sofr.send-nontrading-order-answer; sofr.send-nontrading-order-status-request
- Добавлены топики для consumer: sofr.send-nontrading-order-request; sofr.send-nontrading-order-status-answer (BOSS-5137)
- Добавлены топики sofr.get-brokerage-agreement-info-request; sofr.get-brokerage-agreement-info-response (BOSS-5599)
- Добавлены топики sofr.send-bill-request; sofr.send-bill-deal-request (BOSS-6514)
- Увеличен лимит памяти приложения до 1Гб и увеличен максимальный размер запроса до 10Мб в Kafka продюсере (DEF-75775)

## Версия 1.0.15

- Изменен IP адрес БД Оracle 10.26.93.110 --> 10.26.53.83 для Пром (DEF-76486)

## Версия 1.0.14

- Добавлена возможность передачи сообщений по направлению SOFR QManager --> KAFKA PLATFORM (BOSS-5137)
- Добавлен новый модуль sofr-metrics для получения метрик из SOFR по расписанию (BOSS-4214)
- Добавлены топики ips.document-factory.requests; sinv.get-brokerage-report-info-request (BOSS-5277)

## Версия 1.0.13

- Добавлены топики: ips.dias-sofr.depo-contract; ips.sofr-dias.broker-contract (BIQ-7382)

## Версия 1.0.12

- Реализация подключения к платформенной Kafka

## Версия 1.0.11

- Добавлены топики: ips.dias-sofr.allocated-coupon; ips.sofr-dias.allocated-coupon (BIQ-16598)

## Версия 1.0.10
 
- Добавлен link (type = producer) для топика sofr.mq-adapter.client-info (BIQ-17810)

## Версия 1.0.9

- Добавлена возможность передачи заголовков по направлению KAFKA <-> SOFR QManager
- Рефакторинг Unit тестов
- Добавлены топики: ips.efr-sofr.state-officer-sert; ips.sofr-efr.state-officer-sert (BIQ-18375)

## Версия 1.0.8

- Добавлена возможность настраиваемой обрезки логов
- Добавлена возможность конфигурации разных именований топиков на разных средах
- Добавлен топик maks.transfer-rates.universal-json для чтения трансферных ставок и сохранения в СОФР (BIQ-13038)

## Версия 1.0.7

- Увеличение ресурсов Istio
- Обновлена конфигурация для fake-prod

## Версия 1.0.6

- Обновлена конфигурация для fake-prod

## Версия 1.0.5

- Добавлена возможность конфигурирования обрезки параметра message в сообщениях аудита (по умолчанию 500)
- Исключено событие аудита QMNGR_READ_MSG_PROC_NO_MESSAGES (событие о том, что в AQ Oracle нет сообщений)

## Версия 1.0.4

- Исправлен Oracle datasource url 2

## Версия 1.0.3

- Исправлен Oracle datasource url

## Версия 1.0.2

- Исправлен Oracle SID uppercase

## Версия 1.0.1

- Исправлен Oracle SID

## Версия 1.0.0

- Реализация интеграционного сервиса для обмена Kafka <-> SOFR QManager

***

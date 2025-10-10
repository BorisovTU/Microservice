# Файлы для настройки локального окружения

Чтобы настроить локальное окружение, нужно выполнить команду `docker compose up -d`.

В результате будут развернуты:

1. **Zookeeper**.
2. **Kafka**.
3. **Kafka-UI**.

При этом, в Kafka будут созданы следующие топики (которые необходимы для локального тестирования):

| **Название топика** |
|------|
| **ips.dias-sofr.ndfl** |
| **quik.limits.NewInstMon** |
| **sofr.limits.NewInstMon** |
| **diasoft.sofr.pko-info.req** |
| **sofr.diasoft.pko-info.resp** |
| **diasoft.sofr.pko-info-status-result.req** |
| **sofr.diasoft.pko-info-status-result.resp** |
| **sofr.diasoft.pko-info-status.req** |
| **ips.tss.sofr-qmngr-mq-adapter** |
| **ips.audit.messages** |
| **maks.transfer-rates.universal-json** |
| **ips.efr-sofr.state-officer-sert** |
| **ips.sofr-efr.state-officer-sert** |
| **sofr.mq-adapter.client-info** |
| **ips.dias-sofr.allocated-coupon** |
| **ips.sofr-dias.allocated-coupon** |
| **ips.dias-sofr.depo-contract** |
| **ips.sofr-dias.broker-contract** |
| **ips.document-factory.requests** |
| **sinv.get-brokerage-report-info-request** |
| **sofr.get-brokerage-agreement-info-request** |
| **sofr.get-brokerage-agreement-info-response** |
| **sofr.send-bill-request** |
| **sofr.send-bill-deal-request** |
| **sofr.send-nontrading-order-info-request** |
| **dias.send-depo-corp-actions-red-info-requests** |

Стоит отметить, что в качестве хоста для подключения к Kafka будет использоваться значение переменной окружения
`${DOCKER_HOST_IP:-127.0.0.1}`, то есть по умолчанию - **localhost**. Если контейнер с Kafka запускается в Docker на 
другом хосте, то для корректного подключения к Kafka требуется перед выполнением команды `docker compose up -d` 
выполнить:

`export DOCKER_HOST_IP=<указать host>`

Указанный host требуется использовать для подключения к Kafka, как это сделано, например в 
[application.example.yml](../sofr-qmngr-mq-service/src/main/resources/config/application.example.yml).

**ВАЖНО**

Для локального тестирования требуется база данных Oracle. На данный момент в DockerHub нет официальных образов с
Oracle Database, поэтому можно воспользоваться следующими вариантами:
1. Установить Oracle на машину самостоятельно воспользовавшись
[инструкцией](https://www.oracle.com/webfolder/technetwork/tutorials/obe/db/12c/r1/Windows_DB_Install_OBE/Installing_Oracle_Db12c_Windows.html).
2. Собрать контейнер Docker с Oracle Database самостоятельно с использованием официального 
[Dockerfile](https://github.com/oracle/docker-images/tree/main/OracleDatabase).
3. Подключиться к **dev**/**test** средам (при условии, что все необходимые для локального тестирования процедуры уже
есть на средах).

Для случаев 1 и 2 потребуется накатить [SQL-скрипты](procedures-running.sql) на БД вручную.
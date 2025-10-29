# Прокси-сервис для RSBankWS

[[_TOC_]]

## 1. Общая информация

Прокси-сервис, который должен быть установлен перед сервисом **RSBankWS** и предназначенный оптимизации процесса
загрузки сделок. Позволяет ускорить процесс получения ответа при загрузке новых сделок при выполнении определенных
условий.

Ссылки на требования:
1. [Требования к сервису-прокси](https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=291221750).
2. [Требования к БД СОФР](https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=291221850).
3. [Требования к форматам обмена](https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=291221974).
4. [Общее описание](https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=291213015).

Сервис должен запускаться как **Windows**-сервис путем вызова исполняемого **jar** файла.
Для работы **требуется** JRE версии 17+.

## 2. Конфигурационные параметры сервиса

Конфигурация сервиса осуществляется посредством файла _config.yml_, который должен быть помещен в каталог _config_
рядом с исполняемым jar файлом сервиса. Пример структуры каталогов:

```text
/service/rsbankws-proxy.jar
/service/config/config.yml
```

### 2.1. Конфигурационные параметры подключения к БД СОФР

Для конфигурации подключения сервиса к БД СОФР требуется использовать следующие параметры:

| **Наименование параметра** | **Описание** | **Обязательность** | **Значение по умолчанию** |
|----------------------------|--------------|--------------------|---------------------------|
| **APP_ORACLE_HOST**        | Хост         | +                  | Пустая строка             |
| **APP_ORACLE_PORT**        | Порт         | +                  | Пустая строка             |
| **APP_ORACLE_USER**        | Пользователь | +                  | Пустая строка             |
| **APP_ORACLE_PASSWORD**    | Пароль       | +                  | Пустая строка             |
| **APP_ORACLE_NAME**        | SID          | +                  | Пустая строка             |
| **APP_ORACLE_SCHEMA**      | Схема        | +                  | Пустая строка             |

Пример:

```yml
APP_ORACLE_HOST: 10.7.118.13
APP_ORACLE_PORT: 1521
APP_ORACLE_USER: RSHB_SOFR_UPGRADE
APP_ORACLE_PASSWORD: ${APP_ORACLE_USER}
APP_ORACLE_NAME: sofr
APP_ORACLE_SCHEMA: ${APP_ORACLE_USER}
```

### 2.2. Конфигурационные параметры логирования

Для конфигурации параметров логирования требуется использовать следующие параметры:

| **Наименование параметра**                                               | **Описание**                                                                                                                            | **Обязательность** | **Значение по умолчанию** |
|--------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|--------------------|---------------------------|
| **LOGGING_LEVEL_ROOT**                                                   | Корневой уровень логирования.                                                                                                           | -                  | INFO                      |
| **LOGGING_LOGSTASH_ENABLED**                                             | Требуется ли вести логи в формате _JSON_ для _ELK_: **true** - если требуется, **false** - если требуется вести логи в консольном виде. | -                  | true                      |
| **logging.level.ru.rshbintech.rsbankws.proxy.service.error**             | Уровень логирования запросов и ответов к прокси сервису, а так же к сервису RSBankWS.                                                   | -                  | INFO                      |
| **app.error.logging.processing.xml.enabled**                             | Включено ли логирование xml при возникновении ошибок.                                                                                   | -                  | true                      |
| **app.error.logging.processing.xml.max-length**                          | Максимальная длина xml в логе при возникновении ошибок.                                                                                 | -                  | 10000                     |
| **app.error.logging.processing.proxy.request.body.enabled**              | Включено ли логирование тела запроса, поступающего на прокси-сервис, при возникновении ошибок.                                          | -                  | true                      |
| **app.error.logging.processing.proxy.request.body.max-length**           | Максимальная длина тела запроса, поступающего на прокси-сервис, в логе при возникновении ошибок.                                        | -                  | 10000                     |
| **app.error.logging.processing.proxy.request.headers.enabled**           | Включено ли логирование заголовков запроса, поступающего на прокси-сервис, при возникновении ошибок.                                    | -                  | true                      |
| **app.error.logging.processing.proxy.request.headers.max-length**        | Максимальная длина заголовков запроса, поступающего на прокси-сервис, в логе при возникновении ошибок.                                  | -                  | 10000                     |
| **app.error.logging.processing.rs-bank-ws.request.body.enabled**         | Включено ли логирование тела запроса, отправляемого на сервис RSBankWS, при возникновении ошибок.                                       | -                  | true                      |
| **app.error.logging.processing.rs-bank-ws.request.body.max-length**      | Максимальная длина тела запроса, отправляемого на сервис RSBankWS, в логе при возникновении ошибок.                                     | -                  | 10000                     |
| **app.error.logging.processing.rs-bank-ws.request.headers.enabled**      | Включено ли логирование заголовков запроса, отправляемого на сервис RSBankWS, при возникновении ошибок.                                 | -                  | true                      |
| **app.error.logging.processing.rs-bank-ws.request.headers.max-length**   | Максимальная длина заголовков запроса, отправляемого на сервис RSBankWS, в логе при возникновении ошибок.                               | -                  | 10000                     |


Пример:

```yml
LOGGING_LEVEL_ROOT: INFO
LOGGING_LOGSTASH_ENABLED: false

logging:
  level:
    ru.rshbintech.rsbankws.proxy.service.error: INFO

app:
  error:
    logging:
      processing:
        xml:
          enabled: true
          max-length: 10000
      proxy:
        request:
          body:
            enabled: true
            max-length: 10000
          headers:
            enabled: true
            max-length: 10000
      rs-bank-ws:
        request:
          body:
            enabled: true
            max-length: 10000
          headers:
            enabled: true
            max-length: 10000
```

**ВАЖНО!**
В случае запуска сервиса как **Windows**-сервиса, рекомендуется вести логи в консольном виде (для удобства разбора).
В случае, если сервис переедет в **App.Farm**, нужно переключить ведение логов в вид _JSON_ для _ELK_.
Уровень логирования запросов и ответов к прокси сервису, а так же к сервису RSBankWS, рекомендуется ставить как _INFO_
на продуктовой среде и включать уровень _DEBUG_ только в крайнем случае для разбора серьезных проблем, так как
в этом случае лог будет занимать большое количество места на диске.

### 2.3. Конфигурационные параметры прокси-сервиса

| **Наименование параметра**   | **Описание**        | **Обязательность** | **Значение по умолчанию** |
|------------------------------|---------------------|--------------------|---------------------------|
| **app.rs-bank-ws-proxy.uri** | URI прокси-сервиса  | -                  | /ws/RSBankWS.asmx         |
| **server.port**              | Порт прокси-сервиса | -                  | 8080                      |

Пример:

```yml
app:
  rs-bank-ws-proxy:
    uri: /ws/RSBankWS.asmx

server:
  port: 8080
```

**ВАЖНО!**
Общий URL для доступа к сервису строится следующим образом:

```text
http://localhost:server.port/app.rs-bank-ws-proxy.uri
```

Например:

```text
http://localhost:8080/ws/RSBankWS.asmx 
```

### 2.4. Конфигурационные параметры клиента для вызова сервиса RSBankWS

| **Наименование параметра**                 | **Описание**                                                                                         | **Обязательность** | **Значение по умолчанию** |
|--------------------------------------------|------------------------------------------------------------------------------------------------------|--------------------|---------------------------|
| **app.rs-bank-ws-client.url**              | URL для обращения к сервису RSBankWS.                                                                | -                  | Пустая строка             |
| **app.rs-bank-ws-client.connect-timeout**  | Таймаут подключения к сервису RSBankWS в секундах.                                                   | -                  | 5                         |
| **app.rs-bank-ws-client.read-timeout**     | Таймаут ожидания ответа от сервиса RSBankWS в секундах.                                              | -                  | 60                        |
| **app.rs-bank-ws-client.max-connections**  | Количество одновременно открытых сетевых соединений, которые могут быть доступны в клиенте RSBankWS. | -                  | 10000                     |

Пример:

```yaml
app:
  rs-bank-ws-client:
    url: http://localhost:8500/ws/RSBankWS.asmx
    connect-timeout: 5
    read-timeout: 60
    max-connections: 10000
```

### 2.5. Конфигурационные параметры алгоритма "быстрого" ответа

| **Наименование параметра**                                           | **Описание**                                                                               | **Обязательность** | **Значение по умолчанию**                     |
|----------------------------------------------------------------------|--------------------------------------------------------------------------------------------|--------------------|-----------------------------------------------|
| **app.fast-answer.methods**                                          | Список методов, для которых нужно формировать "быстрый" ответ.                             | -                  | RunMacro.ws_ProcessDeals.ProcessDeals         | 
| **app.fast-answer.deal-rules**                                       | Список правил (для формирования "быстрого" ответа) для различных типов сделок.             | -                  | См. пример                                    |
| **app.fast-answer.deal-rules.<deal_name>.seq-type**                  | Тип сделки: по валюте (CASH); по ценным бумагам (SECURITIES).                              | -                  | Пустая строка                                 |
| **app.fast-answer.deal-rules.<deal_name>.kind**                      | Вид сделки (на данный момент может быть указан в одиночном виде, возможно отрицание).      | -                  | Пустая строка                                 |
| **app.fast-answer.condor-sofr-buffer-table-insert-stored-proc-name** | Наименование хранимой процедуры, генерирующей ID и записывающей данные в буферную таблицу. | -                  | IT_INTEGRATION.Condor_GetLastSOFRSequenceDeal |


Пример:

```yaml
app:
  fast-answer:
    methods: RunMacro.ws_ProcessDeals.ProcessDeals
    deal-rules:
      CurrencyDeal:
        seq-type: CASH
      CurrencySWAPDeal:
        seq-type: CASH
      CurrencyFWDDeal:
        seq-type: CASH
      FRADeal:
        seq-type: CASH
      CurrencyOptionDeal:
        seq-type: CASH
      IRSSWAPDeal:
        seq-type: CASH
      IAMLDDeal:
        seq-type: SECURITIES
        kind: '!CSA'
      BondDeal:
        seq-type: SECURITIES
      REPODeal:
        seq-type: SECURITIES
      CurrencyEquityDeal:
        seq-type: SECURITIES
    condor-sofr-buffer-table-insert-stored-proc-name: IT_INTEGRATION.Condor_GetLastSOFRSequenceDeal
```

## 3. Разбор ошибок прокси-сервиса

В процессе работы прокси-сервиса могут возникать ошибки. В этом случае на вызывающую сторону будет отдан ответ вида:

```xml
<?xml version='1.0' encoding='UTF-8'?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
        <ns2:XMLRPCCallResponse xmlns:ns2="http://rsbank.softlab.ru/">
            <return>
                <![CDATA[
                <?xml version='1.0' encoding='UTF-8'?><methodResponse xmlns="http://www.softlab.ru/xml-rpc/schema"
                xmlns:n0="http://www.softlab.ru/xml-rpc/schema"
                n0:reqId=""><fault><value><struct><member><name>faultType</name><value><integer>5</integer></value>
                </member><member><name>faultCode</name><value><integer>-1</integer></value>
                </member><member><name>faultString</name><value><string>Ошибка прокси сервиса SOFR.RSBankWSProxy.
                Идентификатор ошибки: 664b2979fcee5f15d6da55e00281556f. Причина: Ошибка разбора XML.</string></value>
                </member>
            </struct>
        </value>
    </fault></methodResponse>
]]>
</return>
</ns2:XMLRPCCallResponse>
</soap:Body>
</soap:Envelope>
```

Здесь стоит обратить внимание именно на текст ошибки:

```text
Ошибка прокси сервиса SOFR.RSBankWSProxy. Идентификатор ошибки: 664b2979fcee5f15d6da55e00281556f. Причина: 
Ошибка разбора XML.
```

**ВАЖНО!**
Текст: _Ошибка прокси сервиса SOFR.RSBankWSProxy_, говорит о том, что ошибка произошла **именно** на прокси-сервисе.
В сообщениях с ошибкой от RSBankWS сервиса такого текста не будет.
Также в сообщении об ошибке можно увидеть текст вида: _Идентификатор ошибки: 664b2979fcee5f15d6da55e00281556f_.
Здесь указывается идентификатор ошибки, который представляет собой **traceId**, присваиваемый каждому сообщению в логе в
данной цепочке вызова. Соответственно, по данному идентификатору можно найти всю необходимую информацию о данной ошибке
в логах прокси-сервиса, например:

```text
20-05-2024 13:44:09.210 [664b2979fcee5f15d6da55e00281556f-d6da55e00281556f] 
[reactor-http-nio-4] [ERROR] [r.r.r.p.c.RSBankWSProxyExceptionHandler.handleRSBankWSProxyException - Ошибка разбора XML.
Тип XML = [MethodCall].
Тело XML = [<?xml version='1.0'...].
Причина:
org.xml.sax.SAXParseException; lineNumber: 2; columnNumber: 29; Attribute name "asd" associated with an 
element type "methodCall" must be followed by the ' = ' character.
        at java.xml/com.sun.org.apache.xerces.internal.parsers.DOMParser.parse(DOMParser.java:262)
        at java.xml/com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderImpl.parse(DocumentBuilderImpl.java:342)
...
```

## 4. Технические эндпоинты прокси-сервиса

Прокси-сервис реализован с учётом всех ключевых особенностей платформы **App.Farm** и может быть развернут там с
минимальными усилиями, поэтому предоставляется ряд технических эндпоинтов для отслеживания его состояния:

- _/metrics_ — для сбора метрик в формате Prometheus;
- _/health_ — кумулятивная информация о готовности и работоспособности сервиса;
- _/health/readiness_ — информация о готовности сервиса;
- _/health/liveness_ — информация о работоспособности сервиса;
- _/api_ — спецификация сервиса в формате OpenAPI Specification 3.x c возможностью использования только двух MIME-типов: 
application/json или application/yaml;
- _/api/doc_ — HTML-страница Swagger UI со спецификацией сервиса в формате OpenAPI Specification 3.x;
- _/info_ — метаинформация о сборке приложения;
- _/loggers_ — информация о логгерах сервиса и управлении уровнями логирования.

## 5. Запуск сервиса

Пример bat файла для запуска сервиса:

```text
@echo off
chcp 1251
setlocal

rem Путь к исполняемому файлу java.exe
set JAVA_EXECUTABLE=jre\bin\java.exe

rem Путь к запускаемому jar файлу
set JAR_FILE=bin\rsbankws-proxy.jar

rem Запуск java приложения
"%JAVA_EXECUTABLE%" -Xmx1024m -Xms512m -jar "%JAR_FILE%"

rem Завершение работы скрипта
pause
```

**ВАЖНО!**
Требуется **обязательно** указать настройки памяти для java процесса иначе процесс может потреблять её неограниченное
количество.
На данный момент рекомендуется **-Xmx1024m -Xms512m**.
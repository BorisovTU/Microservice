CREATE OR REPLACE PACKAGE IT_TAXREPORT AS
  
  C_C_SYSTEM_NAME             CONSTANT VARCHAR2(128) := 'SINV';  /* Свои инвестиции*/
  C_PTCK_CFT                  CONSTANT NUMBER(5)     := 101;     /* Код ЦФТ*/
  C_OBJECTTYPE_CONTRACT       CONSTANT NUMBER(5)     := 207;     /* Тип объекта договор*/
  C_CONTRACT_CATEGORY_STATUS  CONSTANT NUMBER(5)     := 101;     /* Статус договора БО */
  C_STATUS_CONTRACT_FINISHED  CONSTANT NUMBER(5)     := 3;       /* Обработка завершена */
  
  C_NAME_REGVAL_NPTXSHORT_ONOFF       VARCHAR2(400) := 'COMMON\НДФЛ\ОБРАБОТКА_ЗАПРОСА_SINV_НФДЛ';
  C_NAME_REGVAL_NPTXSHORT_MAX_TIMEOUT VARCHAR2(400) := 'COMMON\НДФЛ\MAX_ВРЕМЯ_ФОРМ_НДФЛ_SINV';
  
  C_MIN_YEAR          CONSTANT NUMBER := 2023;
  C_LLVALUES_LIST_HDR CONSTANT NUMBER := 5075;
  
 /* константы для возврата ошибок*/
  C_INVALID_PARAM            CONSTANT NUMBER    := -1; /*Не валидные параметры*/ 
  C_SUCCESS_CODE             CONSTANT NUMBER    := 0;  /*Успешная отправка*/ 
  C_ERROR_CODE_VALIDE        CONSTANT NUMBER    := 1;  /*Ошибка при валидации запроса*/ 
  C_ERROR_CODE_GEN_JSON      CONSTANT NUMBER    := 2;  /*Ошибка при формировании JSON*/
  C_ERROR_CODE_GEN_FILE      CONSTANT NUMBER    := 3;  /*Ошибка генерации файла отчета*/
  C_ERROR_CODE_FAILED_KAFKA  CONSTANT NUMBER    := 4;   /*Ошибка генерации отправки в кафку*/
  
/*
  Отправка статуса обработки справки НФДЛ
  @since RSHB 120
  @qtest NO
  @param p_Json_notify JSON-уведомление (CLOB)
  @param p_HeaderMETA  Заголовок/метаданные (XMLTYPE), опционально
  @param p_buf_rec     Строка запроса (DUSERNPTXSHORTREQ_DBT%ROWTYPE)
  @param p_MsgId       ID для QManager-a
*/
  PROCEDURE SendTaxReportStatus(p_Json_notify IN CLOB,
                                p_HeaderMETA  IN XMLTYPE := NULL,
                                p_buf_rec     IN DUSERNPTXSHORTREQ_DBT%ROWTYPE,
                                p_MsgId       IN VARCHAR2,
                                o_MSGCode     OUT INTEGER,
                                o_MSGText     OUT VARCHAR2);
  
 /*
  Парсинг и валидация запроса по налоговому отчёту
  @since RSHB 120
  @qtest NO
  @param p_worklogid  Идентификатор записи QManager
  @param p_json_req   JSON-запрос (CLOB)
  @param o_MSGCode    Код результата (OUT)
  @param o_MSGText    Текст результата/ошибки (OUT)
*/
  PROCEDURE ParseAndValideTaxReport(p_worklogid INTEGER,
                                    p_json_req  IN CLOB,
                                    o_MSGCode   OUT INTEGER,
                                    o_MSGText   OUT VARCHAR2);

 /*
  Формирование запроса для запуска генерации налогового отчёта, отправляем в Qmanager
  @since RSHB 120
  @qtest NO
  @param p_buf_rec    Строка запроса (dusernptxshortreq_dbt%ROWTYPE)
  @param p_Json_Nptx  Сформированный JSON для NPTX (CLOB)
*/
  PROCEDURE GenerateTaxReportRequest(p_buf_rec   IN OUT dusernptxshortreq_dbt%ROWTYPE,
                                     p_Json_Nptx IN CLOB,
                                     o_MSGCode   OUT INTEGER,
                                     o_MSGText   OUT VARCHAR2);

/*
  Получение и разбор входящего запроса для краткой справки НДФЛ
  @since RSHB 120
  @qtest NO
  @param p_worklogid  Идентификатор записи Qmanager
  @param p_messbody   Входящее сообщение (CLOB)
  @param p_messmeta   Заголовок/метаданные сообщения (XMLTYPE)
  @param o_msgid      Идентификатор сообщения (OUT)
  @param o_MSGCode    Код результата (OUT)
  @param o_MSGText    Текст результата/ошибки (OUT)
  @param o_messbody   Подготовленное сообщение (OUT)
  @param o_messmeta   Подготовленные метаданные (OUT)
*/
  PROCEDURE GetTaxReportRequest(p_worklogid INTEGER,
                                p_messbody  CLOB,
                                p_messmeta  XMLTYPE,
                                o_msgid     OUT VARCHAR2,
                                o_MSGCode   OUT INTEGER,
                                o_MSGText   OUT VARCHAR2,
                                o_messbody  OUT CLOB,
                                o_messmeta  OUT XMLTYPE);
/*
  Запуск формирования краткой справки с контролем таймаута
  @since RSHB 120
  @qtest NO
  @param p_buf_rec  Строка запроса (dusernptxshortreq_dbt%ROWTYPE)
*/
  PROCEDURE TaxReportRun(p_buf_rec IN dusernptxshortreq_dbt%ROWTYPE,
                         o_MSGCode   OUT INTEGER,
                         o_MSGText   OUT VARCHAR2);
 
 /*
  Формирование уведомления о готовности файла отчёта
  @since RSHB 120
  @qtest NO
  @param p_worklogid  Идентификатор записи в Qmanager
  @param p_messbody   Входящее сообщение (CLOB)
  @param p_messmeta   Заголовок/метаданные (XMLTYPE)
  @param o_msgid      Идентификатор сообщения (OUT)
  @param o_MSGCode    Код результата (OUT)
  @param o_MSGText    Текст результата/ошибки (OUT)
  @param o_messbody   Сформированное сообщение (OUT)
  @param o_messmeta   Сформированные метаданные (OUT)
*/ 
  PROCEDURE GenerateFileNotification(p_worklogid INTEGER,
                                     p_messbody  CLOB,
                                     p_messmeta  XMLTYPE,
                                     o_msgid     OUT VARCHAR2,
                                     o_MSGCode   OUT INTEGER,
                                     o_MSGText   OUT VARCHAR2,
                                     o_messbody  OUT CLOB,
                                     o_messmeta  OUT XMLTYPE);

END IT_TAXREPORT;
/
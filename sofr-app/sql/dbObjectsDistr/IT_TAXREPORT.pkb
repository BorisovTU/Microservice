CREATE OR REPLACE PACKAGE BODY IT_TAXREPORT AS

/*
  Добавление записи об ошибке в JSON-массив ошибок
  @since RSHB 120
  @qtest NO
  @param p_arr        Массив ошибок (JSON_ARRAY_T), модифицируется на месте
  @param p_code       Код ошибки
  @param p_desc       Текст ошибки
*/
 PROCEDURE AddErrArray( p_arr  IN OUT NOCOPY SYS.JSON_ARRAY_T,
                     p_code IN VARCHAR2,
                     p_desc IN VARCHAR2) 
 IS
  v_obj SYS.JSON_OBJECT_T := SYS.JSON_OBJECT_T();
 BEGIN
  v_obj.put('ErrorCode', p_code);
  v_obj.put('ErrorDesc', p_desc);
  p_arr.append(v_obj);
 END; 
 
  /*
  Проверка: содержит ли массив ошибок хотя бы один элемент
  @since RSHB 120
  @qtest NO
  @param p_arr  Массив ошибок (JSON_ARRAY_T)
  @return TRUE ? есть ошибки; FALSE ? нет
*/
  FUNCTION HasErrArray( p_arr IN SYS.JSON_ARRAY_T) 
   RETURN 
    BOOLEAN 
  IS
  BEGIN
      RETURN p_arr.GET_SIZE() > 0;
  END;   
   
 /*
  Преобразование META (XMLTYPE) в JSON (CLOB)
  @since RSHB 120
  @qtest NO
  @param p_meta       XML META-заголовок
  @return CLOB с JSON-представлением заголовка
 */
  FUNCTION GetHeaderJson(p_meta XMLTYPE) RETURN CLOB IS
    v CLOB;
  BEGIN
    SELECT extractValue(p_meta, '/KAFKA/Header')
    INTO v
    FROM dual;
    
    RETURN v;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL; 
  END; 
      

/*
  Формирование JSON-тела уведомления о результате обработки
  @since RSHB 120
  @qtest NO
  @param p_GuidReq    GUID изначального запроса 
  @param p_GuidReq    GUID ново-сгенерированный
  @param p_IsSuccess  Признак успешности обработки
  @param p_Errors     Массив ошибок (если есть)
  @return CLOB с JSON-телом уведомления
*/
  FUNCTION GenerateJsonBodyNotification(p_GuidReq   VARCHAR2, 
                                        p_Guid      VARCHAR2,
                                        p_IsSuccess BOOLEAN, 
                                        p_Errors    JSON_ARRAY_T) 
   RETURN CLOB 
  IS
   v_resp      SYS.JSON_OBJECT_T := SYS.JSON_OBJECT_T();
   v_root      SYS.JSON_OBJECT_T := SYS.JSON_OBJECT_T();
   v_err_list  SYS.JSON_OBJECT_T := SYS.JSON_OBJECT_T();
   v_errors    SYS.JSON_ARRAY_T;
  BEGIN

    -- 1) Подготовим массив ошибок
    IF HasErrArray(p_Errors) THEN
      v_errors := p_Errors;
    ELSE
      v_errors := SYS.JSON_ARRAY_T();
      IF p_IsSuccess THEN
        AddErrArray(v_errors, 0, '');
      END IF;
    END IF;

    v_resp.put('GUID',        p_Guid);
    v_resp.put('GUIDReq',     p_GuidReq);
    v_resp.put('RequestTime', TO_CHAR(systimestamp, 'YYYY-MM-DD"T"HH24:MI:SS.FF3'));
    v_resp.put('IsSuccess',   CASE WHEN p_IsSuccess THEN TRUE ELSE FALSE END);

    v_err_list.put('Error', v_errors);
    v_resp.put('ErrorList', v_err_list);

    v_root.put('GetTaxReportInfoResp', v_resp);
    it_log.log(p_msg => 'generateJsonBodyNotification: ' || v_root.to_clob, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
    
    RETURN v_root.to_clob;
  END;
  
  /*
  Формирование XML-заголовка (META) для IPS
  @since RSHB 120
  @qtest NO
  @param p_GuidReq        GUID запроса
  @param p_MsgID          Идентификатор сообщения
  @param p_OutputFileName Имя выходного файла
  @return XMLTYPE с META-заголовком
*/
  FUNCTION GenerateJsonHeaderIPS(p_GuidReq        VARCHAR2,
                                 p_Guid         VARCHAR2,
                                 p_OutputFileName VARCHAR2) 
   RETURN xmltype 
  IS
    v_addparams it_ips_dfactory.tt_addparams;
  BEGIN
    v_addparams := NULL;
    v_addparams := it_ips_dfactory.add_addparams(p_addparams => v_addparams
                                                ,p_paramName      => 'x-request-time'
                                                ,p_paramValue     => IT_XML.TIMESTAMP_TO_CHAR_ISO8601(SYSDATE));


    RETURN it_ips_dfactory.add_KafkaHeader_Xmessmeta(p_List_dllvalues_dbt => 5075,
                                                     p_traceid =>  p_GuidReq, --x-trace-id
                                                     p_requestid => p_Guid,    --x-request-id
                                                     p_templateparams => NULL,
                                                     p_addparams => v_addparams,
                                                     p_outputfilename => p_OutputFileName);
  END;
  
 /*
  Формирование XML-заголовка (META) для SINV
  @since RSHB 120
  @qtest NO
  @param p_GuidReq     GUID исходного запроса GetTaxReportInfoReq
  @param p_RequestId   уникальный новый GUID ответа на запрос
  @param p_FileName    Имя файла
  @param p_DocumentPath Путь к документу
  @return XMLTYPE с META-заголовком
*/
  FUNCTION GenerateJsonHeaderSINV(p_GuidReq      VARCHAR2,
                                  p_RequestId    VARCHAR2,
                                  p_FileName     VARCHAR2,
                                  p_DocumentPath VARCHAR2) 
   RETURN XMLTYPE 
  IS
   v_hdr     SYS.JSON_OBJECT_T := SYS.JSON_OBJECT_T();
   v_system_from  dllvalues_dbt.t_note%TYPE;
   v_service_name dllvalues_dbt.t_note%TYPE;
   v_bucket_name  dllvalues_dbt.t_note%TYPE;
  BEGIN
    
   BEGIN 
    SELECT t_note 
      INTO v_system_from
     FROM dllvalues_dbt
    WHERE t_list = C_LLVALUES_LIST_HDR
      AND t_name = 'x-system-from';
   EXCEPTION WHEN OTHERS 
     THEN v_system_from := NULL;
   END;
   
   BEGIN 
    SELECT t_note 
      INTO v_service_name
     FROM dllvalues_dbt
    WHERE t_list = C_LLVALUES_LIST_HDR
      AND t_name = 'x-service-name';
   EXCEPTION WHEN OTHERS 
     THEN v_service_name := NULL;
   END;
   
   BEGIN 
    SELECT t_note 
      INTO v_bucket_name
     FROM dllvalues_dbt
    WHERE t_list = C_LLVALUES_LIST_HDR
      AND t_name = 'x-bucket-name';
   EXCEPTION WHEN OTHERS 
     THEN v_bucket_name := NULL;
   END;
 
    v_hdr.put('x-trace-id',         p_GuidReq);                  
    v_hdr.put('x-request-id',       p_RequestId);  
    v_hdr.put('x-request-time',     TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.FF3'));
    v_hdr.put('x-system-from',      v_system_from); 
    v_hdr.put('x-output-file-name', p_FileName);
    v_hdr.put('x-service-name',     v_service_name); 
    v_hdr.put('x-document-path',    p_DocumentPath);
    v_hdr.put('x-bucket-name',      v_bucket_name); 
    
    RETURN it_kafka.add_Header_Xmessmeta(p_Header => v_hdr.to_clob());

 END;
 
/*
  Создание (вставка) записи запроса в буферную таблицу
  @since RSHB 120
  @qtest NO
  @param p_buf_rec    Строка запроса (dusernptxshortreq_dbt%ROWTYPE)
  @return Возвращает идентификатор созданной записи (VARCHAR2)
*/
  FUNCTION InsertNptxReq( p_buf_rec IN dusernptxshortreq_dbt%ROWTYPE )
  RETURN VARCHAR2 IS
    res_ID dusernptxshortreq_dbt.T_ID%TYPE;
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO dusernptxshortreq_dbt
     VALUES p_buf_rec
    RETURNING T_ID INTO res_ID;
    COMMIT;
    RETURN res_ID;
  END;
  
 /*
  Обновление записи запроса в буферной таблице
  @since RSHB 120
  @qtest NO
  @param p_buf_rec    Строка запроса (dusernptxshortreq_dbt%ROWTYPE)
*/
  PROCEDURE UpdateNptxReq( p_buf_rec IN dusernptxshortreq_dbt%ROWTYPE )
  IS
    err_txt VARCHAR2(2000);
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE dusernptxshortreq_dbt
       SET T_QMESSLOGID =  p_buf_rec.T_QMESSLOGID,
           T_SESSIONID   = p_buf_rec.T_SESSIONID,
           T_GUIDREQ     = p_buf_rec.t_guidreq,
           T_GUIDRESP    = p_buf_rec.T_GUIDRESP,
           T_TAXPERIOD   = p_buf_rec.T_TAXPERIOD,
           T_REGDATEREQ  = p_buf_rec.T_REGDATEREQ,
           T_REGDATERESP = p_buf_rec.T_REGDATERESP,
           T_CLIENTCODE  = p_buf_rec.T_CLIENTCODE,
           T_PARTYID     = p_buf_rec.T_PARTYID,
           T_REQSYSDATE  = p_buf_rec.T_REQSYSDATE,
           T_JSONGENBEG  = p_buf_rec.T_JSONGENBEG,
           T_JSONGENEND  = p_buf_rec.T_JSONGENEND,
           T_REPNAME     = p_buf_rec.T_REPNAME,
           T_ERRORDESC   = p_buf_rec.T_ERRORDESC,
           T_ERRORCODE   = p_buf_rec.T_ERRORCODE,
           T_FILEGENBEG  = p_buf_rec.T_FILEGENBEG,
           T_FILEGENEND  = p_buf_rec.T_FILEGENEND,
           T_DOCUMENTPATH = p_buf_rec.T_DOCUMENTPATH 
     WHERE T_ID = p_buf_rec.T_ID;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      err_txt := 'Произошла непридведенная ошибка во время выполнения запроса UpdateNptxReq: ' || SQLERRM;
      it_log.log(p_msg => err_txt, p_msg_type => it_log.C_MSG_TYPE__ERROR);
      ROLLBACK;
  END;
  
/*
  Фиксация ошибки обработки для записи запроса
  @since RSHB 120
  @qtest NO
  @param p_error_desc Описание ошибки
  @param p_error_code Код ошибки
  @param p_id         Идентификатор записи запроса
*/
  PROCEDURE UpdateErrorNptxReq(p_error_desc IN CLOB,
                              p_error_code IN NUMBER,
                              p_id         IN DUSERNPTXSHORTREQ_DBT.T_ID%TYPE)
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE DUSERNPTXSHORTREQ_DBT
       SET t_errordesc = CASE
                             WHEN t_errordesc IS NOT NULL THEN t_errordesc || chr(10) || p_error_desc
                             ELSE p_error_desc
                           END,
           t_errorcode = p_error_code
     WHERE t_id = p_id;
     
    COMMIT;
  END;
 
 /**
  * Получение идентификатора клиента по виду кода "Код ЦФТ"
  * @since RSHB 120
  * @qtest NO
  * @param p_PartyCode Код клиента
  * @return идентификатор клиента
  */
  FUNCTION GetPartyIDByCFT(p_PartyCode IN VARCHAR2)
    RETURN NUMBER
  IS
    v_PartyID dparty_dbt.t_PartyID%TYPE;
  BEGIN
    SELECT t_ObjectID INTO v_PartyID
      FROM dobjcode_dbt
     WHERE t_ObjectType = PM_COMMON.OBJTYPE_PARTY
       AND t_CodeKind = C_PTCK_CFT
       AND t_Code = p_PartyCode
       AND t_State = 0;
    RETURN v_PartyID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
  END;
  
  
   /**
  * Получение идентификатора клиента по его id
  * @since RSHB 120
  * @qtest NO
  * @param p_PartyCode Код клиента
  * @return идентификатор клиента
  */
  FUNCTION GetPartyIDByCode(p_PartyCode IN VARCHAR2)
    RETURN NUMBER
  IS
    v_PartyID dparty_dbt.t_PartyID%TYPE;
  BEGIN
     SELECT t_partyid INTO v_PartyID 
       FROM dparty_dbt d 
      WHERE t_partyid = to_number(p_PartyCode);   
    RETURN v_PartyID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
  END;
  
/*
  Проверка ФЛ по PartyID
  @since RSHB 120
  @qtest NO
  @param p_PartyID    Идентификатор PartyID
  @return TRUE ? физическое лицо; FALSE ? иное
*/
  FUNCTION GetLegalFormByPartyId(p_PartyID IN dparty_dbt.t_partyid%type)
    RETURN BOOLEAN
  IS
    v_isLegal NUMBER;
  BEGIN
    SELECT count(*) INTO v_isLegal
      FROM dparty_dbt d
     WHERE d.t_partyid = p_PartyID
       AND d.t_legalform = 2;
    RETURN v_isLegal > 0;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN FALSE;
  END;
  
  
/*
  Проверка наличия текущего ДБО за период по PartyID
  @since RSHB 120
  @qtest NO
  @param p_PartyID    Идентификатор PartyID
  @param p_TaxPeriod  Налоговый период (год)
  @return TRUE ? есть; FALSE ? нет
*/
  FUNCTION GetHasContrByPartyId(p_PartyID   IN dparty_dbt.t_partyid%type, 
                                p_TaxPeriod IN NUMBER)
    RETURN BOOLEAN
  IS
    v_hasContr NUMBER;
  BEGIN
    SELECT COUNT(*)
        INTO v_hasContr
        FROM dsfcontr_dbt sf, 
            ddlcontr_dbt dl 
       WHERE sf.t_id = dl.t_sfcontrid 
         AND sf.t_partyid = p_PartyID
         AND EXTRACT(YEAR FROM t_DateBegin) <= p_TaxPeriod
         AND (t_DateClose = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR EXTRACT(YEAR FROM t_DateClose) = p_TaxPeriod)
         AND exists (select 1 from dobjatcor_dbt o where o.t_objecttype = C_OBJECTTYPE_CONTRACT and o.t_groupid = C_CONTRACT_CATEGORY_STATUS 
                      and o.t_attrid = C_STATUS_CONTRACT_FINISHED and (o.t_validtodate >= SYSDATE or o.t_validtodate = to_date('01.01.0001','dd.mm.yyyy')) 
                      and lpad(dl.t_dlcontrid,34,'0') = o.t_object);
    RETURN v_hasContr > 0;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN FALSE;
  END;

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
                                o_messmeta  OUT XMLTYPE) is 
                                     
    vMsgID      itt_q_message_log.msgid%type := lower(it_q_message.get_sys_guid);   
    vTurnOn CHAR(1);

  BEGIN
    --Иницилизируем успехом
    o_MSGCode := C_SUCCESS_CODE;
    o_MSGText := 'Успешно';
    o_msgid   := vMsgID;
    
    it_log.log(p_msg => 'Начало выполнения GetTaxReportRequest p_worklogid: ' || p_worklogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
    
    vTurnOn :=  nvl(RSB_Common.GetRegFlagValue(C_NAME_REGVAL_NPTXSHORT_ONOFF),'0');
    IF vTurnOn <> 'X' THEN
      o_MSGCode := C_INVALID_PARAM;
      o_MSGText := 'Настройка '||C_NAME_REGVAL_NPTXSHORT_ONOFF||' отключена';
      it_log.log(p_msg => o_MSGText, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
      RETURN;
    END IF;
    
    ParseAndValideTaxReport(p_worklogid, 
                            p_messbody, 
                            o_MSGCode, 
                            o_MSGText);
                            
    it_log.log(p_msg => 'Конец выполнения GetTaxReportRequest p_worklogid: ' || p_worklogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG); 
                      
  END; 

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
                                    o_MSGText   OUT VARCHAR2) IS
  -- буфер
  v_buf_rec dusernptxshortreq_dbt%ROWTYPE;
  v_MsgID   itt_q_message_log.msgid%TYPE := lower(lower(it_q_message.get_sys_guid));
  -- извлечённые поля
  v_guid_req     VARCHAR2(50);
  v_req_time_str VARCHAR2(100);
  v_tax_period   NUMBER;
  v_client_code  VARCHAR2(100);
  v_client_sysid VARCHAR2(50);
  v_client_node  VARCHAR2(50);

  -- справочные
  v_current_year NUMBER := TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY'));

  -- бизнес справки
  v_party_id     NUMBER  := NULL;
  v_is_physical  BOOLEAN := NULL;
  v_has_contract BOOLEAN := NULL;

  -- Исключение для остановки при ошибке
  e_validation_failed EXCEPTION;
  send_MSGCode INTEGER;
  send_MSGText VARCHAR2(1000);
    
  -- аккумулируем ошибки
  v_errs  JSON_ARRAY_T := JSON_ARRAY_T();
  
  --Тело нотификации в случае ошибки
  v_resp_body CLOB; 
    
  BEGIN
    
    it_log.log(p_msg => 'Начало обработки запроса GetTaxReportRequest p_worklogid: ' || p_worklogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
    
    IF p_json_req IS NULL THEN
      o_MSGCode := C_INVALID_PARAM; 
      o_MSGText:= 'Не передан входящий параметр p_messbody';
      RETURN;
    END IF;
    
    BEGIN
       SELECT GUID,
         RequestTime,
         TaxPeriod,
         ObjectId,
         SystemId,
         SystemNodeId
    INTO v_guid_req,
         v_req_time_str,
         v_tax_period,
         v_client_code,
         v_client_sysid,
         v_client_node
    FROM json_table (p_json_req,
               '$.GetTaxReportInfoReq'
               COLUMNS (
                 GUID            VARCHAR2(2000) PATH '$.GUID',
                 RequestTime     VARCHAR2(100)  PATH '$.RequestTime',
                 TaxPeriod       VARCHAR2(100)  PATH '$.TaxPeriod',
                 ObjectId        VARCHAR2(100)  PATH '$.ClientId.ObjectId',
                 SystemId        VARCHAR2(100)  PATH '$.ClientId.SystemId',
                 SystemNodeId    VARCHAR2(100)  PATH '$.ClientId.SystemNodeId'
               )
             );
    EXCEPTION 
      WHEN OTHERS THEN
        AddErrArray(v_errs, c_error_code_valide, SUBSTR('Непредвиденная ошибка при разборе входящего JSON-сообщения: '||sqlerrm, 1, 300));
        RAISE E_VALIDATION_FAILED;
    END;
    
    v_buf_rec.T_ID           := 0;
    v_buf_rec.T_QMESSLOGID   := p_worklogid;
    v_buf_rec.T_REQSYSDATE   := SYSDATE;
    v_buf_rec.t_guidreq      := v_guid_req;
    v_buf_rec.t_taxperiod    := v_tax_period;
    v_buf_rec.t_clientcode   := v_client_code;
    v_buf_rec.t_sessionid    := sys_context('userenv','sessionid');
    v_buf_rec.t_guidresp     := v_MsgID;
      
    BEGIN
      v_buf_rec.t_regdatereq := CAST(it_xml.char_to_timestamp_iso8601(v_req_time_str) AS DATE);
    EXCEPTION WHEN OTHERS THEN
      AddErrArray(v_errs, c_error_code_valide, 'Некорректный формат RequestTime');
    END;
  
    IF v_buf_rec.t_taxperiod IS NULL THEN
      AddErrArray(v_errs, c_error_code_valide, 'Не передан обязательный параметр TaxPeriod');
    ELSIF v_buf_rec.t_taxperiod > v_current_year THEN
      AddErrArray(v_errs, c_error_code_valide, 'Налоговый период не может быть больше текущего налогового периода ' || v_current_year);
    ELSIF v_buf_rec.t_taxperiod < C_MIN_YEAR THEN
      AddErrArray(v_errs, c_error_code_valide, 'Налоговый период не может быть меньше 2023');
    END IF;

    IF v_buf_rec.t_clientcode IS NULL THEN
      AddErrArray(v_errs, c_error_code_valide, 'Не передан обязательный параметр ClientId');
    END IF;

    
    IF NOT HasErrArray(v_errs) THEN
      v_party_id := GetPartyIDByCode(v_client_code);
      IF v_party_id = -1 OR v_party_id IS NULL THEN
        AddErrArray(v_errs, c_error_code_valide, 'Не найден клиент в СОФР по коду ClientCode = '||v_client_code);
      ELSE
        v_buf_rec.t_partyid := v_party_id;

        v_is_physical := GetLegalFormByPartyId(v_party_id);
        IF NOT v_is_physical  THEN
          AddErrArray(v_errs, c_error_code_valide, 'Клиент не является физическим лицом');
        END IF;

        v_has_contract := GetHasContrByPartyId(v_party_id, v_tax_period);
        IF NOT v_has_contract THEN
          AddErrArray(v_errs, c_error_code_valide, 'Отсутствует действующий договор в заданном налоговом периоде');
        END IF;
      END IF;
  END IF;

   -- Формирование ответа при наличии ошибок
  IF HasErrArray(v_errs) THEN
    o_MSGCode := c_error_code_valide;
    o_MSGText := v_errs.TO_CLOB;
    v_buf_rec.t_errordesc := o_MSGText;
    v_buf_rec.t_errorcode := c_error_code_valide;
    v_resp_body := generateJsonBodyNotification(v_guid_req, v_MsgID, false, v_errs );
  END IF; 
  
   -- вставка/апдейт буферной записи
  IF NOT HasErrArray(v_errs) THEN 
    v_buf_rec.t_repname := v_buf_rec.t_partyid || '_' || v_buf_rec.t_taxperiod || '_' || TO_CHAR(SYSDATE, 'DDMMYYYYHH24MISS');
  END IF;
  
  v_buf_rec.T_ID := InsertNptxReq( v_buf_rec );
   
  IF HasErrArray(v_errs) THEN
    SendTaxReportStatus(p_Json_notify => v_resp_body, 
                        p_buf_rec     => v_buf_rec,
                        p_MsgId       => v_buf_rec.t_guidresp,
                        o_MSGCode     => send_MSGCode,
                        o_MSGText     => send_MSGText);
  ELSE 
    
   it_log.log(p_msg => 'Переходим в TaxReportRun', p_msg_type => it_log.C_MSG_TYPE__DEBUG);
   /*В ином случае передаем на выполнение процедуры для формирования JSON*/ 
   TaxReportRun(p_buf_rec => v_buf_rec,
                o_MSGCode => send_MSGCode,
                o_MSGText => send_MSGText);
  END IF;
  
  it_log.log(p_msg => 'Конец обработки запроса GetTaxReportRequest p_worklogid: ' || p_worklogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
  
  IF(send_MSGCode = 0) THEN
     COMMIT;
     RETURN;                            
  END IF;
  
  o_MSGCode := send_MSGCode;
  o_MSGText := send_MSGText;
  ROLLBACK;
  
 
    
  EXCEPTION
    WHEN E_VALIDATION_FAILED THEN
      
      o_MSGCode := C_ERROR_CODE_VALIDE;
      o_MSGText := 'Произошла непридведенная во время валидации запроса: ' || SQLERRM;
      
      it_log.log(p_msg => o_MSGText || '  p_worklogid: ' || p_worklogid, p_msg_type => it_log.C_MSG_TYPE__ERROR);
      
      v_buf_rec.t_errordesc :=  v_errs.TO_CLOB;
      v_buf_rec.t_errorcode :=  C_ERROR_CODE_VALIDE;
      v_buf_rec.T_ID := InsertNptxReq( v_buf_rec );
      
      IF v_guid_req IS NOT NULL THEN
         v_resp_body := generateJsonBodyNotification(v_guid_req,v_MsgID, false,v_errs );
         it_log.log(p_msg => 'v_resp_body: ' || v_resp_body, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
         
         SendTaxReportStatus(p_Json_notify => v_resp_body, 
                             p_buf_rec     => v_buf_rec,
                             p_MsgId       => v_buf_rec.t_guidresp,
                             o_MSGCode     => send_MSGCode,
                             o_MSGText     => send_MSGText);
      END IF;
      ROLLBACK;
    WHEN OTHERS THEN
      --Ошибка которую не обработали, отправляем в СИ
      o_MSGCode := C_ERROR_CODE_VALIDE;
      o_MSGText := 'Произошла непридведенная во время валидации запроса: ' || SQLERRM;
      it_log.log(p_msg => o_MSGText || '  p_worklogid: ' || p_worklogid, p_msg_type => it_log.C_MSG_TYPE__ERROR);
      
      AddErrArray(v_errs, o_MSGCode, o_MSGText);
      
      IF(nvl(v_buf_rec.T_ID,0) <> 0) THEN
        --собираем ошибку в буферную таблицу                   
        UpdateErrorNptxReq(v_errs.to_clob, C_ERROR_CODE_VALIDE, v_buf_rec.t_id);
      ELSE 
         v_buf_rec.t_errordesc :=  v_errs.TO_CLOB;
         v_buf_rec.t_errorcode :=  o_MSGCode;
         v_buf_rec.T_ID := InsertNptxReq( v_buf_rec );
      END IF;
      
      IF v_guid_req IS NOT NULL THEN
         v_resp_body := generateJsonBodyNotification(v_guid_req, v_MsgID, false,v_errs );
         SendTaxReportStatus(p_Json_notify => v_resp_body, 
                             p_buf_rec     => v_buf_rec,
                             p_MsgId       => v_buf_rec.t_guidresp,
                             o_MSGCode     => send_MSGCode,
                             o_MSGText     => send_MSGText);
      END IF;
      ROLLBACK;
  END ParseAndValideTaxReport;

/*
  Отправка статуса обработки справки НФДЛ
  @since RSHB 120
  @qtest NO
  @param p_Json_notify JSON-уведомление (CLOB)
  @param p_HeaderMETA  Заголовок/метаданные (XMLTYPE), опционально
  @param p_buf_rec     Строка запроса (DUSERNPTXSHORTREQ_DBT%ROWTYPE)
*/
  PROCEDURE SendTaxReportStatus(p_Json_notify IN CLOB,
                                p_HeaderMETA  IN XMLTYPE := NULL,
                                p_buf_rec     IN DUSERNPTXSHORTREQ_DBT%ROWTYPE,
                                p_MsgId       IN VARCHAR2,
                                o_MSGCode     OUT INTEGER,
                                o_MSGText     OUT VARCHAR2) 
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    v_MSGCode     itt_q_message_log.msgcode%type;
    v_MSGText     itt_q_message_log.msgtext%type;
    v_MsgID       itt_q_message_log.msgid%type;
    
    PROCEDURE UpdateSendDateBuffer(v_buf_rec_id NUMBER) 
    IS
      PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE dusernptxshortreq_dbt d
           SET t_regdateresp = SYSDATE
         WHERE d.t_id = v_buf_rec_id;
      COMMIT;
    END;
  
  BEGIN
    o_MSGCode := C_SUCCESS_CODE;
    o_MSGText := '';
    v_MsgID:= p_MsgId;
    
    it_log.log(p_msg => 'QMsgId: ' || v_MsgID || '. SendTaxReportStatus отправка: ' || p_Json_notify,
                    p_msg_type => it_log.C_MSG_TYPE__DEBUG); 

    it_kafka.load_msg(io_msgid       => v_MsgID,
                      p_message_type => it_q_message.C_C_MSG_TYPE_A,
                      p_ServiceName  => 'SINV.GetTaxReportInfo',
                      p_Receiver     => C_C_SYSTEM_NAME,
                      p_MESSBODY     => p_Json_notify,
                      p_MessMETA     => p_HeaderMETA,
                      p_CORRmsgid    => p_buf_rec.t_guidreq,
                      o_ErrorCode    => v_MSGCode,
                      o_ErrorDesc    => v_MSGText);
    IF v_MSGCode <> 0 THEN
      o_MSGCode := v_MSGCode;
      o_MSGText := v_MSGText;
      it_log.log('Ошибка отправки SendTaxReportStatus: '||v_MSGText, it_log.C_MSG_TYPE__ERROR, p_Json_notify);
      UpdateErrorNptxReq(v_MSGText, C_ERROR_CODE_FAILED_KAFKA, p_buf_rec.t_id);
    ELSE 
      UpdateSendDateBuffer(p_buf_rec.t_id);
    END IF;
    
    it_log.log(p_msg => 'Конец обработки запроса SendTaxReportStatus', p_msg_type => it_log.C_MSG_TYPE__DEBUG);
    
    COMMIT;
    
  EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        it_log.log(p_msg => 'SendTaxReportStatus. Непредвиденная ошибка '||SQLERRM);
  END SendTaxReportStatus;

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
                                     o_MSGText   OUT VARCHAR2) IS
   v_hdr XMLTYPE;
   v_MSGCode     itt_q_message_log.msgcode%type;
   v_MSGText     itt_q_message_log.msgtext%type;
   v_Guid        itt_q_message_log.msgid%type := p_buf_rec.t_guidresp;
   v_GuidReq     itt_q_message_log.msgid%type := p_buf_rec.t_guidreq;
  BEGIN
     o_MSGCode := C_SUCCESS_CODE;
     o_MSGText := '';
     
     it_log.log(p_msg => 'Начало обработки запроса GenerateTaxReportRequest t_qmesslogid: '  || p_buf_rec.t_qmesslogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
     v_hdr := generateJsonHeaderIPS(p_GuidReq        => v_GuidReq,
                                    p_Guid           => v_Guid,
                                    p_OutputFileName => p_buf_rec.t_repname);
                                    
     it_kafka.load_msg_S3(p_msgid        => v_Guid, 
                          p_message_type => it_q_message.C_C_MSG_TYPE_R,
                          p_ServiceName  => 'IPS_DFACTORY.SendTaxReportInfo', -- Имя сервиса
                          p_Receiver     => it_ips_dfactory.C_C_SYSTEM_NAME,
                          p_CORRmsgid    => v_GuidReq,
                          p_MESSBODY     => p_Json_Nptx,  -- JSON c данными
                          p_MessMETA     => v_hdr,
                          p_isquery      => 1,
                          o_ErrorCode    => v_MSGCode,
                          o_ErrorDesc    => v_MSGText);
                          
    p_buf_rec.t_filegenbeg := systimestamp;
    
    IF V_MSGCODE <> 0 THEN
      p_buf_rec.t_errordesc := v_MSGText;
      p_buf_rec.t_errorcode := C_ERROR_CODE_GEN_FILE;
      o_MSGCode := V_MSGCODE;
      o_MSGText := v_MSGText;
    END IF; 
        
    it_log.log(p_msg => 'Конец обработки запроса GenerateTaxReportRequest t_qmesslogid: '  || p_buf_rec.t_qmesslogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
   
  END GenerateTaxReportRequest;

/*
  Запуск формирования краткой справки с контролем таймаута
  @since RSHB 120
  @qtest NO
  @param p_buf_rec  Строка запроса (dusernptxshortreq_dbt%ROWTYPE)
*/
  PROCEDURE TaxReportRun(p_buf_rec IN dusernptxshortreq_dbt%ROWTYPE,
                         o_MSGCode   OUT INTEGER,
                         o_MSGText   OUT VARCHAR2) IS
    v_MsgID        itt_q_message_log.msgid%TYPE := lower(it_q_message.get_sys_guid);                     
    v_out_json_data    CLOB := NULL;
    v_out_guidresp     VARCHAR2(64);
    v_buf_rec      dusernptxshortreq_dbt%ROWTYPE := p_buf_rec;
    v_errs         json_array_t := json_array_t();
    v_resp_body    CLOB;
    send_MSGCode   INTEGER := 0;
    send_MSGText   VARCHAR2(1000) := '';
  
  BEGIN
     it_log.log(p_msg => 'Начало TaxReportRun для клиента t_qmesslogid: '  || p_buf_rec.t_qmesslogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
     o_MSGCode := C_SUCCESS_CODE;
     o_MSGText := '';

    v_buf_rec.t_jsongenbeg := systimestamp;
    
    RSI_NPTXSHORT2025.CreateJSON(p_ClientID             => v_buf_rec.t_partyid,
                                 p_TaxPeriod            => v_buf_rec.t_taxperiod,
                                 p_GUID                 => v_out_guidresp,
                                 p_GUIDReq              => v_buf_rec.t_guidreq,
                                 p_SendTaxReportReqJSON => v_out_json_data,
                                 p_ErrorCode            => send_MSGCode,
                                 p_ErrorDesc            => send_MSGText);

    v_buf_rec.t_guidresp   := lower(v_out_guidresp);                                
    v_buf_rec.t_jsongenend := systimestamp;
    
    IF ((v_out_json_data IS NULL OR DBMS_LOB.GETLENGTH(v_out_json_data) = 0) AND send_MSGCode = 0) THEN
      send_MSGCode := C_ERROR_CODE_GEN_JSON;
      send_MSGText := 'Отсутствуют данные за период'; 
    END IF;
                                 
    IF send_MSGCode = 0 THEN
      GenerateTaxReportRequest(p_buf_rec     => v_buf_rec,
                               p_Json_Nptx   => v_out_json_data,
                               o_MSGCode     => send_MSGCode,
                               o_MSGText     => send_MSGText);
    ELSE 
      v_buf_rec.t_errorcode := send_MSGCode;
      v_buf_rec.t_errordesc := send_MSGText;
      o_MSGCode := send_MSGCode;
      o_MSGText := send_MSGText;
      
      it_log.log(p_msg => 'Ошибка в CreateJSON. t_qmesslogid: '  || p_buf_rec.t_qmesslogid || '. Текст ошибки: ' || send_MSGText || '. Код ошибки: ' || send_MSGCode, p_msg_type => it_log.C_MSG_TYPE__DEBUG);

      
      AddErrArray(v_errs, C_ERROR_CODE_GEN_JSON, send_MSGText);
      
      v_resp_body := generateJsonBodyNotification(v_buf_rec.t_guidreq, v_buf_rec.t_guidresp, false, v_errs );
      
      SendTaxReportStatus(p_Json_notify => v_resp_body, 
                          p_buf_rec     => v_buf_rec,
                          p_MsgId       => v_buf_rec.t_guidresp,
                          o_MSGCode     => send_MSGCode,
                          o_MSGText     => send_MSGText);
      
    END IF;
    
    UpdateNptxReq(v_buf_rec);
    
    it_log.log(p_msg => 'Конец TaxReportRun для клиента t_qmesslogid: '  || p_buf_rec.t_qmesslogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG);

  EXCEPTION
    WHEN OTHERS THEN
      --отправляем ответ SINV в случае ошибки 
      IF NOT HasErrArray(v_errs) THEN
         AddErrArray(v_errs, C_ERROR_CODE_GEN_JSON, 'Ошибка расчета данных: ' || SQLERRM);
      END IF;
      v_resp_body := generateJsonBodyNotification(v_buf_rec.t_guidreq, v_MsgID, FALSE,v_errs );
      

       SendTaxReportStatus(p_Json_notify => v_resp_body, 
                           p_buf_rec     => v_buf_rec,
                           p_MsgId       => v_buf_rec.t_guidresp,
                           o_MSGCode     => send_MSGCode,
                           o_MSGText     => send_MSGText);                   
      --собираем ошибку в буферную таблицу                   
      UpdateErrorNptxReq(v_errs.to_clob, C_ERROR_CODE_GEN_FILE, p_buf_rec.t_id);
      o_MSGCode := C_ERROR_CODE_GEN_FILE;
      o_MSGText := v_errs.to_clob;
      it_log.log(p_msg => 'Ошибка TaxReportRun: ' || o_MSGText || '. t_qmesslogid: '  || p_buf_rec.t_qmesslogid, p_msg_type => it_log.C_MSG_TYPE__ERROR);

      ROLLBACK;
  END TaxReportRun;


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
                                     o_messmeta  OUT XMLTYPE) IS

    v_MsgID      itt_q_message_log.msgid%TYPE := lower(it_q_message.get_sys_guid);
    vTurnOn     CHAR(1);
    v_buf_rec   DUSERNPTXSHORTREQ_DBT%ROWTYPE;
    --Заголовки из p_messmeta
    v_hdr_clob      CLOB;
    v_hdr           SYS.JSON_OBJECT_T;
    v_trace_id      VARCHAR2(200);
    v_document_path VARCHAR2(200);
    --Тело p_messbody
    v_is_success    BOOLEAN;
    v_obj           JSON_OBJECT_T;
    --В случае ошибки 
    v_errors_ips   JSON_ARRAY_T := JSON_ARRAY_T();
    v_errors       JSON_ARRAY_T := JSON_ARRAY_T();
    v_element      JSON_ELEMENT_T;
    v_object       JSON_OBJECT_T;
    --Параметры для ответа SINV
    v_resp_body    CLOB;
    v_resp_head    XMLTYPE;
    --Выходные параметры для вложенных процедур
    send_MSGCode   INTEGER;
    send_MSGText   VARCHAR2(1000);
    
/*
  Обновление записи буфера по результату выполнения
  @since RSHB 120
  @qtest NO
  @param v_buf_rec_id Идентификатор записи буфера
  @param p_ok         Признак успешного выполнения
  @param p_err        Описание ошибки (если есть)
*/
  PROCEDURE UpdateBuffer(v_buf_rec_id NUMBER,
                          p_ok         BOOLEAN, 
                          p_err        CLOB) 
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    IF p_ok THEN
      UPDATE dusernptxshortreq_dbt d
         SET d.t_documentpath = v_document_path,
             d.t_filegenend   = SYSTIMESTAMP,
             d.t_errorcode    = C_SUCCESS_CODE,
             d.t_errordesc    = NULL
       WHERE d.t_id = v_buf_rec_id;
    ELSE
      UPDATE dusernptxshortreq_dbt d
         SET d.t_filegenend   = SYSTIMESTAMP,
             d.t_documentpath = v_document_path,
             d.t_errorcode    = C_ERROR_CODE_GEN_FILE,
             d.t_errordesc    = p_err
       WHERE d.t_id = v_buf_rec_id;
    END IF;
    COMMIT;
  END;

  BEGIN
    it_log.log(p_msg => 'Начало обработки GenerateFileNotification p_worklogid: ' || p_worklogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG);

    o_MSGCode := C_SUCCESS_CODE;
    o_MSGText := 'Успешно';
    o_msgid   := v_MsgID;

    vTurnOn :=  nvl(RSB_Common.GetRegFlagValue(C_NAME_REGVAL_NPTXSHORT_ONOFF),'0');
    IF vTurnOn <> 'X' THEN
      o_MSGCode := -1;
      o_MSGText := 'Настройка '||C_NAME_REGVAL_NPTXSHORT_ONOFF||' отключена';
      RETURN;
    END IF;

    IF p_messbody IS NULL THEN
      o_MSGCode := C_INVALID_PARAM;
      o_MSGText:= 'Не передан входящий параметр p_messbody';
      RETURN;
    END IF;

    v_hdr_clob := getHeaderJson(p_messmeta);
    IF v_hdr_clob IS NULL THEN
      o_MSGCode := C_INVALID_PARAM; 
      o_MSGText := 'В p_messmeta нет KAFKA/Header';
      RETURN;
    END IF;

    v_hdr := SYS.JSON_OBJECT_T(v_hdr_clob);
    v_trace_id      := v_hdr.get_string('x-trace-id');
    v_document_path := v_hdr.get_string('x-document-path');

    IF v_trace_id IS NULL THEN
      o_MSGCode := C_INVALID_PARAM;
      o_MSGText := 'Не найден параметр x-trace-id в p_messmeta';
      RETURN;
    END IF;
    
    IF v_document_path IS NULL THEN
      o_MSGCode := C_INVALID_PARAM;
      o_MSGText := 'Не найден параметр x-document-path в p_messmeta';
      RETURN;
    END IF;

    BEGIN
      SELECT *
        INTO v_buf_rec
        FROM dusernptxshortreq_dbt
       WHERE t_guidreq = v_trace_id;
    EXCEPTION
      WHEN no_data_found THEN
        o_MSGCode := C_INVALID_PARAM;
        o_MSGText := 'Не найдена запись с GUID = '||v_trace_id;
        RETURN;
    END;

    BEGIN

      v_obj        := json_object_t.parse(p_messbody);
      v_is_success := v_obj.get_boolean('isSuccess');
      v_errors_ips := v_obj.get_array('errors');
      
      IF v_errors_ips.GET_SIZE() > 0 THEN
        FOR indx IN 0 .. v_errors_ips.get_size - 1
         LOOP
            v_element := v_errors_ips.get (indx);
            CASE
              WHEN v_element.is_object
               THEN
                   v_object := TREAT (v_element AS json_object_t);
                   AddErrArray(v_errors, C_ERROR_CODE_GEN_FILE, v_object.get_string('errorMessage'));
               ELSE
                  AddErrArray(v_errors, C_ERROR_CODE_GEN_FILE, SUBSTR('Непредвиденная ошибка при разборе входящего JSON-сообщения: '||sqlerrm, 1, 300));
            END CASE;
         END LOOP;
       END IF;
    EXCEPTION
      WHEN OTHERS THEN
        o_MSGCode := C_INVALID_PARAM;
        o_MSGText := SUBSTR('Непредвиденная ошибка при разборе входящего JSON-сообщения: '||sqlerrm, 1, 300);
        v_is_success := FALSE;
        AddErrArray(v_errors, C_ERROR_CODE_GEN_FILE, o_MSGText);
    END;

    v_resp_body:= generateJsonBodyNotification(p_GuidReq   => v_buf_rec.t_guidreq,
                                               p_Guid      => v_MsgID,
                                               p_IsSuccess => v_is_success,
                                               p_Errors    => v_errors);
    IF v_is_success THEN
      UpdateBuffer(v_buf_rec.t_id, TRUE, NULL);
      v_resp_head := generateJsonHeaderSINV(p_GuidReq      => v_buf_rec.t_guidreq,
                                            p_RequestId    => v_MsgID,
                                            p_FileName     => v_buf_rec.t_repname,
                                            p_DocumentPath => v_document_path);
                                            
      SendTaxReportStatus(p_Json_notify => v_resp_body,
                          p_HeaderMETA  => v_resp_head,
                          p_buf_rec     => v_buf_rec,
                          p_MsgId       => v_MsgID,
                          o_MSGCode     => send_MSGCode,
                          o_MSGText     => send_MSGText);
    ELSE
      UpdateBuffer(v_buf_rec.t_id, FALSE, v_errors.to_CLOB);
      SendTaxReportStatus(p_Json_notify => v_resp_body,
                          p_buf_rec     => v_buf_rec,
                          p_MsgId       => v_MsgID,
                          o_MSGCode     => send_MSGCode,
                          o_MSGText     => send_MSGText);
    END IF;
    
    it_log.log(p_msg => 'Конец обработки GenerateFileNotification p_worklogid: ' || p_worklogid, p_msg_type => it_log.C_MSG_TYPE__DEBUG);

  EXCEPTION
    WHEN OTHERS THEN
    o_MSGCode := C_INVALID_PARAM;
    o_MSGText := SUBSTR('Непредвиденная ошибка: '||SQLERRM, 1, 300); 
    
    AddErrArray(v_errors, C_ERROR_CODE_GEN_FILE, o_MSGText);
    
    IF nvl(v_buf_rec.t_id,v_trace_id) IS NOT NULL  THEN
      UpdateBuffer(nvl(v_buf_rec.t_id,v_trace_id),FALSE, v_errors.to_clob);
    END IF;

    -- ответ с ошибкой
    IF v_trace_id IS NOT NULL THEN
      v_resp_body := generateJsonBodyNotification(
                       p_GuidReq   => v_trace_id,
                       p_Guid      => v_MsgID,
                       p_IsSuccess => FALSE,
                       p_Errors    => v_errors
                     );
      v_buf_rec.t_guidreq := v_trace_id;
      v_buf_rec.t_qmesslogid := v_MsgID;
      SendTaxReportStatus(p_Json_notify => v_resp_body,
                          p_buf_rec     => v_buf_rec,
                          p_MsgId       => v_MsgID,
                          o_MSGCode     => send_MSGCode,
                          o_MSGText     => send_MSGText);
    END IF;
    
    it_log.log(p_msg => 'Ошибка GenerateFileNotification: ' || o_MSGText || '. p_worklogid: ' || p_worklogid, p_msg_type => it_log.C_MSG_TYPE__ERROR);
 
  END GenerateFileNotification;

END IT_TAXREPORT;
/
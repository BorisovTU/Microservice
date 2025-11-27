CREATE OR REPLACE PACKAGE BODY it_cft IS
  SERVICE_NAME_PROMISSORY_NOTES_ACCOUNTS CONSTANT VARCHAR2(31) := 'SendPromissoryNotesAccList';

  STATUS_MESSAGE_SUCCESS CONSTANT VARCHAR2(90)  := 'Процедура SendPromissoryNotesAccList: успешная загрузка счетов 91311 и привязка к векселям';
  STATUS_MESSAGE_FAIL    CONSTANT VARCHAR2(122) := 'Процедура SendPromissoryNotesAccList:  ошибки загрузки и привязки счетов 91311. Отчет отправлен на sofr_monitoring@rshb.ru';
  MAIL_TITLE             CONSTANT VARCHAR2(61)  := 'SendPromissoryNotesAccList: Ошибки при загрузке счетов  91311';
  DATA_NOT_FOUND_ERR     CONSTANT NUMBER(5)     := -20003;
  DL_ORDER_VSPAWN        CONSTANT NUMBER(3)     := 107;
  
  FICERT_NOTEKIND_PAWN_ACCOUNT CONSTANT NUMBER(3) := 102; -- Примечание Номер счета залога
  LLCLASS_KINDDOC_CREDITORDERS CONSTANT NUMBER(3) := 491;
  CONTRACT_STATUS_CLOSED       CONSTANT NUMBER(2) := 20;
  PAWN_ACCOUNT_EMAIL_GROUP     CONSTANT NUMBER(2) := 87;
  KIND_OPERATION_ACCEPT_PAWN   CONSTANT NUMBER(4) := 4924; -- Прием в залог
  FEATURE_SWITCH_REGVAL_NAME   VARCHAR2(50)       := 'РСХБ\ИНТЕГРАЦИЯ\ОБРАБОТКА ЗАПР ОТ ЦФТ BOSS-7047';
  
  C_SERVICE_NAME_SENDBROKERDEBT CONSTANT VARCHAR2(31)  := 'SendBrokerDebtAccountClient';
  C_NAME_REGVAL_SENDDEBT_OFF    CONSTANT VARCHAR2(256) := 'РСХБ\ИНТЕГРАЦИЯ С ЦФТ\ОКОНЧАНИЕ ПЕРИОДА РАБОТЫ'; /* Параметр для сервиса SendBrokerDebt */
  C_DEBT_FUNC_CODE              CONSTANT VARCHAR2(20)  := 'WrtOffDebtCFT'; /* Код funcObj */
  C_LLVALUES_LIST_HDR           CONSTANT NUMBER        := 4170;/* Справочник статических header-ов */
  C_SUCCESS_CODE                CONSTANT NUMBER        := 0;   /* Успешная отправка*/ 
  C_ERROR_CODE                  CONSTANT NUMBER        := 1;   /* Ошибка*/ 
  
  -- Запись Счет для PromissoryNotesAccountUdpate
  TYPE Account_rec IS RECORD (
    ContractID  VARCHAR2(30), -- ContractId\ObjectId
    AccountNumber  dAccount_dbt.t_account%type,
    ContractNumber dSpGround_dbt.t_XLD%type,
    ContractDate   DATE
  );
  TYPE Account_t IS TABLE OF Account_rec INDEX BY PLS_INTEGER;
  
  -- Запись Ошибка для PromissoryNotesAccountUdpate
  TYPE Error_rec IS RECORD (
    AccountInfo   Account_rec,
    ErrorCode     Number(10),
    ErrorMessage  VARCHAR(150)
  );
  TYPE Error_t IS TABLE OF Error_rec INDEX BY PLS_INTEGER;
  
  TYPE change_depository_rec IS RECORD (
    guid                VARCHAR2(50),
    request_time        TIMESTAMP,
    debt_id_sofr        NUMBER,
    dbo_number          VARCHAR2(30),
    carry_debt_cft_id   VARCHAR2(30),
    carry_date          DATE,
    carry_debt_sum      NUMBER,
    carry_debt_sum_cur  VARCHAR2(10),
    carry_debt_account  VARCHAR2(64),
    carry_debt_ground   VARCHAR2(4000), 
    client_object_id    VARCHAR2(30)
  );

  TYPE change_depository_tab IS TABLE OF change_depository_rec;
  
  -- Упаковщик исходящих сообшений в ЦФТ через KAFKA
  PROCEDURE out_pack_message(p_message     it_q_message_t
                            ,p_expire      DATE
                            ,o_correlation OUT VARCHAR2
                            ,o_messbody    OUT CLOB
                            ,o_messmeta    OUT XMLTYPE) AS
    v_rootElement   itt_kafka_topic.rootelement%type;
    vj_in_messbody  clob;
    vj_out_messbody clob;
    vz_GUID         itt_q_message_log.msgid%type;
    vz_GUIDReq      itt_q_message_log.corrmsgid%type;
    vz_ErrorCode    itt_q_message_log.msgcode%type;
    vz_ErrorDesc    itt_q_message_log.msgtext%type;
    v_select        varchar2(2000);
  BEGIN
    
    o_messmeta := p_message.MessMETA;
    
    BEGIN
      SELECT t.rootelement
        INTO v_rootElement
        FROM itt_kafka_topic t
       WHERE t.system_name = C_C_SYSTEM_NAME
         AND t.servicename = p_message.ServiceName
         AND t.queuetype = it_q_message.C_C_QUEUE_TYPE_OUT;
    EXCEPTION
      WHEN no_data_found THEN
        raise_application_error(-20000
                               ,'Сервис ' || p_message.ServiceName || ' не зарегистрирован как ' || it_q_message.C_C_QUEUE_TYPE_OUT || ' для KAFKA/' ||
                                C_C_SYSTEM_NAME);
    END;
    
    vj_in_messbody := p_message.Messbody;
   IF p_message.ServiceName NOT IN (C_C_SYSTEM_NAME || '.' || C_SERVICE_NAME_SENDBROKERDEBT) THEN
    IF (vj_in_messbody IS NOT NULL) THEN
      vj_out_messbody := vj_in_messbody;
      v_select        := 'select json_value(:1 ,''$."' || v_rootElement || '".GUID'') from dual';
      
      EXECUTE IMMEDIATE v_select
         INTO vz_GUID
        USING vj_out_messbody;
      
      IF vz_GUID IS NULL THEN
        raise_application_error(-20000, 'Для сервиса ' || p_message.ServiceName || CHR(10) || ' зарегистрированного как ' || it_q_message.C_C_QUEUE_TYPE_OUT ||
                                        ' для KAFKA/' || C_C_SYSTEM_NAME || CHR(10) || ' ожидался RootElement ' || v_rootElement || ' (ошибка формата JSON)');
      ELSIF p_message.msgid != vz_GUID THEN
        raise_application_error(-20001, 'Значение Msgid != GUID ! ');
      ELSIF p_message.message_type = it_q_message.C_C_MSG_TYPE_R AND UPPER(v_rootElement) NOT LIKE '%REQ' THEN
        raise_application_error(-20001, 'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || CHR(10) ||
                                        ' должно быть ответом от сервиса ' || p_message.ServiceName || ' сформирован запрос на отправку! ');
      ELSIF p_message.message_type = it_q_message.C_C_MSG_TYPE_A AND UPPER(v_rootElement) NOT LIKE '%RESP' THEN
        raise_application_error(-20001, 'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || CHR(10) ||
                                        ' должно быть запросом к сервису ' || p_message.ServiceName || ' сформирован  ответ на отправку! ');
      END IF;
      
      IF p_message.message_type = it_q_message.C_C_MSG_TYPE_A THEN
        v_select := 'select json_value(sJSON ,''$."' || v_rootElement || '".GUIDReq'' )
                           ,json_value(sJSON ,''$."' || v_rootElement || '".ErrorList.Error[0].ErrorCode'')
                           ,json_value(sJSON ,''$."' || v_rootElement || '".ErrorList.Error[0].ErrorDesc'')
                       from (select :1 as sJSON from dual)';
        
        EXECUTE IMMEDIATE v_select
          INTO vz_GUIDReq, vz_ErrorCode, vz_ErrorDesc
          USING vj_out_messbody;
        
        IF vz_GUIDReq IS NULL THEN
          raise_application_error(-20001, 'Отсутствует обязательный элемент GUIDReq! ');
        END IF;
        
        IF vz_ErrorCode IS NULL THEN
          raise_application_error(-20001, 'Отсутствует обязательный элемент ErrorCode! ');
        END IF;
        
        IF p_message.CORRmsgid != vz_GUIDReq THEN
          raise_application_error(-20001, 'Значение CORRmsgid != GUIDReq! ');
        END IF;
        
        IF p_message.MSGCode != vz_ErrorCode THEN
          raise_application_error(-20001, 'Значение MSGCode != ErrorCode! ');
        END IF;
        
        IF p_message.MSGCode != 0
           AND (vz_ErrorDesc IS NULL or vz_ErrorDesc != p_message.MSGText) THEN
          raise_application_error(-20001, 'Значение ErrorDesc должно быть "' || p_message.MSGText || '"! ');
        END IF;
      END IF;
    ELSE
      IF p_message.message_type = it_q_message.C_C_MSG_TYPE_A
         AND p_message.MSGCode != 0 THEN
        SELECT json_object(v_rootElement VALUE
                           json_object('GUID' VALUE p_message.msgid
                                      ,'GUIDReq' VALUE p_message.CORRmsgid
                                      ,'RequestTime' VALUE p_message.RequestDT
                                      ,'ErrorList' VALUE json_object('Error' VALUE json_array(json_object('ErrorCode' VALUE to_char(p_message.MSGCode))
                                                                                             ,json_object('ErrorDesc' VALUE p_message.MSGText)))) FORMAT JSON)
          INTO vj_out_messbody
          FROM dual;
      ELSE
        raise_application_error(-20001, 'Отправляемое сообщение не должно быть пустое! ');
      END IF;
    END IF;
   END IF;
   
   IF p_message.ServiceName IN (C_C_SYSTEM_NAME || '.' || C_SERVICE_NAME_SENDBROKERDEBT) THEN
     vj_out_messbody := p_message.MessBODY;
   END IF;
    
    o_messbody := vj_out_messbody;
    o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
    
  END out_pack_message;
  
  -- Функция поиска ID договора залога
  FUNCTION GetSpGroundID(p_ContractNumber dSpGround_dbt.t_XLD%type, p_ContractDate DATE)
    RETURN NUMBER
  IS
    v_spGroundID dSpGround_dbt.t_spGroundID%type;
    v_cnt NUMBER(10);
  BEGIN
    SELECT t_spGroundID
      INTO v_spGroundID
      FROM dSpGround_dbt
     WHERE t_XLD        = p_ContractNumber
       AND t_SignedDate = p_ContractDate
       AND t_Kind       = DL_ORDER_VSPAWN
       AND t_DocLog     = LLCLASS_KINDDOC_CREDITORDERS;
       
    RETURN v_spGroundID;
    
  EXCEPTION 
    WHEN NO_DATA_FOUND THEN
      SELECT COUNT(1)
        INTO v_cnt
        FROM dSpGround_dbt
       WHERE t_XLD    = p_ContractNumber
         AND t_Kind   = DL_ORDER_VSPAWN
         AND t_DocLog = LLCLASS_KINDDOC_CREDITORDERS;
      
      IF v_cnt = 0 THEN
        RAISE_APPLICATION_ERROR(DATA_NOT_FOUND_ERR, 'Договор с таким номером не найден');
      ELSE
        RAISE_APPLICATION_ERROR(DATA_NOT_FOUND_ERR, 'Не совпадает дата у договора залога');
      END IF;
  END;
  
  -- Обновление/установка счета по векселям
  PROCEDURE PromissoryNotesAccountUdpate(p_account Account_rec) IS
    v_spgroundID dSpGround_dbt.t_spGroundID%type;
    v_notetext   dNoteText_dbt%rowtype;
    v_vsCount integer := 0;
  BEGIN
    FOR rec IN (SELECT lnk.t_bcid FROM dSpGround_dbt gnd
                 INNER JOIN dDl_order_dbt ord
                    ON ord.t_ground = gnd.t_spGroundID
                   AND ord.t_kind_operation = KIND_OPERATION_ACCEPT_PAWN
                   AND ord.t_contractKind   = DL_ORDER_VSPAWN
                   AND ord.t_contractStatus = CONTRACT_STATUS_CLOSED
                 INNER JOIN dVsOrdLnk_dbt lnk
                    ON lnk.t_contractID = ord.t_contractID
                WHERE gnd.t_XLD        = p_account.ContractNumber
                  AND gnd.t_SignedDate = p_account.ContractDate
                  AND gnd.t_Kind       = DL_ORDER_VSPAWN
                  AND gnd.t_DocLog     = LLCLASS_KINDDOC_CREDITORDERS)
    LOOP
      v_vsCount := v_vsCount + 1;
      v_notetext := NOTE_READ.get_note_row(p_object_type => RSB_SECUR.OBJTYPE_FICERT
                                          ,p_note_kind => FICERT_NOTEKIND_PAWN_ACCOUNT
                                          ,p_document_id => LPAD(rec.t_bcID, 10, '0'));
      IF v_notetext.t_ID IS NULL THEN
        NOTE_UTILS.save_note(p_object_type => RSB_SECUR.OBJTYPE_FICERT
                            ,p_note_kind => FICERT_NOTEKIND_PAWN_ACCOUNT
                            ,p_document_id => LPAD(rec.t_bcID, 10, '0')
                            ,p_note => p_account.AccountNumber
                            ,p_date => SYSDATE);
      ELSIF NOTE_READ.cast_to_varchar2(v_notetext.t_text) != p_account.AccountNumber THEN
        UPDATE dnotetext_dbt nt
           SET nt.t_text = note_read.cast_to_raw(p_value => p_account.AccountNumber),
               nt.t_date = trunc(SYSDATE),
               nt.t_time = to_date('01.01.0001', 'DD.MM.YYYY') + (SYSDATE - trunc(SYSDATE))
         WHERE nt.t_id = v_notetext.t_ID;
      END IF;
    END LOOP;
    
    IF v_vsCount = 0 THEN
      -- Проверить наличие договора
      v_spGroundID := GetSpGroundID(p_account.ContractNumber, p_account.ContractDate);
    END IF;
  END;

  -- Получить строку заданного формата
  FUNCTION GetFormattedString(p_value1 VARCHAR2, p_value2 VARCHAR2, p_value3 VARCHAR2, p_value4 VARCHAR2, p_value5 VARCHAR2)
    RETURN VARCHAR2
  IS
    v_result VARCHAR2(256);
  BEGIN
    v_result := RPAD(NVL(p_value1, '?'), 20, ' ') || 
                RPAD(NVL(p_value2, '?'), 25, ' ') || 
                RPAD(NVL(p_value3, '?'), 30, ' ') || 
                RPAD(NVL(p_value4, '?'), 25, ' ') || 
                p_value5;
    
    return v_result;
  END;

  --  BOSS-7047 Загрузка из ЦФТ номеров счетов, на которых отражаются суммы векселей, полученных в залог
  PROCEDURE SendPromissoryNotesAccList(p_worklogid INTEGER 
                                      ,p_messbody  CLOB 
                                      ,p_messmeta  XMLTYPE 
                                      ,o_msgid     OUT VARCHAR2 
                                      ,o_MSGCode   OUT INTEGER 
                                      ,o_MSGText   OUT VARCHAR2 
                                      ,o_messbody  OUT CLOB 
                                      ,o_messmeta  OUT XMLTYPE) IS
    
    
    v_accounts Account_t;
    v_errors Error_t;
    v_error  Error_rec;
    v_xml XMLTYPE;
    v_namespace VARCHAR2(128) := IT_KAFKA.get_namespace(p_system_name => IT_CFT.C_C_SYSTEM_NAME, p_rootelement => 'SendPromissoryNotesAccListReq');
    v_emailcontent CLOB;
    v_errtxt VARCHAR2(2000);
    v_msgid itt_q_message_log.msgid%type;
  BEGIN
    IF RSB_Common.GetRegFlagValue(FEATURE_SWITCH_REGVAL_NAME) != 'X' THEN
      RETURN;
    END IF;
    
    v_errors := Error_t();
    v_xml := it_xml.Clob_to_xml(p_messbody);
   
    SELECT extractValue(value(t), 'ClientContract/ContractId/ObjectId', v_namespace) ,
           extractValue(value(t), 'ClientContract/AccountNumber', v_namespace),
           extractValue(value(t), 'ClientContract/ContractNumber', v_namespace),
           it_xml.char_to_date(extractValue(value(t), 'ClientContract/ContractDate', v_namespace))
     BULK COLLECT INTO v_accounts
      FROM table(XMLSequence(extract(v_xml, '/SendPromissoryNotesAccListReq/ClientContractList/ClientContract', v_namespace))) t;
    
    IF v_accounts.count > 0 THEN
      FOR i IN v_accounts.first .. v_accounts.last LOOP
        BEGIN
          PromissoryNotesAccountUdpate(v_accounts(i));
        EXCEPTION
          WHEN OTHERS THEN
            v_error.AccountInfo  := v_accounts(i);
            v_error.ErrorCode    := abs(sqlcode);
            v_error.ErrorMessage := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
            v_errors(v_errors.count) := v_error;
        END;
      END LOOP;
    END IF;
    
    IF v_errors.count = 0 THEN
      it_event.RegisterEvent(p_SystemId    => C_C_SYSTEM_NAME,
                             p_ServiceName => SERVICE_NAME_PROMISSORY_NOTES_ACCOUNTS,
                             p_MsgBODY => STATUS_MESSAGE_SUCCESS,
                             p_MsgMETA => '<XML LevelInfo = "1"/>',
                             o_errtxt  => v_errtxt,
                             o_MsgID   => v_msgid);
    ELSE
      it_event.RegisterError( p_SystemId => C_C_SYSTEM_NAME,
                              p_ServiceName => SERVICE_NAME_PROMISSORY_NOTES_ACCOUNTS,
                              p_ErrorCode => 1,
                              p_ErrorDesc => STATUS_MESSAGE_FAIL,
                              p_LevelInfo => 7);
      v_emailcontent := GetFormattedString('ЦФТ id', 'Номер счета', 'Номер договора залога', 'Дата договора залога', 'Описание ошибки') || CHR(10);
      FOR i IN v_errors.first .. v_errors.last LOOP
        v_emailcontent := v_emailcontent || CHR(10) || GetFormattedString(v_errors(i).AccountInfo.ContractID, 
                                                                         v_errors(i).AccountInfo.AccountNumber,
                                                                         v_errors(i).AccountInfo.ContractNumber,
                                                                         to_char(v_errors(i).AccountInfo.ContractDate, 'DD.MM.YYYY'),
                                                                         v_errors(i).ErrorMessage);
      END LOOP;
      
      RSB_PAYMENTS_API.InsertEmailNotify(p_emailGroup => PAWN_ACCOUNT_EMAIL_GROUP
                                        ,p_head => MAIL_TITLE
                                        ,p_text => v_emailContent);
    END IF;
  
  EXCEPTION 
    WHEN OTHERS THEN
      it_event.RegisterError( p_SystemId => C_C_SYSTEM_NAME,
                              p_ServiceName => SERVICE_NAME_PROMISSORY_NOTES_ACCOUNTS,
                              p_ErrorCode => ABS(sqlcode),
                              p_ErrorDesc => SUBSTR(sqlerrm, 1, 300),
                              p_LevelInfo => 6);
  END;
  

 /*
  Получить код обьекта для ЦФТ
  @since RSHB 123
  @qtest NO
  @param p_partyId ID  клиента
  @return код клиента
 */
  FUNCTION GetCodeByPartyId(p_partyId IN NUMBER) RETURN VARCHAR2
  AS
    v_clientId VARCHAR2(35);
  BEGIN
    SELECT t_code 
      INTO v_clientId
      FROM dobjcode_dbt 
     WHERE t_objecttype = 3 
       AND t_codekind = 101 
       AND t_state = 0 
       AND t_objectid = p_partyId;
    
    RETURN v_clientId;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Ошибка при выполнении функции GetCodeByPartyId.' ||
            ' Описание: ' || SQLERRM); 
  END;
  
  /*
  Получить Тип клиента
  @since RSHB 123
  @qtest NO
  @param p_partyId ID  клиента
  @return тип клиента
 */
  FUNCTION GetClientTypeByPartyId(p_partyId IN NUMBER) RETURN VARCHAR2
  AS
    v_ClientType VARCHAR2(8);
  BEGIN
      SELECT CASE 
                 WHEN pr.t_legalform = 1 
                    THEN 'ЮЛ'  
                  WHEN   pr.t_legalform = 2 and (select t_isemployer from dpersn_dbt where  t_personid = pr.t_partyid) != CHR(88)
                    THEN 'ФЛ'
                  ELSE 'ИП'
             END 
       INTO  v_ClientType
       FROM dparty_dbt pr 
      WHERE pr.t_partyid = p_partyId;
    
    RETURN v_ClientType;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Ошибка при выполнении функции GetSfNumberByPartyId.' ||
            ' Описание: ' || SQLERRM); 
  END;  
  
 /*
  Получить номер договора
  @since RSHB 123
  @qtest NO
  @param p_DlContrId ID  договора
  @return номер договора
 */
  FUNCTION GetSfNumberByDlContrId(p_DlContrId IN NUMBER) RETURN VARCHAR2
  AS
    v_contractNumber VARCHAR2(65);
  BEGIN
      
    SELECT t_number
      INTO v_ContractNumber
      FROM dsfcontr_dbt d 
        INNER JOIN ddlcontr_dbt dl 
        ON d.t_id = dl.t_sfcontrid
      WHERE dl.t_dlcontrid =  p_DlContrId; 
    
    RETURN v_ContractNumber;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Ошибка при выполнении функции GetSfNumberByPartyId.' || 
            ' Описание: ' || SQLERRM);  
  END;
    
 /*
  Получить дату открытия договора
  @since RSHB 123
  @qtest NO
  @param p_DlContrId ID  договора
  @return номер договора
 */
  FUNCTION GetSfDateByDlContrId(p_DlContrId IN NUMBER) RETURN VARCHAR2
  AS
    v_contractDate Date;
  BEGIN
      
    SELECT t_dateconc
      INTO v_contractDate
      FROM dsfcontr_dbt d 
        INNER JOIN ddlcontr_dbt dl 
        ON d.t_id = dl.t_sfcontrid
      WHERE dl.t_dlcontrid =  p_DlContrId; 
    
    RETURN v_contractDate;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Ошибка при выполнении функции GetSfDateByDlContrId.' || 
            ' Описание: ' || SQLERRM);  
  END;  
  
  /*
  Получить код валюты
  @since RSHB 123
  @qtest NO
  @param p_FiId ID фин инструмента
  @return код валюты
 */
  FUNCTION GetCurrencyCodeByFiId(p_FiId IN NUMBER) RETURN VARCHAR2
  AS
    v_currency VARCHAR2(3);
  BEGIN
    SELECT t_ccy 
      INTO v_currency
      FROM dfininstr_dbt 
     WHERE t_fi_kind = 1 
       AND t_fiid = p_FiId;  
    
    RETURN v_currency;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Ошибка при выполнении функции GetCurrencyCodeByFiId.' || 
            ' Описание: ' || SQLERRM); 
  END; 
  
   /*
  Получить код филиал
  @since RSHB 123
  @qtest NO
  @param p_DboNumber номер ДБО
  @return код филиал
 */
  FUNCTION GetFilialCode(p_DboNumber IN VARCHAR2) RETURN VARCHAR2
  AS
    v_filialCode VARCHAR2(4) := '0000';
    v_firstTwo   VARCHAR2(2);
    v_thirdChar  VARCHAR2(1);
  BEGIN
    IF p_DboNumber IS NOT NULL AND LENGTH(p_DboNumber) >= 3 THEN
        v_thirdChar := SUBSTR(p_DboNumber, 3, 1);

        IF v_thirdChar IN ('-', '/') THEN
            v_firstTwo := SUBSTR(p_DboNumber, 1, 2);
            v_filialCode := RPAD(v_firstTwo, 4, '0');
        END IF;
    END IF;

    RETURN v_filialCode;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Ошибка при выполнении функции GetFilialCode.' || 
            ' Описание: ' || SQLERRM); 
  END; 
  
 /*
  Получить Признак возможности частичного списания
  @since RSHB 123
  @qtest NO
  @param p_IsDebtPart 'X' ИЛИ CHR(0)
  @return true/false
 */
  FUNCTION GetIsDebtPart(p_IsDebtPart IN VARCHAR2) RETURN VARCHAR2
  AS
    v_IsDebtPart VARCHAR2(6);
  BEGIN

    SELECT decode(p_IsDebtPart, CHR(88), 'true', 'false') 
      INTO v_IsDebtPart 
     FROM dual;
     
    RETURN v_IsDebtPart;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Ошибка при выполнении функции GetIsDebtPart. p_IsDebtPart: ' || p_IsDebtPart  || 
            ' Описание: ' || SQLERRM); 
  END; 
  
   /*
  Формирование XML-заголовка (META) для CFT
  @since RSHB 123
  @qtest NO
  @return XMLTYPE с META-заголовком
*/
  FUNCTION GenerateHeaderDebt(p_filialCode VARCHAR2)
   RETURN XMLTYPE 
  IS
   v_hdr     CLOB;
  BEGIN
    
   WITH TRACE_T AS    
     (SELECT LOWER(RAWTOHEX(SYS_GUID())) AS VAL FROM dual) 
   SELECT JSON_OBJECTAGG(t_name VALUE t_code) AS trace_json
    INTO v_hdr
     FROM (
       SELECT 'x-b3-traceid' AS t_name,
               val AS t_code,
               99 AS t_flag
          FROM trace_t
        UNION ALL
        SELECT 'x-b3-spanid' AS t_name,
               SUBSTR(val, 1, 16) AS t_code,
               100 AS t_flag
          FROM trace_t
        UNION ALL
        SELECT t_name,
               t_note AS t_code,
               t_flag
          FROM dllvalues_dbt
         WHERE t_list = C_LLVALUES_LIST_HDR
         UNION ALL
         SELECT 'x-to-branch',
               p_filialCode AS t_code,
               50 AS t_flag
         FROM dual
           )
    ORDER BY t_flag; 
    
    RETURN it_kafka.add_Header_Xmessmeta(p_Header => v_hdr);
    
 EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Ошибка при выполнении функции GenerateHeaderDebt. ' || 
            ' Описание: ' || SQLERRM); 
 END;
 
   
  /*
  Получить XML в виде CLOB для отправки задолженносткй
  @since RSHB 123
  @qtest NO
  @param p_FiId ID фин инструмента
  @return код валюты
 */
FUNCTION GetDebtMessBodyClob(p_MsgID      IN VARCHAR2,
                           p_FilialCode     IN VARCHAR2,
                           p_ClientCode     IN VARCHAR2,
                           p_ClientType     IN VARCHAR2,
                           p_ContractDate   IN VARCHAR2,
                           p_ContractNumber IN VARCHAR2,
                           p_IdSOFR         IN VARCHAR2,
                           p_CreateDate     IN VARCHAR2,
                           p_DebtSum        IN VARCHAR2,
                           p_Currency       IN VARCHAR2,
                           p_Account        IN VARCHAR2,
                           p_IsDebtPart     IN VARCHAR2,
                           p_DebtType       IN NUMBER ) RETURN CLOB
  AS
    v_messbody CLOB;
  BEGIN

   SELECT XMLSERIALIZE(
    DOCUMENT 
    XMLELEMENT(
      "SendBrokerDebtAccountClientReq",
      XMLATTRIBUTES(
        'http://www.rshb.ru/csm/sofr/send_broker_debt_account_client/202510/req' AS "xmlns"
      ),
      XMLELEMENT("GUID", p_MsgID),
      XMLELEMENT("RequestTime", TO_CHAR(SYSTIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.FF3')),
      XMLELEMENT("FilialCode", p_FilialCode), 
      XMLELEMENT("ClientInfo",
        XMLELEMENT("ClientId",
          XMLELEMENT("ObjectId", p_ClientCode)
        ),
        XMLELEMENT("ClientType", p_ClientType) 
      ),
      XMLELEMENT("ContractInfo",
        XMLELEMENT("ContractDate", p_ContractDate),
        XMLELEMENT("ContractNumber", p_ContractNumber) 
      ),
      XMLELEMENT("DebtInfo",
        XMLELEMENT("DebtId", 
          XMLELEMENT("ObjectId",p_IdSOFR),
          XMLELEMENT("SystemId",'SOFR')
        ),
        XMLELEMENT("CreateDate",p_CreateDate), 
        XMLELEMENT("Amount",
          XMLELEMENT("Value", p_DebtSum),
          XMLELEMENT("Currency", p_Currency)
        ),
        XMLELEMENT("Account", p_Account), 
        XMLELEMENT("PartialWriteOffPermitted", p_IsDebtPart), 
        XMLELEMENT("Type",
          XMLELEMENT("RecordCode", p_DebtType)
        )
      )
    )
    AS CLOB INDENT SIZE = 2
  )
  INTO v_messbody
  FROM dual;      
 
    RETURN v_messbody;
  EXCEPTION
    WHEN OTHERS THEN
     RAISE_APPLICATION_ERROR(-20010, 'Ошибка во время формирования XML'|| 
            ' Описание: ' || SQLERRM); 
  END;
 
  /*
  Сравнить текущую дату с параметров
  @since RSHB 123
  @qtest NO
  @param p_time_str Текстовый формат параметра HH24MI
  @return больше ли текущая дата чем параметр?
 */ 
  FUNCTION IsAfterTime(p_time_str VARCHAR2)
    RETURN BOOLEAN
  IS
      v_target_date DATE;
  BEGIN
      IF LENGTH(p_time_str) != 4 OR NOT REGEXP_LIKE(p_time_str, '^\d{4}$') THEN
          RAISE_APPLICATION_ERROR(-20002, 'Некорректный формат времени: ' || p_time_str);
      END IF;

      v_target_date := TO_DATE(TO_CHAR(SYSDATE, 'ddmmyyyy') || p_time_str, 'ddmmyyyyhh24mi');

      RETURN SYSDATE > v_target_date;
  END; 
  
 /*
  Изменение таблицы duserdebttocft_dbt поле t_isinwork
  @since RSHB 123
  @qtest NO
  @param p_id Идентификатор таблицы duserdebttocft_dbt
 */ 
  PROCEDURE UpdateUserDebToCft(p_id NUMBER)
    
  IS
  BEGIN
      UPDATE duserdebttocft_dbt
         SET t_isinwork = CHR(88)
       WHERE t_id = p_id;
  END; 
  
  /*
  Изменение таблицы duserdebttocft_dbt поле t_isinwork
  @since RSHB 123
  @qtest NO
  @param p_DlContrId Идентификатор договора
  @param p_DebtSumCur Идентификатор таблицы duserdebttocft_dbt
 */ 
  PROCEDURE UpdateUserDebToCft(p_DlContrId NUMBER, 
                               p_DebtSumCur  NUMBER)
    
  IS
  BEGIN
      UPDATE duserdebttocft_dbt
         SET t_isinwork = CHR(88)
       WHERE t_dlcontrid  = p_DlContrId
         AND t_debtsumcur = p_DebtSumCur;
  END;
  

   /*
  Отправка данных в топик 
  @since RSHB 123
  @qtest NO
  @param p_DlContr ID договора
  @param p_CurrencyId ID фин инструмента
 */
   PROCEDURE SendDebtListToCFT(p_DlContr    IN  NUMBER DEFAULT -1,
                               p_CurrencyId IN  NUMBER DEFAULT -1)
   IS
    v_messbody         CLOB;
    v_HeaderMETA       XMLTYPE;
    v_MsgID            itt_q_message_log.msgid%TYPE;
    
    v_clientCode       VARCHAR2(35);
    v_clientType       VARCHAR2(16);
    v_contractNumber   VARCHAR2(64); 
    v_contractDate     DATE;
    v_currency         VARCHAR2(3);
    v_filialCode       VARCHAR2(4); 
    v_IsDebtPart       VARCHAR2(8);
    
    v_TimeOff           VARCHAR2(16);
    o_ErrorCode         NUMBER;
    o_ErrorDesc         VARCHAR2(1000);

   BEGIN
      o_ErrorCode := C_SUCCESS_CODE;
      o_ErrorDesc := 'OK';
      
    v_TimeOff :=  nvl(RSB_Common.GetRegStrValue(C_NAME_REGVAL_SENDDEBT_OFF),'-');
    IF IsAfterTime(v_TimeOff) THEN
      RETURN;
    END IF;   

  
  
 FOR rec IN ( SELECT t.t_id,
              t.t_debtsourceid,
              t.t_enterdate,
              t.t_debtdate,
              t.t_partyid,
              t.t_dlcontrid,
              t.t_sfcontrid,
              t.t_debtsum,
              t.t_debtsumcur,
              t.t_debtsort,
              t.t_isdebtpart,
              t.t_accountdebttocft,
              t.t_debttype,
              t.t_clienttype,
              t.t_isfactdebt,
              t.t_isinwork,
              t.t_sysdate
      FROM 
      (SELECT row_number() over (PARTITION BY c.t_dlcontrid, c.t_debtsumcur ORDER BY c.t_debtsort) as row_number, 
              c.*
        FROM duserdebttocft_dbt c
       WHERE   NVL(c.t_isfactdebt,CHR(0)) != CHR(88)
         AND ((NVL(c.t_isinwork,CHR(0)) != CHR(88) AND p_DlContr = -1) OR (p_DlContr > 0) )
         AND  (c.t_dlcontrid = p_DlContr OR p_DlContr = -1)
         AND  (c.t_debtsumcur = p_CurrencyId OR p_CurrencyId = -1)) t
       WHERE t.row_number = 1 )
  LOOP
    BEGIN  
    v_clientCode     := GetCodeByPartyId(rec.t_partyid);
    v_contractNumber := GetSfNumberByDlContrId(rec.t_dlcontrid);
    v_contractDate   := GetSfDateByDlContrId(rec.t_dlcontrid);
    v_currency       := GetCurrencyCodeByFiId(rec.t_debtsumcur);
    v_filialCode     := GetFilialCode(v_contractNumber);
    v_IsDebtPart     := GetIsDebtPart(NVL(rec.t_isdebtpart,CHR(0)));
    v_msgID          := lower(lower(it_q_message.get_sys_guid));
    
    IF (TRIM(rec.t_accountdebttocft) IS NULL) OR (rec.t_accountdebttocft = CHR(1)) THEN 
     RAISE_APPLICATION_ERROR(-20010, 'Не заполнено поле t_accountDebtToCft' ); 
   END IF;
    v_HeaderMETA := GenerateHeaderDebt(v_filialCode);
    v_messbody := GetDebtMessBodyClob(p_MsgID      => RAWTOHEX(SYS_GUID()),
                           p_filialCode            => v_filialCode,
                           p_ClientCode            => v_clientCode,
                           p_ClientType            => rec.t_clienttype,          
                           p_ContractNumber        => v_contractNumber,
                           p_ContractDate          => TO_CHAR(v_contractDate,'yyyy-mm-dd'),
                           p_IdSOFR                => rec.t_id,
                           p_CreateDate            => TO_CHAR(rec.t_debtdate,'yyyy-mm-dd'),
                           p_Debtsum               => TO_CHAR(rec.t_debtsum, '9999999990.00', 'NLS_NUMERIC_CHARACTERS = ''. '''),
                           p_IsDebtPart            => v_IsDebtPart,
                           p_Currency              => v_currency,
                           p_Account               => rec.t_accountdebttocft,
                           p_Debttype              => rec.t_debttype);    
                     
     it_kafka.load_msg(io_msgid     => v_msgID
                    ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                    ,p_ServiceName  => C_C_SYSTEM_NAME || '.' || C_SERVICE_NAME_SENDBROKERDEBT
                    ,p_Receiver     => C_C_SYSTEM_NAME
                    ,p_MESSBODY     => v_messbody
                    ,p_MessMETA     => v_HeaderMETA
                    ,o_ErrorCode    => o_ErrorCode
                    ,o_ErrorDesc    => o_ErrorDesc
                    );
                    
     IF o_ErrorCode > 0 THEN
        it_log.log(o_ErrorDesc,it_log.C_MSG_TYPE__ERROR, v_messbody);
        o_ErrorDesc := 'Произошла ошибка при отправке: ' || o_ErrorDesc;
        RSB_DEBTSUM.AddToLog(p_Type      => C_ERROR_CODE,
                             p_DlContrID => rec.t_dlcontrid,
                             p_Text      => o_ErrorDesc);
     ELSE 
       /*Если процедура отправки вызывалась без указания конкретного договора и валюты
         Update по договору и валюте
       */
       IF ( p_DlContr = -1 AND p_CurrencyId = -1 ) THEN
         UpdateUserDebToCft(rec.t_dlcontrid, rec.t_debtsumcur);
       END IF;
       
     END IF;
     
     COMMIT;

    EXCEPTION WHEN OTHERS 
      THEN
        o_ErrorDesc := 'Ошибка формирования сообщения в топик: ' || SQLERRM;
                                     
        it_log.log(p_msg      => o_ErrorDesc || CHR(10)       
                                    || 'p_DlContr: '    || p_DlContr   || CHR(10) 
                                    || 'p_CurrencyId: ' || p_CurrencyId, 
                   p_msg_type => it_log.C_MSG_TYPE__ERROR);
        
        RSB_DEBTSUM.AddToLog(p_Type      => C_ERROR_CODE,
                             p_DlContrID => rec.t_dlcontrid,
                             p_Text      => o_ErrorDesc);
                   
    END;
  END LOOP;
  
  COMMIT;
  
  EXCEPTION WHEN OTHERS 
    THEN
    o_ErrorDesc := 'Произошла непридведенная ошибка во время выполнения запроса: ' || SQLERRM; 
    
    RSB_DEBTSUM.AddToLog(p_Type      => C_ERROR_CODE,
                         p_DlContrID => p_DlContr,
                         p_Text      => o_ErrorDesc);
                             
    it_log.log(p_msg => o_ErrorDesc || ' p_DlContr: '    || p_DlContr 
                                    || ' p_CurrencyId: ' || p_CurrencyId, 
               p_msg_type => it_log.C_MSG_TYPE__ERROR);
               
    ROLLBACK; 
   END;
   
  /*
  Обработка входящих сообщений 
  @since RSHB 123
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
   PROCEDURE WrtoffDebtSumFRomCFT (p_worklogid INTEGER,
                                   p_messbody  CLOB,
                                   p_messmeta  XMLTYPE,
                                   o_msgid     OUT VARCHAR2,
                                   o_MSGCode   OUT INTEGER,
                                   o_MSGText   OUT VARCHAR2,
                                   o_messbody  OUT CLOB,
                                   o_messmeta  OUT XMLTYPE)  
     
   IS

   v_MsgID      itt_q_message_log.msgid%TYPE := lower(it_q_message.get_sys_guid);
   v_tab        it_cft.change_depository_tab := it_cft.change_depository_tab();
   v_namespace  VARCHAR2(128) := it_kafka.get_namespace(p_system_name => IT_CFT.C_C_SYSTEM_NAME, p_rootelement => 'NotifyBrokerDebtClientReq');

   row_userdebttocft duserdebttocft_dbt%ROWTYPE;
   v_fiid      NUMBER;
   
  -- Текст ошибок
  v_errorText  VARCHAR2(4000);
  -- Пользовательские исключения
  currency_not_found EXCEPTION;
  currency_mismatch  EXCEPTION;
  empty_account      EXCEPTION;
  debt_sum_mismatch  EXCEPTION;
  empty_create_date  EXCEPTION;
  
  PRAGMA EXCEPTION_INIT(currency_not_found, -20001);
  PRAGMA EXCEPTION_INIT(currency_mismatch,  -20002);
  PRAGMA EXCEPTION_INIT(empty_account,      -20003);
  PRAGMA EXCEPTION_INIT(debt_sum_mismatch,  -20004);
  PRAGMA EXCEPTION_INIT(empty_create_date,  -20005);
   
   BEGIN
    o_MSGCode := C_SUCCESS_CODE;
    o_MSGText := 'Ок';
    o_msgid   := v_MsgID;


    IF p_messbody IS NULL THEN
      o_MSGCode := C_ERROR_CODE;
      o_MSGText:= 'Не передан входящий параметр p_messbody';
      RETURN;
    END IF;
      
  SELECT guid,
         CAST(it_xml.char_to_timestamp_iso8601(request_time) AS DATE) request_time,
         debt_id_sofr,
         dbo_number,
         carry_debt_cft_id,
         carry_date,
         TO_NUMBER(carry_debt_sum, '9999999990D999999999', 'NLS_NUMERIC_CHARACTERS=''.,''') AS carry_debt_sum,
         carry_debt_sum_cur,
         carry_debt_account,
         carry_debt_ground,
         client_object_id     
  BULK COLLECT INTO v_tab
  FROM XMLTABLE(
    xmlnamespaces(default 'http://www.rshb.ru/csm/sofr/notify_broker_debt_client/202510/req'),
    '/NotifyBrokerDebtClientReq'
    PASSING XMLTYPE(p_messbody)
    COLUMNS
      guid                 VARCHAR2(50)   PATH 'GUID',
      request_time         VARCHAR2(50)   PATH 'RequestTime',
      debt_id_sofr         NUMBER         PATH 'DebtInfo/DebtId/ObjectId',
      dbo_number           VARCHAR2(30)   PATH 'DebtInfo/ContractNumber',
      carry_debt_cft_id    VARCHAR2(30)   PATH 'CarryDebt/TransactionId/ObjectId',
      carry_date           DATE           PATH 'CarryDebt/CreateDate',
      carry_debt_sum       VARCHAR2(256)  PATH 'CarryDebt/Amount/Value',
      carry_debt_sum_cur   VARCHAR2(10)   PATH 'CarryDebt/Amount/Currency',
      carry_debt_account   VARCHAR2(30)   PATH 'CarryDebt/Account',
      carry_debt_ground    VARCHAR2(4000) PATH 'CarryDebt/TransactionBasis',
      client_object_id     VARCHAR2(50)   PATH 'ClientId/ObjectId'
  ) t;

  FOR i IN 1 .. v_tab.COUNT LOOP

    BEGIN
     SELECT d.*
       INTO row_userdebttocft
       FROM duserdebttocft_dbt d 
      WHERE d.t_id = v_tab(i).debt_id_sofr;

    EXCEPTION WHEN OTHERS 
      THEN v_errorText :=  'Не найдена задолженность для списания в СОФР с идентификатором:' || v_tab(i).debt_id_sofr;
           RAISE;
    END;

    IF ((row_userdebttocft.t_isdebtpart = CHR(0) AND row_userdebttocft.t_debtsum !=  v_tab(i).carry_debt_sum)
     OR (row_userdebttocft.t_isdebtpart != CHR(0) AND row_userdebttocft.t_debtsum <  v_tab(i).carry_debt_sum)) THEN 
        v_errorText := 'Сумма списания задолженности в ЦФТ: ' || v_tab(i).carry_debt_sum || ' не соответствует  сумме задолженности в СОФР: ' || row_userdebttocft.t_debtsum;
        RAISE debt_sum_mismatch;
    END IF;

    BEGIN
      SELECT t_fiid
        INTO v_fiid
        FROM dfininstr_dbt 
       WHERE t_ccy = v_tab(i).carry_debt_sum_cur; 
    EXCEPTION 
      WHEN NO_DATA_FOUND THEN
        v_errorText := 'Не найден идентификатор валюты: ' || v_tab(i).carry_debt_sum_cur;
        RAISE currency_not_found;
      WHEN OTHERS THEN
        v_errorText := 'Ошибка поиска валюты: ' || v_tab(i).carry_debt_sum_cur || '. ' || SQLERRM;
        RAISE currency_not_found;
    END;
        
    IF (NOT (row_userdebttocft.t_debtsumcur = v_fiid)) THEN 
        v_errorText := 'Идентификатор валюты задолженности в ЦФТ с кодом: ' || v_fiid || ' не соответствует идентификатору валюты задолженности в СОФР: ' || row_userdebttocft.t_debtsumcur;
         RAISE currency_mismatch;
    END IF;
    
    IF v_tab(i).carry_date IS NULL THEN
       v_errorText := 'Не заполнена дата проводки в ЦФТ';
       RAISE empty_create_date;
    END IF;
      
    IF v_tab(i).carry_debt_account IS NULL THEN
       v_errorText := 'Не заполнен счет дебета по проводке ЦФТ';
       RAISE empty_account;
    END IF;
      
   INSERT INTO duserdebtfromcft_dbt (t_debtid,
                                     t_carrycftid,
                                     t_carrysum,
                                     t_carrysumcur,
                                     t_carryaccount,
                                     t_carryground,
                                     t_carrydate,
                                     t_istakeninsofr,
                                     t_sysdate)
    VALUES (v_tab(i).debt_id_sofr,
            v_tab(i).carry_debt_cft_id,
            v_tab(i).carry_debt_sum,
            v_tab(i).carry_debt_sum_cur,
            v_tab(i).carry_debt_account,
            v_tab(i).carry_debt_ground,
            v_tab(i).carry_date,
            CHR(0),
            SYSDATE);    

    funcobj_utils.save_task(p_objectid => v_tab(i).debt_id_sofr,
                            p_funcid   => funcobj_utils.get_func_id(p_code => C_DEBT_FUNC_CODE),
                            p_param    => v_tab(i).carry_debt_cft_id,
                            p_priority => funcobj_utils.get_priority_from_reserve(p_code => C_DEBT_FUNC_CODE));
    COMMIT; 
                              
  END LOOP;

  EXCEPTION 
    WHEN currency_not_found 
      OR currency_mismatch 
      OR empty_account 
      OR debt_sum_mismatch 
      OR empty_create_date
       THEN
        it_log.log(p_msg => 'Ошибка парсинга при чтении данных из топика. ' || SQLERRM       
                                  || ' p_worklogid: ' || p_worklogid , 
                   p_msg_type => it_log.C_MSG_TYPE__ERROR,p_msg_clob => o_messbody);
                   
        it_event.RegisterError( p_SystemId => C_C_SYSTEM_NAME,
                              p_ServiceName => C_SERVICE_NAME_SENDBROKERDEBT,
                              p_ErrorCode => ABS(sqlcode),
                              p_ErrorDesc => v_errorText,
                              p_LevelInfo => 8); 
       o_MSGCode := C_ERROR_CODE;
       o_MSGText := v_errorText;                     
    WHEN OTHERS 
      THEN 
       it_log.log(p_msg => 'Ошибка парсинга при чтении данных из топика. ' || SQLERRM       
                                    || ' p_worklogid: ' || p_worklogid , 
                   p_msg_type => it_log.C_MSG_TYPE__ERROR,p_msg_clob => o_messbody);
                   
       IF v_errorText IS NULL THEN
        v_errorText := 'Ошибка безакцептного списания задолженности в СОФР по проводке ЦФТ с идентификатором: ' || v_tab(0).carry_debt_cft_id || '. ' || sqlerrm;
       END IF;         
       it_event.RegisterError( p_SystemId => C_C_SYSTEM_NAME,
                              p_ServiceName => C_SERVICE_NAME_SENDBROKERDEBT,
                              p_ErrorCode => ABS(sqlcode),
                              p_ErrorDesc => v_errorText,
                              p_LevelInfo => 8); 
                               
       o_MSGCode := C_ERROR_CODE;
       o_MSGText := v_errorText;  
  END;
  
END it_cft;
/
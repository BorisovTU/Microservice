create or replace package body it_ips_dfactory is

  SERVICE_NAME_QI_NOTIFICATION CONSTANT VARCHAR2(31) := 'SendInvestorStatus';
  
  LIST_NUMBER_QI CONSTANT NUMBER(4) := 4191;
  
  DEFAULT_REASON_STR CONSTANT VARCHAR2(123) := 'В соответствии с п. 4.4. Регламента принятия решения о признании лица квалифицированным инвестором в АО \"Россельхозбанк\".';
  REGPATH_DEFAULT_SIGNER CONSTANT VARCHAR2(64) := 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\КВАЛИФИКАЦИЯ\ПОДПИСАНТ ПО УМОЛЧАНИЮ';
  
  PACKING_ERR CONSTANT NUMBER(5) := -20001;
  INVALID_PARAMETERS_ERR CONSTANT NUMBER(5) := -20002;
  DATA_NOT_FOUND_ERR CONSTANT NUMBER(5) := -20003;
  
  TYPE Party_rec IS RECORD (
    ID dParty_dbt.t_partyID%type,
    ShortName dParty_dbt.t_shortName%type,
    FullName dParty_dbt.t_name%type,
    FullNamePadej dParty_dbt.t_name%type,
    CFTCode dObjCode_dbt.t_code%type,
    IsLegalPerson NUMBER(1), -- ЮЛ
    IsEmployer NUMBER(1)     -- ИП
   );
   
  TYPE Signer_rec IS RECORD (
    ID dParty_dbt.t_partyID%type,
    ShortName dParty_dbt.t_shortName%type,
    Post dOfficer_dbt.t_post%type
  );
  
  -- получить значение справочника
  FUNCTION GetLlValueNote(p_list NUMBER, p_name VARCHAR2) RETURN VARCHAR2 is
    v_note VARCHAR2(2000);
  BEGIN
    SELECT NVL((SELECT t_note
                  FROM dLlValues_dbt
                 WHERE t_list = p_list
                   AND LOWER(t_name) = LOWER(p_name)), '')
      INTO v_note
      FROM dual;
      
    IF v_note IS null OR v_note = CHR(0) OR v_note = CHR(1) THEN
      RAISE_APPLICATION_ERROR(DATA_NOT_FOUND_ERR, 'Ошибка поиска значения справочника номер ' || p_list || ', параметр "' || p_name || '"');
    END IF;
    
    RETURN v_note;
  END;
  
  -- Упаковщик исходящх сообшений в JSON через KAFKA
  procedure out_pack_message_qi(p_message     it_q_message_t
                               ,p_expire      DATE
                               --,o_correlation OUT VARCHAR2
                               ,o_messbody    OUT CLOB
                               ,o_messmeta    OUT XMLType) AS
    vj_out_messbody  CLOB;
    vz_GUID          itt_q_message_log.msgid%type;
    
    v_facsimile      VARCHAR2(50);
    v_CFTCode        dObjCode_dbt.t_code%type;
    v_templateName   VARCHAR2(15);
    v_bucketName     VARCHAR2(30);
    v_outputFilename VARCHAR2(50);
    v_messmetaJSON   CLOB;
    
    v_x_system_from VARCHAR2(100) := GetLlValueNote(LIST_NUMBER_QI, 'x-system-from');
    v_x_system_to VARCHAR2(100) := GetLlValueNote(LIST_NUMBER_QI, 'x-system-to');
    v_x_template_type VARCHAR2(100) := GetLlValueNote(LIST_NUMBER_QI, 'x-template-type');
    v_x_output_type VARCHAR2(100) := GetLlValueNote(LIST_NUMBER_QI, 'x-output-type');
    v_x_template_params_paramName VARCHAR2(100) := GetLlValueNote(LIST_NUMBER_QI, 'x-template-params/paramName');
    v_x_template_params_paramType VARCHAR2(100) := GetLlValueNote(LIST_NUMBER_QI, 'x-template-params/paramType');
    v_x_service_name VARCHAR2(100) := GetLlValueNote(LIST_NUMBER_QI, 'x-service-name');
  BEGIN
    IF p_message.MessBODY IS NOT null THEN
      vj_out_messbody := p_message.MessBODY;
      
      SELECT JSON_VALUE(vj_out_messbody, '$.GUID' NULL ON ERROR) INTO vz_GUID FROM dual;
    
      IF vz_GUID IS NULL THEN
        RAISE_APPLICATION_ERROR(PACKING_ERR, 'Отсутствует обязательный элемент запроса - GUID!');
      END IF;
      
      SELECT extractValue(p_message.messmeta, 'NotificationInfo/facsimile'),
             extractValue(p_message.messmeta, 'NotificationInfo/CFTCode'),
             extractValue(p_message.messmeta, 'NotificationInfo/templateName'),
             extractValue(p_message.messmeta, 'NotificationInfo/bucketName'),
             extractValue(p_message.messmeta, 'NotificationInfo/outputFilename')
        INTO v_facsimile,
             v_CFTCode,
             v_templateName,
             v_bucketName,
             v_outputFilename
        FROM dual;
      
      IF v_facsimile IS null THEN
        RAISE_APPLICATION_ERROR(PACKING_ERR, 'Отсутствует обязательный элемент метаданных - facsimile!');
      END IF;
      
      IF v_CFTCode IS null THEN
        RAISE_APPLICATION_ERROR(PACKING_ERR, 'Отсутствует обязательный элемент метаданных - CFTCode!');
      END IF;
      
      IF v_templateName IS null THEN
        RAISE_APPLICATION_ERROR(PACKING_ERR, 'Отсутствует обязательный элемент метаданных - templateName!');
      END IF;
      
      IF v_bucketName IS null THEN
        RAISE_APPLICATION_ERROR(PACKING_ERR, 'Отсутствует обязательный элемент метаданных - bucketName!');
      END IF;
      
      IF v_outputFilename IS null THEN
        RAISE_APPLICATION_ERROR(PACKING_ERR, 'Отсутствует обязательный элемент метаданных - outputFilename!');
      END IF;
      
      SELECT json_object(key 'x-trace-id' value vz_GUID,
                         key 'x-template-type' value v_x_template_type,
                         key 'x-template-params' value 
                             json_arrayagg(json_object(key 'paramName' value v_x_template_params_paramName,
                                                       key 'paramType' value v_x_template_params_paramType,
                                                       key 'paramValue' value v_facsimile)
                                           ),
                         key 'x-template-name' value v_templateName,
                         key 'x-system-from' value v_x_system_from,
                         key 'x-system-to' value v_x_system_to,
                         key 'x-output-type' value v_x_output_type,
                         key 'x-output-file-name' value v_outputFilename,
                         key 'x-bucket-name' value v_bucketName,
                         key 'x-client-id' value v_CFTCode
                         RETURNING CLOB)
        INTO v_messmetaJSON
        FROM dual;

      o_messmeta := IT_Kafka.Add_Header_Xmessmeta(v_messmetaJSON);
    ELSE
        RAISE_APPLICATION_ERROR(PACKING_ERR, 'Отправляемый запрос не должен быть пустым! ');
    END IF;
    
    o_messbody := vj_out_messbody;
  END;

  -- Упаковка служебной информации для S3
  function out_pack_HederS3_Xmessmeta(p_msgid        itt_q_message_log.msgid%type -- GUID сообщения
                                     ,px_messmeta    xmltype
                                     ,p_len_MessBODY integer -- dbms_lob.getlength(p_message.MessBODY)
                                      ) return xmltype as
    v_S3              pls_integer;
    v_ErrorCode       pls_integer;
    v_ErrorDesc       varchar2(2000);
    vx_MessMETA       xmltype := px_messmeta;
    v_header          clob;
    v_S3xdatafilename varchar2(128);
  begin
    v_S3 := it_kafka.chk_from_KAFKAS3(p_Receiver => C_C_SYSTEM_NAME, p_len_MessBODY => p_len_MessBODY, o_ErrorCode => v_ErrorCode, o_ErrorDesc => v_ErrorDesc);
    if v_ErrorCode != 0
    then
      raise_application_error(-20000, v_ErrorDesc);
    end if;
    if v_S3 = 1
    then
      it_kafka.get_info_Xmessmeta(px_messmeta => vx_MessMETA, ocl_Нeader => v_header, o_S3xdatafilename => v_S3xdatafilename);
      v_S3xdatafilename := nvl(v_S3xdatafilename, p_msgid);
      select json_transform(v_Header, set '$."x-data-source"' = 'S3' replace on EXISTING returning clob) into v_Header from dual;
      select json_transform(v_Header, set '$."x-data-file-name"' = v_S3xdatafilename replace on EXISTING returning clob) into v_Header from dual;
      vx_messmeta := it_kafka.add_S3xdatafilename_Xmessmeta(p_S3xdatafilename => v_S3xdatafilename, px_messmeta => vx_messmeta);
      vx_messmeta := it_kafka.add_Header_Xmessmeta(p_Header => v_Header, px_messmeta => vx_messmeta);
    end if;
    return vx_MessMETA;
  end;

  
  /**
  * Упаковщик исходящих сообшений в Фабрику Документов IPS через KAFKA
  * @since RSHB 110
  * @qtest NO
  */
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype) as
    v_rootElement itt_kafka_topic.rootelement%type;
    v_msgFormat   itt_kafka_topic.msg_format %type;
  begin
    begin
      select t.rootelement
            ,t.msg_format
        into v_rootElement
            ,v_msgFormat
        from itt_kafka_topic t
       where t.system_name = C_C_SYSTEM_NAME
         and t.servicename = p_message.ServiceName
         and t.queuetype = it_q_message.C_C_QUEUE_TYPE_OUT;
    exception
      when no_data_found then
        raise_application_error(-20000
                               ,'Сервис ' || p_message.ServiceName || ' не зарегистрирован как ' || it_q_message.C_C_QUEUE_TYPE_OUT || ' для KAFKA/' || C_C_SYSTEM_NAME);
    end;
    if v_msgFormat = 'DIRECT'
    then
      o_messbody := p_message.MessBODY;
      o_messmeta := out_pack_HederS3_Xmessmeta(p_msgid => p_message.msgid, px_messmeta => p_message.MessMETA, p_len_MessBODY => dbms_lob.getlength(p_message.MessBODY));
      o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
      return;
    elsif p_message.ServiceName = C_C_SYSTEM_NAME || '.' || SERVICE_NAME_QI_NOTIFICATION
    then
      -- Не надо в упаковщике привязываться к сервису. Посмотри add_KafkaHeader_Xmessmeta
      out_pack_message_qi(p_message, p_expire, o_messbody, o_messmeta);
      o_messmeta := out_pack_HederS3_Xmessmeta(p_msgid => p_message.msgid, px_messmeta => o_messmeta, p_len_MessBODY => dbms_lob.getlength(o_messbody));
    else
      o_messmeta := out_pack_HederS3_Xmessmeta(p_msgid => p_message.msgid, px_messmeta => p_message.MessMETA, p_len_MessBODY => dbms_lob.getlength(p_message.MessBODY));
    end if;
    --Оборачиваем в root-элемент
    if  dbms_lob.getlength(p_message.MessBODY) > 0 then
      select JSON_OBJECT(v_rootElement value p_message.MessBODY FORMAT JSON returning clob) into o_messbody from dual;
    else
      o_messbody := null;
    end if; 
    o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
  end out_pack_message;
  
  -- Получить информацию о субъекте
  FUNCTION GetPartyInfo(p_partyID dParty_dbt.t_partyID%type) RETURN Party_rec IS
    v_party Party_rec;
  BEGIN
    v_party := NULL;
    v_party.ID := p_partyID;
    
    BEGIN
      SELECT p.t_shortName, p.t_Name, CASE WHEN p.t_legalForm = 1 THEN 1 ELSE 0 END
        INTO v_party.shortName, v_party.fullName, v_party.IsLegalPerson
        FROM dparty_dbt p
       WHERE p.t_partyID = p_partyID;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(DATA_NOT_FOUND_ERR, 'Ошибка поиска информации по субъекту с ID ' || p_partyID);
    END;
    
    v_party.fullNamePadej := v_party.fullName;
    
    -- Код ЦФТ
    SELECT NVL((SELECT t_code
                  FROM dObjCode_dbt
                 WHERE t_ObjectType = Rsb_Secur.OBJTYPE_PARTY
                   AND t_CodeKind = 101
                   AND t_bankCloseDate = to_date('01010001','ddmmyyyy')
                   AND t_ObjectID = p_partyID), '')
      INTO v_party.CFTCode
      FROM dual;
    
    IF v_party.IsLegalPerson = 0 THEN -- Для ФЛ проверим на ИП
      SELECT NVL((SELECT 1
                    FROM dpersn_dbt persn
                   WHERE persn.t_personid = p_partyID
                     AND persn.t_isemployer = 'X'), 0)
        INTO v_party.IsEmployer
        FROM dual;
    ELSE
      v_party.IsEmployer := 0;
    END IF;
  
    RETURN v_party;
  END;
  
  -- Получить данные по подписанту
  FUNCTION GetSignerInfo(p_signerID dParty_dbt.t_partyID%type) RETURN Signer_rec IS
    v_signer Signer_rec;
  BEGIN
    v_signer := NULL;
    v_signer.ID := p_signerID;
    
    BEGIN
     SELECT p.t_name, NVL(o.t_post, '')
       INTO v_signer.ShortName, v_signer.Post
       FROM dPerson_dbt p, dOfficer_dbt o
      WHERE p.t_partyID = p_signerID
        AND o.t_personID(+) = p.t_partyID;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(DATA_NOT_FOUND_ERR, 'Ошибка поиска информации по подписанту с ID ' || p_signerID);
    END;
  
    RETURN v_signer;
  END;
  
  -- Получить id субъекта
  FUNCTION GetPartyID(p_partyID VARCHAR2) RETURN dParty_dbt.t_partyID%type IS
    v_partyID dParty_dbt.t_partyID%type;
  BEGIN
    IF NOT p_partyID IS NULL THEN
      BEGIN
        SELECT party.t_partyID
          INTO v_partyID
          FROM dParty_dbt party
         WHERE party.t_partyID = p_partyID;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN RAISE_APPLICATION_ERROR(DATA_NOT_FOUND_ERR, 'Ошибка поиска информации по субъекту с ID ' || p_partyID);
      END;
    END IF;
    
    RETURN v_partyID;
  END;
  
  -- Получить ID клиента из запроса
  FUNCTION GetClientPartyID(p_messbody CLOB) RETURN dParty_dbt.t_partyID%type IS
    v_clientID VARCHAR2(50);
  BEGIN
    SELECT JSON_VALUE(p_messbody, '$.ClientID' NULL ON ERROR)
      INTO v_clientID
      FROM dual;
    
    IF v_clientID IS NULL THEN
      RAISE_APPLICATION_ERROR(INVALID_PARAMETERS_ERR, 'Не указан ID клиента');
    END IF;
    
    RETURN GetPartyID(v_clientID);
  END;
  
  -- Получить ID подписанта из запроса
  FUNCTION GetSignerPartyID(p_messbody CLOB) RETURN dParty_dbt.t_partyID%type IS
    v_signerID VARCHAR2(50);
    v_partyID dParty_dbt.t_partyID%type := -1;
  BEGIN
    SELECT JSON_VALUE(p_messbody, '$.SignerID' NULL ON ERROR)
      INTO v_signerID
      FROM dual;
    
    IF v_signerID IS NULL THEN -- Если подписант не задан, возьмем подписанта по умолчанию
      v_signerID := RSB_Common.GetRegIntValue(REGPATH_DEFAULT_SIGNER);
    END IF;
    
    IF v_signerID IS NULL THEN
      RAISE_APPLICATION_ERROR(INVALID_PARAMETERS_ERR, 'Не задан подписант и настройка "Подписант по умолчанию"');
    END IF;
    
    IF NOT v_signerID IS NULL THEN
      BEGIN
        SELECT person.t_partyid
          INTO v_partyID
          FROM dPerson_dbt person
         WHERE person.t_oper IN (SELECT ago.t_oper 
                                   FROM dAcsGroupOper_dbt ago
                                  WHERE ago.t_groupID = (SELECT ag.t_groupID FROM dAcsGroup_dbt ag WHERE ag.t_name = 'Подписанты отчетности по КИ'))
           AND person.t_oper = v_signerID;
      EXCEPTION 
        WHEN NO_DATA_FOUND THEN
          RAISE_APPLICATION_ERROR(INVALID_PARAMETERS_ERR, 'Подписант "' || v_signerID || '" не найден или не принадлежит группе "Подписанты отчетности по КИ"');
      END;
    END IF;
        
    RETURN v_partyID;
  END;
  
  -- Получить тип уведомления из запроса
  FUNCTION GetNotificationType(p_messbody CLOB) RETURN NUMBER IS
    v_notificationType NUMBER(3) := -1;
    v_value VARCHAR2(15);
  BEGIN
    SELECT JSON_VALUE(p_messbody, '$.NotificationType' NULL ON ERROR) INTO v_value FROM dual;
    
    IF v_value = NOTIFICATIONTYPE_RECOGNITION OR v_value = NOTIFICATIONTYPE_REJECTION OR 
       v_value = NOTIFICATIONTYPE_EXCLUSION OR v_value = NOTIFICATIONTYPE_CONFIRMATIONREQ THEN
      v_notificationType := v_value;
    ELSIF v_value is NULL THEN
      RAISE_APPLICATION_ERROR(INVALID_PARAMETERS_ERR, 'Не задан тип уведомления');
    ELSE
      RAISE_APPLICATION_ERROR(INVALID_PARAMETERS_ERR, 'Некорретный тип уведомления ' || v_value);
    END IF;
    
    RETURN v_notificationType;
  END;
  
  -- Получить наименование уведомления
  FUNCTION GetNotificationDescription(p_notificationType NUMBER) RETURN VARCHAR2 IS
    v_description VARCHAR2(50);
  BEGIN
    IF p_notificationType = NOTIFICATIONTYPE_RECOGNITION THEN
      v_description := 'Уведомление о признании лица КИ'; 
    ELSIF p_notificationType = NOTIFICATIONTYPE_REJECTION THEN
      v_description := 'Уведомление об отказе в признании КИ'; 
    ELSIF p_notificationType = NOTIFICATIONTYPE_EXCLUSION THEN
      v_description := 'Уведомление об исключении из реестра КИ'; 
    ELSIF p_notificationType = NOTIFICATIONTYPE_CONFIRMATIONREQ THEN
      v_description := 'Требование о подтверждении статуса КИ'; 
    ELSE
      v_description := 'Неизвестный тип уведомления по КИ'; 
    END IF;
    
    RETURN v_description;
  END;
  
  -- Получить причину отказа из запроса
  FUNCTION GetReason(p_messbody CLOB) RETURN VARCHAR2 IS
    v_reason VARCHAR2(1000);
  BEGIN
    SELECT JSON_VALUE(p_messbody, '$.Reason' NULL ON ERROR) INTO v_reason FROM dual;
    
    IF v_reason IS NULL THEN
      v_reason := DEFAULT_REASON_STR;
    END IF;
    
    RETURN SUBSTR(v_reason, 1, 320);
  END;
  
  -- Получить наименование клиента в предложном падеже
  FUNCTION GetClientNameMod(p_messbody CLOB) RETURN VARCHAR2 IS
    v_padej VARCHAR2(1000);
  BEGIN
    SELECT JSON_VALUE(p_messbody, '$.Padej' NULL ON ERROR) INTO v_padej FROM dual;
    
    RETURN SUBSTR(v_padej, 1, 320);
  END;
  
  -- Получить дату уведомления из запроса
  FUNCTION GetNotificationDate(p_messbody CLOB) RETURN DATE IS
    v_datestr VARCHAR2(100);
    v_date DATE;
  BEGIN
    SELECT JSON_VALUE(p_messbody, '$.NotificationDate' NULL ON ERROR) INTO v_datestr FROM dual;
    
    IF v_datestr IS NULL THEN
      RETURN TRUNC(SYSDATE);
    END IF;
    
    BEGIN
      v_date := TO_DATE(v_datestr, 'YYYY-MM-DD HH24:MI:SS');
    EXCEPTION WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(INVALID_PARAMETERS_ERR, 'Неверный формат даты ' || v_datestr);
    END;
    
    RETURN v_date;
  END;
  
  -- Получить наименование файла для подписанта
  FUNCTION GetFacsimile(p_signerID dParty_dbt.t_partyID%type) RETURN VARCHAR2 IS
    v_result VARCHAR2(100);
  BEGIN
    SELECT NVL(
        (SELECT replace(UTL_RAW.CAST_TO_VARCHAR2(nt.t_Text), CHR(0), '')
          FROM dnotetext_dbt nt
         WHERE nt.t_ObjectType = Rsb_Secur.OBJTYPE_PARTY
           AND nt.t_DocumentID = LPAD(p_signerID, 10, '0')
           AND nt.t_NoteKind = 109
           AND nt.t_ValidToDate >= TRUNC(SYSDATE)
           AND nt.t_Date <= TRUNC(SYSDATE)), CHR(0))
      INTO v_result
      FROM dual;

    IF v_result IS NULL OR v_result = CHR(0) OR v_result = CHR(1) OR INSTR(v_result, '@') <= 0 THEN
      RAISE_APPLICATION_ERROR(DATA_NOT_FOUND_ERR, 'Не удалось получить почтовый логин сотрудника (ID субъекта = ' || p_signerID || ') из соответствующего примечания');
    ELSE
      v_result := SUBSTR(v_result, 1, INSTR(v_result, '@') - 1); 
    END IF;
  
    RETURN v_result;
  END;
  
  -- Получить данные для процедуры out_pack_message
  PROCEDURE GetOutPackParams(p_clientData Party_rec
                            ,p_notificationType NUMBER
                            ,o_templateName OUT VARCHAR2
                            ,o_bucketName OUT VARCHAR2) IS
  BEGIN
    IF p_notificationType = NOTIFICATIONTYPE_RECOGNITION THEN
      o_templateName := 'od-1-05-';
    ELSIF p_notificationType = NOTIFICATIONTYPE_REJECTION
       OR p_notificationType = NOTIFICATIONTYPE_EXCLUSION THEN
      o_templateName := 'od-1-06-';
    ELSIF p_notificationType = NOTIFICATIONTYPE_CONFIRMATIONREQ THEN
      o_templateName := 'od-1-11-';
    END IF;
    
    IF p_clientData.IsLegalPerson = 1 OR p_clientData.IsEmployer = 1 THEN
      o_templateName := o_templateName || 'ul';
    ELSE
      o_templateName := o_templateName || 'fl';
    END IF;
    
    o_bucketName := 'ips-document-factory';
  END;
  
  -- Получить JSON для уведомления о признании
  PROCEDURE GetRecognitionData(p_clientData Party_rec
                              ,p_signerData Signer_rec
                              ,p_notificationDate DATE
                              ,o_data OUT CLOB
                              ,o_key OUT VARCHAR2) IS
  BEGIN
    o_key := 'SuccessfulRecognitionNotice';
    
    SELECT json_object(key 'ClientName' value p_clientData.ShortName,
                       key 'FullName' value p_clientData.FullName,
                       key 'ServicesInfo' value 'брокерское обслуживание',
                       key 'SecuritiesInfo' value ' в отношении акций, облигаций, ПИФ, иных ценных бумаг и ' ||
                                                  'Финансовых инструментов, а также в отношении банковских ' ||
                                                  'продуктов предназначенных для квалифицированных инвесторов '||
                                                  'в соответствии с Российским законодательством',
                       key 'FinancialInstrumentsInfo' value EMPTY_CLOB(),
                       key 'FullNamePadej' value p_clientData.FullNamePadej,
                       key 'RecognitionDateTime' value p_notificationDate,
                       key 'Post' value p_signerData.Post,
                       key 'ShortNameSignatory' value p_signerData.ShortName
                       RETURNING CLOB)
      INTO o_data
      FROM dual;
  END;
  
  -- Получить JSON для требования о подтверждении статуса КИ
  PROCEDURE GetConfirmationReqData(p_clientData Party_rec
                                  ,p_signerData Signer_rec
                                  ,p_notificationDate DATE
                                  ,o_data OUT CLOB
                                  ,o_key OUT VARCHAR2) IS
  BEGIN
    o_key := 'VerificationRecognitionNotice';
    
    SELECT json_object(key 'ClientName' value p_clientData.ShortName,
                       key 'Post' value p_signerData.Post,
                       key 'ShortNameSignatory' value p_signerData.ShortName,
                       key 'CurrentDateTime' value p_notificationDate
                       RETURNING CLOB)
      INTO o_data
      FROM dual;
  END;
  
  -- Получить JSON для уведомления об исключении/отказе
  PROCEDURE GetNegativeNotificationData(p_clientData Party_rec
                                       ,p_signerData Signer_rec
                                       ,p_notificationType NUMBER
                                       ,p_notificationDate DATE
                                       ,p_reason VARCHAR2
                                       ,o_data OUT CLOB
                                       ,o_key OUT VARCHAR2) IS
  BEGIN
    o_key := 'NegativeRecognitionNotice';
    
    SELECT json_object(key 'ClientName' value CASE WHEN p_clientData.IsLegalPerson = 1 THEN p_clientData.ShortName 
                                                                                       ELSE p_clientData.FullName END,
                       key 'IsRejection' value CASE WHEN p_notificationType = NOTIFICATIONTYPE_REJECTION THEN 'true' ELSE 'false' END FORMAT JSON,
                       key 'RejectionReason' value CASE WHEN p_notificationType = NOTIFICATIONTYPE_REJECTION THEN p_reason ELSE '' END,
                       key 'IsExclusion' value CASE WHEN p_notificationType = NOTIFICATIONTYPE_EXCLUSION THEN 'true' ELSE 'false' END FORMAT JSON,
                       key 'FullName' value p_clientData.FullName,
                       key 'IsCompletely' value CASE WHEN p_notificationType = NOTIFICATIONTYPE_EXCLUSION THEN 'true' ELSE 'false' END FORMAT JSON,
                       key 'Post' value p_signerData.Post,
                       key 'ShortNameSignatory' value p_signerData.ShortName,
                       key 'CurrentDateTime' value p_notificationDate
                       RETURNING CLOB)
      INTO o_data
      FROM dual;
    
    o_data := replace(o_data, to_clob(':null'), to_clob(':""'));
  END;
  
  -- Получить JSON для формирования уведомления
  PROCEDURE GetNotificationData(p_clientData Party_rec
                               ,p_notificationType NUMBER
                               ,p_notificationDate DATE
                               ,p_signerData Signer_rec
                               ,p_reason VARCHAR2
                               ,o_data OUT CLOB
                               ,o_key OUT VARCHAR2) IS
  BEGIN
    IF p_notificationType = NOTIFICATIONTYPE_RECOGNITION THEN
      GetRecognitionData(p_clientData, p_signerData, p_notificationDate, o_data, o_key);
    ELSIF p_notificationType = NOTIFICATIONTYPE_REJECTION
       OR p_notificationType = NOTIFICATIONTYPE_EXCLUSION THEN
      GetNegativeNotificationData(p_clientData, p_signerData, p_notificationType, p_notificationDate, p_reason, o_data, o_key);
    ELSIF p_notificationType = NOTIFICATIONTYPE_CONFIRMATIONREQ THEN
      GetConfirmationReqData(p_clientData, p_signerData, p_notificationDate, o_data, o_key);
    END IF;
  END;
  
  -- Записать ошибку в лог и отправить письмо ОД
  PROCEDURE LogError(p_notificationType NUMBER
                    ,p_clientData Party_rec
                    ,p_errorCode NUMBER
                    ,p_errorDesc VARCHAR2) IS
    v_errorMessage VARCHAR2(2000);
    v_notificationEmail VARCHAR2(200) := RSB_Common.GetRegStrValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\КВАЛИФИКАЦИЯ\НАПРАВЛЯТЬ СКРЫТУЮ КОПИЮ ПО ЮЛ');
  BEGIN
    v_errorMessage := 'При формировании уведомления "' || GetNotificationDescription(p_notificationType) || '"' ||
                      ' для клиента ' || CASE WHEN p_clientData.FullName IS null THEN '<не определен>' ELSE p_clientData.FullName END|| 
                      ' (ID клиента '  || CASE WHEN p_clientData.ID IS null THEN '<не определен>' ELSE to_char(p_clientData.ID) END || ')' ||
                      ' получена ошибка : ' || p_errorDesc || '. Код ошибки: ' || p_errorCode;

    it_event.RegisterError( p_SystemId => C_C_SYSTEM_NAME,
                            p_ServiceName => SERVICE_NAME_QI_NOTIFICATION,
                            p_ErrorCode => p_errorCode,
                            p_ErrorDesc => p_errorDesc,
                            p_LevelInfo => 8);
    
    IF NOT v_notificationEmail IS NULL THEN
      INSERT INTO DEMAIL_NOTIFY_DBT (t_head, t_dateadd, t_email,  t_text)
                             VALUES ('Формирование уведомления по КИ (IPS_DFACTORY.SendInvestorStatus)', SYSDATE, v_notificationEmail, v_errorMessage);
    END IF;
  END;
  
  -- BOSS-319 Cбор данных о клиенте, упаковки и отправки в Kafka сообщения с json содержащим данные для генерации уведомлениях по КИ
  PROCEDURE SendInvestorStatus_json(p_worklogid integer
                                   ,p_messbody  CLOB
                                   ,p_messmeta  xmltype
                                   ,o_msgid     out varchar2
                                   ,o_MSGCode   out integer
                                   ,o_MSGText   out varchar2
                                   ,o_messbody  out CLOB
                                   ,o_messmeta  out xmltype) is
    v_msgid itt_q_message_log.msgid%type;
    v_clientData Party_rec := NULL;
    v_signerData Signer_rec;
    v_notificationType NUMBER(3);
    v_reason VARCHAR2(320);
    v_padej VARCHAR2(320);
    v_date DATE;
    v_notification CLOB;
    
    v_jsonData CLOB;
    v_jsonKey CLOB;
    
    v_templateName VARCHAR2(15);
    v_bucketName VARCHAR2(30);
    v_facsimile VARCHAR(50);
  BEGIN
    o_MSGCode := 0;
    o_MSGText := '';

    BEGIN
      IF NOT NVL(RSB_COMMON.GetRegBoolValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ИНТЕГРАЦИЯ КАФКА\УВЕД. О СТАТУСЕ КИ У ЮЛ'), false) THEN
        RETURN;
      END IF;
      
      v_clientData := GetPartyInfo(GetClientPartyID(p_messbody));
      v_notificationType := GetNotificationType(p_messbody);
      
      -- Убрать ИП из обработки до согласования
      IF v_clientData.IsEmployer = 1 THEN
        RETURN;
      END IF;
      
      IF v_clientData.IsLegalPerson = 0 AND v_clientData.IsEmployer = 0 THEN
        RAISE_APPLICATION_ERROR(INVALID_PARAMETERS_ERR, 'Отправка уведомлений по КИ для ФЛ не поддерживается! (ID субъекта = ' || V_clientData.ID || ')');
      END IF;
      
      v_signerData := GetSignerInfo(GetSignerPartyID(p_messbody));
      v_reason := GetReason(p_messbody);
      v_padej := GetClientNameMod(p_messbody);
      v_date := GetNotificationDate(p_messbody);
      
      IF NOT v_padej IS NULL THEN
        v_clientData.FullNamePadej := v_padej;
      END IF;
      
      GetNotificationData(v_clientData, v_notificationType, v_date, v_signerData, v_reason, v_jsonData, v_jsonKey);

      v_msgID := it_q_message.get_sys_guid();
    
      SELECT json_object(key 'GUID' value v_msgid,
                         key 'RequestTime' value (SELECT IT_XML.TIMESTAMP_TO_CHAR_ISO8601(SYSDATE) FROM DUAL),
                         key CAST(v_jsonKey AS VARCHAR2(50)) value v_jsonData
                         FORMAT json RETURNING CLOB)
        INTO o_messbody
        FROM dual;
    
      v_facsimile := GetFacsimile(v_signerData.ID)||'.png';
      GetOutPackParams(v_clientData ,v_notificationType, v_templateName, v_bucketName);
    
      SELECT XMLElement("NotificationInfo", XMLElement("facsimile", v_facsimile), 
                                            XMLElement("CFTCode", v_clientData.CFTCode),
                                            XMLElement("templateName", v_templateName),
                                            XMLElement("bucketName", v_bucketName),
                                            XMLElement("outputFilename", TO_CHAR(v_clientData.ID)))
        INTO o_messmeta
        FROM dual;
      
    EXCEPTION WHEN OTHERS THEN
      o_MSGCode := ABS(sqlcode);
      o_MSGText := sqlerrm;
    END;
    
    IF o_MSGCode = 0 THEN
      BEGIN
        IT_Kafka.load_msg(io_msgid => v_msgid
                         ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                         ,p_ServiceName => C_C_SYSTEM_NAME || '.' || SERVICE_NAME_QI_NOTIFICATION
                         ,p_Receiver => it_ips_dfactory.C_C_SYSTEM_NAME
                         ,p_MESSBODY => o_messbody
                         ,o_ErrorCode => o_MSGCode
                         ,o_ErrorDesc => o_MSGText
                         --,p_CORRmsgid => 
                         ,p_MessMETA => o_messmeta);
      EXCEPTION WHEN OTHERS THEN
        o_MSGCode := abs(sqlcode);
        o_MSGText := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
      END;
    END IF;
    
    IF o_MSGCode != 0 THEN
      LogError(v_notificationType, v_clientData, o_MSGCode, o_MSGText);
    END IF;
  END;
  
  -- BOSS-319 Cбор данных о клиенте, упаковки и отправки в Kafka сообщения с json содержащим данные для генерации уведомлениях по КИ - адаптер для RSL
  PROCEDURE SendInvestorStatus(p_ClientID NUMBER,
                               p_NotificationType NUMBER,
                               p_NotificationDate DATE,
                               p_SignerID NUMBER,
                               p_Reason VARCHAR2,
                               p_ClientNameMod VARCHAR2,
                               o_ErrorCode OUT NUMBER,
                               o_ErrorDesc OUT VARCHAR2)
  IS
    v_msgID itt_q_message_log.msgid%type;
    v_json CLOB;
    v_messBody CLOB;
    v_messMeta XMLType;
  BEGIN
    SELECT json_object(key 'ClientID' value p_ClientID,
                       key 'NotificationType' value p_NotificationType,
                       key 'NotificationDate' value TO_CHAR(p_NotificationDate, 'YYYY-MM-DD HH24:MI:SS'),
                       key 'SignerID' value p_SignerID,
                       key 'Reason' value p_Reason,
                       key 'Padej' value p_ClientNameMod
                       ABSENT ON null RETURNING CLOB)
      INTO v_json
      FROM dual;
    
    SendInvestorStatus_json(p_worklogid => 0
                           ,p_messbody  => v_json
                           ,p_messmeta  => null
                           ,o_msgid     => v_msgID
                           ,o_MSGCode   => o_ErrorCode
                           ,o_MSGText   => o_ErrorDesc
                           ,o_messbody  => v_messBody
                           ,o_messmeta  => v_messMeta);
                           
    EXCEPTION WHEN OTHERS THEN
      o_ErrorCode := ABS(sqlcode);
      o_ErrorDesc := sqlerrm;
  END;

-- Формирование Header для  IPS и упаковка его в MessMETA
/*
Описание заголовков СОФР -> IPS
---------------------------------
При отправке запросов на генерацию документов требуется в HEADERS топика Kafka передавать перечень обязательных заголовков.

Заголовок Описание  Обязательность  Требования к значениям
"x-trace-id"  Уникальный идентификатор для трассировки, заполняется значением из GUID входящего запроса на формирование брокерского отчета из Своего бизнеса  Да  Формат - UUID
"x-request-id"  Уникальный идентификатор СОФР Да  GUID
"x-request-time"  Дата и время запроса  Да  
"x-template-type" Тип шаблона Нет (можем отказаться)  Допустимые значения: "JRXML"
"x-template-params" Динамически изменяемые параметры (факсимиле)
Обязательно с экранированием  Да  Передается объектом с указателем на требуемый динамический параметр. 
Пример: 
"x-template-params": [
  {
   "paramName": "df_facsimile",
   "paramType": "FILE",
   "paramValue": "FileName.png"
  }
]
"x-template-name" Наименование шаблона.
Значение может быть длиной не более 64 символов и содержать строчные латинские буквы, дефисы, точки, подчеркивания, цифры - без расширения  Да  legal-entity-report
"x-system-to" Система - получатель.
Заглавные латинские буквы. Допустимы цифры, точки, нижние подчеркивания, тире. Не допускается использование других специальных символов и пробелов, ограничение 64 символа. Да  SOFR
"x-system-from" Система - источник данных
Заглавные латинские буквы. Допустимы цифры, точки, нижние подчеркивания, тире. Не допускается использование других специальных символов и пробелов, ограничение 64 символа. Да  SOFR
"x-output-type" Формат подготовленного документа  Нет (можем отказаться)  Допустимые значения: "PDF"
"x-output-file-name"  Наименование файла
Свободный формат без расширения. Недопустимо использование слеш-знаков (/ \ |). Ограничение наименования: 128 с.  Да  Пример: "Отчет брокера за период с 28.08.2024 по 29.08.2024"
"x-bucket-name" Наименование места хранения подготовленного документа.
Строчные латинские буквы, допустимы дефисы. Не допускается использование других специальных символов и пробелов.  Да  ips-document-factory
"x-data-source" Источник данных
Заполняется:
1.  S3 - если JSON > 1 Мб был передан в S3
2.  Kafka - если JSON в теле сообщения (но в этом случае можно заголовок не передавать, по умолчанию IPS считает, что сообщение передано в Kafka) Условно да  Допустимые значения: <S3>, <KAFKA>
"x-data-file-name"  Наименование файла - источника данных
Передается в случае, если источник данных - s3
Значение может быть длиной не более 128 символов и содержать заглавные и строчные латинские буквы, цифры, дефисы, точки, подчеркивания  Условно нет Значение может быть длиной не более 128 символов и содержать заглавные и строчные латинские буквы, цифры, дефисы, точки, подчеркивания
"x-service-name"  Наименование бизнес-сервиса Да  GenerateLegalEntityReport
*/
  function add_KafkaHeader_Xmessmeta(px_messmeta          xmltype default null
                                    ,p_List_dllvalues_dbt number -- Справочник статических header-ов
                                    ,p_traceid            varchar2 --  Уникальный идентификатор для трассировки, заполняется значением из GUID входящего запроса   
                                    ,p_requestid          varchar2 -- Уникальный идентификатор СОФР Да  GUID
                                    ,p_requesttime        timestamp default systimestamp -- Дата и время запроса  Да  
                                    ,p_templateparams     tt_templateparams -- Динамически изменяемые параметры (факсимиле)
                                    ,p_addparams          tt_addparams default null -- дополнительные параметры заголовка
                                    ,p_outputfilename     varchar2 -- Наименование файла Свободный формат без расширения. Недопустимо использование слеш-знаков (/ \ |). Ограничение наименования: 128 с.  Да  Пример: "Отчет брокера за период с 28.08.2024 по 29.08.2024"
                                    ,p_datafilename       varchar2 default null --Если null будет p_requestid.  Наименование файла - источника данных Передается в случае, если источник данных - s3 Значение может быть длиной не более 128 символов и содержать заглавные и строчные латинские буквы, цифры, дефисы, точки, подчеркивания  Условно нет 
                                     ) return xmltype as
    v_Header       clob;
    vx_messmeta    xmltype := px_messmeta;
    v_datafilename varchar2(128) := nvl(p_datafilename, p_requestid);
  begin
    select JSON_MERGEPATCH(json_object('x-trace-id' value p_traceid
                                      ,'x-request-id' value p_requestid
                                      ,'x-request-time' value p_requesttime
                                      ,'x-output-file-name' value p_outputfilename)
                          ,(select JSON_OBJECTAGG(key t_Key value t_Value)
                              from (select t_Name as t_Key
                                          ,t_Note as t_Value
                                      from dllvalues_dbt
                                     where t_List = p_List_dllvalues_dbt)) returning clob)
      into v_Header
      from dual;
    select JSON_MERGEPATCH(v_Header
                          ,(select JSON_OBJECT('x-template-params' is
                                               JSON_ARRAYAGG(JSON_OBJECT('paramName' is tp.paramName, 'paramType' is tp.paramType, 'paramValue' is tp.paramValue)))
                              from table(p_templateparams) tp) returning clob)
      into v_Header
      from dual;
    if p_addparams is not null then 
       FOR i IN p_addparams.FIRST .. p_addparams.LAST LOOP  
        select JSON_MERGEPATCH( v_Header
                               , JSON_OBJECT(p_addparams(i).paramName is p_addparams(i).paramValue) returning CLOB )
          into v_Header
          from dual;
       END LOOP;
    end if;     
    vx_messmeta := it_kafka.add_S3xdatafilename_Xmessmeta(p_S3xdatafilename => v_datafilename, px_messmeta => vx_messmeta);
    vx_messmeta := it_kafka.add_Header_Xmessmeta(p_Header => v_Header, px_messmeta => vx_messmeta);
    return vx_messmeta;
  end;

 function add_addparams(p_addparams tt_addparams default null
                       ,p_paramName      varchar2
                       ,p_paramValue     varchar2) return tt_addparams as
   v_addparams tt_addparams := tt_addparams();
 begin
   if p_addparams is not null then
     v_addparams := p_addparams;
   end if;
   v_addparams.EXTEND;
   v_addparams(v_addparams.COUNT).paramName := p_paramName;
   v_addparams(v_addparams.COUNT).paramValue := p_paramValue;
   return v_addparams;
 end;
  

 function add_templateparams(p_templateparams tt_templateparams default null
                            ,p_paramName      varchar2
                            ,p_paramType      varchar2 default 'FILE'
                            ,p_paramValue     varchar2) return tt_templateparams as
   v_templateparams tt_templateparams := tt_templateparams();
 begin
   if p_templateparams is not null then
     v_templateparams := p_templateparams ;
   end if;
   v_templateparams.EXTEND;
   v_templateparams(v_templateparams.COUNT).paramName := p_paramName;
   v_templateparams(v_templateparams.COUNT).paramType := p_paramType;
   v_templateparams(v_templateparams.COUNT).paramValue := p_paramValue;
   return v_templateparams;
 end;

end it_ips_dfactory;
/
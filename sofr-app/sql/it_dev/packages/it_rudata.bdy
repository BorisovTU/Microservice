CREATE OR REPLACE PACKAGE BODY IT_RUDATA
IS

  PROCEDURE Send_Err_Mail (
    p_Head IN VARCHAR2,
    p_Text IN VARCHAR2 )
  IS
  BEGIN
    execute immediate 'INSERT INTO DEMAIL_NOTIFY_DBT(T_DATEADD,  
                                                      T_EMAIL,  
                                                      T_HEAD,  
                                                      T_TEXT) 
                          SELECT SYSDATE, 
                                 uea.t_email, 
                                 :p_Head, 
                                 :p_Text
                            FROM usr_email_addr_dbt uea
                           WHERE uea.t_group = :p_EmailGroup' USING p_Head, p_Text, 50 /*p_EmailGroup*/;
  END;
  
  PROCEDURE Fill_Nominal_Table (
      p_json IN CLOB)
  IS
      v_UserNominalRec USER_NOMINAL_ENDMONTH_DBT%rowtype;
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE USER_NOMINAL_ENDMONTH_DBT';
    FOR one_rec IN  (
          select jt.tradedate, jt.fintoolId, jt.currentFaceValue
           from json_table ( p_json, '$[*]'
      columns ( tradedate date path '$.date', fintoolId number(10), currentFaceValue number(32,12))
                ) jt)
      LOOP
         v_UserNominalRec.t_TradeDate := one_rec.TradeDate;
         v_UserNominalRec.t_FinToolID := one_rec.FinToolID;
         v_UserNominalRec.t_CurrentFaceValue := one_rec.CurrentFaceValue;
       
         BEGIN
           select nvl(t_objectid, 0) into v_UserNominalRec.t_Fiid from dobjcode_dbt where t_codekind=104 and t_objecttype=9 and t_code=to_char(one_rec.fintoolId);
         EXCEPTION
           WHEN NO_DATA_FOUND THEN
              v_UserNominalRec.t_Fiid := 0;
         END;
         
         INSERT INTO USER_NOMINAL_ENDMONTH_DBT VALUES v_UserNominalRec;
      END LOOP;

     EXCEPTION
       WHEN OTHERS THEN 
          Send_Err_Mail('Произошла ошибка при выполнении процедуры Set_Nominal_From_Rudata', 'Произошла ошибка при выполнении процедуры Set_Nominal_From_Rudata: ' || SQLCODE || ' ' || SQLERRM);
  END Fill_Nominal_Table;
  
  
  PROCEDURE Fill_FIVLHIST
  IS
  BEGIN
     FOR one_rec IN  (
         SELECT UNE.t_RecID, UNE.t_TradeDate, UNE.t_FinToolID, UNE.t_FIID, UNE.t_CurrentFaceValue 
           FROM USER_NOMINAL_ENDMONTH_DBT UNE
             JOIN DAVOIRISS_DBT AVR ON AVR.T_FIID = UNE.T_FIID
         WHERE UNE.t_FIID > 0 
             AND AVR.t_IndexNom = chr(88)
             AND NOT EXISTS (
                SELECT 1
                 FROM DFIVLHIST_DBT 
               WHERE t_FIID    = UNE.t_FIID
                   AND t_ValKind = 1
                   AND t_EndDate = UNE.t_TradeDate
                )
             )
      LOOP
         INSERT INTO DFIVLHIST_DBT(T_FIID,T_VALKIND,T_ENDDATE,T_VALUE) VALUES(one_rec.t_FIID, 1, one_rec.t_TradeDate, one_rec.t_CurrentFaceValue);
          
      END LOOP;
      
  EXCEPTION
       WHEN OTHERS THEN 
          Send_Err_Mail('Произошла ошибка при выполнении процедуры Set_Nominal_From_Rudata', 'Произошла ошибка при выполнении процедуры Set_Nominal_From_Rudata: ' || SQLCODE || ' ' || SQLERRM);
  END;

  PROCEDURE Set_Nominal_From_Rudata (
      p_json IN CLOB)
  IS
  BEGIN
    Fill_Nominal_Table(p_json);
    Fill_FIVLHIST();
  END Set_Nominal_From_Rudata;
  
  
/*
  Обёртка для вызова DateOptionsTable_Rq без указания ключа инструмента
  Используется для планировщика, p_instr_key передается null, для выгрузки по всем инструментам 
  @since RSHB 118
  @qtest NO
  @param p_result_code Код результата выполнения (0 ? успех, 1 ? ошибка)
  @param p_result_text Описание результата выполнения или текста ошибки
*/  
  PROCEDURE DateOptionsTableWrapper_Rq(p_result_code   OUT NUMBER,  
                                       p_result_text   OUT VARCHAR) 
     
   IS
    p_qman_send     BOOLEAN := TRUE;         
    p_bdy           CLOB;                    
    p_hdr           CLOB;                    

  BEGIN
    
     p_result_code := 0;
     p_result_text := 'Processing completed successfully.';
           
      it_rudata.DateOptionsTable_Rq(
          p_instr_key     => null,
          p_qman_send     => p_qman_send,
          p_result_code   => p_result_code,
          p_result_text   => p_result_text,
          p_bdy           => p_bdy,
          p_hdr           => p_hdr
      );
    it_log.log(p_msg => 'DateOptionsTableWrapper_Rq debug - sheduler. ' || p_result_text, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
         
  EXCEPTION
      WHEN OTHERS THEN
         p_result_text := 'Произошла неожиданная ошибка: ' || SQLERRM;
         it_error.put_error_in_stack;
         it_log.log(p_msg => 'Error occurred while processing.', p_msg_type => it_log.c_msg_type__error);
         it_error.clear_error_stack;
  END;
  
  
 /*
  Обёртка для вызова процедуры формирования JSON-запроса DateOptionsTable_Rq.
  Используется при вызове пользовательской функции на ЦБ
  @since RSHB 118
  @qtest NO
  @param p_fiid Ключ инструмента (ISIN/LSIN). 
  @param p_result_code   Код результата выполнения (0 ? успех, 1 ? ошибка)
  @param p_result_text   Описание результата выполнения или текста ошибки
*/   
  PROCEDURE DateOptionsTableWrapper_Rq(p_fiid     IN davoiriss_dbt.t_fiid%type,
                                       p_result_code   OUT NUMBER,  
                                       p_result_text   OUT VARCHAR) 
     
   IS
    p_qman_send     BOOLEAN := TRUE;         
    p_instr_key     VARCHAR2(128);       
    p_bdy           CLOB;                    
    p_hdr           CLOB;                    

  BEGIN
    
     p_result_code := 0;
     p_result_text := 'Processing completed successfully.';
     p_instr_key:= getАvoirissIsinByFiid(p_fiid); 
     if(p_instr_key is null) then
        p_result_code := 1;
        p_result_text := 'Ошибка: ЦБ не помечена флагом "изменяемый номинал"';
       return;             
     end if;  
  
    if (getOffRudataByFiid(p_fiid) > 0) then
      p_result_code := 1;
      p_result_text := 'Ошибка: есть признак "не получать ИН из RuData"';
      return;
    end if;

       it_rudata.DateOptionsTable_Rq(
          p_instr_key     => p_instr_key,
          p_qman_send     => p_qman_send,
          p_result_code   => p_result_code,
          p_result_text   => p_result_text,
          p_bdy           => p_bdy,
          p_hdr           => p_hdr
      );
     it_log.log(p_msg => 'DateOptionsTableWrapper_Rq debug - function user.', p_msg_type => it_log.C_MSG_TYPE__DEBUG);
         
  EXCEPTION
      WHEN OTHERS THEN
         p_result_text := 'Произошла неожиданная ошибка: ' || SQLERRM;
         it_error.put_error_in_stack;
         it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
         it_error.clear_error_stack;
  END;
  
/*
  Обёртка для вызова процедуры формирования JSON-запроса DateOptionsTable_Rq
  @since RSHB 118
  @qtest NO
  @param p_instr_key Ключ инструмента (ISIN/LSIN), передаваемый во внутреннюю процедуру DateOptionsTable_Rq
*/  
   PROCEDURE DateOptionsTableWrapper_Rq( p_instr_key IN VARCHAR2 ) 
     
   IS
    p_qman_send     BOOLEAN := TRUE;         
    p_result_code   NUMBER;                
    p_result_text   VARCHAR2(4000);         
    p_bdy           CLOB;                    
    p_hdr           CLOB;                    

  BEGIN
      it_rudata.DateOptionsTable_Rq(
          p_instr_key     => p_instr_key,
          p_qman_send     => p_qman_send,
          p_result_code   => p_result_code,
          p_result_text   => p_result_text,
          p_bdy           => p_bdy,
          p_hdr           => p_hdr
      );
     it_log.log(p_msg => 'DateOptionsTableWrapper_Rq debug - trigger.', p_msg_type => it_log.C_MSG_TYPE__DEBUG);


  EXCEPTION
      WHEN OTHERS THEN
         p_result_text := 'Произошла неожиданная ошибка: ' || SQLERRM;
         it_error.put_error_in_stack;
         it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
         it_error.clear_error_stack;
  END;

  
 /*
  Формирование тела и заголовка JSON-запроса к методу v2/Bond/DateOptionsTable
  @since RSHB 118
  @qtest NO
  @param p_instr_key     Ключ инструмента (ISIN/LSIN). Если NULL ? формируется список по условиям отбора из базы
  @param p_qman_send     Признак необходимости отправки сообщения в очередь QManager
  @param p_result_code   Код результата выполнения (0 ? успех, 1 ? ошибка)
  @param p_result_text   Описание результата выполнения или текста ошибки
  @param p_bdy           Сформированный JSON body для запроса
  @param p_hdr           Сформированный JSON header для запроса
*/
  PROCEDURE DateOptionsTable_Rq(  p_instr_key     IN VARCHAR2,  
                                  p_qman_send     IN BOOLEAN DEFAULT TRUE,  
                                  p_result_code   OUT NUMBER,  
                                  p_result_text   OUT VARCHAR2,  
                                  p_bdy           OUT CLOB,      
                                  p_hdr           OUT CLOB)
as
    l_date    DATE := SYSDATE;  
    l_symbols sys.odcivarchar2list;
    l_GUID VARCHAR2(32);
    l_messmeta XMLTYPE;
BEGIN
   p_result_code := 0;

   if p_instr_key is null then
     SELECT NVL(T_ISIN, T_LSIN) AS symbol
      bulk collect into l_symbols
      FROM davoiriss_dbt av inner join DNOTETEXT_DBT nt
          on LPAD(av.t_fiid, 10, '0') = nt.t_documentid
      WHERE av.T_INDEXNOM = 'X' 
        AND NVL(av.T_ISIN, av.T_LSIN) IS NOT NULL
        AND nt.t_objecttype = 12
        AND nt.t_notekind = 126
        AND sysdate between nt.t_date and nt.t_validtodate
        AND nt.t_text = hextoraw(lpad('0', 1500 * 2, '0'));

      IF l_symbols.COUNT = 0 THEN
          p_result_code :=  1;
          p_result_text := 'Ошибка: не найдена ИН облигация для выгрузки';
          it_log.log(p_msg => p_result_text, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
          RETURN;
      END IF;
   else 
       l_symbols := sys.odcivarchar2list();
       l_symbols.extend;
       l_symbols(1):= p_instr_key;
   end if;
    SELECT CAST(SYS_GUID() AS VARCHAR2(32)) into l_GUID FROM dual;      
   SELECT 
   JSON_OBJECT(
        'ID' VALUE l_GUID, 
        'endpoint' VALUE 'v2/Bond/DateOptionsTable',
        'endpoint-method' VALUE 'POST',
        'schema-uri' VALUE 'https://dh2.efir-net.ru/swagger/v2/swagger.json',
        'items-ref' VALUE 'symbol', 
        'default-call' VALUE JSON_OBJECT(
            'symbol' VALUE 'RU000A1062M5',
            'date' VALUE '2025-04-17',
            'isCloseRegister' VALUE 'true',
            'useFixDate' VALUE 'true'
        )
    )
    into p_hdr
    from dual;
        
    select  
    JSON_OBJECT(
        'symbol' VALUE JSON_ARRAYAGG(column_value),
        'date' VALUE JSON_ARRAY(TO_CHAR(l_date, 'YYYY-MM-DD')) ,
        'isCloseRegister' value 'true' ,
        'useFixDate' value 'true'
    )
    into p_bdy
    from (select column_value from  TABLE(l_symbols));
    
    p_hdr:= cleanJsonTrueFields(p_hdr);
    p_bdy:= cleanJsonTrueFields(p_bdy);
    
    IF  NOT (p_hdr IS JSON) THEN
         p_result_code :=  1;
         p_result_text := 'Ошибка: p_hdr не валидный JSON';
         it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
         RETURN;
    END IF;
    
    IF  NOT (p_bdy IS JSON) THEN
         p_result_code :=  1;
         p_result_text := 'Ошибка: p_bdy не валидный JSON';
         it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
         RETURN;
    END IF;
    
    -- Если нужно отправить сообщение в очередь QManager
    IF p_qman_send THEN             
        IF p_result_code = 0 THEN
          it_log.log(p_msg => 'Sending message to QManager: ' || p_bdy || ', Header: ' || p_hdr, 
                    p_msg_type => it_log.C_MSG_TYPE__DEBUG);
          l_messmeta := it_kafka.add_Header_Xmessmeta(p_Header => p_hdr);
          it_kafka.load_msg(io_msgid       => l_guid
                         ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                         ,p_ServiceName  => 'IPS_RUDATA.FaceValue'
                         ,p_Receiver     => C_C_SYSTEM_NAME
                         ,p_MESSBODY     => p_bdy
                         ,p_MessMETA     => l_messmeta
                         ,o_ErrorCode    => p_result_code
                         ,o_ErrorDesc    => p_result_text);
          IF(p_result_code <> 0) then
            it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
            RETURN;
          END IF;
        END IF;

    ELSE
        it_log.log(p_msg => 'Not sending to QManager. Body: ' || p_bdy || ', Header: ' || p_hdr, 
                    p_msg_type => it_log.C_MSG_TYPE__DEBUG);
    END IF;
    
    it_log.log(p_msg => 'p_bdy = '||p_bdy, p_msg_type => it_log.C_MSG_TYPE__DEBUG); 
    it_log.log(p_msg => 'p_hdr = '||p_hdr, p_msg_type => it_log.C_MSG_TYPE__DEBUG); 

    p_result_text := 'Сообщение успешно выгружено в топик.';
    
EXCEPTION
    WHEN OTHERS THEN
       p_result_code := 1;
       p_result_text := 'Произошла неожиданная ошибка: ' || SQLERRM;
       it_error.put_error_in_stack;
       it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
       it_error.clear_error_stack;
END DateOptionsTable_Rq;

/*
  Обработка ответа от сервиса DateOptionsTable: извлечение и сохранение данных номинале инструмента
  @since RSHB 118
  @qtest NO
  @param p_hdr           JSON-заголовок ответа, содержащий поле symbol
  @param p_bdy           JSON-тело ответа, содержащее данные по current_fv и fv_last_known_date
  @param p_result_code   Код результата выполнения (0 ? успех, 1 ? ошибка)
  @param p_result_text   Описание результата выполнения или текста ошибки
*/
  PROCEDURE DateOptionsTable_Resp ( p_hdr           IN CLOB,   
                                    p_bdy           IN CLOB,       
                                    p_result_code   OUT NUMBER,   
                                    p_result_text   OUT VARCHAR2)
  AS
      l_symbol        VARCHAR2(255); 
      l_current_fv    VARCHAR2(512);
      l_last_date     VARCHAR2(256);
      v_header_string VARCHAR2(4000);
      l_row_fivlhist  DFIVLHIST_DBT%ROWTYPE;
      l_fiid          DAVOIRISS_DBT.T_FIID%type;
  BEGIN
      p_result_code := 0;
      
      IF NOT (p_hdr IS JSON) THEN
          p_result_code := 1;
          p_result_text := 'Ошибка: p_hdr не валидный JSON';
          it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
          RETURN;
      END IF;

      IF NOT (p_bdy IS JSON) THEN
          p_result_code := 1;
          p_result_text := 'Ошибка: p_bdy не валидный JSON';
          it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
          RETURN;
      END IF;
      
      SELECT JSON_VALUE(p_hdr, '$.header')
      INTO v_header_string
      FROM dual;

      -- парсим symbol
      SELECT REGEXP_SUBSTR(v_header_string, 'symbol\s*:\s*([^;]+)', 1, 1, NULL, 1)
      INTO l_symbol
      FROM dual;
        
      -- Извлекаем значение current_fv из тела JSON
      SELECT json_value(p_bdy, '$[0].current_fv')
        INTO l_current_fv
        FROM dual;
      
      SELECT json_value(p_bdy, '$[0].fv_last_known_date') 
        INTO l_last_date
        FROM dual;
      
      l_fiid:=getАvoirissFiidByIsin(trim(l_symbol));
      
      IF(l_fiid <= 0) THEN
         p_result_code := 1;
         p_result_text := l_symbol || '. Для инструмента не найден fiid в таблице davoiriss_dbt';
         it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
         RETURN;          
      END IF;
       
  BEGIN     
   SELECT f.*
     INTO l_row_fivlhist
    FROM davoiriss_dbt a INNER JOIN DFIVLHIST_DBT f ON a.t_fiid= f.t_fiid 
   WHERE a.t_fiid = l_fiid
     AND f.t_valkind = 1
     ORDER BY f.t_enddate DESC
     FETCH FIRST 1 ROWS ONLY;
   EXCEPTION WHEN no_data_found 
     THEN l_row_fivlhist.t_fiid:= getАvoirissFiidByIsin(trim(l_symbol));
          l_row_fivlhist.t_valkind:= 1;
          l_row_fivlhist.t_enddate:= convertFromTextToDate(l_last_date);
          l_row_fivlhist.t_value:= converFromTextToNumber(l_current_fv);

   END;  
     
    IF (l_row_fivlhist.t_id IS NOT NULL) AND
       (l_row_fivlhist.t_enddate <> convertFromTextToDate(l_last_date)) THEN
      INSERT INTO dfivlhist_dbt(t_fiid, t_valkind, t_enddate, t_value)
      VALUES (l_row_fivlhist.t_fiid, 
              l_row_fivlhist.t_valkind, 
              convertFromTextToDate(l_last_date), 
              converFromTextToNumber(l_current_fv));   
    ELSIF (l_row_fivlhist.t_id IS NOT NULL)  AND
          (l_row_fivlhist.t_enddate = convertFromTextToDate(l_last_date)) THEN
       UPDATE dfivlhist_dbt 
          SET t_value = converFromTextToNumber(l_current_fv)
        WHERE t_id = l_row_fivlhist.t_id;
    ELSIF getIsFirstOrLastDayOff(SYSDATE) = 1 THEN
         INSERT INTO dfivlhist_dbt(t_fiid, t_valkind, t_enddate, t_value)
          VALUES (l_row_fivlhist.t_fiid, 
                  l_row_fivlhist.t_valkind, 
                  SYSDATE, 
                  converFromTextToNumber(l_current_fv));
    END IF;
    
    COMMIT;

      -- Успешный статус
      p_result_text := NULL;
      
  EXCEPTION
      WHEN NO_DATA_FOUND THEN
          p_result_code := 1;
          p_result_text := l_symbol || '. Ошибка: Инструмент не найден в базе данных.';
          it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
      WHEN OTHERS THEN
          p_result_code := 1;
          IF(L_SYMBOL IS NOT NULL) THEN
           p_result_text := l_symbol || '. Ошибка: ' || SQLERRM;
          ELSE
            p_result_text := 'Ошибка: ' || SQLERRM;
          END IF;
          it_error.put_error_in_stack;
          it_log.log(p_msg => p_result_text, p_msg_type => it_log.c_msg_type__error);
          it_error.clear_error_stack;
  END DateOptionsTable_Resp;
  
/*
  Конвертация текстовой даты в тип DATE (ISO 8601)
  @since RSHB 118
  @qtest NO
  @param p_textDate Дата в строковом формате 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
  @return Значение типа DATE
*/
  FUNCTION convertFromTextToDate(p_textDate VARCHAR2) RETURN DATE   
  AS 
  l_date DATE;
  BEGIN
    
   SELECT CAST( TO_TIMESTAMP(p_textDate, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')  AS DATE)
     INTO l_date
     FROM dual;
   
   RETURN l_date;

  END;
  
/**
  Получение идентификатора avoiriss по ISIN/LSIN
  @since RSHB 118
  @qtest NO
  @param p_isin Значение ISIN или LSIN
  @return t_fiid или -1 при ошибке
*/
  FUNCTION getАvoirissFiidByIsin(p_isin VARCHAR2) RETURN davoiriss_dbt.t_fiid%TYPE
  AS
   l_fiid davoiriss_dbt.t_fiid%TYPE;
  BEGIN
   SELECT a.t_fiid
     INTO l_fiid
     FROM davoiriss_dbt a 
    WHERE NVL(a.T_ISIN, a.T_LSIN) = TRIM(p_isin);
   RETURN l_fiid;
  EXCEPTION WHEN OTHERS 
    THEN RETURN -1;
  END;


/*
  Получение ISIN/LSIN по fiid
  @since RSHB 118
  @qtest NO
  @param p_fiid Идентификатор fiid
  @return ISIN или LSIN, NULL при ошибке
*/ 
  FUNCTION getАvoirissIsinByFiid(p_fiid davoiriss_dbt.t_fiid%type) RETURN DAVOIRISS_DBT.T_ISIN%TYPE
  AS
   l_isin davoiriss_dbt.t_isin%type;
  BEGIN
   SELECT NVL(a.T_ISIN, a.T_LSIN)
     INTO l_isin
     FROM davoiriss_dbt a 
    WHERE a.t_fiid = p_fiid
      AND a.t_indexnom = chr(88);
   RETURN l_isin;
  EXCEPTION WHEN OTHERS 
    THEN RETURN NULL; 
  END;

/*
  Конвертация текстового значения в число (с учётом замены точки на запятую)
  @since RSHB 118
  @qtest NO
  @param p_text Строковое представление числа
  @return Число с округлением до 12 знаков
*/ 
  FUNCTION converFromTextToNumber(p_text VARCHAR2) RETURN NUMBER
    
  AS
   l_result NUMBER(32,12);
  BEGIN  
    l_result :=  ROUND(TO_NUMBER(p_text),12);
   RETURN l_result;
  END;
  
 /*
  Проверка признака исключения записи по fiid в справочнике dnotetext_dbt
  @since RSHB 118
  @qtest NO
  @param p_fiid Идентификатор fiid
  @return 1 ? исключаем; 0 ? не исключаем
*/
  FUNCTION getOffRudataByFiid(p_fiid davoiriss_dbt.t_fiid%TYPE) RETURN NUMBER  
  AS
  l_result NUMBER;
  BEGIN
     SELECT COUNT(*)
      INTO l_result
      FROM DNOTETEXT_DBT nt
      WHERE nt.t_documentid = LPAD(p_fiid, 10, '0')
        AND nt.t_objecttype = 12
        AND nt.t_notekind = 126
        AND sysdate between nt.t_date and nt.t_validtodate
        AND nt.t_text <> hextoraw(lpad('0', 1500 * 2, '0'));
  RETURN l_result;
  END;
  
 /*
  Проверка: является ли дата первым или последним выходным днём месяца
  @since RSHB 118
  @qtest NO
  @param p_date Дата для проверки
  @return 1 ? да; 0 ? нет
*/
  FUNCTION getIsFirstOrLastDayOff(p_date DATE) RETURN NUMBER
  AS
  l_result          NUMBER := 0;
  l_cntWorkDayFirst NUMBER;
  l_cntWorkDayLast  NUMBER;
  BEGIN
    IF(RSI_RsbCalendar.IsWorkDay(p_date,0) = 0) then
         l_cntWorkDayFirst:=  RSI_RsbCalendar.getWorkDayCount(trunc(p_date,'mm'),p_date,0);     
         l_cntWorkDayLast:=   RSI_RsbCalendar.getWorkDayCount(p_date, trunc(LAST_DAY(p_date)),0);
         IF((nvl(l_cntWorkDayFirst,1) = 0) or (nvl(l_cntWorkDayLast,1) = 0)) THEN
           l_result:= 1;
         END IF;
    END IF;   

  RETURN L_RESULT;
  END;
 
/*
  Удаление кавычек вокруг true-значений в JSON
  @since RSHB 118
  @qtest NO
  @param p_json JSON в виде CLOB
  @return JSON с приведёнными к булевому виду значениями true
*/
  FUNCTION cleanJsonTrueFields(p_json CLOB) RETURN CLOB
  IS
  l_json CLOB;
  BEGIN
   l_json := REGEXP_REPLACE(p_json, '"([^"]+)":"true"', '"\1":true');  
     
   RETURN L_JSON;   
  EXCEPTION WHEN 
    OTHERS THEN
    l_json:= NULL;
  END;
  
  
 /*
  При выполнении сервисной операции начисления доходов расходов 
  исполненяется проверка
  @since RSHB 118
  @qtest NO
  @param p_date Дата проверки
  @param p_end_date дата окончания периода начисления
  @return p_fiid Идентификатор fiid инструмента ЦБ
*/
  FUNCTION checkNDR_FaceValue(p_date     date,
                              p_end_date date,
                              p_fiid     davoiriss_dbt.t_fiid%TYPE) RETURN NUMBER
  IS
  l_result NUMBER := 0;
  l_cnt    NUMBER;
  BEGIN
   IF (getIsFirstOrLastDayOff(p_end_date) = 1) AND (getOffRudataByFiid(p_fiid) = 0) THEN
      SELECT COUNT(*)
      INTO l_cnt
      FROM davoiriss_dbt av  INNER JOIN DFIVLHIST_DBT f ON av.t_fiid= f.t_fiid 
     WHERE av.T_INDEXNOM = 'X'
       AND f.t_valkind = 1
       AND f.t_enddate = p_date
       AND av.t_fiid = p_fiid;

      IF(L_CNT = 0) THEN
        l_result := 1;
      END IF;
   END IF;  
    RETURN l_result;  
  END;
  
  
  
/**
  * Упаковщик исходящх сообшений через KAFKA
  * @since RSHB 118
  * @qtest NO
  * @param p_message Исходное сообщение
  * @param p_expire 
  * @param o_correlation
  * @param o_messbody Упакованное сообщение
  * @param o_messmeta 
  */
  procedure out_pack_message( p_message     it_q_message_t
                             ,p_expire      date
                             ,o_correlation out varchar2
                             ,o_messbody    out clob
                             ,o_messmeta    out xmltype) as
    v_rootElement    itt_kafka_topic.rootelement%type;
    v_msgFormat      itt_kafka_topic.msg_format %type;
    vj_in_messbody   clob;
  begin
    begin
      select t.rootelement, t.msg_format
        into v_rootElement, v_msgFormat
        from itt_kafka_topic t
       where t.system_name = C_C_SYSTEM_NAME
         and t.servicename = p_message.ServiceName
         and t.queuetype = it_q_message.C_C_QUEUE_TYPE_OUT;
    exception
      when no_data_found then
        raise_application_error(-20000
                               ,'Сервис ' || p_message.ServiceName || ' не зарегистрирован как ' || it_q_message.C_C_QUEUE_TYPE_OUT || ' для KAFKA/' ||
                                C_C_SYSTEM_NAME);
    end;
    
    if v_msgFormat = 'DIRECT' then
      o_messbody := p_message.MessBODY;
      o_messmeta := p_message.MessMETA;
      o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
      return;
    end if;

    o_messbody := p_message.MessBODY;
    o_messmeta := p_message.MessMETA;
    o_correlation := null;
  end out_pack_message;
  
END IT_RUDATA;
/
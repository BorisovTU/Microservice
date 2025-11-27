CREATE OR REPLACE PROCEDURE ValidateAndParseCftClient(p_input_line IN VARCHAR2) IS
  -- Переменные после парсинга
  v_cftId       VARCHAR2(50);
  v_cftFIO      VARCHAR2(200);
  v_cftAccount  VARCHAR2(50);
  v_cftBIC      VARCHAR2(50);

  -- Служебные переменные
  v_partyId      NUMBER;
  v_bankPartyId  NUMBER;
  v_FIOSofr      VARCHAR2(200);
  v_ourBankId    NUMBER := NVL(RsbSessionData.OurBank,1); -- идентификатор  банка
  v_curSettAccId NUMBER;
  v_inn          VARCHAR2(20);
  v_bankcode     VARCHAR2(20);
  v_bankname     VARCHAR2(200);
  v_bankcorrid   NUMBER;
  v_bankcorrcode VARCHAR2(20);
  v_bankcorrname VARCHAR2(200);
  v_corracc      VARCHAR2(50);
  v_code         VARCHAR2(20);
  v_order        NUMBER;
  v_pmauto_order NUMBER;
  v_dummy        NUMBER;

  -- Ошибки
  e_msg       VARCHAR2(4000);
  c_success_code  CONSTANT NUMBER := 0;
  c_error_code    CONSTANT NUMBER := 1;

  -- Исключение для остановки текущей строки
  e_critical  EXCEPTION;
  
  /*
  Обновление таблицы для вывода в протокол
  @param p_message         Сообщение 
  @param p_error_Code      Код ошибки
*/
  PROCEDURE InsertProtocol(p_message      VARCHAR2,
                          p_Error_Code    NUMBER) 
  IS
    l_message VARCHAR2(256);
  BEGIN
    it_log.log(p_msg => 'Сообщение для протокола : '  || p_message, p_msg_type => it_log.C_MSG_TYPE__DEBUG);
    l_message := SUBSTR(REPLACE(p_message, CHR(10),'. '), 0, 220);
    INSERT INTO dprotocolvalidatecft_tmp(t_message, t_Error_Code) VALUES (l_message, p_Error_Code);
  END;
  
BEGIN
  IF (TRIM(p_input_line) IS NULL) THEN 
    RETURN;
  END IF;

  -- 1. Парсим строку
  v_cftId      := REGEXP_SUBSTR(p_input_line, '[^;]+', 1, 1);
  v_cftFIO     := REGEXP_SUBSTR(p_input_line, '[^;]+', 1, 2);
  v_cftAccount := REGEXP_SUBSTR(p_input_line, '[^;]+', 1, 3);
  v_cftBIC     := REGEXP_SUBSTR(p_input_line, '[^;]+', 1, 4);

  -- 2. Проверяем v_CftId в dobjcode_dbt
  BEGIN
    SELECT t_objectid INTO v_partyId
      FROM dobjcode_dbt
     WHERE t_code = v_CftId
       AND t_objecttype = 3
       AND t_codekind = 101
       AND t_state = 0;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      e_msg := 'Не найден субъект в СОФР по CFT ID = ' || v_cftId;
      InsertProtocol(e_msg, c_error_code);
      RAISE e_critical;
  END;

  -- 3. Проверяем совпадение ФИО
  SELECT t_name INTO v_FIOSofr FROM dparty_dbt WHERE t_partyid = v_partyId;

  IF UPPER(v_FIOSofr) != UPPER(v_cftFIO) THEN
    e_msg := 'Не совпадает ФИО клиента в СОФР и ЦФТ. CFT ID=' || v_cftId ||
             ', ФИО СОФР = ' || v_FIOSofr || ', ФИО ЦФТ = ' || v_cftFIO;
    InsertProtocol(e_msg, c_error_code);
    RAISE e_critical;
  END IF;

  -- 4. Проверяем БИК
  BEGIN
    SELECT t_objectid INTO v_BankPartyId
      FROM dobjcode_dbt
     WHERE t_code = v_CftBIC
       AND t_objecttype = 3
       AND t_codekind = 3
       AND t_state = 0;
       
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      e_msg := 'Не найден субъект в СОФР с БИК = ' || v_cftBIC;
      InsertProtocol(e_msg, c_error_code);
      RAISE e_critical;
  END;
  
  -- 5.1 Является ли счет рублевым?
  IF SUBSTR(v_cftAccount,6,3) != '810' THEN
     e_msg := 'Код валюты загружаемого счета не равен 810, загрузка невозможна. Счет: ' || v_cftAccount;
     RAISE_APPLICATION_ERROR(-20010, e_msg); 
  END IF;

  -- 5.2 Если БанкPartyId = Наш банк -> проверяем счёт
  IF v_BankPartyId = v_OurBankId THEN
      
    BEGIN
      SELECT 1 INTO v_dummy
        FROM daccount_dbt
       WHERE t_account = v_cftAccount
         AND t_close_date = TO_DATE('01010001','ddmmyyyy')
         AND t_code_currency = 0 
         AND rownum = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        e_msg := 'В СОФР для клиента с CFT ID = ' || v_cftId ||
                 ' не найден открытый счёт по номеру счета = ' || v_cftAccount || ' в нашем банке.';
        InsertProtocol(e_msg, c_error_code);
        RAISE e_critical;
    END;
  END IF;

  -- 6. Поиск или вставка СПИ
  BEGIN
    SELECT t_settaccid
      INTO v_curSettAccId
      FROM dsettacc_dbt
     WHERE t_partyid = v_partyId
       AND t_account = v_cftAccount
       AND t_bankid = v_bankPartyId
       AND t_fiid = 0 
       AND rownum = 1;
      
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      -- Подготовка параметров для вставки
      SELECT NVL(MAX(t_order), 0) + 5 INTO v_order FROM dsettacc_dbt;

      -- ИНН
      SELECT NVL(MAX(t_code), CHR(1))
        INTO v_inn
        FROM dobjcode_dbt
       WHERE t_objecttype = 3
         AND t_codekind = 16
         AND t_objectid = v_partyId
         AND t_state = 0;

      -- Название клиента
      SELECT t_name INTO v_bankname FROM dparty_dbt WHERE t_partyid = v_bankPartyId;

      -- Код банка
      SELECT NVL(MAX(t_code), CHR(1))
        INTO v_bankcode
        FROM dobjcode_dbt
       WHERE t_objecttype = 3
         AND t_codekind = 3
         AND t_objectid = v_bankPartyId
         AND t_state = 0;

      -- Разные ветки для ГО/не ГО
      IF v_bankPartyId != v_ourBankId THEN
       BEGIN
        SELECT t_bic_rcc, t_coracc
          INTO v_bankcorrcode, v_corracc
          FROM dbankdprt_dbt
         WHERE t_partyid = v_bankPartyId;
       
        SELECT t_objectid, t_name
          INTO v_bankcorrid, v_bankcorrname
          FROM dobjcode_dbt d
               JOIN dparty_dbt p
               ON p.t_partyid = d.t_objectid
         WHERE d.t_objecttype = 3
           AND d.t_codekind = 3
           AND d.t_code = v_bankcorrcode;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            v_bankcorrid   := -1;
            v_bankcorrcode := CHR(1);
            v_bankcorrname := CHR(1);
            v_corracc      := CHR(1); 
        END;
      ELSE
        v_bankcorrid   := -1;
        v_bankcorrcode := CHR(1);
        v_bankcorrname := CHR(1);
        v_corracc      := CHR(1);
      END IF;

      -- Код банка для codekind=1
      SELECT NVL(MAX(t_code), CHR(1))
        INTO v_code
        FROM dobjcode_dbt
       WHERE t_objecttype = 3
         AND t_codekind = 1
         AND t_objectid = v_bankPartyId
         AND t_state = 0;

      -- Вставка СПИ
      INSERT INTO dsettacc_dbt (
        t_partyid, t_bankid, t_fiid, t_chapter,
        t_account, t_inn, t_recname,
        t_bankcodekind, t_bankcode, t_bankname,
        t_bankcorrid, t_bankcorrcodekind, t_bankcorrcode,
        t_bankcorrname, t_corracc, t_fikind, t_beneficiaryid,
        t_codekind, t_code, t_description, t_order,
        t_shortname, t_noaccept, t_kzpartycode, t_spi_ident
      )
      VALUES (
        v_partyId, v_bankPartyId, 0,
        CASE WHEN v_bankPartyId != v_ourBankId THEN 0 ELSE 1 END,
        v_CftAccount, v_inn, v_FIOSofr,
        3, v_bankcode, v_bankname,
        v_bankcorrid, 3, v_bankcorrcode,
        v_bankcorrname, v_corracc, 1, v_partyId,
        1, v_code, 'Выплата излишне уплаченного налога',
        v_order, 'Выплата излишне уплаченного налога',
        CHR(0), CHR(1), CHR(1)
      )
      RETURNING t_settaccid INTO v_curSettAccId;
  END;
  
  -- 7. Поиск или вставка параметров СПИ
  BEGIN
    SELECT 1
      INTO v_dummy
      FROM dpmautoac_dbt
     WHERE t_settaccid = v_curSettAccId
       AND t_servicekind = 1 
       AND t_kindoper IN (2037, 2039);
     
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      SELECT NVL(MAX(t_order), 0) + 5
        INTO v_pmauto_order
        FROM dpmautoac_dbt
       WHERE t_partyid = v_partyId
         AND t_servicekind = 1;

      INSERT INTO dpmautoac_dbt (
        t_partyid, t_fiid, t_kindoper, t_purpose,
        t_settaccid, t_fikind, t_servicekind,
        t_order, t_account, t_reserve
      )
      VALUES (
        v_partyId, 0, 2039, 0, v_curSettAccId,
        1, 1, v_pmauto_order, CHR(1), CHR(1)
      );
  END;

  InsertProtocol('СПИ и параметры успешно обработаны. CFT ID=' || v_cftId, c_success_code);
  COMMIT;

EXCEPTION
  WHEN e_critical THEN
    NULL; -- пропускаем строку
  WHEN OTHERS THEN
    IF e_msg IS NOT NULL THEN
      InsertProtocol(e_msg, c_error_code);     
    ELSE
      InsertProtocol('Ошибка при обработке: ' || SQLERRM, c_error_code);   
    END IF;
      
END;
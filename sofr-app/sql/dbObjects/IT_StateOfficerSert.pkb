CREATE OR REPLACE PACKAGE BODY IT_STATEOFFICERSERT
IS
  /**
  * Сбор данных по доходам клиента по объектам НДР: Купонный доход
  * @since RSHB 104
  * @qtest NO
  * @param p_RefID     идентификатор экземпляра запроса от ЕФР
  * @param p_PartyID   идентификатор клиента
  * @param p_StartDate дата начала периода
  * @param p_EndDate   дата окончания периода
  */
  PROCEDURE GetDataByIncomeTypeCoupon(p_RefID IN NUMBER, p_PartyID IN NUMBER, p_StartDate IN DATE, p_EndDate IN DATE)
  IS
    v_CoupSum DNPTXOBJ_DBT.T_SUM%TYPE;
  BEGIN
    --Отбор данных по купонному доходу: сумма НДР 4го уровня с кодом "Plus_Sec", по виду аналитики 3 = "ЦБ", по отобранному клиенту, с датами НДР, входящими в период запроса; без НДР с признаком "Технический расчет"
    SELECT NVL(SUM(nptx.t_Sum), 0) INTO v_CoupSum
      FROM dnptxobj_dbt nptx
     WHERE nptx.t_Client = p_PartyID
       AND nptx.t_Level = 4
       AND nptx.t_Kind = RSI_NPTXC.TXOBJ_PROC_SEC --Proc_Sec
       AND nptx.t_Date BETWEEN p_StartDate AND p_EndDate
       AND nptx.t_AnaliticKind3 = RSI_NPTXC.TXOBJ_KIND3010 --ЦБ
       AND nptx.t_Technical = CHR(0); --Не технический расчет

    IF v_CoupSum > 0 THEN
      INSERT INTO DUSERREFGOSREGINC_DBT 
                  (T_REFID, 
                   T_INCOMETYPE, 
                   T_INCOMETYPENUM, 
                   T_INCOMEAMOUNT, 
                   T_FROMSOFR
                  )
           VALUES (p_RefID, 
                   'Купонный доход', 
                   INCOME_TYPE_COUPON, 
                   v_CoupSum, 
                   'X'
                  ); 
    END IF;
  END GetDataByIncomeTypeCoupon;

  /**
  * Сбор данных по доходам клиента по объектам НДР: Доходы от операций с ценными бумагами
  * @since RSHB 104
  * @qtest NO
  * @param p_RefID     идентификатор экземпляра запроса от ЕФР
  * @param p_PartyID   идентификатор клиента
  * @param p_StartDate дата начала периода
  * @param p_EndDate   дата окончания периода
  */
  PROCEDURE GetDataByIncomeTypeSecurities(p_RefID IN NUMBER, p_PartyID IN NUMBER, p_StartDate IN DATE, p_EndDate IN DATE)
  IS
    v_PlusSecSum  DNPTXOBJ_DBT.T_SUM%TYPE;
    v_MinusSecSum DNPTXOBJ_DBT.T_SUM%TYPE;
    v_ItogSecSum  DNPTXOBJ_DBT.T_SUM%TYPE;
  BEGIN
    --Отбор данных по доходу от реализации (суммарные данные по доходам от реализации ЦБ за вычетом суммарных данных  по расходам от реализации ЦБ)
    --Отбор данных по доходам: сумма НДР 4го уровня с кодом "Plus_Sec", по виду аналитики 3 = "ЦБ", по отобранному клиенту, с датами НДР, входящими в период запроса; без НДР с признаком "Технический расчет"
    SELECT NVL(SUM(nptx.t_Sum), 0) INTO v_PlusSecSum
      FROM dnptxobj_dbt nptx
     WHERE nptx.t_Client = p_PartyID
       AND nptx.t_Level = 4
       AND nptx.t_Kind = RSI_NPTXC.TXOBJ_PLUS_SEC --Plus_Sec
       AND nptx.t_Date BETWEEN p_StartDate AND p_EndDate
       AND nptx.t_AnaliticKind3 = RSI_NPTXC.TXOBJ_KIND3010 --ЦБ
       AND nptx.t_Technical = CHR(0); --Не технический расчет

    --Отбор данных по расходам: сумма НДР 4го уровня с кодом "Minus_Sec", по виду аналитики 3 = "ЦБ", по отобранному клиенту, с датами НДР, входящими в период запроса; без НДР с признаком "Технический расчет"
    SELECT NVL(SUM(nptx.t_Sum), 0) INTO v_MinusSecSum
      FROM dnptxobj_dbt nptx
     WHERE nptx.t_Client = p_PartyID
       AND nptx.t_Level = 4
       AND nptx.t_Kind = RSI_NPTXC.TXOBJ_MINUS_SEC --Minus_Sec
       AND nptx.t_Date BETWEEN p_StartDate AND p_EndDate
       AND nptx.t_AnaliticKind3 = RSI_NPTXC.TXOBJ_KIND3010 --ЦБ
       AND nptx.t_Technical = CHR(0); --Не технический расчет

    v_ItogSecSum := v_PlusSecSum - v_MinusSecSum;

    IF v_ItogSecSum > 0 THEN
      INSERT INTO DUSERREFGOSREGINC_DBT 
                  (T_REFID, 
                   T_INCOMETYPE, 
                   T_INCOMETYPENUM, 
                   T_INCOMEAMOUNT, 
                   T_FROMSOFR
                  )
           VALUES (p_RefID, 
                   'Доходы от операций с ценными бумагами', 
                   INCOME_TYPE_SECURITIES, 
                   v_ItogSecSum, 
                   'X'
                  ); 
    END IF;
  END GetDataByIncomeTypeSecurities;

  /**
  * Сбор данных по доходам клиента по объектам НДР: Доходы по векселям
  * @since RSHB 104
  * @qtest NO
  * @param p_RefID     идентификатор экземпляра запроса от ЕФР
  * @param p_PartyID   идентификатор клиента
  * @param p_StartDate дата начала периода
  * @param p_EndDate   дата окончания периода
  */
  PROCEDURE GetDataByIncomeTypeBills(p_RefID IN NUMBER, p_PartyID IN NUMBER, p_StartDate IN DATE, p_EndDate IN DATE)
  IS
    v_BillSum DNPTXOBJ_DBT.T_SUM%TYPE;
  BEGIN
    --Отбор данных по купонному доходу: сумма НДР 5го уровня с кодом "PlusG_2800", по отобранному клиенту, с датами НДР, входящими в период запроса; без НДР с признаком "Технический расчет"
    SELECT NVL(SUM(nptx.t_Sum), 0) INTO v_BillSum
      FROM dnptxobj_dbt nptx
     WHERE nptx.t_Client = p_PartyID
       AND nptx.t_Level = 5
       AND nptx.t_Kind = RSI_NPTXC.TXOBJ_PLUSG_2800 --PlusG_2800
       AND nptx.t_Date BETWEEN p_StartDate AND p_EndDate
       AND nptx.t_Technical = CHR(0); --Не технический расчет

    IF v_BillSum > 0 THEN
      INSERT INTO DUSERREFGOSREGINC_DBT 
                  (T_REFID, 
                   T_INCOMETYPE, 
                   T_INCOMETYPENUM, 
                   T_INCOMEAMOUNT, 
                   T_FROMSOFR
                  )
           VALUES (p_RefID, 
                   'Доходы по векселям', 
                   INCOME_TYPE_BILLS, 
                   v_BillSum, 
                   'X'
                  ); 
    END IF;
  END GetDataByIncomeTypeBills;

  /**
  * Преобразование числа в строку при необходимости с добавлением нуля в целой части
  * @since RSHB 104
  * @qtest NO
  * @param p_Val Число
  * @return число в формате строки
  */     
  FUNCTION LeadZero(p_Val IN NUMBER)
    RETURN VARCHAR2 
  IS
  BEGIN
    IF INSTR(REPLACE(TO_CHAR(p_Val),',','.'), '.') = 1 THEN
      RETURN '0' || REPLACE(TO_CHAR(p_Val),',','.');
    ELSE
      RETURN REPLACE(TO_CHAR(p_Val),',','.');
    END IF;  
  END;

  /**
  * Сбор данных по стоимости приобретения по ценным бумагам
  * @since RSHB 104
  * @qtest NO
  * @param p_RefID   идентификатор экземпляра запроса от ЕФР
  * @param p_PartyID идентификатор клиента
  * @param p_ReportingDate отчетная дата
  */
  PROCEDURE GetDataByAvoirTotalAmount(p_RefID IN NUMBER, p_PartyID IN NUMBER, p_ReportingDate IN DATE)
  IS
    v_ErrText    DUSERREFGOSREGAVOIR_DBT.T_ERRORTEXT%TYPE;
    v_SofrAmount DUSERREFGOSREGAVOIR_DBT.T_TOTALQUANTITYNONSTOCK%TYPE;
    v_GUID       VARCHAR2(32);
    v_ExpAmount  DSCSUMCONFEXP_DBT.T_AMOUNT%TYPE; 
    v_ExpCost    DSCSUMCONFEXP_DBT.T_DEALSUM_NAT%TYPE;
  BEGIN
    FOR cur IN (SELECT *
                  FROM DUSERREFGOSREGAVOIR_DBT
                 WHERE t_RefID = p_RefID
                   AND t_ErrorNum = 0
               )
    LOOP
      BEGIN
        --Определение количества бумаг в остатке: отбираются суммарные остатки по счетам внутреннего учета по учету ценных бумаг на конец даты p_ReportingDate
        SELECT NVL(SUM(AccRest), 0) INTO v_SofrAmount
          FROM (SELECT ABS(rsb_account.restac(t_Account, t_Currency, p_ReportingDate, t_Chapter, null)) as AccRest
                  FROM (SELECT DISTINCT accd.t_Account, accd.t_Chapter, accd.t_Currency
                          FROM (SELECT * FROM dmccateg_dbt WHERE t_LevelType = 1 AND t_Code = 'ЦБ Клиента, ВУ') cat, dmcaccdoc_dbt accd
                         WHERE accd.t_CatID = cat.t_ID
                           AND accd.t_Owner = p_PartyID
                           AND accd.t_Currency = cur.t_FIID
                           AND accd.t_IsCommon = 'X'
                           AND accd.t_ActivateDate <= p_ReportingDate
                           AND (accd.t_DisablingDate = TO_DATE('01.01.0001','DD.MM.YYYY') or accd.t_DisablingDate >= p_ReportingDate)
                       ) 
               );
 
        --Сверка количества бумаг в СОФР и ЕФР
        IF v_SofrAmount != cur.t_TOTALQUANTITYNONSTOCK THEN
          v_ErrText := 'Количество бумаг  выпуска '||cur.t_ISINLSIN||' в СОФР равно '||LeadZero(v_SofrAmount)||' и не совпадает с количеством бумаг от Диасофт '||LeadZero(cur.t_TOTALQUANTITYNONSTOCK);
          UPDATE DUSERREFGOSREGAVOIR_DBT 
             SET t_ErrorNum = ERROR_DIFFERENT_AVOIRISS_COUNT,
                 t_ErrorText = v_ErrText
           WHERE t_RefID = p_RefID
             AND t_FIID = cur.t_FIID;
        ELSE
          --Определение покупной стоимости бумаг в остатке (аналогично отчету "Отчет "Суммы подтвержденных расходов"")         
          SELECT CAST(SYS_GUID() AS VARCHAR2(32)) INTO v_GUID FROM dual; --Получение уникального текстового GUID

          --Определение списка сделок\операций покупок по остатку бумаг, на основании которого будет заполнена данными таблица dscsumconfexp_dbt 
          RSI_SUMCONFEXP.CreateSumConfirmExpRepData(p_GUID => v_GUID, 
                                                    p_OnDate => p_ReportingDate, 
                                                    p_LaunchMode => 0, 
                                                    p_ClientID => p_PartyID,
                                                    p_DlContrID => 0,
                                                    p_FIID => cur.t_FIID);

          SELECT SUM(t_Amount), SUM(t_DealSum_Nat)
            INTO v_ExpAmount, v_ExpCost
            FROM (SELECT NVL(dexp.t_Amount, 0) t_Amount, 
                         CASE WHEN NVL(dexp.t_DealSum_Nat, 0) <> 0 THEN dexp.t_DealSum_Nat
                              ELSE NVL(dexp.t_Amount, 0) * NVL(dexp.t_FaceValue, 0) * NVL(RSI_RSB_FIInstr.ConvSum(1, fin.t_FaceValueFI, RSI_RSB_FIInstr.NATCUR, dexp.t_SettlDate), 0) --Для нулевых значений рассчитываем сумму, исходя из номинала
                         END t_DealSum_Nat
                    FROM dscsumconfexp_dbt dexp, dfininstr_dbt fin
                   WHERE dexp.t_GUID = v_GUID
                     AND dexp.t_FIID = fin.t_FIID
                 );

          IF v_ExpAmount != cur.t_TOTALQUANTITYNONSTOCK THEN
            v_ErrText := 'Ошибка определения покупной стоимости. Количество бумаг  выпуска '||cur.t_ISINLSIN||' по сделкам в СОФР равно '||LeadZero(v_ExpAmount)||' и не совпадает с количеством бумаг от Диасофт '||LeadZero(cur.t_TOTALQUANTITYNONSTOCK);
            UPDATE DUSERREFGOSREGAVOIR_DBT 
               SET t_ErrorNum = ERROR_DETERMINATION_COST,
                   t_ErrorText = v_ErrText
             WHERE t_RefID = p_RefID
               AND t_FIID = cur.t_FIID;
          ELSE
            --Обновляем сумму в случае успеха
            UPDATE DUSERREFGOSREGAVOIR_DBT 
               SET t_GUID = v_GUID,
                   t_FromSOFR = 'X',
                   t_TOTALAMOUNTNONSTOCK = v_ExpCost
             WHERE t_RefID = p_RefID
               AND t_FIID = cur.t_FIID;
          END IF;
        END IF;

      EXCEPTION WHEN OTHERS THEN
        v_ErrText := SUBSTR('Непредвиденная ошибка определения покупной стоимости по выпуску с кодом '||cur.t_ISINLSIN||'. '||sqlerrm, 1, 300);
        UPDATE DUSERREFGOSREGAVOIR_DBT 
           SET t_ErrorNum = ERROR_UNEXPECTED_GET_COST,
               t_ErrorText = v_ErrText
         WHERE t_RefID = p_RefID
           AND t_FIID = cur.t_FIID;
      END;
    END LOOP;

  END GetDataByAvoirTotalAmount;

  /**
  * Обогащениe полученной от ЕФР информации данными по видам дохода и по стоимости приобретения ценных бумаг из СОФР 
  * @since RSHB 104
  * @qtest NO
  * @param p_RefID идентификатор экземпляра запроса от ЕФР
  */
  PROCEDURE AddingDataFromSOFR(p_RefID IN NUMBER)
  IS
    v_userrefgosreg DUSERREFGOSREG_DBT%ROWTYPE;
  BEGIN
    SELECT * INTO v_userrefgosreg
      FROM DUSERREFGOSREG_DBT 
     WHERE t_ID = p_RefID; 

    GetDataByIncomeTypeCoupon(p_RefID, v_userrefgosreg.t_PartyID, v_userrefgosreg.t_StartDate, v_userrefgosreg.t_EndDate);
    GetDataByIncomeTypeSecurities(p_RefID, v_userrefgosreg.t_PartyID, v_userrefgosreg.t_StartDate, v_userrefgosreg.t_EndDate);
    GetDataByIncomeTypeBills(p_RefID, v_userrefgosreg.t_PartyID, v_userrefgosreg.t_StartDate, v_userrefgosreg.t_EndDate);

    GetDataByAvoirTotalAmount(p_RefID, v_userrefgosreg.t_PartyID, v_userrefgosreg.t_ReportingDate);

  END AddingDataFromSOFR;  

  /**
  * Получение идентификатора клиента по виду кода "Код ЦФТ"  
  * @since RSHB 104
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
       AND t_CodeKind = PTCK_CFT
       AND t_Code = p_PartyCode 
       AND t_State = 0;
    RETURN v_PartyID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
  END GetPartyIDByCFT;

  /**
  * Проверка наличия хотя бы одного действующего ДБО по клиенту за период   
  * @since RSHB 104
  * @qtest NO
  * @param p_PartyID   Идентификатор клиента
  * @param p_StartDate Дата начала периода
  * @param p_EndDate   Дата окончания периода
  * @return 1, если есть ДБО; 0, если нет ДБО
  */
  FUNCTION CheckDBO(p_PartyID IN NUMBER, p_StartDate IN DATE, p_EndDate IN DATE) 
    RETURN NUMBER
  IS
    v_ret NUMBER;
  BEGIN
    SELECT 1 INTO v_ret
      FROM dsfcontr_dbt 
     WHERE t_ServKind = 0
       AND t_PartyID = p_PartyID
       AND t_DateBegin <= p_EndDate 
       AND (t_DateClose = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DateClose >= p_StartDate)
       AND ROWNUM = 1;
    RETURN v_ret;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END CheckDBO;
  
  /**
  * Получение внутреннего идентификатора ценной бумаги в СОФР по коду ISIN/LSIN, возможно содержащемуся в наименовании бумаги 
  * @since RSHB 104
  * @qtest NO
  * @param p_Name     Наименование ц/б
  * @param p_ISINLSIN Идентификатор выпуска ISIN/LSIN
  * @return идентификатор ц/б
  */
  FUNCTION GetAvoirFIIDByName(p_Name IN VARCHAR2, p_ISINLSIN OUT VARCHAR2) 
    RETURN NUMBER
  IS
    v_FIID davoiriss_dbt.t_FIID%TYPE;
    v_CurStr VARCHAR2(1000) := p_Name;
    v_SubStr VARCHAR2(100);
    v_Ind NUMBER;
  BEGIN
    WHILE TRUE
    LOOP
      v_Ind := INSTR(v_CurStr, ' ', 1);
      v_SubStr := CASE WHEN v_Ind <> 0 THEN SUBSTR(v_CurStr, 1, v_Ind - 1) ELSE v_CurStr END;
     
      BEGIN
        SELECT t_FIID, t_ISIN 
          INTO v_FIID, p_ISINLSIN
          FROM davoiriss_dbt 
         WHERE t_ISIN = v_SubStr;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        BEGIN
          SELECT t_FIID, CASE WHEN t_ISIN = CHR(1) THEN t_LSIN ELSE t_ISIN END
            INTO v_FIID, p_ISINLSIN 
            FROM davoiriss_dbt
           WHERE t_LSIN = v_SubStr;
        EXCEPTION WHEN NO_DATA_FOUND THEN
          v_FIID := -1;
          p_ISINLSIN := '';
        END;
      END;

      IF v_FIID > -1 OR v_Ind = 0 THEN 
        EXIT;
      ELSE
        v_CurStr := SUBSTR(v_CurStr, v_Ind + 1);
      END IF;
    END LOOP;
    
    RETURN v_FIID;
  EXCEPTION
    WHEN OTHERS THEN 
    p_ISINLSIN := CHR(1);
    RETURN -1;
  END GetAvoirFIIDByName;

  /**
  * Получение внутреннего идентификатора ценной бумаги в СОФР по коду ISIN/LSIN 
  * @since RSHB 104
  * @qtest NO
  * @param p_ISINLSIN Идентификатор выпуска ISIN/LSIN
  * @return идентификатор ц/б
  */
  FUNCTION GetAvoirFIID(p_ISINLSIN IN VARCHAR2) 
    RETURN NUMBER
  IS
    v_FIID davoiriss_dbt.t_FIID%TYPE;
  BEGIN
    BEGIN
      SELECT t_FIID INTO v_FIID
        FROM davoiriss_dbt 
       WHERE t_ISIN = p_ISINLSIN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        SELECT t_FIID INTO v_FIID 
          FROM davoiriss_dbt
         WHERE t_LSIN = p_ISINLSIN;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        v_FIID := -1;
      END;
    END;
    RETURN v_FIID;
  END GetAvoirFIID;

  /**
  * Обработчик EFR.SendStateOfficerSertSofr
  * BIQ-18375 Автоматизация отчетной формы "Справка 5798-У" (справка для гос. служащего)
  * Доработка формирования отчетной формы путем ее обогащения данными из СОФР входящего сообщения XML из топика Кафки от ЕФР 
  * @since RSHB 104
  * @qtest NO
  */
  PROCEDURE GetReferenceFromEFR(p_worklogid integer     
                               ,p_messbody  clob        
                               ,p_messmeta  xmltype     
                               ,o_msgid     out varchar2
                               ,o_MSGCode   out integer 
                               ,o_MSGText   out varchar2
                               ,o_messbody  out clob    
                               ,o_messmeta  out xmltype)
  IS
    v_xml           XMLTYPE;
    v_namespace     VARCHAR2(128) := it_kafka.get_namespace(it_efr.C_C_SYSTEM_NAME, 'SendStateOfficerSertSofrReq');

    v_userrefgosreg DUSERREFGOSREG_DBT%ROWTYPE;
    v_check         NUMBER;

    v_CurFiid       DUSERREFGOSREGAVOIR_DBT.T_FIID%TYPE;
    v_ISINLSIN      DUSERREFGOSREGAVOIR_DBT.T_ISINLSIN%TYPE;
    v_ErrNum        DUSERREFGOSREGAVOIR_DBT.T_ERRORNUM%TYPE;
    v_ErrText       DUSERREFGOSREGAVOIR_DBT.T_ERRORTEXT%TYPE;
    v_IncomeTypeNum DUSERREFGOSREGINC_DBT.T_INCOMETYPENUM%TYPE;
    
    v_AvoirCnt      NUMBER;
    v_IncCnt        NUMBER;
    vx_messbody     XMLTYPE;
  BEGIN
    o_MSGCode := 0;
    o_MSGText := '-';
 
    BEGIN
      v_xml := it_xml.Clob_to_xml(p_messbody);
   
      --Получение основных атрибутов входящего xml
      WITH efr AS (SELECT v_xml AS x FROM dual)
      SELECT 0,
             TO_NUMBER(EXTRACTVALUE(efr.x, '*/RequestNumber', v_namespace)),
             TO_DATE(EXTRACTVALUE(efr.x, '*/StartDate', v_namespace), 'YYYY-MM-DD'),
             TO_DATE(EXTRACTVALUE(efr.x, '*/EndDate', v_namespace), 'YYYY-MM-DD'),
             it_xml.char_to_timestamp_iso8601(EXTRACTVALUE(efr.x, '*/RequestRegistrationDate', v_namespace)),
             EXTRACTVALUE(efr.x, '*/ClientId/ObjectId', v_namespace),
             EXTRACTVALUE(efr.x, '*/ClientId/SystemId', v_namespace),
             EXTRACTVALUE(efr.x, '*/ClientId/SystemNodeId', v_namespace),
             -1,
             TO_DATE('01.01.0001', 'DD.MM.YYYY'),
             TO_DATE(EXTRACTVALUE(efr.x, '*/ReportingDate', v_namespace), 'YYYY-MM-DD')
        INTO v_userrefgosreg
        FROM efr;

      IF v_userrefgosreg.t_RequestNumber IS NULL THEN
        o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
        o_MSGText := 'Непредвиденная ошибка получения данных в СОФР. Не задано значение тега RequestNumber или xmlns не соответствует ожидаемому';
      ELSE
        --Проверка наличия клиента в СОФР
        v_userrefgosreg.t_PartyID := GetPartyIDByCFT(v_userrefgosreg.t_ClientID);
        IF v_userrefgosreg.t_PartyID = -1 THEN
          o_MSGCode := ERROR_CLIENT_NOT_DEFINED;
          o_MSGText := 'Не удалось определить клиента в СОФР по коду ClientId = '||v_userrefgosreg.t_ClientID;
        ELSE
          --Проверка наличия договора у клиента в СОФР
          v_check := CheckDBO(v_userrefgosreg.t_PartyID, v_userrefgosreg.t_StartDate, v_userrefgosreg.t_ReportingDate);
          IF v_check = 0 THEN
            o_MSGCode := ERROR_CONTRACT_NOT_DEFINED;
            o_MSGText := 'Не удалось определить действующий договор по клиенту в СОФР с кодом клиента ClientId = '||v_userrefgosreg.t_ClientID;
          END IF;
        END IF;
      END IF;
   
      IF o_MSGCode = 0 OR o_MSGCode = ERROR_CONTRACT_NOT_DEFINED /*при отсутствии ДБО возвращаем все данные*/ THEN 
        --Вставка данных в таблицу экземпляров запроса справки
        INSERT INTO DUSERREFGOSREG_DBT 
             VALUES v_userrefgosreg 
          RETURNING t_ID INTO v_userrefgosreg.t_ID;

        --Вставка информации в таблицу данных по ЦБ
        FOR cur IN (WITH efr AS (SELECT v_xml AS x FROM dual)
                    SELECT EXTRACTVALUE(tf.column_value, '/NonStockInfo/TypeNameNonStockInfo', v_namespace) TypeNameNonStockInfo,
                           EXTRACTVALUE(tf.column_value, '/NonStockInfo/ISINRegistrationNumber', v_namespace) ISINRegistrationNumber,
                           EXTRACTVALUE(tf.column_value, '/NonStockInfo/IssuerName', v_namespace) IssuerName,
                           it_xml.char_to_number(EXTRACTVALUE(tf.column_value, '/NonStockInfo/NominalSum', v_namespace)) NominalSum,
                           it_xml.char_to_number(EXTRACTVALUE(tf.column_value, '/NonStockInfo/TotalQuantityNonStock', v_namespace)) TotalQuantityNonStock,
                           it_xml.char_to_number(EXTRACTVALUE(tf.column_value, '/NonStockInfo/TotalAmountNonStock', v_namespace)) TotalAmountNonStock
                      FROM efr, TABLE(XMLSEQUENCE(EXTRACT(efr.x, '*/DepoInfo/NonStockInfoList/NonStockInfo', v_namespace))) tf
                   )
        LOOP
          v_ErrNum  := 0;
          v_ErrText := CHR(1);
     
          IF cur.ISINRegistrationNumber IS NOT NULL AND cur.ISINRegistrationNumber != CHR(1) THEN
            v_CurFiid  := GetAvoirFIID(cur.ISINRegistrationNumber); --Получение идентификатора ЦБ по ISIN/LSIN
            v_ISINLSIN := cur.ISINRegistrationNumber;
          ELSE
            v_CurFiid := GetAvoirFIIDbyName(cur.TypeNameNonStockInfo, v_ISINLSIN); --Попытка получения идентификатора ЦБ по фрагментам наименования
          END IF;
          IF v_CurFiid = -1 THEN
            v_ErrNum  := ERROR_AVOIRISS_NOT_DEFINED;
            v_ErrText := 'Не удалось определить в СОФР выпуск по коду ' || v_ISINLSIN;
          END IF;
     
          INSERT INTO DUSERREFGOSREGAVOIR_DBT 
                      (T_REFID, 
                       T_TYPENAMENONSTOCKINFO, 
                       T_ISINLSIN, 
                       T_FIID, 
                       T_ISSUERNAME, 
                       T_NOMINALSUM, 
                       T_TOTALQUANTITYNONSTOCK, 
                       T_TOTALAMOUNTNONSTOCK, 
                       T_FROMSOFR, 
                       T_ERRORNUM, 
                       T_ERRORTEXT, 
                       T_GUID
                      )
               VALUES (v_userrefgosreg.t_ID, 
                       cur.TypeNameNonStockInfo, 
                       v_ISINLSIN, 
                       v_CurFiid, 
                       cur.IssuerName, 
                       cur.NominalSum, 
                       cur.TotalQuantityNonStock, 
                       cur.TotalAmountNonStock, 
                       CHR(0),
                       v_ErrNum,
                       v_ErrText,
                       CHR(1)
                      ); 
        END LOOP;
     
        --Вставка информации в таблицу данных по видам дохода
        FOR cur IN (WITH efr AS (SELECT v_xml AS x FROM dual)
                    SELECT EXTRACTVALUE(tf.column_value, '/IncomeSecuritiesInfo/IncomeType', v_namespace) IncomeType,
                           it_xml.char_to_number(EXTRACTVALUE(tf.column_value, '/IncomeSecuritiesInfo/IncomeAmount', v_namespace)) IncomeAmount
                      FROM efr, TABLE(XMLSEQUENCE(EXTRACT(efr.x, '*/DepoInfo/IncomeSecuritiesInfoList/IncomeSecuritiesInfo', v_namespace))) tf
                   )
        LOOP
          v_IncomeTypeNum := CASE WHEN cur.IncomeType = 'Купонный доход' THEN INCOME_TYPE_COUPON
                                  ELSE 0 
                             END;
     
          INSERT INTO DUSERREFGOSREGINC_DBT 
                      (T_REFID, 
                       T_INCOMETYPE, 
                       T_INCOMETYPENUM, 
                       T_INCOMEAMOUNT, 
                       T_FROMSOFR
                      )
               VALUES (v_userrefgosreg.t_ID, 
                       cur.IncomeType, 
                       v_IncomeTypeNum, 
                       cur.IncomeAmount, 
                       CHR(0)
                      ); 
        END LOOP;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText := 'Непредвиденная ошибка получения данных в СОФР. '||sqlerrm;
    END;
   
    IF o_MSGCode = 0 THEN
      AddingDataFromSOFR(v_userrefgosreg.t_ID); --Обогащение данными из СОФР 
    ELSIF o_MSGCode = ERROR_CONTRACT_NOT_DEFINED THEN 
      GetDataByIncomeTypeBills(v_userrefgosreg.t_ID, v_userrefgosreg.t_PartyID, v_userrefgosreg.t_StartDate, v_userrefgosreg.t_EndDate); --При отсутствии активного ДБО всё равно соберем данные по векселям
      COMMIT; --Закоммитим вручную, т.к. в случае ошибки транзакция откатится
    END IF;

    --ОТВЕТ
    --Получим кол-во данных по ЦБ и видам дохода (может измениться после обогащения данными), чтобы знать, формировать ли соответствующие разделы 
    SELECT COUNT(1) INTO v_AvoirCnt FROM DUSERREFGOSREGAVOIR_DBT WHERE t_RefID = v_userrefgosreg.t_ID;

    SELECT COUNT(1) INTO v_IncCnt FROM DUSERREFGOSREGINC_DBT 
     WHERE t_RefID = v_userrefgosreg.t_ID 
       AND (o_MSGCode = ERROR_CONTRACT_NOT_DEFINED /*при отсутствии ДБО возвращаем все данные*/ OR t_IncomeTypeNum IN (INCOME_TYPE_COUPON, INCOME_TYPE_SECURITIES, INCOME_TYPE_BILLS));
       
    SELECT XMLELEMENT("ResendStateOfficerSertEfrReq",
                      XMLELEMENT("RequestNumber", v_userrefgosreg.t_RequestNumber),
                      CASE WHEN v_userrefgosreg.t_ClientId IS NULL AND v_userrefgosreg.t_SystemId IS NULL AND v_userrefgosreg.t_SystemNodeId IS NULL THEN NULL
                           ELSE XMLELEMENT("ClientId", CASE WHEN v_userrefgosreg.t_ClientId IS NULL THEN NULL 
                                                            ELSE XMLELEMENT("ObjectId", v_userrefgosreg.t_ClientId) 
                                                       END,
                                                       CASE WHEN v_userrefgosreg.t_SystemId IS NULL THEN NULL 
                                                            ELSE XMLELEMENT("SystemId", v_userrefgosreg.t_SystemId) 
                                                       END,
                                                       CASE WHEN v_userrefgosreg.t_SystemNodeId IS NULL THEN NULL 
                                                            ELSE XMLELEMENT("SystemNodeId", v_userrefgosreg.t_SystemNodeId) 
                                                       END
                                          )
                      END,
                      CASE WHEN v_AvoirCnt = 0 AND v_IncCnt = 0 THEN NULL
                           ELSE XMLELEMENT("DepoInfo", CASE WHEN v_AvoirCnt = 0 THEN NULL 
                                                            ELSE XMLELEMENT("NonStockInfoList", (SELECT XMLAGG(XMLELEMENT("NonStockInfo", XMLELEMENT("TypeNameNonStockInfo", avoir.t_TypeNameNonStockInfo),
                                                                                                                                          XMLELEMENT("IssuerName", avoir.t_IssuerName),
                                                                                                                                          XMLELEMENT("NominalSum", LeadZero(avoir.t_NominalSum)),
                                                                                                                                          XMLELEMENT("TotalQuantityNonStock", LeadZero(avoir.t_TotalQuantityNonStock)),
                                                                                                                                          XMLELEMENT("TotalAmountNonStock", LTRIM(TO_CHAR(avoir.t_TotalAmountNonStock, '9999999999999999999999990.00'))),
                                                                                                                                          XMLELEMENT("ISINRegistrationNumber", avoir.t_ISINLSIN),
                                                                                                                                          CASE WHEN avoir.t_ErrorNum = 0 THEN NULL
                                                                                                                                               ELSE XMLELEMENT("BusinessErrorList", XMLELEMENT("BusinessError", XMLELEMENT("BusinessErrorCode", avoir.t_ErrorNum),
                                                                                                                                                                                                                XMLELEMENT("BusinessErrorDesc", avoir.t_ErrorText)
                                                                                                                                                                                              )
                                                                                                                                                              ) 
                                                                                                                                          END
                                                                                                                         )
                                                                                                              )
                                                                                                  FROM DUSERREFGOSREGAVOIR_DBT avoir
                                                                                                 WHERE avoir.t_RefID = v_userrefgosreg.t_ID
                                                                                                )
                                                                           ) 
                                                       END,
                                                       CASE WHEN v_IncCnt = 0 THEN NULL 
                                                            ELSE XMLELEMENT("IncomeSecuritiesInfoList", (SELECT XMLAGG(XMLELEMENT("IncomeSecuritiesInfo", XMLELEMENT("IncomeType", inc.t_IncomeType),
                                                                                                                                                          XMLELEMENT("IncomeAmount", LTRIM(TO_CHAR(SUM(inc.t_IncomeAmount), '9999999999999999999999990.00')))
                                                                                                                                 )
                                                                                                                      )
                                                                                                           FROM DUSERREFGOSREGINC_DBT inc
                                                                                                          WHERE inc.t_RefID = v_userrefgosreg.t_ID
                                                                                                            AND (o_MSGCode = ERROR_CONTRACT_NOT_DEFINED /*при отсутствии ДБО возвращаем все данные*/ 
                                                                                                                 OR inc.t_IncomeTypeNum IN (INCOME_TYPE_COUPON, INCOME_TYPE_SECURITIES, INCOME_TYPE_BILLS))
                                                                                                          GROUP BY inc.t_IncomeType
                                                                                                        )
                                                                           ) 
                                                       END
                                          ) 
                      END
                     )
      INTO vx_messbody
      FROM dual;
    o_messbody := vx_messbody.getClobVal;
    
  END GetReferenceFromEFR;

END IT_STATEOFFICERSERT;
/
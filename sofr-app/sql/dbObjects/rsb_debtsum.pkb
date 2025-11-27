CREATE OR REPLACE PACKAGE BODY RSB_DEBTSUM
IS
 /**
 * Получим номер брокерской комиссии по коду 
 * @since RSHB 115
 * @qtest NO
 * @param p_ComCode код комиссии
 * @return номер комиссии
 */                    
  FUNCTION GetCommissNumber(p_ComCode IN VARCHAR2)
    RETURN NUMBER
  IS
    v_Number dsfcomiss_dbt.t_Number%TYPE;
  BEGIN
    SELECT t_Number INTO v_Number 
      FROM dsfcomiss_dbt 
     WHERE t_FeeType = SF_FEE_TYPE_PERIOD
       AND t_ReceiverID = RSBSESSIONDATA.OurBank 
       AND LOWER(t_Code) = LOWER(p_ComCode)
       AND ROWNUM = 1;  
    RETURN v_Number;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END GetCommissNumber;

 /**
 * Добавление записи об ошибке/предупреждении в лог операции списания/выносе на просрочку задолженности
 * @since RSHB 115
 * @qtest NO
 * @param p_Type Тип сообщения
 * @param p_DlContrID Идентификатор ДБО
 * @param p_Text Текст сообщения
 */
  PROCEDURE AddToLog(p_Type IN NUMBER, p_DlContrID IN NUMBER, p_Text IN VARCHAR2)
  IS
  BEGIN
    INSERT INTO ddl_debtsumlog_tmp (t_Type, t_DlContrID, t_Text)
         VALUES (p_Type, p_DlContrID, SUBSTR(p_Text, 1, 500)); 
  END AddToLog;

 /**
 * Сохранение блока протокола в операции списания задолженности
 * @since RSHB 115
 * @qtest NO
 * @param p_ID Идентификатор операции
 * @param p_Chunk Блок данных для записи в Clob
 */
  PROCEDURE WrtOffAppendLogToClob(p_ID IN NUMBER, p_Chunk IN CLOB)
  IS
    v_Clob CLOB;
  BEGIN
    SELECT t_Log
      INTO v_Clob
      FROM ddl_wrtoffdebtsumop_dbt
     WHERE t_ID = p_ID
    FOR UPDATE;
  
    IF v_clob is null THEN
      UPDATE ddl_wrtoffdebtsumop_dbt SET t_Log = p_Chunk where t_ID = p_ID;
    ELSE
      DBMS_LOB.Append(v_Clob, p_Chunk);
    END IF;
  
    COMMIT;
  END WrtOffAppendLogToClob;

 /**
 * Сохранение блока протокола в операции выноса задолженности на просрочку
 * @since RSHB 116
 * @qtest NO
 * @param p_ID Идентификатор операции
 * @param p_Chunk Блок данных для записи в Clob
 */
  PROCEDURE EnrollAppendLogToClob(p_ID IN NUMBER, p_Chunk IN CLOB)
  IS
    v_Clob CLOB;
  BEGIN
    SELECT t_Log
      INTO v_Clob
      FROM ddl_enrolldebtsumop_dbt
     WHERE t_ID = p_ID
    FOR UPDATE;
  
    IF v_clob is null THEN
      UPDATE ddl_enrolldebtsumop_dbt SET t_Log = p_Chunk where t_ID = p_ID;
    ELSE
      DBMS_LOB.Append(v_Clob, p_Chunk);
    END IF;
  
    COMMIT;
  END EnrollAppendLogToClob;

 /**
 * Подготовка данных о задолженности клиентов к списанию
 * @since RSHB 115
 * @qtest NO
 * @param p_OperID Идентификатор операции списания
 * @param p_ProcDate Дата процедуры
 * @param p_ClientID Идентификатор клиента
 * @param p_DlContrID Идентификатор ДБО
 */
  PROCEDURE GetWrtOffDebtSumData(p_OperID IN NUMBER, p_ProcDate IN DATE, p_ClientID IN NUMBER, p_DlContrID IN NUMBER)
  IS
    v_sql VARCHAR2(10000);
    v_RestAcc ddl_wrtoffdebtsum_dbt.t_RestAcc%TYPE;
    v_DebtSumID ddl_wrtoffdebtsum_dbt.t_ID%TYPE;
    v_DebtSum ddl_wrtoffdebtsum_dbt.t_DebtSum%TYPE;
  BEGIN
    DELETE FROM ddl_wrtoffdebtsumval_tmp;

    v_sql := ' 
      INSERT INTO ddl_wrtoffdebtsumval_tmp (
             t_Sort,
             t_DebtDate,                   
             t_DebtType,                   
             t_DebtSourceID,                     
             t_DebtSum,                    
             t_DebtCur,                   
             t_DebtSfContr,
             t_ClientID,                
             t_DlContrID
             )                               
      WITH dt AS (SELECT :p_ProcDate t_ProcDate FROM dual),                                                                
           sf AS (SELECT dt.t_ProcDate, sf.t_ID t_SfContrID, sf.t_PartyID t_ClientID, mp.t_DlContrID,
                         sf.t_ServKind, sf.t_ServKindSub, mp.t_MarketID
                    FROM dt, dsfcontr_dbt sf, ddlcontrmp_dbt mp                                                 
                   WHERE sf.t_ID = mp.t_SfContrID                                                                
                     AND sf.t_DateBegin <= dt.t_ProcDate                                                        
                     AND (sf.t_DateClose >= dt.t_ProcDate OR sf.t_DateClose = TO_DATE(''01.01.0001'', ''DD.MM.YYYY'')) ';
    
    IF p_ClientID > -1 THEN
      v_sql := v_sql||' AND sf.t_PartyID = :p_ClientID ';
      IF p_DlContrID > -1 THEN
        v_sql := v_sql||' AND mp.t_DlContrID = :p_DlContrID ';
      END IF;
    END IF;

    v_sql := v_sql||' ) 
      SELECT ROW_NUMBER() OVER (ORDER BY q.t_ClientID ASC, q.t_DlContrID ASC, q.t_DebtCur ASC, q.t_DebtDate ASC, DECODE(q.t_DebtType, 5 /*DEBT_EXPIRED*/, 0, q.t_DebtType) ASC, 
                                         CASE WHEN q.t_ServKind = 1 AND q.t_ServKindSub = 8 AND q.t_MarketID = 2 THEN 1 /*ММВБ фондовый*/
                                              WHEN q.t_ServKind = 21 AND q.t_ServKindSub = 8 AND q.t_MarketID = 2 THEN 2 /*Валютный*/
                                              WHEN q.t_ServKind = 1 AND q.t_ServKindSub = 9 AND q.t_MarketID = -1 THEN 3 /*Внебиржа*/
                                              WHEN q.t_ServKind = 15 AND q.t_ServKindSub = 8 AND q.t_MarketID = 2 THEN 4 /*Срочный*/
                                              WHEN q.t_ServKind = 1 AND q.t_ServKindSub = 8 AND q.t_MarketID = 151337 THEN 5 /*СПБ фондовый*/
                                         END ASC,
                                         q.t_DebtSourceID ASC),
             q.t_DebtDate, 
             q.t_DebtType, 
             q.t_DebtSourceID, 
             q.t_DebtSum, 
             q.t_DebtCur, 
             q.t_SfContrID,
             q.t_ClientID, 
             q.t_DlContrID
        FROM (SELECT sfdef.t_DatePeriodEnd as t_DebtDate, 
                     1 /*DEBT_FIX_COM*/ as t_DebtType, 
                     sfdef.t_ID as t_DebtSourceID, 
                     sfdef.t_Sum as t_DebtSum, 
                     sfdef.t_FIID_Sum as t_DebtCur, 
                     sf.t_SfContrID, sf.t_ClientID, sf.t_DlContrID,
                     sf.t_ServKind, sf.t_ServKindSub, sf.t_MarketID                                                  
                FROM sf, dsfdef_dbt sfdef, dsfcomiss_dbt com                                                              
               WHERE com.t_Code = ''БрокерФикс''                                                                            
                 AND sfdef.t_FeeType = 1                                                                                  
                 AND sfdef.t_CommNumber = com.t_Number                                                                    
                 AND sfdef.t_Status = 10                                                                                  
                 AND sfdef.t_SfContrID = sf.t_SfContrID                                                                          
                 AND sfdef.t_DatePeriodEnd <= sf.t_ProcDate
              UNION ALL
              SELECT sfdef.t_DatePeriodEnd as t_DebtDate, 
                     2 /*DEBT_INVEST_COM*/ as t_DebtType, 
                     sfdef.t_ID as t_DebtSourceID, 
                     sfdef.t_Sum as t_DebtSum, 
                     sfdef.t_FIID_Sum as t_DebtCur, 
                     sf.t_SfContrID, sf.t_ClientID, sf.t_DlContrID,
                     sf.t_ServKind, sf.t_ServKindSub, sf.t_MarketID                                                  
                FROM sf, dsfdef_dbt sfdef, dsfcomiss_dbt com                                                              
               WHERE com.t_Code = ''ИнвестСоветник''                                                                            
                 AND sfdef.t_FeeType = 1                                                                                  
                 AND sfdef.t_CommNumber = com.t_Number                                                                    
                 AND sfdef.t_Status = 10                                                                                  
                 AND sfdef.t_SfContrID = sf.t_SfContrID                                                                          
                 AND sfdef.t_PlanPayDate <= sf.t_ProcDate 
              UNION ALL
              SELECT rstr.t_DebtDate,
                     5 /*DEBT_EXPIRED*/ as t_DebtType,
                     rstr.t_ID as t_DebtSourceID,
                     rstr.t_DebtSum,
                     rstr.t_DebtCurrency as t_DebtCur,
                     sf.t_SfContrID, sf.t_ClientID, sf.t_DlContrID,
                     sf.t_ServKind, sf.t_ServKindSub, sf.t_MarketID                                                  
                FROM sf, ddl_debtreestr_dbt rstr
               WHERE rstr.t_SfContrID = sf.t_SfContrID
                 AND rstr.t_DebtSum <> 0 
                 AND rstr.t_DebtDate <= sf.t_ProcDate 
                 AND rstr.t_State = 1 /*DEBTREESTR_STATE_ACTIVE*/
                 AND Rsb_Secur.GetMainObjAttr(207, LPAD(sf.t_DlContrID, 34, ''0''), 105, sf.t_ProcDate) <> 1  
             ) q';   

    IF p_ClientID > -1 THEN
      IF p_DlContrID > -1 THEN      
        execute immediate v_sql
          using p_ProcDate, p_ClientID, p_DlContrID;
      ELSE
        execute immediate v_sql
          using p_ProcDate, p_ClientID;
      END IF;
    ELSE
      execute immediate v_sql
        using p_ProcDate;
    END IF;

    FOR cData IN (SELECT val.t_DebtSum, val.t_DebtType, val.t_ClientID, val.t_DlContrID, val.t_DebtCur
                    FROM ddl_wrtoffdebtsumval_tmp val 
                   WHERE val.t_Sort = (SELECT MIN(val1.t_Sort) 
                                         FROM ddl_wrtoffdebtsumval_tmp val1 
                                        WHERE val1.t_ClientID = val.t_ClientID
                                          AND val1.t_DlContrID = val.t_DlContrID
                                          AND val1.t_DebtCur = val.t_DebtCur)
                   ORDER BY val.t_Sort
                 )
    LOOP
      WITH cat AS (SELECT t_ID FROM dmccateg_dbt WHERE t_LevelType = 1 AND t_Code = 'ДС клиента, ц/б'),
            sf AS (SELECT sf.t_ID, sf.t_ServKind, sf.t_ServKindSub, sf.t_PartyID, mp.t_MarketID
                     FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf
                    WHERE mp.t_DlContrID = cData.t_DlContrID
                      AND sf.t_ID = mp.t_SfContrID)
      SELECT SUM(RSB_Account.restac(t_Account, t_Code_Currency, p_ProcDate, t_Chapter, NULL)) 
        INTO v_RestAcc
        FROM (SELECT DISTINCT acc.t_Account, acc.t_Chapter, acc.t_Code_Currency
                FROM sf, cat, dmcaccdoc_dbt mc, daccount_dbt acc
               WHERE mc.t_CatID = cat.t_ID
                 AND mc.t_IsCommon = 'X'
                 AND mc.t_Owner = sf.t_PartyID
                 AND mc.t_ClientContrID = sf.t_ID
                 AND acc.t_Account = mc.t_Account
                 AND acc.t_Chapter = mc.t_Chapter 
                 AND acc.t_Code_Currency = cData.t_DebtCur
                 AND acc.t_Code_Currency = mc.t_Currency
                 AND acc.t_Open_Date <= p_ProcDate
                 AND (acc.t_Close_Date = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR acc.t_Close_Date >= p_ProcDate)
                 AND (mc.t_DisablingDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR mc.t_DisablingDate >= p_ProcDate)
             ) q;
                 
      IF (cData.t_DebtType = DEBT_EXPIRED AND v_RestAcc > 0) OR v_RestAcc >= cData.t_DebtSum THEN
        SELECT SUM(t_DebtSum)
          INTO v_DebtSum    
          FROM ddl_wrtoffdebtsumval_tmp  
         WHERE t_ClientID = cData.t_ClientID
           AND t_DlContrID = cData.t_DlContrID
           AND t_DebtCur = cData.t_DebtCur;

        --Вставим общую задолженность клиентов к списанию
        INSERT INTO ddl_wrtoffdebtsum_dbt (
               t_OpID,
               t_ClientID,   
               t_DlContrID,   
               t_RestDate,     
               t_DebtSum,    
               t_DebtCurrency,    
               t_RestAcc,
               t_PayDebtSum
               )
        VALUES (
               p_OperID,
               cData.t_ClientID,               
               cData.t_DlContrID,   
               p_ProcDate,     
               v_DebtSum,    
               cData.t_DebtCur,    
               v_RestAcc,
               0
               )
        RETURNING t_ID INTO v_DebtSumID;

        --Вставим расшифровки суммы задолженности к списанию
        INSERT INTO ddl_wrtoffdebtsumval_dbt (
               t_DebtSumID,
               t_DebtDate,   
               t_DebtType,   
               t_DebtSourceID,     
               t_DebtSum,    
               t_DebtCur,    
               t_DebtSfContr,
               t_IsPayDebt,
               t_DebtPaySum,
               t_PayDebtOperID
               )
        SELECT v_DebtSumID,
               t_DebtDate,   
               t_DebtType,   
               t_DebtSourceID,     
               t_DebtSum,    
               t_DebtCur,    
               t_DebtSfContr,
               CHR(0),
               0,
               -1 
          FROM ddl_wrtoffdebtsumval_tmp  
         WHERE t_ClientID = cData.t_ClientID
           AND t_DlContrID = cData.t_DlContrID
           AND t_DebtCur = cData.t_DebtCur
         ORDER BY t_Sort;
      END IF;
    END LOOP;

  END GetWrtOffDebtSumData;

 /**
 * Подготовка данных о задолженности клиентов к выносу на просрочку
 * @since RSHB 116
 * @qtest NO
 * @param p_OperID Идентификатор операции списания
 * @param p_ProcDate Дата процедуры
 * @param p_ClientID Идентификатор клиента
 * @param p_DlContrID Идентификатор ДБО
 */
  PROCEDURE GetEnrollDebtSumData(p_OperID IN NUMBER, p_ProcDate IN DATE, p_ClientID IN NUMBER, p_DlContrID IN NUMBER)
  IS
    v_sql VARCHAR2(10000);
  BEGIN
    v_sql := ' 
      INSERT INTO ddl_enrolldebtsum_dbt (
             t_OpID,
             t_ClientID,                
             t_DlContrID,
             t_SfContrID,
             t_RestDate,                   
             t_DebtDate,                   
             t_DebtType,                   
             t_DebtSum,                    
             t_DebtCurrency,                   
             t_DebtAccID,
             t_Debt458AccID,
             t_IsEnrollDebt,
             t_DebtSourceID
             )                               
      WITH dt AS (SELECT :p_ProcDate t_ProcDate FROM dual), 
          cat AS (SELECT t_ID FROM dmccateg_dbt WHERE t_LevelType = 1 AND t_Code = ''ДС клиента, ц/б''),                                                               
           sf AS (SELECT dt.t_ProcDate, sf.t_ID t_SfContrID, sf.t_PartyID t_ClientID, mp.t_DlContrID
                    FROM dt
                   INNER JOIN dsfcontr_dbt sf
                      ON sf.t_DateBegin <= dt.t_ProcDate AND (sf.t_DateClose >= dt.t_ProcDate OR sf.t_DateClose = TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
                   INNER JOIN ddlcontrmp_dbt mp
                      ON mp.t_SfContrID = sf.t_ID
                    LEFT JOIN dobjatcor_dbt at
                      ON at.t_ObjectType = 659 AND at.t_GroupID = 102 AND at.t_Object = LPAD(sf.t_ID, 10, ''0'')
                   WHERE (    NVL(at.t_AttrID, 0) <> 1
                           OR (NVL(at.t_AttrID, 0) = 1 AND sf.t_ServKind = 1 AND sf.t_ServKindSub = 8 AND mp.t_MarketID = 2 /*ММВБ*/)
                         ) ';
    
    IF p_ClientID > -1 THEN
      v_sql := v_sql||' AND sf.t_PartyID = :p_ClientID ';
      IF p_DlContrID > -1 THEN
        v_sql := v_sql||' AND mp.t_DlContrID = :p_DlContrID ';
      END IF;
    END IF;

    v_sql := v_sql||' ) 
      SELECT :p_OperID,
             q.t_ClientID, 
             q.t_DlContrID,
             q.t_SfContrID,
             q.t_RestDate, 
             q.t_DebtDate, 
             q.t_DebtType,
             q.t_DebtSum,
             q.t_DebtCurrency,
             q.t_DebtAccID,
             q.t_Debt458AccID,
             CHR(0),
             q.t_DebtSourceID
        FROM (SELECT q1.t_ClientID,
                     q1.t_DlContrID, 
                     q1.t_SfContrID,
                     q1.t_ProcDate as t_RestDate,
                     q1.t_ProcDate as t_DebtDate,
                     5 /*DEBT_EXPIRED*/ as t_DebtType,
                     ABS(r.t_Rest) as t_DebtSum,
                     q1.t_Code_Currency as t_DebtCurrency,
                     q1.t_AccountID as t_DebtAccID,
                     -1 as t_Debt458AccID,
                     -1 as t_DebtSourceID
                FROM (SELECT /*+ ordered full(acc)*/
                            DISTINCT acc.t_AccountID, acc.t_Account, acc.t_Chapter, acc.t_Code_Currency,
                                     sf.t_ClientID, sf.t_SfContrID, sf.t_DlContrID, sf.t_ProcDate
                        FROM sf, cat, dmcaccdoc_dbt mc, daccount_dbt acc
                       WHERE mc.t_CatID = cat.t_ID
                         AND mc.t_IsCommon = ''X''
                         AND mc.t_Owner = sf.t_ClientID
                         AND mc.t_ClientContrID = sf.t_SfContrID
                         AND acc.t_Account = mc.t_Account
                         AND acc.t_Chapter = mc.t_Chapter
                         AND acc.t_Code_Currency = mc.t_Currency
                         AND acc.t_Open_Date <= sf.t_ProcDate
                         AND (acc.t_Close_Date = TO_DATE(''01.01.0001'', ''DD.MM.YYYY'') OR acc.t_Close_Date >= sf.t_ProcDate)
                         AND (mc.t_DisablingDate = TO_DATE(''01.01.0001'', ''DD.MM.YYYY'') OR mc.t_DisablingDate >= sf.t_ProcDate)
                     ) q1, drestdate_dbt r
               WHERE q1.t_AccountID = r.t_AccountID
                 AND r.t_Rest < 0
                 AND r.t_RestCurrency = q1.t_Code_Currency
                 AND r.t_RestDate = (SELECT MAX(r1.t_RestDate)
                                       FROM drestdate_dbt r1
                                      WHERE r1.t_AccountID = q1.t_AccountID
                                        AND r1.t_RestCurrency = q1.t_Code_Currency
                                        AND r1.t_RestDate <= q1.t_ProcDate)
                 AND Rsb_Secur.GetMainObjAttr(207, LPAD(q1.t_DlContrID, 34, ''0''), 105, q1.t_ProcDate) <> 1 ';

    IF Rsb_Common.GetRegBoolValue(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ВЫНОС ПЕРИОД.КОМ.НА ПРОСРОЧКУ') = TRUE THEN
      v_sql := v_sql||' 
              UNION ALL
              SELECT sf.t_ClientID,
                     sf.t_DlContrID, 
                     sf.t_SfContrID,
                     sf.t_ProcDate as t_RestDate,
                     sfdef.t_PlanPayDate as t_DebtDate, 
                     1 /*DEBT_FIX_COM*/ as t_DebtType,
                     sfdef.t_Sum as t_DebtSum,
                     sfdef.t_FIID_Sum as t_DebtCurrency,
                     -1 as t_DebtAccID,
                     -1 as t_Debt458AccID,
                     sfdef.t_ID as t_DebtSourceID
                FROM sf, dsfdef_dbt sfdef, dsfcomiss_dbt com                                                              
               WHERE com.t_Code = ''БрокерФикс''                                                                            
                 AND sfdef.t_FeeType = 1                                                                                  
                 AND sfdef.t_CommNumber = com.t_Number                                                                    
                 AND sfdef.t_Status = 10                                                                                  
                 AND sfdef.t_SfContrID = sf.t_SfContrID                                                                          
                 AND sfdef.t_PlanPayDate <= sf.t_ProcDate
                 AND sfdef.t_IsDebtOverdue <> CHR(88)
              UNION ALL
              SELECT sf.t_ClientID,
                     sf.t_DlContrID, 
                     sf.t_SfContrID,
                     sf.t_ProcDate as t_RestDate,
                     sfdef.t_PlanPayDate as t_DebtDate, 
                     2 /*DEBT_INVEST_COM*/ as t_DebtType, 
                     sfdef.t_Sum as t_DebtSum,
                     sfdef.t_FIID_Sum as t_DebtCurrency,
                     -1 as t_DebtAccID,
                     -1 as t_Debt458AccID,
                     sfdef.t_ID as t_DebtSourceID
                FROM sf, dsfdef_dbt sfdef, dsfcomiss_dbt com                                                              
               WHERE com.t_Code = ''ИнвестСоветник''                                                                            
                 AND sfdef.t_FeeType = 1                                                                                  
                 AND sfdef.t_CommNumber = com.t_Number                                                                    
                 AND sfdef.t_Status = 10                                                                                  
                 AND sfdef.t_SfContrID = sf.t_SfContrID                                                                          
                 AND sfdef.t_PlanPayDate <= sf.t_ProcDate 
                 AND sfdef.t_IsDebtOverdue <> CHR(88) ';
    END IF;

    v_sql := v_sql||' 
             ) q 
       ORDER BY q.t_ClientID ASC, q.t_DlContrID ASC, q.t_DebtCurrency ASC, q.t_DebtDate ASC, DECODE(q.t_DebtType, 5 /*DEBT_EXPIRED*/, 0, q.t_DebtType) ASC ';

    IF p_ClientID > -1 THEN
      IF p_DlContrID > -1 THEN      
        execute immediate v_sql
          using p_ProcDate, p_ClientID, p_DlContrID, p_OperID;
      ELSE
        execute immediate v_sql
          using p_ProcDate, p_ClientID, p_OperID;
      END IF;
    ELSE
      execute immediate v_sql
        using p_ProcDate, p_OperID;
    END IF;

  END GetEnrollDebtSumData;

 /**
 * Перенос предоставляемых задолженностей для ЦФТ и данных по оплаченным задолженностям в ЦФТ в историю 
 * @since RSHB 123
 * @qtest NO
 */
  PROCEDURE TransferCFTDataToHist 
  IS
  BEGIN
    INSERT INTO duserdebttocfthist_dbt (
           t_debtid,
           t_debtsourceid,
           t_enterdate,
           t_debtdate,
           t_partyid,
           t_dlcontrid,
           t_sfcontrid,
           t_debtsum,
           t_debtsumcur,
           t_debtsort,
           t_isdebtpart,
           t_accountdebttocft,
           t_debttype,
           t_clienttype,
           t_isfactdebt,
           t_isinwork,
           t_sysdate
           )
    SELECT c.t_id,
           c.t_debtsourceid,
           c.t_enterdate,
           c.t_debtdate,
           c.t_partyid,
           c.t_dlcontrid,
           c.t_sfcontrid,
           c.t_debtsum,
           c.t_debtsumcur,
           c.t_debtsort,
           c.t_isdebtpart,
           c.t_accountdebttocft,
           c.t_debttype,
           c.t_clienttype,
           c.t_isfactdebt,
           c.t_isinwork,
           c.t_sysdate
      FROM duserdebttocft_dbt c;

    DELETE FROM duserdebttocft_dbt;

    INSERT INTO duserdebtfromcfthist_dbt (
           t_carryid,
           t_debtid,
           t_carrycftid,
           t_carrysum,
           t_carrysumcur, 
           t_carryaccount,
           t_carryground, 
           t_carrydate,
           t_istakeninsofr,
           t_sysdate
           )
    SELECT c.t_id,
           c.t_debtid,
           c.t_carrycftid,
           c.t_carrysum,
           c.t_carrysumcur, 
           c.t_carryaccount,
           c.t_carryground, 
           c.t_carrydate,
           c.t_istakeninsofr,
           c.t_sysdate
      FROM duserdebtfromcft_dbt c;

    DELETE FROM duserdebtfromcft_dbt;
  END;

 /**
 * Подготовка данных о задолженности клиентов к списанию со счетов в ЦФТ с заполнением буферной таблицы
 * @since RSHB 123
 * @qtest NO
 * @param p_ProcDate  Дата процедуры
 * @param p_ClientID  Идентификатор клиента
 * @param p_DlContrID Идентификатор ДБО
 */
  PROCEDURE GetWrtOffDebtDataToCFT(p_ProcDate IN DATE, p_ClientID IN NUMBER, p_DlContrID IN NUMBER)
  IS
    v_sql VARCHAR2(10000);
  BEGIN
    v_sql := ' 
      INSERT INTO duserdebttocft_dbt (
             t_debtsourceid,
             t_enterdate,
             t_debtdate,
             t_partyid,
             t_dlcontrid,
             t_sfcontrid,
             t_debtsum,
             t_debtsumcur,
             t_debtsort,
             t_isdebtpart,
             t_accountdebttocft,
             t_debttype,
             t_clienttype,
             t_isfactdebt,
             t_isinwork
             )
      WITH dt AS (SELECT :p_ProcDate t_ProcDate FROM dual),
          cat AS (SELECT t_ID FROM dmccateg_dbt WHERE t_LevelType = 1 AND t_Code = ''Треб. с н.с. брок'')                                                                                              
      SELECT q.t_DebtSourceID,
             dt.t_ProcDate,
             q.t_DebtDate,
             sf.t_PartyID,
             mp.t_DlContrID,
             q.t_SfContrID,
             q.t_DebtSum,
             q.t_DebtSumCur,
             ROW_NUMBER() OVER (PARTITION BY mp.t_DlContrID, q.t_DebtSumCur 
                                    ORDER BY q.t_DebtDate ASC, DECODE(q.t_DebtType, 5 /*DEBT_EXPIRED*/, 0, q.t_DebtType) ASC, 
                                             CASE WHEN sf.t_ServKind = 1 AND sf.t_ServKindSub = 8 AND mp.t_MarketID = 2 THEN 1 /*ММВБ фондовый*/
                                                  WHEN sf.t_ServKind = 21 AND sf.t_ServKindSub = 8 AND mp.t_MarketID = 2 THEN 2 /*Валютный*/
                                                  WHEN sf.t_ServKind = 1 AND sf.t_ServKindSub = 9 AND mp.t_MarketID = -1 THEN 3 /*Внебиржа*/
                                                  WHEN sf.t_ServKind = 15 AND sf.t_ServKindSub = 8 AND mp.t_MarketID = 2 THEN 4 /*Срочный*/
                                                  WHEN sf.t_ServKind = 1 AND sf.t_ServKindSub = 8 AND mp.t_MarketID = 151337 THEN 5 /*СПБ фондовый*/
                                             END ASC,
                                             q.t_DebtSourceID ASC),
             q.t_IsDebtPart,
             NVL((SELECT /*+ ordered use_nl(mc) index(mc DMCACCDOC_DBT_IDXC)*/ 
                         CASE WHEN pt.t_LegalForm = 1 OR NVL(pn.t_IsEmployer, CHR(0)) = ''X'' OR INSTR(acc.t_UserTypeAccount, ''!'') > 0 THEN acc.t_Account 
                              ELSE NVL(brok.t_Account, CHR(1)) END                                 
                   FROM cat, dmcaccdoc_dbt mc 
                  INNER JOIN daccount_dbt acc ON acc.t_Account = mc.t_Account AND acc.t_Chapter = mc.t_Chapter AND acc.t_Code_Currency = mc.t_Currency
                   LEFT JOIN dbrokacc_dbt brok ON brok.t_ServKind = 0 AND brok.t_ServKindSub = 0 AND brok.t_Currency = mc.t_Currency AND SUBSTR(brok.t_Account, 1, 5) = SUBSTR(acc.t_Account, 1, 5)                                                                             
                  WHERE mc.t_CatID = cat.t_ID
                    AND mc.t_IsCommon = ''X''                                                                                             
                    AND mc.t_Owner = sf.t_PartyID                                                                                                 
                    AND mc.t_ClientContrID = q.t_SfContrID                                                                                         
                    AND mc.t_Currency = q.t_DebtSumCur                                                                                        
                    AND (acc.t_Close_Date = TO_DATE(''01.01.0001'', ''DD.MM.YYYY'') OR acc.t_Close_Date >= dt.t_ProcDate)
                    AND (mc.t_DisablingDate = TO_DATE(''01.01.0001'', ''DD.MM.YYYY'') OR mc.t_DisablingDate >= dt.t_ProcDate)
                    AND ROWNUM = 1), CHR(1)),
             q.t_DebtType,
             CASE WHEN pt.t_LegalForm = 1 THEN ''ЮЛ'' WHEN NVL(pn.t_IsEmployer, CHR(0)) = ''X'' THEN ''ИП'' ELSE ''ФЛ'' END, 
             CHR(0),
             CHR(0) 
       FROM dt,
            (SELECT sfdef.t_DatePeriodEnd as t_DebtDate, 
                    1 /*DEBT_FIX_COM*/ as t_DebtType, 
                    sfdef.t_ID as t_DebtSourceID, 
                    sfdef.t_Sum as t_DebtSum, 
                    sfdef.t_FIID_Sum as t_DebtSumCur, 
                    CHR(0) as t_IsDebtPart,
                    sfdef.t_SfContrID
               FROM dt, dsfcomiss_dbt com, dsfdef_dbt sfdef
              WHERE com.t_Code = ''БрокерФикс''                                                                            
                AND sfdef.t_FeeType = 1 
                AND sfdef.t_CommNumber = com.t_Number                                                                                                                                                     
                AND sfdef.t_Status = 10                                                                                  
                AND sfdef.t_PlanPayDate <= dt.t_ProcDate
                AND sfdef.t_IsDebtOverdue = ''X''
             UNION ALL
              SELECT sfdef.t_DatePeriodEnd as t_DebtDate, 
                     2 /*DEBT_INVEST_COM*/ as t_DebtType, 
                     sfdef.t_ID as t_DebtSourceID, 
                     sfdef.t_Sum as t_DebtSum, 
                     sfdef.t_FIID_Sum as t_DebtSumCur, 
                     CHR(0) as t_IsDebtPart,
                     sfdef.t_SfContrID
                FROM dt, dsfcomiss_dbt com, dsfdef_dbt sfdef
               WHERE com.t_Code = ''ИнвестСоветник''                                                                            
                 AND sfdef.t_FeeType = 1
                 AND sfdef.t_CommNumber = com.t_Number                                                                                                                                                                                                                                       
                 AND sfdef.t_Status = 10                                                                                  
                 AND sfdef.t_PlanPayDate <= dt.t_ProcDate
                 AND sfdef.t_IsDebtOverdue = ''X'' 
              UNION ALL
              SELECT rstr.t_DebtDate,
                     5 /*DEBT_EXPIRED*/ as t_DebtType,
                     rstr.t_ID as t_DebtSourceID,
                     rstr.t_DebtSum,
                     rstr.t_DebtCurrency as t_DebtSumCur,
                     ''X'' as t_IsDebtPart,
                     rstr.t_SfContrID
                FROM dt, ddl_debtreestr_dbt rstr 
               WHERE rstr.t_DebtSum <> 0 
                 AND rstr.t_DebtDate <= dt.t_ProcDate 
                 AND rstr.t_State = 1 /*DEBTREESTR_STATE_ACTIVE*/
                 AND Rsb_Secur.GetMainObjAttr(207, LPAD(rstr.t_DlContrID, 34, ''0''), 105, dt.t_ProcDate) <> 1 
              UNION ALL
              SELECT TO_DATE(''03.03.2023'', ''DD.MM.YYYY'') as t_DebtDate,
                     4 /*DEBT_DEAL_COM_2023*/ as t_DebtType,
                     u.t_CalcID as t_DebtSourceID,
                     SUM(u.t_ReqSum - u.t_Payed_Tr) as t_DebtSum,
                     u.t_FIID as t_DebtSumCur,
                     ''X'' as t_IsDebtPart,
                     sf_real.t_ID as t_SfContrID
                FROM u_compay_dbt u, ddlcontrmp_dbt mp_real, dsfcontr_dbt sf_real 
               WHERE u.t_CalcID = (SELECT MAX(u1.t_CalcID) FROM u_compay_dbt u1)                 
                 AND u.t_SysDate IS NOT NULL                                                
                 AND (u.t_ReqSum - u.t_Payed_Tr) <> 0
                 AND mp_real.t_DlContrID = u.t_DlContrID 
                 AND sf_real.t_ID = mp_real.t_SfContrID  
                 AND sf_real.t_ServKind = 21 AND sf_real.t_ServKindSub = 8 AND mp_real.t_MarketID = 2
               GROUP BY u.t_CalcID, u.t_DlContrID, sf_real.t_ID, u.t_FIID 
             ) q
       INNER JOIN dsfcontr_dbt sf ON sf.t_ID = q.t_SfContrID
       INNER JOIN ddlcontrmp_dbt mp ON mp.t_SfContrID = sf.t_ID                                                             
       INNER JOIN dparty_dbt pt ON pt.t_PartyID = sf.t_PartyID 
        LEFT JOIN dpersn_dbt pn ON pn.t_PersonID = pt.t_PartyID                                               
       WHERE sf.t_DateBegin <= dt.t_ProcDate                                                        
         AND (sf.t_DateClose >= dt.t_ProcDate OR sf.t_DateClose = TO_DATE(''01.01.0001'', ''DD.MM.YYYY''))
         AND pt.t_NotResident <> ''X''
         AND NOT EXISTS (SELECT 1 FROM duserdebttocft_dbt t WHERE t.t_DlContrID = mp.t_DlContrID) '; 
        
    IF p_ClientID > -1 THEN
      v_sql := v_sql||' AND sf.t_PartyID = :p_ClientID ';
      IF p_DlContrID > -1 THEN
        v_sql := v_sql||' AND mp.t_DlContrID = :p_DlContrID ';
      END IF;
    END IF;
     
    IF p_ClientID > -1 THEN
      IF p_DlContrID > -1 THEN      
        execute immediate v_sql
          using p_ProcDate, p_ClientID, p_DlContrID;
      ELSE
        execute immediate v_sql
          using p_ProcDate, p_ClientID;
      END IF;
    ELSE
      execute immediate v_sql
        using p_ProcDate;
    END IF; 
    
  END GetWrtOffDebtDataToCFT;
     
END  RSB_DEBTSUM;
/
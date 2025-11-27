CREATE OR REPLACE PACKAGE BODY RSB_BASESUM
IS
  COMMISS_INVEST_ADVISER CONSTANT VARCHAR2(20) := 'ИнвестСоветник'; 

 /**
 * Получим номер комиссии по коду 
 * @since RSHB 101
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
 * Получение идентификатора тарифного плана по договору обслуживания на дату 
 * @since RSHB 101
 * @qtest NO
 * @param p_SfContrID Идентификатор субдоговора
 * @param p_Date Дата действия тарифного плана
 * @return идентификатор тарифного плана
 */
  FUNCTION GetSfPlanID(p_SfContrID IN NUMBER, p_Date IN DATE) 
    RETURN NUMBER
  IS
    v_PlanID dsfcontrplan_dbt.t_SfPlanID%TYPE;
  BEGIN
    SELECT t_SfPlanID INTO v_PlanID
      FROM dsfcontrplan_dbt  
     WHERE t_SfContrID = p_SfContrID
       AND t_Begin <= p_Date 
       AND (t_End >= p_Date OR t_End = to_date('01.01.0001','DD.MM.YYYY'));
    RETURN v_PlanID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END GetSfPlanID;

 /**
 * Идентификатор субдоговора фондового рынка ММВБ по ДБО  
 * @since RSHB 101
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @param p_Date Дата действия субдоговора
 * @return идентификатор субдоговора
 */
  FUNCTION GetSfContrIDByStockMMVB(p_DlContrID IN NUMBER, p_Date IN DATE)
    RETURN NUMBER
  IS
    v_SfContrID dsfcontr_dbt.t_ID%TYPE;
  BEGIN
    SELECT sf.t_ID INTO v_SfContrID
      FROM dsfcontr_dbt sf, ddlcontrmp_dbt mp  
     WHERE mp.t_SfContrID = sf.t_ID
       AND sf.t_ServKind = 1 
       AND sf.t_ServKindSub = 8
       AND mp.t_MarketID = 2 /*ММВБ*/
       AND mp.t_DlContrID = p_DlContrID                            
       AND (sf.t_DateClose > p_Date OR sf.t_DateClose = to_date('01.01.0001','DD.MM.YYYY'));
    RETURN v_SfContrID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END GetSfContrIDByStockMMVB;
  
 /**
 * Получение процента по комиссии 
 * @since RSHB 101
 * @qtest NO
 * @param p_ConComID Идентификатор связки комиссии с договором обслуживания 
 * @return процент по комиcсии
 */
  FUNCTION GetCommissPercent(p_ConComID IN NUMBER)
    RETURN NUMBER
  IS
    v_ComPerc ddl_basesum_dbt.t_ComRate%TYPE := 0;
  BEGIN
    SELECT t_TarifSum/10000 INTO v_ComPerc
      FROM dsfcomtarscl_dbt comtar, dsftarif_dbt tarif
     WHERE comtar.t_ConComID = p_ConComID
       AND comtar.t_Level = (SELECT MIN(comtar1.t_Level)
                               FROM dsfcomtarscl_dbt comtar1
                              WHERE comtar1.t_ConComID = p_ConComID)
       AND tarif.t_TarSclID = comtar.t_TarSclID
       AND ROWNUM = 1; --Пока не берем в расчет, что в тарифной сетке может быть несколько строк, т.к. начисление на это не рассчитано, но добавим ограничение, чтобы запрос если что не падал
    RETURN v_ComPerc;  
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END GetCommissPercent;
  
 /**
 * Получение итоговой преобразованной суммы комиссии до взятия процента
 * @since RSHB 101
 * @qtest NO
 * @param p_SfContrID Идентификатор субдоговора
 * @param p_ConComID Идентификатор связки комиссии с договором обслуживания 
 * @param p_BeginDate Дата начала периода начисления
 * @param p_EndDate Дата окончания периода начисления
 * @return преобразованная итоговая сумма комиссии 
 */
  FUNCTION GetItogBaseSum(p_SfContrID IN NUMBER, p_ConComID IN NUMBER, p_BeginDate IN DATE, p_EndDate IN DATE)
    RETURN NUMBER
  IS 
    v_FactSum ddl_basesum_dbt.t_ComSum%TYPE := 0;
    v_ComRate ddl_basesum_dbt.t_ComRate%TYPE := 0;
  BEGIN
    v_ComRate := GetCommissPercent(p_ConComID);
  
    IF v_ComRate > 0 THEN
      SELECT NVL(SUM(t_ComSum), 0)  INTO v_FactSum
        FROM dsfconcom_dbt concom, ddl_basesum_dbt bs, ddlcontrmp_dbt mp
       WHERE concom.t_ID = p_ConComID
         AND mp.t_SfContrID = p_SfContrID
         AND bs.t_DlContrID = mp.t_DlContrID
         AND bs.t_State = CHR(0) 
         AND bs.t_ComNumber = concom.t_CommNumber
         AND bs.t_FeeType = concom.t_FeeType
         AND bs.t_Date BETWEEN p_BeginDate AND p_EndDate;
       
       RETURN ROUND(v_FactSum / v_ComRate * 100, 2);
    END IF;

    RETURN 0;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END GetItogBaseSum;

 /**
 * Дополнительные действия по комиссии "ИнвестСоветник" при подключении услуги Инвестиционного консультирования 
 * @since RSHB 101
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @param p_BegDate Дата подключения услуги
 */
  PROCEDURE ConnectingInvestmentAdvisor(p_DlContrID IN NUMBER, p_BegDate IN DATE)
  IS
    v_SfContrID dsfcontr_dbt.t_ID%TYPE;
    v_ComNumber dsfcomiss_dbt.t_Number%TYPE;
    v_ConComID  dsfconcom_dbt.t_ID%TYPE;
    v_SfPlanID  dsfcontrplan_dbt.t_SfPlanID%TYPE;
  BEGIN
    v_ComNumber := GetCommissNumber(COMMISS_INVEST_ADVISER); --Получение номера комиссии с кодом "ИнвестСоветник"
    v_SfContrID := GetSfContrIDByStockMMVB(p_DlContrID, p_BegDate); --Получение субдоговора фондового рынка ММВБ

    IF v_SfContrID > 0 AND v_ComNumber > 0 THEN 
      --Проверка состояния комиссии "ИнвестСоветник" в таблице комиссий по договору обслуживания dsfconcom_dbt
      BEGIN
        SELECT t_ID INTO v_ConComID
          FROM dsfconcom_dbt
         WHERE t_ObjectID = v_SfContrID
           AND t_ObjectType = Rsb_Secur.OBJTYPE_SFCONTR
           AND t_FeeType = SF_FEE_TYPE_PERIOD
           AND t_CommNumber = v_ComNumber
           AND t_DateBegin <= p_BegDate + 1
           AND t_DateEnd = to_date('01.01.0001','dd.mm.yyyy');
      EXCEPTION
        WHEN NO_DATA_FOUND THEN v_ConComID := 0;
      END;
     
      IF v_ConComID > 0 THEN --Запись найдена (услуга подключается впервые)
        --Укажем дату начала действия комиссии равной следующей календарной дате от даты начала действия услуги инвестиционного консультирования
        UPDATE dsfconcom_dbt
           SET t_DateBegin = p_BegDate + 1
         WHERE t_ID = v_ConComID;
      ELSE
        v_SfPlanID := GetSfPlanID(v_SfContrID, p_BegDate);
      
        --Вставка новой записи в таблицу комиссий по договору обслуживания dsfconcom_dbt
        INSERT INTO dsfconcom_dbt
          (T_OBJECTID, T_FEETYPE, T_COMMNUMBER, T_STATUS, T_CALCPERIODTYPE, 
           T_CALCPERIODNUM, T_DATE, T_GETSUMMIN, T_SUMMIN, T_SUMMAX, 
           T_DATEBEGIN, T_DATEEND, T_OBJECTTYPE, T_ID, T_SFPLANID, 
           T_ISFREEPERIOD, T_ISINDIVIDUAL, T_ISBANKEXPENSES, T_ISCOMPENSATIONCOM)
        VALUES
          (v_SfContrID, SF_FEE_TYPE_PERIOD, v_ComNumber, 0, SF_TYPE_PERIOD_MONTH, 
           1, to_date('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), chr(0), 0, 0, 
           p_BegDate + 1, to_date('01/01/0001 00:00:00', 'MM/DD/YYYY HH24:MI:SS'), Rsb_Secur.OBJTYPE_SFCONTR, 0, v_SfPlanID, 
          'X', chr(0), chr(0), chr(0))
        RETURNING t_ID INTO v_ConComID;
   
        --Вставка записей в таблицу dsfcomtarscl_dbt для связи созданной записи в таблице комиссии по договору с тарифной сеткой для разных уровней локализации комиссии 
   
        --Запись с уровнем локализации на конкретный договор обслуживания 
        INSERT INTO dsfcomtarscl_dbt 
               (t_ConComID, t_TarSclID, t_Level)  
        SELECT v_ConComID, tar.t_ID, 0    
          FROM dsftarscl_dbt tar, dsfconcom_dbt concom   
         WHERE concom.t_ID = tar.t_ConComID     
           AND tar.t_FeeType = SF_FEE_TYPE_PERIOD 
           AND tar.t_CommNumber = v_ComNumber     
           AND concom.t_ObjectType = Rsb_Secur.OBJTYPE_SFCONTR 
           AND concom.t_ObjectID = v_SfContrID 
           AND concom.t_SfPlanID = v_SfPlanID     
           AND(   (tar.t_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy') AND tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy') )          
                OR tar.t_BeginDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')    
                OR tar.t_EndDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')     
                OR tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy')        
              ); 
        
        --Запись с уровнем локализации на конкретный филиал, не являющийся головным 
        INSERT INTO dsfcomtarscl_dbt 
              (t_ConComID, t_TarSclID, t_Level)  
        SELECT v_ConComID, tar.t_ID, 1    
          FROM dsftarscl_dbt tar, dsfcontr_dbt contr, dsfconcom_dbt concom   
         WHERE concom.t_ID = tar.t_ConComID 
           AND tar.t_FeeType = SF_FEE_TYPE_PERIOD 
           AND tar.t_CommNumber = v_ComNumber    
           AND concom.t_ObjectType = 80 /*OBJTYPE_DEPARTMENT*/ --Узел ТС
           AND concom.t_ObjectID = contr.t_Branch 
           AND concom.t_SfPlanID = v_SfPlanID 
           AND contr.t_ID = v_SfContrID 
           AND contr.t_Branch > 0 
           AND contr.t_Branch <> contr.t_Department    
           AND(   (tar.t_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy') AND tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy') )          
                OR tar.t_BeginDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')    
                OR tar.t_EndDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')     
                OR tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy')        
              );       
   
        --Запись с уровнем локализации на головной филиал
        INSERT INTO dsfcomtarscl_dbt 
              (t_ConComID, t_TarSclID, t_Level)  
        SELECT v_ConComID, tar.t_ID, 2    
          FROM dsftarscl_dbt tar, dsfcontr_dbt contr, dsfconcom_dbt concom   
         WHERE concom.t_ID = tar.t_ConComID 
           AND tar.t_FeeType = SF_FEE_TYPE_PERIOD 
           AND tar.t_CommNumber = v_ComNumber    
           AND concom.t_ObjectType = 80 /*OBJTYPE_DEPARTMENT*/ --Узел ТС
           AND concom.t_ObjectID = contr.t_Department 
           AND concom.t_SfPlanID = v_SfPlanID 
           AND contr.t_ID = v_SfContrID
           AND(   (tar.t_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy') AND tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy') )          
                OR tar.t_BeginDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')    
                OR tar.t_EndDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')     
                OR tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy')        
              );       
                 
        --Запись с уровнем локализации на тарифный план
        INSERT INTO dsfcomtarscl_dbt 
              (t_ConComID, t_TarSclID, t_Level)  
        SELECT v_ConComID, tar.t_ID, 3    
          FROM dsftarscl_dbt tar, dsfconcom_dbt concom   
         WHERE concom.t_ID = tar.t_ConComID 
           AND tar.t_FeeType = SF_FEE_TYPE_PERIOD 
           AND tar.t_CommNumber = v_ComNumber    
           AND concom.t_ObjectType = 57 /*OBJTYPE_SFPLAN*/ --Тарифный план
           AND concom.t_ObjectID = v_SfPlanID    
           AND(   (tar.t_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy') AND tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy') )          
                OR tar.t_BeginDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')    
                OR tar.t_EndDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')     
                OR tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy')        
              );       
        
        --Запись с уровнем локализации на комиссию из списка комиссий 
        INSERT INTO dsfcomtarscl_dbt 
              (t_ConComID, t_TarSclID, t_Level)  
        SELECT v_ConComID, tar.t_ID, 4    
          FROM dsftarscl_dbt tar   
         WHERE tar.t_ConComID = 0 
           AND tar.t_FeeType = SF_FEE_TYPE_PERIOD 
           AND tar.t_CommNumber = v_ComNumber    
           AND(   (tar.t_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy') AND tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy') )          
                OR tar.t_BeginDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')    
                OR tar.t_EndDate BETWEEN p_BegDate + 1 AND to_date('31.12.9999', 'dd.mm.yyyy')     
                OR tar.t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy')        
              );       
      END IF; 
    END IF; 
  END ConnectingInvestmentAdvisor;

 /**
 * Дополнительные действия по комиссии "ИнвестСоветник" при отключении услуги Инвестиционного консультирования 
 * @since RSHB 101
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @param p_EndDate Дата отключения услуги
 */
  PROCEDURE DisablingInvestmentAdvisor(p_DlContrID IN NUMBER, p_EndDate IN DATE)
  IS
    v_SfContrID dsfcontr_dbt.t_ID%TYPE;
    v_ComNumber dsfcomiss_dbt.t_Number%TYPE;
  BEGIN
    v_ComNumber := GetCommissNumber(COMMISS_INVEST_ADVISER); --Получение номера комиссии с кодом "ИнвестСоветник"
    v_SfContrID := GetSfContrIDByStockMMVB(p_DlContrID, p_EndDate); --Получение субдоговора фондового рынка ММВБ

    IF v_SfContrID > 0 AND v_ComNumber > 0 THEN 
      UPDATE dsfconcom_dbt
         SET t_DateEnd = p_EndDate
       WHERE t_ObjectID = v_SfContrID
         AND t_ObjectType = Rsb_Secur.OBJTYPE_SFCONTR
         AND t_FeeType = SF_FEE_TYPE_PERIOD
         AND t_CommNumber = v_ComNumber
         AND t_DateEnd = to_date('01.01.0001','dd.mm.yyyy');

      UPDATE dsfdef_dbt
         SET t_PlanPayDate = p_EndDate
       WHERE t_SfContrID = v_SfContrID
         AND t_FeeType = SF_FEE_TYPE_PERIOD
         AND t_CommNumber = v_ComNumber
         AND t_Status IN (10/*Начисляется*/, 40/*Оплачена*/)
         AND t_PlanPayDate > p_EndDate;
    END IF;      
  END DisablingInvestmentAdvisor;

 /**
 * Добавление записи об ошибке/предупреждении в лог
 * @since RSHB 101
 * @qtest NO
 * @param p_Type Тип сообщения
 * @param p_DlContrID Идентификатор ДБО
 * @param p_Date Дата расчета
 * @param p_Text Текст сообщения
 */
  PROCEDURE AddToLog(p_Type IN NUMBER, p_DlContrID IN NUMBER, p_Date IN DATE, p_Text IN VARCHAR2)
  IS
  BEGIN
    INSERT INTO ddl_basesumlog_tmp (t_Type, t_DlContrID, t_Date, t_Text)
                            VALUES (p_Type, p_DlContrID, p_Date, SUBSTR(p_Text, 1, 500)); 
  END AddToLog;

 /**
 * Сохранение блока протокола в операции расчета базовых сумм
 * @since RSHB 101
 * @qtest NO
 * @param p_ID Идентификатор операции
 * @param p_Chunk Блок данных для записи в Clob
 */
  PROCEDURE AppendLogToClob(p_ID IN NUMBER, p_Chunk IN CLOB)
  IS
    v_Clob CLOB;
  BEGIN
    SELECT t_Log
      INTO v_Clob
      FROM ddl_basesumop_dbt
     WHERE t_ID = p_ID
    FOR UPDATE;
  
    IF v_clob is null THEN
      UPDATE ddl_basesumop_dbt SET t_Log = p_Chunk where t_ID = p_ID;
    ELSE
      DBMS_LOB.Append(v_Clob, p_Chunk);
    END IF;
  
    COMMIT;
  END AppendLogToClob;

 /**
 * Проверка договора ЕДП
 * @since RSHB 101
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @return 1 - договор ЕДП, 0 - договор не ЕДП
 */
  FUNCTION IsEDP(p_DlContrID IN NUMBER) 
    RETURN NUMBER
  IS
    v_RetVal NUMBER(5) := 0;
  BEGIN
    SELECT 1 INTO v_RetVal                                 
      FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf, dobjatcor_dbt atcor    
     WHERE mp.t_DlContrID = p_DlContrID
       AND sf.t_ID = mp.t_SfContrID
       AND sf.t_ServKindSub = 8             
       AND atcor.t_ObjectType = RSB_SECUR.OBJTYPE_SFCONTR           
       AND atcor.t_GroupID = 102 /*Ведение Единой денежной позиции*/               
       AND atcor.t_AttrID = 1 /*Да*/                  
       AND atcor.t_Object = LPAD(sf.t_ID, 10, '0')
       AND ROWNUM = 1;  
    RETURN v_RetVal;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END IsEDP;
  
 /**
 * Проверка что по ДБО уже существует комиссия со статусом Начислена или Оплачена, в период которой входит дата расчета 
 * @since RSHB 101
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @param p_CalcDate Дата расчета
 * @param p_ComNumber Номер комиссии
 * @param p_FeeType Тип взимания комиссии
 * @return 1 - комиссия существует, 0 - комиссия не существует
 */
  FUNCTION CheckSfDef(p_DlContrID IN NUMBER, p_CalcDate IN DATE, p_ComNumber IN NUMBER, p_FeeType IN NUMBER) 
    RETURN NUMBER
  IS
    v_RetVal NUMBER(5) := 0;
  BEGIN
    SELECT 1 INTO v_RetVal                                 
      FROM ddlcontrmp_dbt mp, dsfdef_dbt sfdef
     WHERE sfdef.t_SfContrID = mp.t_SfContrID
       AND sfdef.t_FeeType = p_FeeType
       AND sfdef.t_CommNumber = p_ComNumber
       AND p_CalcDate BETWEEN sfdef.t_DatePeriodBegin AND sfdef.t_DatePeriodEnd
       AND sfdef.t_Status > 0 /*SFDEFCOM_STATUS_CREATED*/
       AND mp.t_DlContrID = p_DlContrID
       AND ROWNUM = 1;  
    RETURN v_RetVal;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END CheckSfDef;
  
   /**
 * Поиск связки комиссии с договором обслуживания 
 * @since RSHB 101
 * @qtest NO
 * @param p_DlContrID Идентификатор ДБО
 * @param p_CalcDate Дата расчета
 * @param p_ComNumber Номер комиссии
 * @param p_FeeType Тип взимания комиссии
 * @return идентификатор связки
 */
  FUNCTION GetConComID(p_DlContrID IN NUMBER, p_CalcDate IN DATE, p_ComNumber IN NUMBER, p_FeeType IN NUMBER)
    RETURN NUMBER
  IS
    v_SfContrID dsfcontr_dbt.t_ID%TYPE;
    v_ConComID dsfconcom_dbt.t_ID%TYPE := 0;
  BEGIN
    v_SfContrID := GetSfContrIDByStockMMVB(p_DlContrID, p_CalcDate); --Получение субдоговора фондового рынка ММВБ
  
    IF v_SfContrID > 0 THEN 
      SELECT t_ID INTO v_ConComID
        FROM dsfconcom_dbt concom
       WHERE t_ObjectID = v_SfContrID
         AND t_ObjectType = Rsb_Secur.OBJTYPE_SFCONTR
         AND t_FeeType = p_FeeType
         AND t_CommNumber = p_ComNumber
         AND t_DateBegin <= p_CalcDate 
         AND (t_DateEnd = to_date('01.01.0001','dd.mm.yyyy') OR t_DateEnd >= p_CalcDate);
    END IF;
    RETURN v_ConComID;  
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END GetConComID;

 /**
 * Получение Id курса ФИ для определения рыночной стоимости на дату
 * @since RSHB 101
 * @qtest NO
 * @param p_FIID Идентификатор ц/б
 * @param p_Date Дата котировки
 * @return идентификатор котировки 
 */  
  FUNCTION GetActiveRateId(p_FIID IN NUMBER, p_Date IN DATE) RETURN NUMBER deterministic result_cache
  IS
    v_CourseTypeMP  NUMBER;
    v_CourseTypeAVR NUMBER;
  
    v_RateId        NUMBER := -1;
  BEGIN
    v_CourseTypeMP := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА РЫНОЧНАЯ ЦЕНА', 0);
    v_RateId := RSB_SPREPFUN.GetRateIdByMPWithMaxTradeVolume(p_Date, p_FIID, v_CourseTypeMP, 90);
  
    IF v_RateId = -1 THEN
     v_CourseTypeAVR := Rsb_Common.GetRegIntValue('SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0);
     v_RateId := RSB_SPREPFUN.GetRateIdByMPWithMaxTradeVolume(p_Date, p_FIID, v_CourseTypeAVR, 90);
    END IF;

    RETURN v_RateID;
  EXCEPTION
    WHEN OTHERS THEN RETURN -1;
  END GetActiveRateId;

 /**
 * Расчет/пересчет базовой стоимости и суммы комиссии по ДБО на дату по комиссии ИнвестСоветник
 * @since RSHB 101
 * @qtest NO
 * @param p_OperID Идентификатор операции расчета
 * @param p_DlContrID Идентификатор ДБО
 * @param p_ClientID Идентификатор клиента
 * @param p_Action Вид действия
 * @param p_CalcDate Дата расчета
 * @param p_ComNumber Номер комиссии
 * @param p_FeeType Тип взимания комиссии
 */
  PROCEDURE CalcBaseSumOnInvestAdviser(p_OperID IN NUMBER, p_DlContrID IN NUMBER, p_ClientID IN NUMBER, p_Action IN NUMBER, p_CalcDate IN DATE, p_ComNumber IN NUMBER, p_FeeType IN NUMBER)
  IS
    v_BaseSumID ddl_basesum_dbt.t_ID%TYPE;
    v_Sum ddl_basesum_dbt.t_Sum%TYPE;
    v_ComRate ddl_basesum_dbt.t_ComRate%TYPE;
    v_ComSum ddl_basesum_dbt.t_ComSum%TYPE;
    v_State ddl_basesum_dbt.t_State%TYPE;
    v_IsInvConnected NUMBER := 0;
    v_IsSfDef   NUMBER := 0;
    v_IsEDP     NUMBER := 0;
    v_Err       NUMBER := 0; 
  BEGIN
 
    IF p_Action = ACTION_RECALC OR p_Action = ACTION_DELETE OR p_Action = ACTION_CALC THEN

      IF CheckSfDef(p_DlContrID, p_CalcDate, p_ComNumber, p_FeeType) = 1 THEN 
        AddToLog(ISSUE_ERROR, p_DlContrID, p_CalcDate, 'За дату уже выполнено начисление');
        RETURN;
      END IF;

      IF p_Action = ACTION_RECALC OR p_Action = ACTION_DELETE THEN
        UPDATE ddl_basesum_dbt 
           SET t_State = 'D', 
               t_LnkOpID = p_OperID
         WHERE t_State = CHR(0) 
           AND t_ComNumber = p_ComNumber    
           AND t_FeeType = p_FeeType    
           AND t_Date = p_CalcDate   
           AND t_DlContrID = p_DlContrID; 
           
        IF p_Action = ACTION_DELETE THEN
          RETURN;
        END IF;
      END IF;

    END IF;
    
    v_IsEDP := IsEDP(p_DlContrID);

    --Отбор счетов 306, 47423, 458: действующие открытые счета по категориям "ДС клиента, ц/б" (с плюсом), "+РасчетыКомисс1" (с минусом) и "Треб. с н.с. брок" (с минусом), открытые по всем субдоговорам ДБО 
    INSERT INTO ddl_basesumval_tmp (
           t_ObjKind,                                                
           t_ObjectID,                                     
           t_Kind,
           t_Amount,                                            
           t_RateID,                                              
           t_RateDate,              
           t_Rate,                                                   
           t_NKD,                                                    
           t_Cost,                                              
           t_CostCur,                                  
           t_CBRate,                                            
           t_CostRub,                                  
           t_SfContrID,                                                   
           t_MarketID                                                     
           )            
    SELECT 1, 
           t_AccountID, 
           DECODE(t_CatCode, 'ДС клиента, ц/б', 1, '+РасчетыКомисс1', 3, 4), 
           t_Rest, 
           -1, 
           to_date('01.01.0001','dd.mm.yyyy'), 
           0, 
           0, 
           t_Rest, 
           t_Code_Currency, 
           t_Rate, 
           ROUND(t_Rest * t_Rate, 2), 
           t_SfContrID, 
           t_MarketID 
      FROM (SELECT t_AccountID,
                   t_SfContrID,
                   t_MarketID,
                   t_Code_Currency,
                   t_CatCode,
                   RSB_Account.restac(t_Account, t_Code_Currency, p_CalcDate, t_Chapter, null) t_Rest,
                   NVL(RSI_RSB_FIInstr.ConvSum(1, t_Code_Currency, RSI_RSB_FIInstr.NATCUR, p_CalcDate), 0) t_Rate
              FROM (SELECT DISTINCT acc.t_AccountID, acc.t_Account, acc.t_Chapter, acc.t_Code_Currency, cat.t_Code as t_CatCode,
                                   (CASE WHEN v_IsEDP = 0 OR sf.t_ServKindSub = 9/*Внебиржа*/ THEN sf.t_ID ELSE 0 END) as t_SfContrID,
                                   (CASE WHEN v_IsEDP = 0 OR sf.t_ServKindSub = 9/*Внебиржа*/ THEN mp.t_MarketID ELSE 0 END) as t_MarketID
                      FROM ddlcontrmp_dbt mp, dsfcontr_dbt sf, dmccateg_dbt cat, dmcaccdoc_dbt mc, daccount_dbt acc
                     WHERE mc.t_CatID = cat.t_ID
                       AND mc.t_Owner = sf.t_PartyID
                       AND mc.t_ClientContrID = sf.t_ID
                       AND acc.t_Account = mc.t_Account
                       AND acc.t_Chapter = mc.t_Chapter
                       AND acc.t_Code_Currency = mc.t_Currency
                       AND acc.t_Open_Date <= p_CalcDate
                       AND (acc.t_Close_Date = TO_DATE('01.01.0001','DD.MM.YYYY') OR acc.t_Close_Date >= p_CalcDate)
                       AND cat.t_LevelType = 1 
                       AND cat.t_Code in ('ДС клиента, ц/б', '+РасчетыКомисс1', 'Треб. с н.с. брок')
                       AND sf.t_ID = mp.t_SfContrID
                       AND mp.t_DlContrID = p_DlContrID
                   )
           )
     WHERE t_Rest <> 0;
     
    --Расчет стоимости по ц/б: отбор ц/б по субдоговорам МБ и СПБ (без признака блокировки) из таблицы клиентских лотов, действующих на дату расчета; 
    --получение последней действующей котировки вида "Рыночная цена" по каждому выпуску до даты расчета включительно, но не старше 90 дней
    INSERT INTO ddl_basesumval_tmp (
           t_ObjKind,                                                
           t_ObjectID,                                     
           t_Kind,
           t_Amount,                                            
           t_RateID,                                              
           t_RateDate,              
           t_Rate,                                                   
           t_NKD,                                                    
           t_Cost,                                              
           t_CostCur,                                  
           t_CBRate,                                            
           t_CostRub,                                  
           t_SfContrID,                                                   
           t_MarketID                                                     
           )            
    SELECT 2, 
           t_FIID, 
           2, 
           t_Amount, 
           t_RateID, 
           t_RateDate, 
           t_Rate, 
           t_NKD, 
           CASE WHEN t_RateID = -1 THEN 0 
                WHEN t_AvrKind = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN t_Amount * (t_Rate + t_NKD) 
                ELSE t_Amount * t_Rate 
           END, 
           t_RateFIID, 
           t_RateCB, 
           ROUND((CASE WHEN t_RateID = -1 THEN 0 
                       WHEN t_AvrKind = RSI_RSB_FIInstr.AVOIRKIND_BOND THEN t_Amount * (t_Rate + t_NKD) 
                       ELSE t_Amount * t_Rate 
                  END) * t_RateCB, 2), 
           t_SfContrID,                                                   
           t_MarketID                                                     
      FROM (SELECT q.t_Amount, q.t_AvrKind, q.t_SfContrID, q.t_MarketID, q.t_FIID, q.t_NKD, q.t_RateID,  
                   NVL(ratedef.t_FIID, -1) t_RateFIID, 
                   Rsb_SPRepFun.GetCourse(NVL(ratedef.t_RateID, 0), p_CalcDate) t_Rate,
                   CASE WHEN NVL(ratedef.t_SinceDate, to_date('31.12.9999','dd.mm.yyyy')) <= p_CalcDate THEN ratedef.t_SinceDate
                        ELSE NVL((SELECT MAX(hist.t_SinceDate) 
                                    FROM dratehist_dbt hist 
                                   WHERE hist.t_RateID = ratedef.t_RateID 
                                     AND hist.t_SinceDate <= p_CalcDate), 
                                 to_date('01.01.0001','dd.mm.yyyy')) 
                   END t_RateDate, 
                   NVL(RSI_RSB_FIInstr.ConvSum(1, NVL(ratedef.t_FIID, -1), RSI_RSB_FIInstr.NATCUR, p_CalcDate), 0) t_RateCB
              FROM (SELECT pmwrtcl.t_Amount, 
                           RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_Kind, fin.t_AvoirKind) t_AvrKind, 
                           mp.t_SfContrID, 
                           mp.t_MarketID, 
                           pmwrtcl.t_FIID, 
                           GetActiveRateId(pmwrtcl.t_FIID, p_CalcDate) t_RateID,
                           RSI_RSB_FIInstr.CalcNKD_Ex_NoRound(pmwrtcl.t_FIID, p_CalcDate, 1, 1, 0) t_NKD
                      FROM dpmwrtcl_dbt pmwrtcl 
                      JOIN dfininstr_dbt fin 
                        ON fin.t_FIID = pmwrtcl.t_FIID 
                      JOIN ddlcontrmp_dbt mp 
                        ON mp.t_SfContrID = pmwrtcl.t_Contract 
                       AND mp.t_MarketID <> -1 
                       AND mp.t_DlContrID = p_DlContrID
                     WHERE p_CalcDate BETWEEN pmwrtcl.t_BegDate AND pmwrtcl.t_EndDate 
                       AND pmwrtcl.t_Amount > 0) q 
              LEFT JOIN dratedef_dbt ratedef 
                     ON ratedef.t_RateID = q.t_RateID); 
    
    --Вставка ошибок, если не удалось определить курс ЦБ РФ 
    FOR cData IN (SELECT DISTINCT fin.t_CCY
                    FROM ddl_basesumval_tmp bsval, dfininstr_dbt fin
                   WHERE bsval.t_CBRate = 0
                     AND bsval.t_CostCur = fin.t_FIID
                     AND NOT (bsval.t_ObjKind = 2 AND bsval.t_RateID = -1) --Если котировки нет совсем, то и курса не будет
                 )
    LOOP
      AddToLog(ISSUE_ERROR, p_DlContrID, p_CalcDate, 'Не удалось определить курс ЦБ РФ по валюте '||cData.t_CCY);
      v_Err := 1;
    END LOOP; 
    
    --Вставка предупреждений, если не удалось определить котировку по бумаге (или дата котировки более 90 дней от даты расчета)
    FOR cData IN (SELECT DISTINCT avoir.t_ISIN, fin.t_Name
                    FROM ddl_basesumval_tmp bsval, dfininstr_dbt fin, davoiriss_dbt avoir
                   WHERE bsval.t_ObjKind = 2
                     AND bsval.t_RateID = -1
                     AND bsval.t_ObjectID = fin.t_FIID
                     AND fin.t_FIID = avoir.t_FIID
                 )
    LOOP
      AddToLog(ISSUE_WARNING, p_DlContrID, p_CalcDate, 'По бумаге '||cData.t_ISIN||' '||cData.t_Name||' не удалось определить котировку. Стоимость актива = 0');
    END LOOP; 

    --Вставка ошибок при отсутствии Номинала на дату расчета по облигации с индексируемым номиналом (по ОФЗ-ИН), если расчет производится за последний календарный день месяца и этот день приходится на выходной по системному календарю 
    IF p_CalcDate = LAST_DAY(p_CalcDate) AND Rsi_RsbCalendar.IsWorkDay(p_CalcDate, -1) = 0 THEN
      FOR cData IN (SELECT DISTINCT bsval.t_ObjKind, bsval.t_ObjectID, avoir.t_ISIN, fin.t_Name
                      FROM ddl_basesumval_tmp bsval, dfininstr_dbt fin, davoiriss_dbt avoir
                     WHERE bsval.t_ObjKind = 2
                       AND bsval.t_ObjectID = fin.t_FIID
                       AND fin.t_FIID = avoir.t_FIID
                       AND RSB_FIInstr.FI_AvrKindsGetRoot(fin.t_FI_Kind, fin.t_AvoirKind) = RSI_RSB_FIInstr.AVOIRKIND_BOND
                       AND avoir.t_IndexNom = 'X'
                       AND NOT EXISTS (SELECT 1
                                         FROM dfivlhist_dbt fivl
                                        WHERE fivl.t_FIID = avoir.t_FIID
                                          AND t_ValKind = 1 /*FI_CHANGE_NOMINAL*/ --Номинал
                                          AND t_EndDate = p_CalcDate)
                   )
      LOOP
        AddToLog(ISSUE_ERROR, p_DlContrID, p_CalcDate, 'По бумаге '||cData.t_ISIN||' '||cData.t_Name||' не удалось определить номинал на дату '||p_CalcDate);
        v_Err := 1;

        --Поскольку номинал считаем нулевым, устанавливаем нулевой стоимость актива
        UPDATE ddl_basesumval_tmp
           SET t_Cost = 0, 
               t_CostRub = 0
         WHERE t_ObjKind = cData.t_ObjKind
           AND t_ObjectID = cData.t_ObjectID;
      END LOOP;
    END IF;
    
    SELECT NVL(ROUND(SUM(t_CostRub), 2), 0) INTO v_Sum 
      FROM ddl_basesumval_tmp;        

    IF v_Err = 1 THEN
      v_State := 'E';
    ELSIF p_Action = ACTION_INFO THEN
      v_State := 'I';
    ELSIF p_Action = ACTION_CHECK THEN
      v_State := 'N';
    ELSE
      v_State := CHR(0);
    END IF;

    IF p_Action = ACTION_INFO THEN
      SELECT COUNT(1) INTO v_IsInvConnected                                                                
        FROM ddlcontrserv_dbt                                                           
       WHERE t_DlContrID = p_DlContrID
         AND t_ServKind = DLCONTR_SERVKIND_INVESTADVICE                              
         AND t_BeginDate < p_CalcDate                                                 
         AND (t_EndDate >= p_CalcDate OR t_EndDate = to_date('01.01.0001', 'dd.mm.yyyy'));
    ELSIF p_Action = ACTION_CHECK THEN
      v_IsInvConnected := 0;
    ELSE
      v_IsInvConnected := 1;
    END IF;  
  
    IF v_IsInvConnected = 1 THEN
      v_ComRate := GetCommissPercent(GetConComID(p_DlContrID, p_CalcDate, p_ComNumber, p_FeeType));
    ELSE
      v_ComRate := 0;
    END IF;

    IF v_Sum > 0 AND v_ComRate > 0 THEN
      v_ComSum := ROUND((v_Sum * v_ComRate/100 / (ADD_MONTHS(TRUNC(p_CalcDate,'Y'),12) - TRUNC(p_CalcDate,'Y'))), 2);
    ELSE
      v_ComSum := 0;
    END IF;

    --Вставим базовую сумму 
    INSERT INTO ddl_basesum_dbt (t_OpID, t_Date, t_ComNumber, t_FeeType, t_ClientID, t_DlContrID, t_Sum, t_Currency, t_LnkOpID, t_State, t_ComRate, t_ComSum)
                         VALUES (p_OperID, p_CalcDate, p_ComNumber, p_FeeType, p_ClientID, p_DlContrID, v_Sum, RSI_RSB_FIInstr.NATCUR, -1, v_State, v_ComRate, v_ComSum) RETURNING t_ID INTO v_BaseSumID; 
    
    --Вставим расшифровки базовой суммы
    INSERT INTO ddl_basesumval_dbt (
           t_BaseSumID,
           t_ObjKind,                                                
           t_ObjectID,                                     
           t_Kind,
           t_Amount,                                            
           t_RateID,                                              
           t_RateDate,              
           t_Rate,                                                   
           t_NKD,                                                    
           t_Cost,                                              
           t_CostCur,                                  
           t_CBRate,                                            
           t_CostRub,                                  
           t_SfContrID,                                                   
           t_MarketID                                                     
           )
    SELECT v_BaseSumID,
           t_ObjKind,                                                
           t_ObjectID,                                     
           t_Kind,
           t_Amount,                                            
           t_RateID,                                              
           t_RateDate,              
           t_Rate,                                                   
           t_NKD,                                                    
           t_Cost,                                              
           t_CostCur,                                  
           t_CBRate,                                            
           t_CostRub,                                  
           t_SfContrID,                                                   
           t_MarketID 
      FROM ddl_basesumval_tmp;

  EXCEPTION
    WHEN OTHERS THEN NULL;
  END CalcBaseSumOnInvestAdviser;

END RSB_BASESUM;
/
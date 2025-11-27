CREATE OR REPLACE PACKAGE BODY IT_BrokerReport
IS 
  --Очистка временных таблиц
  PROCEDURE TrancateTables
  IS
  BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPDEAL_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPINACC_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPACTIVEAVOIR_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPACTIVEDERIV_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPPOOL_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPCASHE_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPACC_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPACCITOG_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPCURDEAL_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPDVDEAL_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPDEBT_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPSVODINFO_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPCASHEMOVING_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPCASHEGROUPMOVING_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPCOURSES_TMP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE DBRKREPINACCTICK_TMP';
  END TrancateTables;
  
  --Подготовка отчетных данных
  PROCEDURE CreateAllData( p_DlContrID     IN NUMBER,
                           p_BegDate       IN DATE,  
                           p_EndDate       IN DATE,
                           p_ByExchange    IN NUMBER,
                           p_ByOutExchange IN NUMBER,
                           p_IsEDP         IN NUMBER 
                         )
  IS
  BEGIN
    TrancateTables();

    --Сбор информации по клиентским счетам
    RSB_BRKREP_RSHB_NEW.LoadAccInTmp(p_DlContrID, p_BegDate, p_EndDate, p_IsEDP, p_ByOutExchange, 1);
  
    --Подготовка данных по остаткам и движениям ДС
    RSB_BRKREP_RSHB_NEW.CreateCoursesData(p_BegDate, p_EndDate); --Курсы валют
    RSB_BRKREP_RSHB_NEW.CreateAccMoveData(p_BegDate, p_EndDate, p_IsEDP); --Оценка денежной позиции
    RSB_BRKREP_RSHB_NEW.CreateCasheMoveData(p_BegDate, p_EndDate, p_IsEDP); --Движение денежных средств
  
    --Подготовка данных для разделов по оценке позиций по активам (ц/б и ПФИ) и составу портфеля
    RSB_BRKREP_RSHB_NEW.CreateActiveData(p_DlContrID, p_BegDate, p_EndDate, p_IsEDP); --Оценка позиций по ЦБ
    IF p_ByExchange = 1 THEN
      RSB_BRKREP_RSHB_NEW.CreateActiveDerivData(p_DlContrID, p_BegDate, p_EndDate, p_IsEDP); --Оценка позиций по ПФИ
    END IF;
  
    --Подготовка данных по сделкам с ц/б (фондовый рынок ММВБ/СПБ, внебиржа)
    RSB_BRKREP_RSHB_NEW.CreateDealData(p_DlContrID, p_BegDate, p_EndDate, p_ByExchange, p_ByOutExchange, p_IsEDP); 
  
    IF p_ByExchange = 1 THEN
      --Подготовка данных по обязательствам перед банком
      RSB_BRKREP_RSHB_NEW.CreateDebtData(p_DlContrID, p_BegDate, p_EndDate); 
  
      --Подготовка данных по сделкам валютного рынка
      RSB_BRKREP_RSHB_NEW.CreateCurDealData(p_DlContrID, p_BegDate, p_EndDate, p_IsEDP);
  
      --Подготовка данных по сделкам срочного рынка
      RSB_BRKREP_RSHB_NEW.CreateDvDealData(p_DlContrID, p_BegDate, p_EndDate, p_IsEDP);
    END IF;
    
    --Подготовка данных по сводной информации
    RSB_BRKREP_RSHB_NEW.CreateSvodInfoData(p_BegDate, p_EndDate);
  
  END CreateAllData;

  --Получение идентификатора клиента по виду кода "Код ЦФТ"  
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

  --Получение идентификатора ДБО клиента за период   
  FUNCTION GetDlContrIDByNumber(p_ContrNumber IN VARCHAR2, p_PartyID IN NUMBER, p_StartDate IN DATE, p_EndDate IN DATE) 
    RETURN NUMBER
  IS
    v_DlContrID ddlcontr_dbt.t_DlContrID%TYPE;
  BEGIN
    SELECT dl.t_DlContrID INTO v_DlContrID
      FROM dsfcontr_dbt sf, ddlcontr_dbt dl 
     WHERE sf.t_PartyID = p_PartyID
       AND sf.t_Number = p_ContrNumber
       AND sf.t_DateBegin <= p_EndDate 
       AND (sf.t_DateClose = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR sf.t_DateClose >= p_StartDate)
       AND dl.t_SfContrID = sf.t_ID;
    RETURN v_DlContrID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
  END GetDlContrIDByNumber;

  --Проверка наличия признака ЕДП на договоре   
  FUNCTION CheckEDP(p_DlContrID IN NUMBER) 
    RETURN NUMBER
  IS
    v_ret NUMBER;
  BEGIN
    SELECT 1 INTO v_ret
      FROM dsfcontr_dbt sf, ddlcontrmp_dbt mp, dobjatcor_dbt at 
     WHERE sf.t_ServKindSub = 8 
       AND at.t_ObjectType = 659
       AND at.t_GroupID = 102
       AND at.t_AttrID = 1
       AND at.t_Object = LPAD(sf.t_ID, 10, '0')
       AND sf.t_ID = mp.t_SfContrID 
       AND mp.t_DlContrID = p_DlContrID
       AND ROWNUM = 1;
    RETURN v_ret;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END CheckEDP;

  --Получение идентификатора подписанта по-умолчанию
  FUNCTION GetDefaultSigner(p_ErrorText OUT VARCHAR2)
    RETURN NUMBER
  IS
    v_ret NUMBER;
    v_regPath VARCHAR2(100) := 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ОТЧЕТ БРОКЕРА\AUTOMATION\DEFAULT_SIGNER';
    v_PartyID dperson_dbt.t_PartyID%TYPE;
    v_UserClosed dperson_dbt.t_UserClosed%TYPE;  
  BEGIN
    v_ret := Rsb_Common.GetRegIntValue(v_regPath);
    IF v_ret IS NULL OR v_ret <= 0 THEN
      v_ret := -1;
      p_ErrorText := 'Не задана настройка в реестре банка: '||v_regPath;
    ELSE
      BEGIN
        SELECT t_PartyID, t_UserClosed
          INTO v_PartyID, v_UserClosed
          FROM dperson_dbt
         WHERE t_oper = v_ret;

        IF v_PartyID <> 1 THEN
          IF v_UserClosed = 'X' THEN 
            v_ret := -1;
            p_ErrorText := 'Операционист '||v_ret||' уволен';          
          ELSE
            v_ret := v_PartyID;
          END IF;
        ELSE
          v_ret := -1;
          p_ErrorText := 'Операционист '||v_ret||' не является сотрудником банка';          
        END IF;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN 
          v_ret := -1;
          p_ErrorText := 'Операционист '||v_ret||' не найден. Проверьте настройку '||v_regPath;          
      END;
    END IF;

    RETURN v_ret; 
  END GetDefaultSigner;

  --Получение ЕКК кода клиента по идентификатору договора
  FUNCTION GetClientCode(p_DlContrID IN NUMBER)
    RETURN VARCHAR2
  IS
    v_Code ddlobjcode_dbt.t_Code%TYPE;
  BEGIN
    SELECT t_Code INTO v_Code
      FROM ddlobjcode_dbt 
     WHERE t_ObjectType = 207
       AND t_CodeKind = 1
       AND t_ObjectID = p_DlContrID;
    RETURN v_Code;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN '';
  END GetClientCode;

  --Получение ФИО клиента по его идентификатору
  FUNCTION GetClientName(p_PartyID IN NUMBER)
    RETURN VARCHAR2
  IS
    v_Name dparty_dbt.t_Name%TYPE;
  BEGIN
    SELECT t_Name INTO v_Name
      FROM dparty_dbt 
     WHERE t_PartyID = p_PartyID;
    RETURN v_Name;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN '';
  END GetClientName;

  --Получение даты заключения договора по его идентификатору
  FUNCTION GetContrDate(p_DlContrID IN NUMBER)
    RETURN DATE
  IS
    v_ConcDate dsfcontr_dbt.t_DateConc%TYPE;
  BEGIN
    SELECT sf.t_DateConc INTO v_ConcDate
      FROM ddlcontr_dbt dlc, dsfcontr_dbt sf 
     WHERE dlc.t_DlContrID = p_DlContrID
       AND sf.t_ID = dlc.t_SfContrID;
    RETURN v_ConcDate;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN TO_DATE('01.01.0001','DD.MM.YYYY');
  END GetContrDate;
  
  /* Получение текущих параметров фин.инструмента */
  FUNCTION GetCurrentParamFIID(p_FIID in number, p_date in date) RETURN varchar2
  IS 
    res varchar2(2000);
  BEGIN
    BEGIN 
      select 'купон '||case when fiwarnts.t_relativeincome = chr(88) 
                            then trim(to_char(fiwarnts.t_incomerate,'99999999999999999990.'||lpad('0',fiwarnts.t_incomepoint,'9'))) 
                            else trim(to_char(fiwarnts.t_incomevolume,'99999999999999999990.'||lpad('0',fiwarnts.t_incomepoint,'9')))||' '||nvl(fininstr.t_ccy,'?') 
                       end 
                     ||case when dininstrp.t_drawingdate != to_date('01.01.0001','dd.mm.yyyy') 
                            then ' дата погашения '||to_char(dininstrp.t_drawingdate,'dd.mm.yyyy') 
                            else '' 
                       end  
                     || ' номинал '||case when RSB_FIINSTR.FI_GETNOMINALONDATE(d.t_fiid, p_date) > 0 
                                          then RSB_FIINSTR.FI_GETNOMINALONDATE(d.t_fiid, p_date) 
                                          else dininstrp.t_facevalue 
                                     end
                     ||' '|| (select (select t_ccy from dfininstr_dbt where t_fiid = f.t_facevaluefi) from dfininstr_dbt f where f.t_fiid = d.t_fiid) t_cupon 
        into res
        from dfiwarnts_dbt fiwarnts 
          left join dfininstr_dbt fininstr on fininstr.t_fiid = fiwarnts.t_incomefi 
          join (select t_fiid, t_id 
                  from (select fiwarnts.t_fiid, fiwarnts.t_id 
                          from dfiwarnts_dbt fiwarnts 
                         where fiwarnts.t_fiid = p_FIID 
                           and fiwarnts.t_drawingdate >= p_date 
                      order by fiwarnts.t_drawingdate, fiwarnts.t_id desc) 
                 where rownum = 1) d on rsi_rsb_fiinstr.FI_IsCouponAvoiriss(d.t_fiid) = 1 and fiwarnts.t_id = d.t_id 
          join dfininstr_dbt dininstrp on dininstrp.t_fiid = d.t_fiid;
    EXCEPTION
      when no_data_found then res := chr(0);
    END;

    if res = chr(0) then
      BEGIN
        select 'дата погашения '||to_char(fininstr.t_drawingdate,'dd.mm.yyyy') 
            || ' номинал '||case when RSB_FIINSTR.FI_GETNOMINALONDATE(fininstr.t_fiid, p_date) > 0 
                                 then RSB_FIINSTR.FI_GETNOMINALONDATE(fininstr.t_fiid, p_date) 
                                 else fininstr.t_facevalue 
                            end 
            ||' '|| (select (select t_ccy from dfininstr_dbt where t_fiid = f.t_facevaluefi) from dfininstr_dbt f where f.t_fiid = fininstr.t_fiid) t_cupon 
          into res
          from dfininstr_dbt fininstr
         where fininstr.t_fiid = p_FIID 
           and fininstr.t_drawingdate != to_date('01.01.0001','dd.mm.yyyy');
      EXCEPTION
        when no_data_found then res := null;
      END;
    end if;
    
    return res;
  END;
  

  --Получение JSON по блоку "Сводная информация"
  FUNCTION GetSummaryInformationBlock(p_IsOtc IN NUMBER)
    RETURN CLOB
  IS
    v_SummaryInformationBlock CLOB;
    v_NodeName VARCHAR2(50) := (CASE WHEN p_IsOtc = 0 THEN '' ELSE 'Otc' END)||'SummaryInformation';
  BEGIN
    SELECT JSON_OBJECT(v_NodeName||'List' IS 
             JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('Name' IS t_Name
                                                                        ,'StartDateSum' IS to_char(t_InSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'EndDateSum' IS to_char(t_OutSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'Difference' IS to_char(t_Difference, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ABSENT ON NULL)
                                                            )                                 
                                          FROM dbrkrepsvodinfo_tmp 
                                         WHERE t_IsItog = CHR(0) 
                                           AND ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                                       )
                       RETURNING CLOB),  
                       v_NodeName||'TotalList' IS
             JSON_OBJECT(v_NodeName||'Total' IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('TotalName' IS t_Name
                                                                                 ,'StartDateSum' IS to_char(t_InSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'EndDateSum' IS to_char(t_OutSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'Difference' IS to_char(t_Difference, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ABSENT ON NULL)
                                                                     )
                                                   FROM dbrkrepsvodinfo_tmp 
                                                  WHERE t_IsItog = 'X' 
                                                    AND ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                                                )
                         RETURNING CLOB)
                       RETURNING CLOB)
      INTO v_SummaryInformationBlock
      FROM dual;

    RETURN v_SummaryInformationBlock;
  END GetSummaryInformationBlock;

  --Получение JSON по блоку "Курсы валют"
  FUNCTION GetRateExchangeList(p_IsOtc IN NUMBER)
    RETURN CLOB
  IS
    v_RateExchangeList CLOB;
    v_NodeName VARCHAR2(50) := (CASE WHEN p_IsOtc = 0 THEN '' ELSE 'Otc' END)||'RateExchange';
  BEGIN
    SELECT JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('CurrencyName' IS t_CCY
                                                                      ,'StartDateRate' IS to_char(t_InRate, 'FM99999999999999999990'||rpad('D',t_InPoint+1,'0'),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                      ,'EndDateRate' IS to_char(t_OutRate, 'FM99999999999999999990'||rpad('D',t_OutPoint+1,'0'),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                      ABSENT ON NULL)
                                                           ORDER BY t_Currency
                                                           RETURNING CLOB)                                 
                                        FROM dbrkrepcourses_tmp 
                                       WHERE ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                                     )
                       RETURNING CLOB)        
      INTO v_RateExchangeList
      FROM dual;

    RETURN v_RateExchangeList;
  END GetRateExchangeList;

  --Получение JSON по блоку "Оценка денежной позиции"
  FUNCTION GetCashPositionValuationList(p_IsOtc IN NUMBER)
    RETURN CLOB
  IS
    v_CashPositionValuationList CLOB;
    v_NodeName VARCHAR2(50) := (CASE WHEN p_IsOtc = 0 THEN '' ELSE 'Otc' END)||'CashPositionValuation';
  BEGIN
    SELECT JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('CurrencyName' IS t_CCY
                                                                      ,'StartDateOpeningBalance' IS to_char(t_InRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                      ,'EndDateClosingBalance' IS to_char(t_OutRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                      ,'EndDatePlannedBalance' IS to_char(t_PlanRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                       ABSENT ON NULL)
                                                           ORDER BY t_MarketName, t_Currency
                                                           RETURNING CLOB
                                                          )
                                        FROM dbrkrepaccitog_tmp
                                       WHERE t_IsItog = CHR(0)
                                         AND ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                                     )
                       RETURNING CLOB)        
      INTO v_CashPositionValuationList
      FROM dual;

    RETURN v_CashPositionValuationList;
  END GetCashPositionValuationList;
  
    --Получение JSON по блоку "Оценка денежной позиции"
  FUNCTION OtcCashPositionValuationTotalOTC
    RETURN CLOB
  IS
    v_OtcCashPositionValuationTotalOTC CLOB;
  BEGIN
    SELECT JSON_OBJECT('OtcCashPositionValuationTotal' IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('CurrencyName' IS t_CCY
                                                                      ,'StartDateOpeningBalance' IS to_char(t_InRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                      ,'EndDateClosingBalance' IS to_char(t_OutRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                      ,'EndDatePlannedBalance' IS to_char(t_PlanRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                       ABSENT ON NULL)
                                                           ORDER BY t_MarketName, t_Currency
                                                           RETURNING CLOB
                                                          )                                 
                                        FROM dbrkrepaccitog_tmp 
                                       WHERE t_IsItog = CHR(88)
                                         AND t_ServKindSub = 9
                                     )
                       RETURNING CLOB)        
      INTO v_OtcCashPositionValuationTotalOTC
      FROM dual;

    RETURN v_OtcCashPositionValuationTotalOTC;
  END OtcCashPositionValuationTotalOTC;
  
  --Получение JSON по блоку "Оценка денежной позиции" для ЮЛ
  FUNCTION GetExchangePlatform(p_IsOtc IN NUMBER)
    RETURN CLOB
  IS
    v_GetExchangePlatform CLOB;
    v_NodeNameList VARCHAR2(50);
    v_NodeName VARCHAR2(50);
  BEGIN
    if p_IsOtc = 0 then 
      v_NodeNameList := 'CashPositionValuationList';
      v_NodeName := 'CashPositionValuation';
    else
      v_NodeNameList := 'OtcCashPositionValuationList';
      v_NodeName := 'OtcCashPositionValuation';
    end if;

    if p_IsOtc = 0 then
        SELECT JSON_ARRAYAGG( JSON_OBJECT ('ExchangePlatformName' IS t_MarketName,
                                           v_NodeNameList IS JSON_OBJECT (v_NodeName IS
                                              JSON_ARRAYAGG( JSON_OBJECT('CurrencyName' IS t_CCY
                                                                        ,'StartDateOpeningBalance' IS to_char(t_InRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'EndDateClosingBalance' IS to_char(t_OutRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'EndDatePlannedBalance' IS to_char(t_PlanRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ABSENT ON NULL)
                                                             ORDER BY t_MarketName, t_Currency
                                                             RETURNING CLOB
                                                             )
                                              RETURNING CLOB)
                                              ABSENT ON NULL
                                           )
                              RETURNING CLOB )
          INTO v_GetExchangePlatform
          FROM dbrkrepaccitog_tmp
         WHERE t_IsItog = CHR(0)
           AND t_ServKindSub <> 9
        group by t_MarketName;
    else
        SELECT JSON_OBJECT (v_NodeName IS (SELECT JSON_ARRAYAGG( JSON_OBJECT('CurrencyName' IS t_CCY
                                                                            ,'StartDateOpeningBalance' IS to_char(t_InRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                            ,'EndDateClosingBalance' IS to_char(t_OutRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                            ,'EndDatePlannedBalance' IS to_char(t_PlanRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                            ABSENT ON NULL)
                                                                     ORDER BY t_MarketName, t_Currency
                                                                     RETURNING CLOB
                                                                    )
                                                FROM dbrkrepaccitog_tmp 
                                               WHERE t_IsItog = CHR(0)
                                                 AND t_ServKindSub = 9
                                           )
                            )
          INTO v_GetExchangePlatform
          FROM dual;
    end if;    

    RETURN v_GetExchangePlatform;
  END GetExchangePlatform;
    
  
  --Получение JSON по блоку "Обязательства перед банком"
  FUNCTION GetCommitmentBlock(p_OnDate IN DATE)
    RETURN CLOB
  IS
    v_CommitmentBlock CLOB;
  BEGIN
    SELECT JSON_OBJECT('CommitmentList' IS 
             JSON_OBJECT('Commitment' IS (SELECT nvl(JSON_ARRAYAGG(JSON_OBJECT('OperationName' IS t_Text
                                                                          ,'DebtAppearanceDate' IS DECODE(t_IsTotal, 'X', NULL, to_char(t_OriginDate, 'DD.MM.YYYY'))
                                                                          ,'PlannedPaymentDate' IS DECODE(t_IsTotal, 'X', NULL, to_char(t_PayDate, 'DD.MM.YYYY'))
                                                                          ,'CurrencyName' IS t_CCY
                                                                          ,'PaymentSum' IS to_char(t_Sum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                          ,'IsTotal' IS DECODE(t_IsTotal, 'X', 'true', 'false') 
                                                                          ABSENT ON NULL)
                                                               ORDER BY t_Type, t_Currency, t_IsTotal DESC, t_OriginDate
                                                               RETURNING CLOB),'[]' )                                 
                                                    FROM dbrkrepdebt_tmp 
                                                   WHERE t_IsItog = CHR(0) 
                                                     AND t_OnDate = p_OnDate
                                         ) FORMAT JSON
                       RETURNING CLOB),  
                       'CommitmentTotalList' IS
             JSON_OBJECT('CommitmentTotal' IS (SELECT nvl(JSON_ARRAYAGG(JSON_OBJECT('TotalName' IS t_Text
                                                                               ,'PaymentSum' IS to_char(t_Sum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                               ABSENT ON NULL)
                                                                   ),'[]')
                                                 FROM dbrkrepdebt_tmp 
                                                WHERE t_IsItog = 'X' 
                                                  AND t_OnDate = p_OnDate) format json
                       RETURNING CLOB)
                     RETURNING CLOB)
      INTO v_CommitmentBlock
      FROM dual;

    RETURN v_CommitmentBlock;
  END GetCommitmentBlock;

  --Получение формата числа
  FUNCTION GetFormatSample(p_Amount IN NUMBER)
    RETURN VARCHAR2
  IS
  BEGIN
    RETURN 'FM99999999999999999990'||rtrim(rpad('D',RSB_BRKREP_RSHB_NEW.AmountPrecision(p_Amount,20)+1,'0'),'D');
  END GetFormatSample;

  --Получение JSON по блоку "Оценка позиций по ценным бумагам и иностранным финансовым инструментам, не квалифицированным в качестве ценных бумаг"
  FUNCTION GetSecuritiesValuationBlock(p_IsOtc IN NUMBER)
    RETURN CLOB
  IS
    v_SecuritiesValuationBlock CLOB;
    v_NodeName VARCHAR2(50) := (CASE WHEN p_IsOtc = 0 THEN '' ELSE 'Otc' END)||'SecuritiesValuation';
  BEGIN
    SELECT JSON_OBJECT(v_NodeName||'List' IS 
             JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('SecuritiesName' IS t_Name
                                                                        ,'IdentificationNumber' IS t_ISINLSIN
                                                                        ,'CurrencyName' IS t_FaceValueFICcy
                                                                        ,'StartDateOpeningBalance' IS to_char(t_InRest, GetFormatSample(t_InRest),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'StartDateQuotation' IS to_char(t_InCourse, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'StartDateQuotationCurrency' IS case t_InCourseFICcy when chr(1) then null else t_InCourseFICcy end
                                                                        ,'StartDateAllocatedCouponSum' IS to_char(t_InNKD, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'StartDateMarketPrice' IS to_char(t_InCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'CreditQuantity' IS to_char(t_Buy, GetFormatSample(t_Buy),'NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'WithdrawalsQuantity' IS to_char(t_Sale, GetFormatSample(t_Sale),'NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'EndDateClosingBalance' IS to_char(t_OutRest, GetFormatSample(t_OutRest),'NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'EndDateQuotation' IS to_char(t_OutCourse, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'EndDateQuotationCurrency' IS case t_OutCourseFICcy when chr(1) then null else t_OutCourseFICcy end
                                                                        ,'EndDateAllocatedCouponSum' IS to_char(t_OutNKD, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'EndDateMarketPrice' IS to_char(t_OutCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'PlannedBalanceQuantity' IS to_char(t_OutPlanRest, GetFormatSample(t_OutPlanRest),'NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'PlannedBalanceSum' IS to_char(t_OutPlanCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ABSENT ON NULL RETURNING CLOB)
                                                             ORDER BY t_Name
                                                             RETURNING CLOB)                                 
                                         FROM dbrkrepactiveavoir_tmp 
                                        WHERE t_IsItog = CHR(0) 
                                          AND ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                                       )
                       RETURNING CLOB),  
                       v_NodeName||'TotalList' IS
             JSON_OBJECT(v_NodeName||'Total' IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('TotalName' IS t_Name
                                                                                 ,'StartDateMarketPrice' IS to_char(t_InCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'EndDateMarketPrice' IS to_char(t_OutCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'PlannedBalanceSum' IS to_char(t_OutPlanCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ABSENT ON NULL)
                                                                     )
                                                  FROM dbrkrepactiveavoir_tmp 
                                                 WHERE t_IsItog = 'X' 
                                                   AND ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                                                )
                       RETURNING CLOB)
                     RETURNING CLOB)
      INTO v_SecuritiesValuationBlock
      FROM dual;

    RETURN v_SecuritiesValuationBlock;
  END GetSecuritiesValuationBlock;

  --Получение JSON по блоку "Оценка позиций по Производным финансовым инструментам"
  FUNCTION GetDerivativeFinancialInstrumentBlock
    RETURN CLOB
  IS
    v_DerivativeFinancialInstrumentBlock CLOB;
  BEGIN
    SELECT JSON_OBJECT('DerivativeFinancialInstrumentList' IS 
             JSON_OBJECT('DerivativeFinancialInstrument' IS (SELECT nvl(JSON_ARRAYAGG(JSON_OBJECT('AgreementType' IS t_FIKind
                                                                                             ,'AgreementCode' IS t_FIName
                                                                                             ,'StartDateOpeningBalanceQuantity' IS to_char(t_InRest, GetFormatSample(t_InRest),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                             ,'StartDateOpeningBalanceSum' IS to_char(t_InCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                             ,'StartDateGuarantee' IS to_char(t_InGuaranty, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                             ,'PurchaseQuantity' IS to_char(t_Buy, GetFormatSample(t_Buy),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                             ,'SaleQuantity' IS to_char(t_Sale, GetFormatSample(t_Sale),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                             ,'EndDateClosingBalanceQuantity' IS to_char(t_OutRest, GetFormatSample(t_OutRest),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                             ,'EndDateClosingBalanceSum' IS to_char(t_OutCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                                             ,'EndDateGuarantee' IS to_char(t_OutGuaranty, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                                             ,'PlannedBalanceQuantity' IS to_char(t_OutPlanRest, GetFormatSample(t_OutPlanRest),'NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                                             ,'PlannedBalanceSum' IS to_char(t_OutPlanCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                             ABSENT ON NULL RETURNING CLOB)
                                                                                  ORDER BY t_FIKind, t_FIName
                                                                                  RETURNING CLOB),'[]')                                 
                                                               FROM dbrkrepactivederiv_tmp 
                                                              WHERE t_IsItog = CHR(0) 
                                                                AND t_ServKindSub <> 9
                                                            ) format json
                       RETURNING CLOB),  
                       'DerivativeFinancialInstrumentTotalList' IS
             JSON_OBJECT('DerivativeFinancialInstrumentTotal' IS (SELECT nvl(JSON_ARRAYAGG(JSON_OBJECT('TotalName' IS t_FIKind
                                                                                                   ,'StartDateOpeningBalanceSum' IS to_char(t_InCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                                   ,'StartDateGuarantee' IS to_char(t_InGuaranty, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                                   ,'EndDateClosingBalanceSum' IS to_char(t_OutCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                                   ,'EndDateGuarantee' IS to_char(t_OutGuaranty, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                                   ,'PlannedBalanceSum' IS to_char(t_OutPlanCostRest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                                   ABSENT ON NULL)
                                                                                       ),'[]')
                                                                     FROM dbrkrepactivederiv_tmp 
                                                                    WHERE t_IsItog = 'X' 
                                                                      AND t_ServKindSub <> 9
                                                                  ) format json
                       RETURNING CLOB)
                     RETURNING CLOB)
      INTO v_DerivativeFinancialInstrumentBlock
      FROM dual;

    RETURN v_DerivativeFinancialInstrumentBlock;
  END GetDerivativeFinancialInstrumentBlock;

  --Получение JSON по блоку "Движения по счету"
  FUNCTION GetAccountMovementsList(p_IsOtc IN NUMBER)
    RETURN CLOB
  IS
    v_AccountMovementsList CLOB;
    v_ParentNodeName VARCHAR2(50) := (CASE WHEN p_IsOtc = 0 THEN '' ELSE 'Otc' END)||'AccountMovements';
    v_NodeName VARCHAR2(50) := (CASE WHEN p_IsOtc = 0 THEN '' ELSE 'Otc' END)||'CashFlow';
    v_Cnt NUMBER;
  BEGIN
    SELECT COUNT(1) INTO v_Cnt
      FROM dbrkrepcashegroupmoving_tmp
     WHERE ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9));

    IF v_Cnt > 0 THEN
      WITH q1 AS (SELECT DISTINCT t_Currency
                    FROM dbrkrepcashegroupmoving_tmp
                   WHERE ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                 )
      SELECT JSON_OBJECT(v_ParentNodeName IS 
                          (SELECT JSON_ARRAYAGG(JSON_OBJECT('CurrencyName' IS DECODE(q1.t_Currency, PM_COMMON.NATCUR, 'Рубли', fin.t_Name)||' ('||fin.t_CCY||')',
                                                            v_NodeName||'List' IS 
                                                              JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('Date' IS t.t_PrintDate
                                                                                                                         ,'OperationName' IS t.t_OperName
                                                                                                                         ,'Marketplace' IS t.t_MarketName
                                                                                                                         ,'CreditSum' IS DECODE(t.t_InSum, 0, NULL, to_char(t.t_InSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.'''))
                                                                                                                         ,'WithdrawalsSum' IS DECODE(t.t_OutSum, 0, NULL, to_char(t.t_OutSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.'''))
                                                                                                                         ,'PaymentsBalance' IS NULL
                                                                                                                         ABSENT ON NULL)
                                                                                                              RETURNING CLOB)                                 
                                                                                           FROM dbrkrepcashegroupmoving_tmp t
                                                                                          WHERE t.t_IsItog = CHR(0) 
                                                                                            AND ((p_IsOtc = 0 AND t.t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t.t_ServKindSub = 9))
                                                                                            AND t.t_Currency = q1.t_Currency
                                                                                        )
                                                                        RETURNING CLOB),  
                                                            v_NodeName||'TotalList' IS
                                                              JSON_OBJECT(v_NodeName||'Total' IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('CashFlowTotalName' IS 'Итого в '||fin.t_CCY
                                                                                                                                  ,'CreditSum' IS DECODE(t.t_InSum, 0, NULL, to_char(t.t_InSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.'''))
                                                                                                                                  ,'WithdrawalsSum' IS DECODE(t.t_OutSum, 0, NULL, to_char(t.t_OutSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.'''))
                                                                                                                                  ,'PaymentsBalance' IS to_char(t.t_Rest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                                                                  ABSENT ON NULL)
                                                                                                                       RETURNING CLOB)
                                                                                                    FROM dbrkrepcashegroupmoving_tmp t
                                                                                                   WHERE t.t_IsItog = 'X' 
                                                                                                     AND ((p_IsOtc = 0 AND t.t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t.t_ServKindSub = 9))
                                                                                                     AND t.t_Currency = q1.t_Currency
                                                                                                 )
                                                                          RETURNING CLOB)
                                                            RETURNING CLOB)
                                               RETURNING CLOB)                                 
                             FROM q1, dfininstr_dbt fin
                            WHERE fin.t_FIID = q1.t_Currency
                          )
                         RETURNING CLOB)
        INTO v_AccountMovementsList
        FROM dual;
    ELSE
      SELECT JSON_OBJECT(v_ParentNodeName IS 
                          (SELECT JSON_ARRAYAGG(JSON_OBJECT('CurrencyName' IS 'Рубли'||' ('||fin.t_CCY||')',
                                                            v_NodeName||'List' IS 
                                                              JSON_OBJECT(v_NodeName IS NULL),  
                                                            v_NodeName||'TotalList' IS
                                                              JSON_OBJECT(v_NodeName||'Total' IS NULL)
                                                           )
                                               )                                 
                             FROM dfininstr_dbt fin
                            WHERE fin.t_FIID = PM_COMMON.NATCUR
                          )
                        RETURNING CLOB)
        INTO v_AccountMovementsList
        FROM dual;
    END IF;

    RETURN v_AccountMovementsList;
  END GetAccountMovementsList;

  /**
   @brief Получение наименования раздела отчета по сделкам
   @param[in] p_Mode раздел
   @param[in] p_Market блок  
   @param[in] p_BegDate дата начала периода 
   @param[in] p_EndDate дата окончания периода   
   @return наименование раздела 
  */
  FUNCTION GetPartNameTransactions(p_Mode IN NUMBER, p_Market IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE)
    RETURN VARCHAR2
  IS
    p_NumStr VARCHAR2(3) := '';
  BEGIN
    IF p_Market = 3 THEN
      p_NumStr := p_Market||'.'||p_Mode;
    ELSE
      IF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD THEN
        p_NumStr := p_Market||'.1';
      ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL THEN
        p_NumStr := p_Market||'.2';
      END IF;
    END IF;

    IF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD THEN
      RETURN p_NumStr||'. Сделки, заключенные в период с '||TO_CHAR(p_BegDate, 'DD.MM.YYYY')||' по '||TO_CHAR(p_EndDate, 'DD.MM.YYYY');
    ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_REPOINPERIOD THEN
      RETURN p_NumStr||'. Сделки РЕПО, заключенные в период с '||TO_CHAR(p_BegDate, 'DD.MM.YYYY')||' по '||TO_CHAR(p_EndDate, 'DD.MM.YYYY');
    ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL THEN
      RETURN p_NumStr||'. Сделки, заключенные ранее '||TO_CHAR(p_BegDate, 'DD.MM.YYYY');
    ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECREPO THEN
      RETURN p_NumStr||'. Сделки РЕПО, заключенные ранее '||TO_CHAR(p_BegDate, 'DD.MM.YYYY');
    ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_CANCELDEAL THEN
      RETURN p_NumStr||'. Сделки, отмененные в период с '||TO_CHAR(p_BegDate, 'DD.MM.YYYY')||' по '||TO_CHAR(p_EndDate, 'DD.MM.YYYY');
    ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_CANCELREPO THEN
      RETURN p_NumStr||'. Сделки РЕПО, отмененные в период с '||TO_CHAR(p_BegDate, 'DD.MM.YYYY')||' по '||TO_CHAR(p_EndDate, 'DD.MM.YYYY');
    END IF;

    RETURN '';
  END;

  /**
   @brief Получение наименования узла JSON для блоков по сделкам
   @param[in] p_IsOtc признак внебиржи
   @param[in] p_Mode раздел
   @param[in] p_Market блок  
   @return наименование узла JSON 
  */
  FUNCTION GetNodeNameTransactions(p_IsOtc IN NUMBER, p_Mode IN NUMBER, p_Market IN NUMBER)
    RETURN VARCHAR2
  IS
  BEGIN
    IF p_IsOtc = 0 THEN 
      IF p_Market = 3 THEN   
        IF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD THEN
          RETURN 'StockMarketTransactions';
        ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_REPOINPERIOD THEN
          RETURN 'StockMarketREPOTransactions';
        ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL THEN
          RETURN 'EarlierStockMarketTransactions';
        ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECREPO THEN
          RETURN 'EarlierStockMarketREPOTransactions';
        END IF;
      ELSIF p_Market = 4 THEN
        IF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD THEN
          RETURN 'DerivativesMarketTransactions';
        ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL THEN
          RETURN 'EarlierDerivativesMarketTransactions';
        END IF;
      ELSIF p_Market = 5 THEN
        IF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD THEN
          RETURN 'ForeignExchangeMarketTransactions';
        ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL THEN
          RETURN 'EarlierForeignExchangeMarketTransactions';
        END IF;
      ELSIF p_Market = 6 THEN
        IF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD THEN
          RETURN 'SPBMarketTransactions';
        ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL THEN
          RETURN 'EarlierSPBMarketTransactions';
        END IF;
      END IF;
    ELSE
      IF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD THEN
        RETURN 'OtcTransactions';
      ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_REPOINPERIOD THEN
        RETURN 'OtcREPOTransactions';
      ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL THEN
        RETURN 'EarlierOtcTransactions';
      ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECREPO THEN
        RETURN 'EarlierOtcREPOTransactions';
      ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_CANCELDEAL THEN
        RETURN 'CancelledOtcTransactions';
      ELSIF p_Mode = RSB_BRKREP_RSHB_NEW.BROKERREP_PART_CANCELREPO THEN
        RETURN 'CancelledOtcREPOTransactions';
      END IF;
    END IF;

    RETURN '';
  END;

  --Получение JSON по блокам "Сделки на Фондовом рынке" и "Сделки на Внебиржевом рынке" в зависимости от раздела
  FUNCTION GetStockMarketOrOtcTransactionsBlock(p_IsOtc IN NUMBER, p_Mode IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, 
                                                p_MarketId in Number default 0, p_NumPart in Number default 3)
    RETURN CLOB
  IS
    v_StockMarketOrOtcTransactionsBlock CLOB;
    v_PartName VARCHAR2(100);
    v_NodeName VARCHAR2(50);
  BEGIN
    v_PartName := GetPartNameTransactions(p_Mode, p_NumPart, p_BegDate, p_EndDate);
    v_NodeName := GetNodeNameTransactions(p_IsOtc, p_Mode, p_NumPart);
  
    SELECT JSON_OBJECT(v_NodeName||'Title' IS v_PartName,
                       v_NodeName||'List' IS 
             JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('TransactionDateTime' IS to_char(t_A05_D, 'DD.MM.YYYY')||' '||to_char(t_A05_T, 'HH24:MI:SS')
                                                                        ,'TransactionExecutionDate' IS to_char(t_A06, 'DD.MM.YYYY')
                                                                        ,'Marketplace' IS t_A09
                                                                        ,'TransactionKind' IS t_A10
                                                                        ,'Issuer' IS t_A11
                                                                        ,'SecuritiesType' IS t_A25
                                                                        ,'IdentificationNumber' IS t_A12
                                                                        ,'BidPrice' IS to_char(t_A95, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'TransactionPrice' IS to_char(t_A13, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'RepurchaseAgreementCurrency' IS to_char(t_A13_i, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'TransactionCurrency' IS t_A14 
                                                                        ,'TransactionQuantity' IS to_char(t_A15, GetFormatSample(t_A15),'NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'TransactionSum' IS to_char(t_A16, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'TransactionRubSum' IS to_char(t_A17, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'BrokerCommissionSum' IS to_char(t_A18, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'BrokerCommissionCurrency' IS t_A19 
                                                                        ,'ExchangeFee' IS to_char(t_A20, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'AllocatedCouponSum' IS to_char(t_A26, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'AllocatedCouponRubSum' IS to_char(t_A27, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'TransactionType' IS t_A23 
                                                                        ,'Number' IS t_A08
                                                                        ,'Counterparty' IS t_A24 
                                                                        ,'CashCommitment' IS to_char(t_A28, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'SecuritiesCommitment' IS to_char(t_A29, GetFormatSample(t_A29),'NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'Note' IS case t_A95_M when chr(1) then null else t_A95_M end
                                                                        ABSENT ON NULL RETURNING CLOB)
                                                             ORDER BY t_A05_D, t_A05_T, t_A08, t_A10
                                                             RETURNING CLOB)
                                          FROM dbrkrepdeal_tmp 
                                         WHERE t_IsItog = CHR(0) 
                                           AND ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                                           AND ( (p_MarketId = 0) OR (p_MarketId <> 0 AND t_Marketid = p_MarketId) )
                                           AND t_Part = p_Mode
                                       )
                       RETURNING CLOB),  
                       v_NodeName||'TotalList' IS
             JSON_OBJECT(v_NodeName||'Total' IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('TotalName' IS 'Итого '||DECODE(t_Direction, 1, 'покупок', 'продаж')||' в '||t_A30
                                                                                 ,'TransactionSum' IS to_char(t_A16, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'TransactionRubSum' IS to_char(t_A17, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'BrokerCommissionSum' IS to_char(t_A18, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'ExchangeFee' IS to_char(t_A20, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'AllocatedCouponRubSum' IS to_char(t_A27, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ABSENT ON NULL RETURNING CLOB)
                                                                      ORDER BY t_A30, t_Direction
                                                                      RETURNING CLOB)
                                                   FROM dbrkrepdeal_tmp 
                                                  WHERE t_IsItog = 'X' 
                                                    AND ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                                                    AND ( (p_MarketId = 0) OR (p_MarketId <> 0 AND t_Marketid = p_MarketId) )
                                                    AND t_Part = p_Mode
                                                )
                       RETURNING CLOB)
                     RETURNING CLOB)
      INTO v_StockMarketOrOtcTransactionsBlock
      FROM dual;

    RETURN v_StockMarketOrOtcTransactionsBlock;
  END GetStockMarketOrOtcTransactionsBlock;

  --Получение JSON по блокам "Сделки на Срочном рынке" в зависимости от раздела
  FUNCTION GetDerivativesMarketTransactionsBlock(p_Mode IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE)
    RETURN CLOB
  IS
    v_DerivativesMarketTransactionsBlock CLOB;
    v_PartName VARCHAR2(100) := GetPartNameTransactions(p_Mode, 4, p_BegDate, p_EndDate);
    v_NodeName VARCHAR2(50) := GetNodeNameTransactions(0, p_Mode, 4);
  BEGIN
    SELECT JSON_OBJECT(v_NodeName||'Title' IS v_PartName,
                       v_NodeName||'List' IS 
             JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('TransactionDateTime' IS t_ConcDate
                                                                        ,'TransactionSettlementDate' IS to_char(t_ClrDate, 'DD.MM.YYYY')
                                                                        ,'TransactionPlace' IS t_MarketPlace
                                                                        ,'TransactionKind' IS t_DealKind
                                                                        ,'ContractType' IS t_FIKind
                                                                        ,'Contract' IS t_FIName
                                                                        ,'BidPrice' IS to_char(t_ReqPrice, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'FuturesContractPrice' IS to_char(t_PriceFuturesOrBonus, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'OptionExercisePrice' IS to_char(t_PriceOption, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'Quantity' IS to_char(t_Amount, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'TransactionRubSum' IS to_char(t_DealSumRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'VariationMargin' IS to_char(t_MarginRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'BrokerCommissionSum' IS to_char(t_BrokerComissRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'ExchangeFee' IS to_char(t_MarketComiss, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'TransactionNumber' IS t_Code
                                                                        ,'Note' IS case t_Note when chr(1) then null else t_Note end
                                                                        ABSENT ON NULL RETURNING CLOB)
                                                             ORDER BY t_DealID
                                                             RETURNING CLOB)                                 
                                          FROM dbrkrepdvdeal_tmp 
                                         WHERE t_IsItog = CHR(0) 
                                           AND t_ServKindSub <> 9
                                           AND t_Part = p_Mode
                                       )
                       RETURNING CLOB),  
                       v_NodeName||'TotalList' IS
             JSON_OBJECT(v_NodeName||'Total' IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('TotalName' IS t_ConcDate
                                                                                 ,'TransactionRubSum' IS to_char(t_DealSumRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'VariationMargin' IS to_char(t_MarginRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'BrokerCommissionSum' IS to_char(t_BrokerComissRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'ExchangeFee' IS to_char(t_MarketComiss, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ABSENT ON NULL RETURNING CLOB)
                                                                      RETURNING CLOB)                                 
                                                   FROM dbrkrepdvdeal_tmp 
                                                  WHERE t_IsItog = 'X' 
                                                    AND t_ServKindSub <> 9
                                                    AND t_Part = p_Mode
                                                )
                       RETURNING CLOB)
                     RETURNING CLOB)
      INTO v_DerivativesMarketTransactionsBlock
      FROM dual;

    RETURN v_DerivativesMarketTransactionsBlock;
  END GetDerivativesMarketTransactionsBlock;

  --Получение JSON по блокам "Сделки на Валютном рынке" в зависимости от раздела
  FUNCTION GetForeignExchangeMarketTransactionsBlock(p_Mode IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE)
    RETURN CLOB
  IS
    v_ForeignExchangeMarketTransactionsBlock CLOB;
    v_PartName VARCHAR2(100) := GetPartNameTransactions(p_Mode, 5, p_BegDate, p_EndDate);
    v_NodeName VARCHAR2(50) := GetNodeNameTransactions(0, p_Mode, 5);
  BEGIN
    SELECT JSON_OBJECT(v_NodeName||'Title' IS v_PartName,
                       v_NodeName||'List' IS 
             JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('TransactionDateTime' IS t_ConcDate
                                                                        ,'TransactionExecutionDate' IS to_char(t_ExecDate, 'DD.MM.YYYY')
                                                                        ,'Marketplace' IS t_MarketPlace
                                                                        ,'TransactionKind' IS t_DealKind
                                                                        ,'Instrument' IS t_Instrument
                                                                        ,'TransactionVolume' IS to_char(t_Volume, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'BidPrice' IS to_char(t_ReqPrice, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'TransactionPrice' IS to_char(t_Price, 'FM99999999999999999990D0000','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'TransactionCurrency' IS t_BaseCurr 
                                                                        ,'PaymentCurrency' IS t_ContrCurr 
                                                                        ,'TransactionSum' IS to_char(t_BaseSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'PaymentSum' IS to_char(t_ContrSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'PaymentRubSum' IS to_char(t_ContrSumRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'BrokerCommissionSum' IS to_char(t_BrokerComissContr, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'BrokerCommissionRubSum' IS to_char(t_BrokerComissRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'ExchangeFee' IS to_char(t_MarketComiss, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                        ,'Number' IS t_CodeTS
                                                                        ,'TransactionType' IS t_DealType 
                                                                        ,'Counterparty' IS t_Contractor 
                                                                        ,'TransactionCurrencyCommitment' IS to_char(t_LiabilityBase, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'PaymentCurrencyCommitment' IS to_char(t_LiabilityContr, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''') 
                                                                        ,'Note' IS case t_Note when chr(1) then null else t_Note end
                                                                        ABSENT ON NULL RETURNING CLOB)
                                                             ORDER BY t_DealID
                                                             RETURNING CLOB)
                                          FROM dbrkrepcurdeal_tmp 
                                         WHERE t_IsItog = CHR(0) 
                                           AND t_ServKindSub <> 9
                                           AND t_Part = p_Mode
                                       )
                       RETURNING CLOB),  
                       v_NodeName||'TotalList' IS
             JSON_OBJECT(v_NodeName||'Total' IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('TotalName' IS t_ConcDate
                                                                                 ,'PaymentRubSum' IS to_char(t_ContrSumRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'BrokerCommissionRubSum' IS to_char(t_BrokerComissRub, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ,'ExchangeFee' IS to_char(t_MarketComiss, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                 ABSENT ON NULL RETURNING CLOB)
                                                                      RETURNING CLOB)                                 
                                                   FROM dbrkrepcurdeal_tmp 
                                                  WHERE t_IsItog = 'X' 
                                                    AND t_ServKindSub <> 9
                                                    AND t_Part = p_Mode
                                                )
                       RETURNING CLOB)
                     RETURNING CLOB)
      INTO v_ForeignExchangeMarketTransactionsBlock
      FROM dual;

    RETURN v_ForeignExchangeMarketTransactionsBlock;
  END GetForeignExchangeMarketTransactionsBlock;


  FUNCTION GetPartyID_ByCode(p_code in varchar2, p_codekind in number) RETURN number
  IS
    v_partyid number(10);
  BEGIN
    select t_objectid
      into v_partyid
      from dobjcode_dbt
     where t_objecttype = PM_COMMON.OBJTYPE_PARTY
       and t_codekind = p_codekind
       and t_state = 0
       and t_code = p_code;
       
    return v_partyid;
  EXCEPTION 
    when no_data_found then return 0;
  END GetPartyID_ByCode;


  FUNCTION GetTradingResultsPortfolioBlock(p_Mode IN NUMBER, p_BegDate IN DATE, p_EndDate IN DATE, p_DlContrID IN NUMBER, p_marketid IN NUMBER, pNumStr in NUMBER)
    RETURN CLOB
  IS
    v_GetTradingResultsPortfolioBlock CLOB;
    v_json_TradingResultsPortfolio CLOB;
    
    v_micex_id number(10);
    v_spb_id number(10);

    v_depo_owner varchar2(2000);
    v_depo_owner_section varchar2(2000);

    v_title varchar2(2000);
    v_prefix varchar2(3);
  BEGIN

    v_micex_id := GetPartyID_ByCode(RSB_Common.GetRegStrValue('SECUR\MICEX_CODE'), PM_COMMON.PTCK_CLIENT);
    v_spb_id := GetPartyID_ByCode(RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE'), PM_COMMON.PTCK_CLIENT);

  
    v_depo_owner := pm_common.GetNoteTextStr(p_DlContrID, IT_BrokerReport.OBJTYPE_BROKERCONTR_DL, IT_BrokerReport.NOTEKIND_DEPO_OWNER, p_EndDate);

	if p_marketid = v_micex_id then -- ММВБ
		v_depo_owner_section := pm_common.GetNoteTextStr(p_DlContrID, IT_BrokerReport.OBJTYPE_BROKERCONTR_DL, IT_BrokerReport.NOTEKIND_DEPO_OWNER_SECTION_MMVB, p_EndDate);
 	elsif p_marketid = v_spb_id then -- СПБ
		v_depo_owner_section := pm_common.GetNoteTextStr(p_DlContrID, IT_BrokerReport.OBJTYPE_BROKERCONTR_DL, IT_BrokerReport.NOTEKIND_DEPO_OWNER_SECTION_SPB, p_EndDate);
	else
		v_depo_owner_section := chr(0);
	end if;
    
    if v_depo_owner = chr(0) then 
      v_depo_owner := '';
    else 
      if v_depo_owner_section <> chr(0) then
        v_depo_owner := v_depo_owner||'/'||v_depo_owner_section;
      end if;
    end if;
  
    if pNumStr = 3 then 
      v_title := '3.5';
      v_prefix := '';
    else 
      v_title := '6.3';
      v_prefix := 'SPB';
    end if;

    SELECT JSON_ARRAYAGG(JSON_OBJECT('DepositAccountSectionNumber' IS v_depo_owner
                                    ,v_prefix||'PortfolioInstrumentList' IS
                                      JSON_OBJECT(v_prefix||'PortfolioInstrument' IS
                                        (SELECT JSON_ARRAYAGG(JSON_OBJECT( 'IdentificationNumber' IS T_A53 
                                                                  ,'SecuritiesIssue' IS T_A54
                                                                  ,'CurrentParameters' IS GetCurrentParamFIID(T_FIID, p_EndDate)
                                                                  ,'IncomingBalance' IS to_char(T_A55, GetFormatSample(T_A55),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ,'SecuritiesCreditedQuantity' IS to_char(T_A56, GetFormatSample(T_A56),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ,'SecuritiesDeductedQuantity' IS to_char(T_A57, GetFormatSample(T_A57),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ,'OutgoingBalance' IS to_char(T_A58, GetFormatSample(T_A58),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ,'PlannedOutgoingBalance' IS to_char(T_A58_1, GetFormatSample(T_A58_1),'NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ,'SecurityQuotation' IS to_char(T_A59, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ,'SecurityQuotationCurrency' IS (select f.t_ccy from dfininstr_dbt f where f.t_FIID = inacc.T_A59_1) 
                                                                  ,'SecurityPrice' IS to_char(T_A60, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ,'SecurityRubPrice' IS to_char(T_A61, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ,'AllocatedCouponValue' IS to_char(T_A62, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ,'AllocatedCouponRubValue' IS to_char(T_A63, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                ABSENT ON NULL RETURNING CLOB) )
                                           FROM dbrkrepinacc_tmp inacc
                                          WHERE t_IsItog = CHR(0) 
                                            AND t_ServKindSub <> 9
                                            AND t_marketid = p_marketid)
                                          RETURNING CLOB) 
                                    , v_prefix||'PortfolioInstrumentTotalList' IS
                                      JSON_OBJECT(v_prefix||'PortfolioInstrumentTotal' IS
                                        (SELECT JSON_ARRAYAGG(JSON_OBJECT('TotalName' IS 'Итого в RUB'
                                                                         ,'SecurityRubPrice' IS to_char(T_A61, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                         ,'AllocatedCouponRubValue' IS to_char(T_A63, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                  ABSENT ON NULL RETURNING CLOB) )
                                           FROM dbrkrepinacc_tmp 
                                          WHERE t_IsItog = 'X' 
                                            AND t_ServKindSub <> 9
                                            AND t_marketid = p_marketid)
                                          RETURNING CLOB) 
                                    RETURNING CLOB)
                         RETURNING CLOB)
      INTO v_json_TradingResultsPortfolio
      FROM dual
      GROUP BY 'PortfolioInstrument';
  
    v_title := v_title||' Состав портфеля по итогам торгов в период с '||to_char(p_BegDate,'DD.MM.YYYY')||' по '||to_char(p_EndDate,'DD.MM.YYYY');
    
    SELECT JSON_OBJECT(v_prefix||'TradingResultsPortfolioTitle' IS v_title,
                       v_prefix||'TradingResultsPortfolioList' IS JSON_OBJECT( v_prefix||'TradingResultsPortfolio' IS v_json_TradingResultsPortfolio FORMAT JSON
                                                                      RETURNING CLOB)
                     RETURNING CLOB)
      INTO v_GetTradingResultsPortfolioBlock
      FROM dual;

    RETURN v_GetTradingResultsPortfolioBlock;
  END GetTradingResultsPortfolioBlock;

  --Получение JSON по блоку "Оценка денежной позиции"
  FUNCTION GetCashPositionValuationList_UL(p_IsOtc IN NUMBER)
    RETURN CLOB
  IS
    v_CashPositionValuationList CLOB;
    v_NodeName VARCHAR2(50) := (CASE WHEN p_IsOtc = 0 THEN '' ELSE 'Otc' END)||'CashPositionValuation';
  BEGIN
    SELECT JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('CurrencyName' IS 'Итого в '||t_CCY
                                                                      ,'StartDateOpeningBalance' IS to_char(sum(t_InRest), 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                      ,'EndDateClosingBalance' IS to_char(sum(t_OutRest), 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                      ,'EndDatePlannedBalance' IS to_char(sum(t_PlanRest), 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                       ABSENT ON NULL)
                                                           ORDER BY t_Currency
                                                           RETURNING CLOB
                                                          )
                                        FROM dbrkrepaccitog_tmp
                                       WHERE t_IsItog = CHR(0)
                                         AND ((p_IsOtc = 0 AND t_ServKindSub <> 9) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                                       group by t_Currency, t_CCY
                                     )
                       RETURNING CLOB)        
      INTO v_CashPositionValuationList
      FROM dual;

    RETURN v_CashPositionValuationList;
  END GetCashPositionValuationList_UL;

  --Получение JSON по блоку "Движения по счету"
  FUNCTION GetAccountMovementsList_UL(p_IsOtc IN NUMBER)
    RETURN CLOB
  IS
    v_AccountMovementsList CLOB;
    v_ParentNodeName VARCHAR2(50) := (CASE WHEN p_IsOtc = 0 THEN '' ELSE 'Otc' END)||'AccountMovements';
    v_NodeName VARCHAR2(50) := (CASE WHEN p_IsOtc = 0 THEN '' ELSE 'Otc' END)||'CashFlow';
    v_Cnt NUMBER;
  BEGIN
    SELECT COUNT(1) INTO v_Cnt
      FROM dbrkrepcashegroupmoving_tmp
     WHERE ((p_IsOtc = 0 AND t_ServKindSub = 8) OR (p_IsOtc <> 0 AND t_ServKindSub = 9));

    IF v_Cnt > 0 THEN
      WITH q1 AS (SELECT DISTINCT t_Currency
                    FROM dbrkrepcashegroupmoving_tmp
                   WHERE ((p_IsOtc = 0 AND t_ServKindSub = 8) OR (p_IsOtc <> 0 AND t_ServKindSub = 9))
                 )
      SELECT JSON_OBJECT(v_ParentNodeName IS 
                          (SELECT JSON_ARRAYAGG(JSON_OBJECT('CurrencyName' IS DECODE(q1.t_Currency, PM_COMMON.NATCUR, 'Рубли', fin.t_Name)||' ('||fin.t_CCY||')',
                                                            v_NodeName||'List' IS 
                                                              JSON_OBJECT(v_NodeName IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('Date' IS t.t_PrintDate
                                                                                                                         ,'OperationName' IS t.t_OperName
                                                                                                                         ,'Marketplace' IS t.t_MarketName
                                                                                                                         ,'CreditSum' IS DECODE(t.t_InSum, 0, NULL, to_char(t.t_InSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.'''))
                                                                                                                         ,'WithdrawalsSum' IS DECODE(t.t_OutSum, 0, NULL, to_char(t.t_OutSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.'''))
                                                                                                                         ,'PaymentsBalance' IS NULL
                                                                                                                         ABSENT ON NULL)
                                                                                                              RETURNING CLOB)                                 
                                                                                           FROM dbrkrepcashegroupmoving_tmp t
                                                                                          WHERE t.t_IsItog = CHR(0) 
                                                                                            AND ((p_IsOtc = 0 AND t.t_ServKindSub = 8) OR (p_IsOtc <> 0 AND t.t_ServKindSub = 9))
                                                                                            AND t.t_Currency = q1.t_Currency
                                                                                        )
                                                                        RETURNING CLOB),  
                                                            v_NodeName||'TotalList' IS
                                                              JSON_OBJECT(v_NodeName||'Total' IS (SELECT JSON_ARRAYAGG(JSON_OBJECT('CashFlowTotalName' IS 'Итого в '||fin.t_CCY
                                                                                                                                  ,'CreditSum' IS DECODE(t1.t_InSum, 0, NULL, to_char(t1.t_InSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.'''))
                                                                                                                                  ,'WithdrawalsSum' IS DECODE(t1.t_OutSum, 0, NULL, to_char(t1.t_OutSum, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.'''))
                                                                                                                                  ,'PaymentsBalance' IS to_char(t1.t_Rest, 'FM99999999999999999990D00','NLS_NUMERIC_CHARACTERS = '',.''')
                                                                                                                                  ABSENT ON NULL)
                                                                                                                       RETURNING CLOB)
                                                                                                    FROM  
                                                                                                    (Select sum(t.t_InSum) t_InSum, sum(t.t_OutSum) t_OutSum, sum(t.t_rest) t_rest
                                                                                                       from dbrkrepcashegroupmoving_tmp t
                                                                                                      WHERE t.t_IsItog = 'X' 
                                                                                                        AND ((p_IsOtc = 0 AND t.t_ServKindSub = 8) OR (p_IsOtc <> 0 AND t.t_ServKindSub = 9))
                                                                                                        AND t.t_Currency = q1.t_Currency
                                                                                                     ) t1
                                                                                                 )
                                                                          RETURNING CLOB)
                                                            RETURNING CLOB)
                                               RETURNING CLOB)                                 
                             FROM q1, dfininstr_dbt fin
                            WHERE fin.t_FIID = q1.t_Currency
                          )
                         RETURNING CLOB)
        INTO v_AccountMovementsList
        FROM dual;
    ELSE
      SELECT JSON_OBJECT(v_ParentNodeName IS 
                          (SELECT JSON_ARRAYAGG(JSON_OBJECT('CurrencyName' IS 'Рубли'||' ('||fin.t_CCY||')',
                                                            v_NodeName||'List' IS 
                                                              JSON_OBJECT(v_NodeName IS NULL),  
                                                            v_NodeName||'TotalList' IS
                                                              JSON_OBJECT(v_NodeName||'Total' IS NULL)
                                                           )
                                               )                                 
                             FROM dfininstr_dbt fin
                            WHERE fin.t_FIID = PM_COMMON.NATCUR
                          )
                        RETURNING CLOB)
        INTO v_AccountMovementsList
        FROM dual;
    END IF;

    RETURN v_AccountMovementsList;
  END GetAccountMovementsList_UL;


  /**
   @brief Проверка наличия данных по остаткам ДС на внебирже
   @return 1 - данные есть, 0 - данных нет 
  */
  FUNCTION ExistsOtcDataForAccMoneyMoving
    RETURN NUMBER
  IS
    v_ret NUMBER;
  BEGIN
    SELECT 1 INTO v_ret
      FROM dbrkrepaccitog_tmp 
     WHERE t_IsItog = CHR(0) 
       AND t_ServKindSub = 9
       AND rownum = 1;
    RETURN v_ret;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END ExistsOtcDataForAccMoneyMoving;

  /**
   @brief Проверка наличия данных по оценке позиций по ц/б на внебирже
   @return 1 - данные есть, 0 - данных нет 
  */
  FUNCTION ExistsOtcDataForActiveAvoiriss
    RETURN NUMBER
  IS
    v_ret NUMBER;
  BEGIN
    SELECT 1 INTO v_ret
      FROM dbrkrepactiveavoir_tmp 
     WHERE t_IsItog = CHR(0) 
       AND t_ServKindSub = 9
       AND rownum = 1;
    RETURN v_ret;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN 0;
  END ExistsOtcDataForActiveAvoiriss;
  
  --Регистрация события для отправки на почту и в телеграмм сопровождения
  PROCEDURE RegisterBrkRepError(p_ErrorText IN VARCHAR2, p_ID IN NUMBER DEFAULT NULL)
  IS
  BEGIN 
    it_event.RegisterError(p_SystemId => 'SINV'
                    ,p_ServiceName => 'GetBrokerageReportInfo'
                    ,p_ErrorCode => -1
                    ,p_ErrorDesc => 'Сервис GetBrokerageReportInfoReq: '||p_ErrorText
                    ,p_LevelInfo => 8);
    
    IF p_ID IS NOT NULL THEN
      --Сохраним ошибку в БД
      UPDATE DUSERBROKREPREQ_DBT 
         SET t_ErrorText = p_ErrorText
       WHERE t_ID = p_ID;
      COMMIT; 
    END IF;

  END RegisterBrkRepError;

  --Обработчик SINV.GetBrokerageReportInfo
  PROCEDURE GetReportFromSINV(p_worklogid integer     
                             ,p_messbody  clob        
                             ,p_messmeta  xmltype     
                             ,o_msgid     out varchar2
                             ,o_MSGCode   out integer 
                             ,o_MSGText   out varchar2
                             ,o_messbody  out clob    
                             ,o_messmeta  out xmltype)
  IS
    v_userbrokrepreq DUSERBROKREPREQ_DBT%ROWTYPE;
    v_check          NUMBER;
    v_SignerParty    dparty_dbt.t_PartyID%TYPE;
    v_ContrDate      DATE;
    v_out_header     clob;
    v_ServiceName constant itt_q_message_log.servicename%type := 'IPS_DFACTORY.GetBrokerageReport';
    v_RequestTimeStr VARCHAR2(30);
  BEGIN 
    BEGIN
      SELECT 
      /*T_ID*/          0,
      /*T_QMESSLOGID*/  p_worklogid, 
      /*T_GUIDREQ*/     GUID, 
      /*T_STARTDATE*/   TO_DATE(StartDate, 'DD.MM.YYYY'), 
      /*T_ENDDATE*/     TO_DATE(EndDate, 'DD.MM.YYYY'),
      /*T_REQREGDATE*/  CAST(it_xml.char_to_timestamp_iso8601(RequestTime) AS DATE),
      /*T_CLIENTCODE*/  ClientCode,
      /*T_CONTRNUMBER*/ ContrNumber,  
      /*T_PARTYID*/     -1,  
      /*T_DLCONTRID*/   -1,
      /*T_REQSYSDATE*/  SYSDATE,    
      /*T_JSONSYSDATE*/ TO_DATE('01.01.0001 00:00:00', 'DD.MM.YYYY HH24:MI:SS'),
      /*T_REPNAME*/     CHR(1),
      /*T_FACSIMILE*/   CHR(1),
      /*T_ERRORTEXT*/   CHR(1)   
        INTO v_userbrokrepreq
        FROM JSON_TABLE (
                 p_messbody,
                 '$.*'
                 COLUMNS GUID VARCHAR2(100) PATH '$.GUID',
                   RequestTime VARCHAR2(30) PATH '$.RequestTime', 
                   ContrNumber VARCHAR2(64) PATH '$.BrokerageContractNumber',
                   StartDate VARCHAR2(15) PATH '$.ReportStartDate',
                   EndDate VARCHAR2(15) PATH '$.ReportEndDate',
                   ClientCode VARCHAR2(100) PATH '$.ClientCode.ObjectId');
    EXCEPTION 
      WHEN OTHERS THEN
        v_userbrokrepreq.t_ErrorText := SUBSTR('Непредвиденная ошибка при разборе входящего JSON-сообщения: '||sqlerrm, 1, 300);
        RegisterBrkRepError(v_userbrokrepreq.t_ErrorText);
        RAISE_APPLICATION_ERROR(-20001, v_userbrokrepreq.t_ErrorText);
    END;

    --Вставка данных в таблицу экземпляров запроса выпуска отчета брокера
    INSERT INTO DUSERBROKREPREQ_DBT 
         VALUES v_userbrokrepreq 
      RETURNING t_ID INTO v_userbrokrepreq.t_ID;
    COMMIT; 

    --Проверка наличия клиента в СОФР
    v_userbrokrepreq.t_PartyID := GetPartyIDByCFT(v_userbrokrepreq.t_ClientCode);
    IF v_userbrokrepreq.t_PartyID = -1 THEN
      v_userbrokrepreq.t_ErrorText := 'Не найден клиент в СОФР по коду ClientCode = '||v_userbrokrepreq.t_ClientCode;
      RegisterBrkRepError(v_userbrokrepreq.t_ErrorText, v_userbrokrepreq.t_ID);
      RAISE_APPLICATION_ERROR(-20001, v_userbrokrepreq.t_ErrorText);
    ELSE
      UPDATE DUSERBROKREPREQ_DBT 
        SET t_PartyID = v_userbrokrepreq.t_PartyID
      WHERE t_ID = v_userbrokrepreq.t_ID; 
      COMMIT; 
 
      --Проверка наличия договора у клиента в СОФР
      v_userbrokrepreq.t_DlContrID := GetDlContrIDByNumber(v_userbrokrepreq.t_ContrNumber, v_userbrokrepreq.t_PartyID, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate);
      IF v_userbrokrepreq.t_DlContrID = -1 THEN
        v_userbrokrepreq.t_ErrorText := 'Не удалось определить действующий договор по клиенту в СОФР с кодом клиента ClientCode = '||v_userbrokrepreq.t_PartyID||' и номером договора BrokerageContractNumber = '||v_userbrokrepreq.t_ContrNumber;
        RegisterBrkRepError(v_userbrokrepreq.t_ErrorText, v_userbrokrepreq.t_ID);
        RAISE_APPLICATION_ERROR(-20001, v_userbrokrepreq.t_ErrorText);      
      ELSE
        UPDATE DUSERBROKREPREQ_DBT 
           SET t_DlContrID = v_userbrokrepreq.t_DlContrID
         WHERE t_ID = v_userbrokrepreq.t_ID;
        COMMIT;  

        --Проверка наличия признака ЕДП
        v_check := CheckEDP(v_userbrokrepreq.t_DlContrID);
        IF v_check = 0 THEN
          v_userbrokrepreq.t_ErrorText := 'ДБО с номером договора BrokerageContractNumber = '||v_userbrokrepreq.t_ContrNumber||' не является договором ЕДП. Договора не ЕДП данным сервисом не обрабатываются';
          RegisterBrkRepError(v_userbrokrepreq.t_ErrorText, v_userbrokrepreq.t_ID);
          RAISE_APPLICATION_ERROR(-20001, v_userbrokrepreq.t_ErrorText);
        END IF;
      END IF;
    END IF;

    v_SignerParty := GetDefaultSigner(v_userbrokrepreq.t_ErrorText);
    IF v_SignerParty = -1 THEN
      RegisterBrkRepError(v_userbrokrepreq.t_ErrorText, v_userbrokrepreq.t_ID);
      RAISE_APPLICATION_ERROR(-20001, v_userbrokrepreq.t_ErrorText);
    ELSE
      v_userbrokrepreq.t_Facsimile := pm_common.GetNoteTextStr(v_SignerParty, Rsb_Secur.OBJTYPE_PARTY, 109, TRUNC(SYSDATE));
      IF v_userbrokrepreq.t_Facsimile IS NULL OR v_userbrokrepreq.t_Facsimile = CHR(0) OR v_userbrokrepreq.t_Facsimile = CHR(1) THEN
        v_userbrokrepreq.t_ErrorText := 'Не удалось получить почтовый логин сотрудника (ID субъекта = '||v_SignerParty||') из соответствующего примечания';
        RegisterBrkRepError(v_userbrokrepreq.t_ErrorText, v_userbrokrepreq.t_ID);
        RAISE_APPLICATION_ERROR(-20001, v_userbrokrepreq.t_ErrorText);
      ELSE
        v_userbrokrepreq.t_Facsimile := SUBSTR(v_userbrokrepreq.t_Facsimile, 1, INSTR(v_userbrokrepreq.t_Facsimile, '@') - 1); 
      END IF;
    END IF;
    RsbSessionData.SetOurBank(1);

    --Подготовка отчетных данных
    BEGIN
      CreateAllData(v_userbrokrepreq.t_DlContrID, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate, 1, 1, 1); 
    EXCEPTION
      WHEN OTHERS THEN
        v_userbrokrepreq.t_ErrorText := SUBSTR('Непредвиденная ошибка при подготовке отчетных данных: '||sqlerrm, 1, 300);
        RegisterBrkRepError(v_userbrokrepreq.t_ErrorText, v_userbrokrepreq.t_ID);
        RAISE_APPLICATION_ERROR(-20001, v_userbrokrepreq.t_ErrorText);
    END;

    v_ContrDate := GetContrDate(v_userbrokrepreq.t_DlContrID);

    o_msgid := it_q_message.get_sys_guid;
    v_RequestTimeStr := it_xml.timestamp_to_char_iso8601(SYSDATE);

    BEGIN
      SELECT JSON_OBJECT('BrokerageContractNumber' IS '№'||v_userbrokrepreq.t_ContrNumber||' от '||TO_CHAR(v_ContrDate, 'DD.MM.YYYY')
                        ,'ExecutorName' IS RSB_BRKREP_RSHB_NEW.GetSignerName(v_SignerParty)
                        ,'ClientFullName' IS GetClientName(v_userbrokrepreq.t_PartyID)
                        ,'BrokerageReportName' IS 'Отчет брокера за период с '||TO_CHAR(v_userbrokrepreq.t_StartDate, 'DD.MM.YYYY')||' по '||TO_CHAR(v_userbrokrepreq.t_EndDate, 'DD.MM.YYYY')||', дата формирования '||TO_CHAR(SYSDATE, 'DD.MM.YYYY')
                        ,'GUID' value o_msgid
                        ,'RequestTime' value v_RequestTimeStr
                        ,'GUIDReq' value v_userbrokrepreq.t_GUIDReq
                        ,'ClientCode' IS JSON_OBJECT('ObjectId' IS GetClientCode(v_userbrokrepreq.t_DlContrID))
                        ,'StockPortfolio' IS JSON_OBJECT('PortfolioAssessment' IS JSON_OBJECT('SummaryInformationBlock' IS GetSummaryInformationBlock(0) FORMAT JSON,
                                                                                              'RateExchangeList' IS GetRateExchangeList(0) FORMAT JSON,
                                                                                              'CashPositionValuationList' IS GetCashPositionValuationList(0) FORMAT JSON,
                                                                                              'CommitmentBlock' IS GetCommitmentBlock(v_userbrokrepreq.t_EndDate) FORMAT JSON, 
                                                                                              'SecuritiesValuationBlock' IS GetSecuritiesValuationBlock(0) FORMAT JSON,
                                                                                              'DerivativeFinancialInstrumentBlock' IS GetDerivativeFinancialInstrumentBlock() FORMAT JSON
                                                                                              RETURNING CLOB), 
                                                         'AccountMovementsList' IS GetAccountMovementsList(0) FORMAT JSON,
                                                         'StockMarketTransactionsInfo' IS JSON_OBJECT('StockMarketTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(0, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                      'StockMarketREPOTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(0, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_REPOINPERIOD, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                      'EarlierStockMarketTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(0, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                      'EarlierStockMarketREPOTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(0, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECREPO, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON
                                                                                                      RETURNING CLOB),
                                                         'DerivativesMarketTransactionsInfo' IS JSON_OBJECT('DerivativesMarketTransactionsBlock' IS GetDerivativesMarketTransactionsBlock(RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                            'EarlierDerivativesMarketTransactionsBlock' IS GetDerivativesMarketTransactionsBlock(RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON
                                                                                                            RETURNING CLOB),
                                                         'ForeignExchangeMarketTransactionsInfo' IS JSON_OBJECT('ForeignExchangeMarketTransactionsBlock' IS GetForeignExchangeMarketTransactionsBlock(RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                                'EarlierForeignExchangeMarketTransactionsBlock' IS GetForeignExchangeMarketTransactionsBlock(RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON
                                                                                                                RETURNING CLOB) 
                                                         RETURNING CLOB)
                        RETURNING CLOB) 
       INTO o_messbody
       FROM dual;

      --Не выводим страницу с внебиржевым отчетом, если у клиента нет внебиржевых активов и нет движений по внебиржевым счетам
      IF ExistsOtcDataForAccMoneyMoving() = 1 OR ExistsOtcDataForActiveAvoiriss() = 1 THEN 
        WITH q1 AS (SELECT JSON_OBJECT('OTCPortfolio' IS JSON_OBJECT('Subaccount' IS 'Внебиржа',
                                                                     'PortfolioAssessment' IS JSON_OBJECT('OtcSummaryInformationBlock' IS GetSummaryInformationBlock(1) FORMAT JSON,
                                                                                                          'OtcRateExchangeList' IS GetRateExchangeList(1) FORMAT JSON,
                                                                                                          'OtcCashPositionValuationList' IS GetCashPositionValuationList(1) FORMAT JSON,
                                                                                                          'OtcSecuritiesValuationBlock' IS GetSecuritiesValuationBlock(1) FORMAT JSON
                                                                                                          RETURNING CLOB),
                                                                     'OtcAccountMovementsList' IS GetAccountMovementsList(1) FORMAT JSON,
                                                                     'OtcTransactionsInfo' IS JSON_OBJECT('OtcTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(1, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                          'OtcREPOTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(1, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_REPOINPERIOD, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                          'EarlierOtcTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(1, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                          'EarlierOtcREPOTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(1, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECREPO, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                          'CancelledOtcTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(1, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_CANCELDEAL, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON,
                                                                                                          'CancelledOtcREPOTransactionsBlock' IS GetStockMarketOrOtcTransactionsBlock(1, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_CANCELREPO, v_userbrokrepreq.t_StartDate, v_userbrokrepreq.t_EndDate) FORMAT JSON
                                                                                                          RETURNING CLOB)                                                                                                          
                                                                     RETURNING CLOB)
                                       RETURNING CLOB) json
                      FROM dual
                   )
        SELECT SUBSTR(o_messbody, 1, length(o_messbody)-1)||','||substr(q1.json, 2)
          INTO o_messbody
          FROM q1;
      END IF;

      o_messbody := REPLACE(o_messbody, '\u0001', '');

    EXCEPTION
      WHEN OTHERS THEN
        v_userbrokrepreq.t_ErrorText := SUBSTR('Непредвиденная ошибка при формировании исходящего JSON-сообщения: '||sqlerrm, 1, 300);
        RegisterBrkRepError(v_userbrokrepreq.t_ErrorText, v_userbrokrepreq.t_ID);
        RAISE_APPLICATION_ERROR(-20001, v_userbrokrepreq.t_ErrorText);
    END;

    v_userbrokrepreq.t_RepName := replace(v_userbrokrepreq.t_ContrNumber,'/','-')||'_'||TO_CHAR(v_userbrokrepreq.t_StartDate, 'DDMMYYYY')||'_'||TO_CHAR(v_userbrokrepreq.t_EndDate, 'DDMMYYYY');

    UPDATE DUSERBROKREPREQ_DBT 
       SET t_RepName = v_userbrokrepreq.t_RepName,
           t_Facsimile = v_userbrokrepreq.t_Facsimile,
           t_JsonSysDate = SYSDATE
     WHERE t_ID = v_userbrokrepreq.t_ID;

    --ФОРМИРУЕМ HEADER
    --Формируем JSON
    WITH q1 AS (SELECT JSON_OBJECTAGG (key t_Key value t_Value) json
                 FROM (SELECT t_Name AS t_Key, t_Note AS t_Value
                         FROM dllvalues_dbt
                        WHERE t_List = OBJTYPE_BRKREP_HEADERS
                        UNION ALL
                       SELECT 'x-trace-id' AS t_Key, v_userbrokrepreq.t_GUIDReq AS t_Value 
                         FROM dual
                        UNION ALL
                       SELECT 'x-request-id' AS t_Key, o_msgid AS t_Value 
                         FROM dual
                        UNION ALL
                       SELECT 'x-request-time' AS t_Key, v_RequestTimeStr AS t_Value 
                         FROM dual
                        UNION ALL
                       SELECT 'x-output-file-name' AS t_Key, v_userbrokrepreq.t_RepName AS t_Value 
                         FROM dual
                      )
               ),
         q2 as (SELECT JSON_OBJECT('x-template-params' IS JSON_ARRAYAGG( JSON_OBJECT('paramName'  IS 'df_facsimile' || decode(level,1,'',2,'1'),
                                                                                     'paramType'  IS 'FILE',
                                                                                     'paramValue' IS v_userbrokrepreq.t_Facsimile||'.png')
                                                                       )
                                  ) json 
                  FROM dual
               CONNECT BY LEVEL <= 2   
               )
    SELECT SUBSTR(q1.json, 1, length(q1.json)-1)||','||substr(q2.json, 2) --автоматически JSON_OBJECTAGG и JSON_ARRAYAGG на одном уровне не дружат
      INTO v_out_header
      FROM q1, q2;

    o_messmeta := it_kafka.add_Header_Xmessmeta(v_out_header); --Завернем JSON в XML

    it_kafka.load_msg(io_msgid => o_msgid
                    ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                    ,p_ServiceName => v_ServiceName
                    ,p_Receiver => it_ips_dfactory.C_C_SYSTEM_NAME
                    ,p_MESSBODY => o_messbody
                    ,o_ErrorCode => o_MSGCode
                    ,o_ErrorDesc => o_MSGText
                    ,p_CORRmsgid => v_userbrokrepreq.t_GUIDReq
                    ,p_MessMETA => o_messmeta);

  END GetReportFromSINV;
  

  
  

  /**
   @brief Cбор и расчет данных для Отчета Брокера для ДБО ЮЛ
   @param[in]  p_buf_rec запись таблицы duserlebrokrepreq_dbt
   @param[in]  p_isOTC 1 - внебиржа, 0 - биржа
   @param[out] o_json_resp сформированный json с данными отчета
   @return текст ошибки/ОК
  */
  FUNCTION BrokerReportRun(p_buf_rec in duserlebrokrepreq_dbt%rowtype,
                           p_isOTC in number, 
                           o_json_resp out CLOB) RETURN varchar2
  IS
    v_micex_id number(10);
    v_spb_id number(10);
  BEGIN
   
    v_micex_id := GetPartyID_ByCode(RSB_Common.GetRegStrValue('SECUR\MICEX_CODE'), PM_COMMON.PTCK_CLIENT);
    v_spb_id := GetPartyID_ByCode(RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE'), PM_COMMON.PTCK_CLIENT);
       
    -- Формирование json
    IF p_isOTC = 0 THEN 
      SELECT JSON_OBJECT('PortfolioAssessment' IS JSON_OBJECT('SummaryInformationBlock' IS GetSummaryInformationBlock(p_isOTC) FORMAT JSON,
                                                  'RateExchangeList' IS GetRateExchangeList(p_isOTC) FORMAT JSON,
                                                  'CashPositionValuationBlock' 
                                                      IS JSON_OBJECT('ExchangePlatformList' IS JSON_OBJECT('ExchangePlatform' IS GetExchangePlatform(p_isOTC) FORMAT JSON),
                                                                     'CashPositionValuationTotalList' IS GetCashPositionValuationList_UL(p_isOTC) FORMAT JSON 
                                                                     RETURNING CLOB) ,
                                                  'CommitmentBlock' IS GetCommitmentBlock(p_buf_rec.t_EndDate) FORMAT JSON, 
                                                  'SecuritiesValuationBlock' IS GetSecuritiesValuationBlock(p_isOTC) FORMAT JSON,
                                                  'DerivativeFinancialInstrumentBlock' IS GetDerivativeFinancialInstrumentBlock() FORMAT JSON
                                                  RETURNING CLOB), 
                         'AccountMovementsList' IS GetAccountMovementsList_UL(0) FORMAT JSON,
                         'StockMarketTransactionsInfo' 
                            IS JSON_OBJECT('StockMarketTransactionsBlock'
                                              IS GetStockMarketOrOtcTransactionsBlock(p_isOTC, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate, v_micex_id) FORMAT JSON,
                                           'StockMarketREPOTransactionsBlock'
                                              IS GetStockMarketOrOtcTransactionsBlock(p_isOTC, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_REPOINPERIOD, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate, v_micex_id) FORMAT JSON,
                                           'EarlierStockMarketTransactionsBlock'
                                              IS GetStockMarketOrOtcTransactionsBlock(p_isOTC, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate, v_micex_id) FORMAT JSON,
                                           'EarlierStockMarketREPOTransactionsBlock'
                                              IS GetStockMarketOrOtcTransactionsBlock(p_isOTC, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECREPO, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate, v_micex_id) FORMAT JSON,
                                           'TradingResultsPortfolioBlock' 
                                              IS GetTradingResultsPortfolioBlock(p_isOTC, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate, p_buf_rec.t_dlcontrid, v_micex_id, 3) FORMAT JSON 
                                            RETURNING CLOB),
                         'DerivativesMarketTransactionsInfo' IS JSON_OBJECT('DerivativesMarketTransactionsBlock' 
                                                                              IS GetDerivativesMarketTransactionsBlock(RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON,
                                                                            'EarlierDerivativesMarketTransactionsBlock' 
                                                                              IS GetDerivativesMarketTransactionsBlock(RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON
                                                                            RETURNING CLOB),
                         'ForeignExchangeMarketTransactionsInfo' IS JSON_OBJECT('ForeignExchangeMarketTransactionsBlock' 
                                                                                  IS GetForeignExchangeMarketTransactionsBlock(RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON,
                                                                                'EarlierForeignExchangeMarketTransactionsBlock' 
                                                                                  IS GetForeignExchangeMarketTransactionsBlock(RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON
                                                                                RETURNING CLOB),
                         'SPBMarketTransactionsInfo' 
                            IS JSON_OBJECT('SPBMarketTransactionsBlock'
                                              IS GetStockMarketOrOtcTransactionsBlock(p_isOTC, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate, v_spb_id, 6) FORMAT JSON,
                                           'EarlierSPBMarketTransactionsBlock'
                                              IS GetStockMarketOrOtcTransactionsBlock(p_isOTC, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate, v_spb_id, 6) FORMAT JSON,
                                           'SPBTradingResultsPortfolioBlock'
                                              IS GetTradingResultsPortfolioBlock(p_isOTC, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate, p_buf_rec.t_dlcontrid, v_spb_id, 6) FORMAT JSON
                                            RETURNING CLOB)
                          RETURNING CLOB )
       INTO o_json_resp
       FROM dual;
    ELSE 
      --Не выводим страницу с внебиржевым отчетом, если у клиента нет внебиржевых активов и нет движений по внебиржевым счетам
      IF ExistsOtcDataForAccMoneyMoving() = 1 OR ExistsOtcDataForActiveAvoiriss() = 1 THEN 
        SELECT JSON_OBJECT('Subaccount' IS 'Внебиржа',
                           'PortfolioAssessment' 
                              IS JSON_OBJECT('OtcSummaryInformationBlock' IS GetSummaryInformationBlock(p_isOTC) FORMAT JSON,
                                             'OtcCashPositionValuationBlock' 
                                                      IS JSON_OBJECT('OtcCashPositionValuationList' IS GetExchangePlatform(p_isOTC) FORMAT JSON,
                                                                     'OtcCashPositionValuationTotalList' IS OtcCashPositionValuationTotalOTC FORMAT JSON 
                                                                     RETURNING CLOB) ,                                                                 
                                             'OtcRateExchangeList' IS GetRateExchangeList(p_isOTC) FORMAT JSON,
                                             'OtcSecuritiesValuationBlock' IS GetSecuritiesValuationBlock(p_isOTC) FORMAT JSON
                                  RETURNING CLOB),
                           'OtcAccountMovementsList' IS GetAccountMovementsList_UL(p_isOTC) FORMAT JSON,
                           'OtcTransactionsInfo' 
                              IS JSON_OBJECT('OtcTransactionsBlock' 
                                                IS GetStockMarketOrOtcTransactionsBlock(p_isOTC, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_DEALINPERIOD, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON,
                                             'OtcREPOTransactionsBlock' 
                                                IS GetStockMarketOrOtcTransactionsBlock(p_isOTC, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_REPOINPERIOD, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON,
                                              'EarlierOtcTransactionsBlock' 
                                                IS GetStockMarketOrOtcTransactionsBlock(p_isOTC, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECDEAL, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON,
                                              'EarlierOtcREPOTransactionsBlock' 
                                                IS GetStockMarketOrOtcTransactionsBlock(1, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_EXECREPO, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON,
                                              'CancelledOtcTransactionsBlock' 
                                                IS GetStockMarketOrOtcTransactionsBlock(1, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_CANCELDEAL, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON,
                                              'CancelledOtcREPOTransactionsBlock' 
                                                IS GetStockMarketOrOtcTransactionsBlock(1, RSB_BRKREP_RSHB_NEW.BROKERREP_PART_CANCELREPO, p_buf_rec.t_StartDate, p_buf_rec.t_EndDate) FORMAT JSON
                                    RETURNING CLOB)
                            RETURNING CLOB)
          INTO o_json_resp
          FROM dual;
      ELSE 
        o_json_resp := null;
      END IF;
    END IF;
      
    return 'OK';
  EXCEPTION
    WHEN OTHERS THEN
      return sqlerrm;
  END;
  
END IT_BrokerReport;
/
CREATE OR REPLACE PACKAGE BODY RSI_NPTX6CALC
IS

   FUNCTION GetBase(p_Kind IN NUMBER, p_Client IN NUMBER, p_BeginDate IN DATE, p_EndDate IN DATE) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN
      select nvl(sum(obj.t_Sum0), 0) as t_S INTO v_Sum
      from dnptxobj_dbt obj
      where obj.t_Kind = p_Kind
        and obj.t_Date between p_BeginDate and p_EndDate
        and obj.t_Client = p_Client
        and (
               ROUND(obj.t_Sum0, 2) >= 0.01
               or ROUND(obj.t_Sum0, 2) <= -0.01
            )
        and obj.t_FromOutSyst = CHR(0);
            
      RETURN v_Sum;
   END;

   FUNCTION GetBaseDepo(p_Kind IN NUMBER, p_Client IN NUMBER, p_BeginDate IN DATE, p_EndDate IN DATE) RETURN NUMBER
   IS
     v_Sum NUMBER := 0;
   BEGIN
      select nvl(sum(obj.t_Sum0), 0) as t_S INTO v_Sum
      from dnptxobj_dbt obj
      where obj.t_Kind = p_Kind
        and obj.t_Date between p_BeginDate and p_EndDate
        and obj.t_Client = p_Client
        and (
               ROUND(obj.t_Sum0, 2) >= 0.01
               or ROUND(obj.t_Sum0, 2) <= -0.01
            )
        and obj.t_FromOutSyst = 'X';
            
      RETURN v_Sum;
   END;
   
   FUNCTION GetLastWorkDayYear(pDate DATE) RETURN DATE
   IS
      IsWorkDay NUMBER := 0;
      LastWorkDay DATE;
   BEGIN
      LastWorkDay := to_date('31.12.' || to_char(pDate, 'yyyy'), 'dd.mm.yyyy');
      
      WHILE IsWorkDay = 0
      LOOP
         IsWorkDay := rsi_rsbcalendar.IsWorkDay(LastWorkDay);
         IF IsWorkDay = 0
         THEN
            LastWorkDay := LastWorkDay - 1;
         END IF;
      END LOOP;
      
      RETURN LastWorkDay;
   END;
   
   FUNCTION GetCloseDateIIS(p_Client NUMBER, p_BeginDate DATE, p_EndDate DATE) RETURN DATE
   IS
      v_CloseDate DATE := to_date('01.01.0001', 'dd.mm.yyyy');
   BEGIN
      select q.t_DateClose into v_CloseDate
      from (
              select qq.t_DateClose
              from (
                      --Определение даты первого договора ИИС для ДО
                      select nptxop.t_OperDate as t_DateClose
                      from dnptxop_dbt nptxop,
                           dnptxobdc_dbt obdc,
                           dnptxobj_dbt obj,
                           dsfcontr_dbt sfcontr
                      where (
                               p_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy')
                               or nptxop.t_OperDate >= p_BeginDate
                            )
                            and nptxop.t_OperDate <= p_EndDate
                            and nptxop.t_DocKind = RSB_SECUR.DL_CALCNDFL
                            and nptxop.t_SubKind_Operation = DL_TXBASECALC_OPTYPE_CLOSE_IIS
                            and nptxop.t_Client = p_Client
                            and nptxop.t_Status > 0
                            and obdc.t_DocID = nptxop.t_ID
                            and obj.t_ObjID = obdc.t_ObjID
                            and obj.t_AnaliticKind6 = RSI_NPTXC.TXOBJ_KIND6020
                            and rsi_npto.CheckObjIIS(obj.t_AnaliticKind6, obj.t_Analitic6) = 1
                            and sfcontr.t_ID = obj.t_Analitic6
                            and (
                                   p_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy')
                                   or sfcontr.t_DateClose >= p_BeginDate
                                )
                            and sfcontr.t_DateClose <= p_EndDate
                            and not exists(
                                             select 1
                                             from ddlcontrmp_dbt dlcontrmp
                                             where dlcontrmp.t_SfContrID = sfcontr.t_ID
                                          )
                      --Определение даты первого договора ИИС для ДБО
                      union
                      select nptxop.t_OperDate as t_DateClose
                      from ddlcontr_dbt dlcontr,
                           dsfcontr_dbt subcontr,
                           ddlcontrmp_dbt dlcontrmp,
                           dsfcontr_dbt sfcontr,
                           dnptxop_dbt nptxop,
                           dnptxobdc_dbt obdc,
                           dnptxobj_dbt obj
                      where (
                               p_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy')
                               or nptxop.t_OperDate >= p_BeginDate
                            )
                            and nptxop.t_OperDate <= p_EndDate
                            and nptxop.t_DocKind = RSB_SECUR.DL_CALCNDFL
                            and nptxop.t_SubKind_Operation = DL_TXBASECALC_OPTYPE_CLOSE_IIS
                            and nptxop.t_Client = p_Client
                            and obdc.t_DocID = nptxop.t_ID
                            and obj.t_ObjID = obdc.t_ObjID
                            and obj.t_AnaliticKind6 = RSI_NPTXC.TXOBJ_KIND6020
                            and rsi_npto.CheckObjIIS(obj.t_AnaliticKind6, obj.t_Analitic6) = 1
                            and subcontr.t_ID = obj.t_Analitic6
                            and (
                                   p_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy')
                                   or subcontr.t_DateClose >= p_BeginDate
                                )
                            and subcontr.t_DateClose <= p_EndDate
                            and dlcontrmp.t_SfContrID = subcontr.t_ID
                            and dlcontr.t_DlContrID = dlcontrmp.t_DlContrID
                            and sfcontr.t_ID = dlcontr.t_SfContrID
                   ) qq
              order by qq.t_DateClose desc
           ) q
      where ROWNUM = 1;
      
      return v_CloseDate;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN return to_date('01.01.0001', 'dd.mm.yyyy');
   END;
   
   FUNCTION GetFirstDateIIS(p_Client NUMBER, p_BeginDate DATE, p_EndDate DATE) RETURN DATE
   IS
      v_FirstDate DATE := p_BeginDate;
   BEGIN
      select q.t_FirstIISDate into v_FirstDate
      from (
              select (
                        case
                           when qq.t_FirstIISDate is not NULL
                           then qq.t_FirstIISDate
                           else qq.t_DateBegin
                        end
                     ) as t_FirstIISDate
              from (
                      --Определение даты первого договора ИИС для ДО
                      select RSI_NPTO.GetDateFromNoteText(RSB_SECUR.OBJTYPE_SFCONTR, sfcontr.t_ID, 3) as t_FirstIISDate,
                             sfcontr.t_DateBegin
                      from dnptxop_dbt nptxop,
                           dnptxobdc_dbt obdc,
                           dnptxobj_dbt obj,
                           dsfcontr_dbt sfcontr
                      where (
                               p_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy')
                               or nptxop.t_OperDate >= p_BeginDate
                            )
                            and nptxop.t_OperDate <= p_EndDate
                            and nptxop.t_DocKind = RSB_SECUR.DL_CALCNDFL
                            and nptxop.t_SubKind_Operation = DL_TXBASECALC_OPTYPE_CLOSE_IIS
                            and nptxop.t_Client = p_Client
                            and nptxop.t_Status > 0
                            and obdc.t_DocID = nptxop.t_ID
                            and obj.t_ObjID = obdc.t_ObjID
                            and obj.t_AnaliticKind6 = RSI_NPTXC.TXOBJ_KIND6020
                            and rsi_npto.CheckObjIIS(obj.t_AnaliticKind6, obj.t_Analitic6) = 1
                            and sfcontr.t_ID = obj.t_Analitic6
                            and (
                                   p_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy')
                                   or sfcontr.t_DateClose >= p_BeginDate
                                )
                            and sfcontr.t_DateClose <= p_EndDate
                            and not exists(
                                             select 1
                                             from ddlcontrmp_dbt dlcontrmp
                                             where dlcontrmp.t_SfContrID = sfcontr.t_ID
                                          )
                      --Определение даты первого договора ИИС для ДБО
                      union
                      select (
                                case
                                   when dlcontr.t_IISTransfer = chr(88) AND dlcontr.t_IISLastOpenDate > to_date('01.01.0001', 'dd.mm.yyyy')
                                   then dlcontr.t_IISLastOpenDate
                                   else sfcontr.t_DateBegin
                                end
                             ) as t_FirstIISDate,
                             to_date('01.01.0001', 'dd.mm.yyyy') as t_DateBegin
                             --nptxop.t_OperDate as t_DateClose
                      from ddlcontr_dbt dlcontr,
                           dsfcontr_dbt subcontr,
                           ddlcontrmp_dbt dlcontrmp,
                           dsfcontr_dbt sfcontr,
                           dnptxop_dbt nptxop,
                           dnptxobdc_dbt obdc,
                           dnptxobj_dbt obj
                      where (
                               p_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy')
                               or nptxop.t_OperDate >= p_BeginDate
                            )
                            and nptxop.t_OperDate <= p_EndDate
                            and nptxop.t_DocKind = RSB_SECUR.DL_CALCNDFL
                            and nptxop.t_SubKind_Operation = DL_TXBASECALC_OPTYPE_CLOSE_IIS
                            and nptxop.t_Client = p_Client
                            and obdc.t_DocID = nptxop.t_ID
                            and obj.t_ObjID = obdc.t_ObjID
                            and obj.t_AnaliticKind6 = RSI_NPTXC.TXOBJ_KIND6020
                            and rsi_npto.CheckObjIIS(obj.t_AnaliticKind6, obj.t_Analitic6) = 1
                            and subcontr.t_ID = obj.t_Analitic6
                            and (
                                   p_BeginDate = to_date('01.01.0001', 'dd.mm.yyyy')
                                   or subcontr.t_DateClose >= p_BeginDate
                                )
                            and subcontr.t_DateClose <= p_EndDate
                            and dlcontrmp.t_SfContrID = subcontr.t_ID
                            and dlcontr.t_DlContrID = dlcontrmp.t_DlContrID
                            and sfcontr.t_ID = dlcontr.t_SfContrID
                   ) qq
              order by qq.t_FirstIISDate
           ) q
      where ROWNUM = 1;
      
      return v_FirstDate;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN return p_BeginDate;
   END;
   
   FUNCTION GetDateOfIncomeOperation(p_MinDate DATE, p_MaxDate DATE, p_ClientID NUMBER) RETURN DATE
   IS
      v_OperDate DATE;
   BEGIN
      IF p_ClientID = g_LastClient
      THEN
         v_OperDate := g_LastDate;
      ELSE
         select nvl(max(q.t_OperDate), p_MaxDate) into v_OperDate
         from (
                 select max(nptxop.t_OperDate) as t_OperDate
                 from dnptxop_dbt nptxop
                 where nptxop.t_Client = p_ClientID
                   and nptxop.t_OperDate between p_MinDate and p_MaxDate
                   and nptxop.t_Status > 0
                   and (
                          (
                             nptxop.t_DocKind = RSB_SECUR.DL_WRTMONEY
                             and nptxop.t_SubKind_Operation = DL_NPTXOP_WRTKIND_WRTOFF
                          )
                       or (
                             nptxop.t_DocKind = RSB_SECUR.DL_CALCNDFL
                             and nptxop.t_SubKind_Operation in(
                                                                 DL_TXBASECALC_OPTYPE_ENDYEAR,
                                                                 DL_TXBASECALC_OPTYPE_CLOSE_IIS
                                                              )
                          )
                       )
                   and exists(
                                select 1
                                from dnptxobdc_dbt obdc
                                where obdc.t_DocID = nptxop.t_ID
                             )
                 union
                 select max(tick.t_DealDate) as t_OperDate
                 from ddl_tick_dbt tick
                 where tick.t_ClientID = p_ClientID
                   and tick.t_DealDate between p_MinDate and p_MaxDate
                   and tick.t_DealStatus > 0
                   and (
                          (
                             tick.t_BOfficeKind = RSB_SECUR.DL_AVRWRT
                             and tick.t_DealType = 2010
                          )
                       or (
                             tick.t_BOfficeKind = RSB_SECUR.DL_RETIREMENT_OWN
                             and tick.t_DealType = 2052
                             and exists(
                                          select 1
                                          from dnptxobj_dbt obj
                                          where obj.t_AnaliticKind1 = 1020
                                            and obj.t_Analitic1 = tick.t_DealID
                                            and obj.t_Client = p_ClientID
                                       )
                          )
                       or (
                             tick.t_BOfficeKind = RSB_SECUR.DL_RETURN_DIVIDEND
                             and tick.t_DealType = 12108
                             and exists(
                                          select 1
                                          from dnptxobj_dbt obj
                                          where obj.t_AnaliticKind1 = 1100
                                            and obj.t_Analitic1 = tick.t_DealID
                                            and obj.t_Client = p_ClientID
                                       )
                          )
                       )
                 union
                 select max(dl_order.t_SignDate) as t_OperDate
                 from ddl_order_dbt dl_order
                 where dl_order.t_Contractor = p_ClientID
                   and dl_order.t_SignDate between p_MinDate and p_MaxDate
                   and dl_order.t_DocKind = DL_VEKSELDRAWORDER
                   and dl_order.t_ContractStatus > 0
              ) q;
              
         g_LastClient := p_ClientID;
         g_LastDate := v_OperDate;
      END IF;
           
      RETURN v_OperDate;
      
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN RETURN to_date('01.01.0001', 'dd.mm.yyyy');
   END;
   
   PROCEDURE InsertDataInNPTXOP2OPLNK(p_ClientID IN NUMBER, p_BeginDate IN DATE, p_EndDate IN DATE)
   IS
   BEGIN
      DELETE FROM dnptxop2oplnk_tmp;
   
      INSERT INTO dnptxop2oplnk_tmp
      (
         t_DocID,
         t_Parent_DocID,
         t_PartyID
      )
      SELECT nptxop.t_ID,
             (
               SELECT MIN(n.MinID)
                 FROM ( SELECT MIN(nptxop2.t_ID) as MinID
                          FROM dnptxop_dbt nptxop2
                         WHERE nptxop2.t_DocKind = RSB_SECUR.DL_HOLDNDFL
                           AND nptxop2.t_Client = nptxop.t_Client
                           AND nptxop2.t_OperDate >= nptxop.t_OperDate
                           AND nptxop2.t_SubKind_Operation IN (
                                                                 DL_TXHOLD_OPTYPE_ENDYEAR,
                                                                 DL_TXHOLD_OPTYPE_OUTMONEY,
                                                                 DL_TXHOLD_OPTYPE_OUTAVOIR,
                                                                 DL_TXHOLD_OPTYPE_LUCRE,
                                                                 DL_TXHOLD_OPTYPE_CLOSE
                                                              )
                           AND (nptxop2.t_OperDate between p_BeginDate and p_EndDate
                                OR (nptxop2.t_SubKind_Operation = DL_TXHOLD_OPTYPE_ENDYEAR AND EXTRACT(YEAR FROM nptxop2.t_PrevDate) = EXTRACT(YEAR FROM p_EndDate) )
                               )
                        UNION   
                        SELECT MIN(nptxop2.t_ID) as MInID
                          FROM dnptxop_dbt nptxop2
                         WHERE nptxop2.t_DocKind = RSB_SECUR.DL_WRTMONEY
                           AND nptxop2.t_Client = nptxop.t_Client
                           AND nptxop2.t_OperDate between p_BeginDate and p_EndDate
                           AND nptxop2.t_OperDate >= nptxop.t_OperDate
                           AND nptxop2.t_SubKind_Operation = DL_NPTXOP_WRTKIND_WRTOFF
                           AND nptxop2.t_FlagTax = 'X'
                      ) n
             ) AS t_PID,
             nptxop.t_Client
      FROM dnptxop_dbt nptxop,
           (
              SELECT q.t_Client,
                     MIN(q.t_EarlyDate) AS t_EarlyDate,
                     MAX(q.t_OperDate) AS t_LateDate
              FROM (
                      SELECT nptxop.t_DocKind,
                             nptxop.t_SubKind_Operation,
                             nptxop.t_Client,
                             nptxop.t_OperDate,
                             nptxop.t_PrevDate,
                             nptxop.t_FlagTax,
                             NVL(LEAD(nptxop.t_OperDate) OVER (
                                                                 PARTITION BY nptxop.t_Client
                                                                 ORDER BY nptxop.t_OperDate DESC, nptxop.t_ID DESC
                                                              ), to_date('01.01.0001', 'dd.mm.yyyy')) AS t_EarlyDate
                      FROM dnptxop_dbt nptxop
                      WHERE (
                               nptxop.t_DocKind = RSB_SECUR.DL_HOLDNDFL
                               AND nptxop.t_SubKind_Operation IN (
                                                                    DL_TXHOLD_OPTYPE_ENDYEAR,
                                                                    DL_TXHOLD_OPTYPE_OUTMONEY,
                                                                    DL_TXHOLD_OPTYPE_OUTAVOIR,
                                                                    DL_TXHOLD_OPTYPE_LUCRE,
                                                                    DL_TXHOLD_OPTYPE_CLOSE
                                                                 )
                               OR nptxop.t_DocKind = RSB_SECUR.DL_WRTMONEY
                               AND nptxop.t_SubKind_Operation = DL_NPTXOP_WRTKIND_WRTOFF
                               AND nptxop.t_FlagTax = 'X'
                            )
                            AND nptxop.t_Client = p_ClientID
                   ) q
              WHERE q.t_DocKind = RSB_SECUR.DL_HOLDNDFL
                AND q.t_SubKind_Operation IN (
                                                  DL_TXHOLD_OPTYPE_ENDYEAR,
                                                  DL_TXHOLD_OPTYPE_OUTMONEY,
                                                  DL_TXHOLD_OPTYPE_OUTAVOIR,
                                                  DL_TXHOLD_OPTYPE_LUCRE,
                                                  DL_TXHOLD_OPTYPE_CLOSE
                                             )
                AND (
                       q.t_OperDate between p_BeginDate and p_EndDate 
                       OR q.t_SubKind_Operation = DL_TXHOLD_OPTYPE_ENDYEAR
                       AND EXTRACT(YEAR FROM q.t_PrevDate) = EXTRACT(YEAR FROM p_EndDate)
                    )
                OR q.t_DocKind = RSB_SECUR.DL_WRTMONEY
                AND q.t_SubKind_Operation = DL_NPTXOP_WRTKIND_WRTOFF
                AND q.t_FlagTax = 'X'
                AND q.t_OperDate between p_BeginDate and p_EndDate
             GROUP BY q.t_Client
           ) q2
      WHERE nptxop.t_DocKind = RSB_SECUR.DL_CALCNDFL
        AND EXISTS (
                      SELECT /*+ leading(opr)*/ 1
                      FROM doproper_dbt opr,
                           doprstep_dbt st,
                           dnptxobdc_dbt nptxobdc,
                           dnptxobj_dbt nptxobj
                      WHERE opr.t_DocKind = nptxop.t_DocKind
                        AND opr.t_DocumentID = LPAD(nptxop.t_ID, 34, '0')
                        AND st.t_ID_Operation = opr.t_ID_Operation
                        AND st.t_Number_Step IN (70, 80, 90, 100) --Номера шагов, где создаются объекты PlusG, MinusG, BaseG. Так работает гораздо быстрее
                        AND nptxobdc.t_DocID = nptxop.t_ID
                        AND nptxobdc.t_Step = st.t_ID_Step
                        AND nptxobj.t_ObjID = nptxobdc.t_ObjID
                        AND nptxobj.t_Kind IN  (SELECT t_Element 
                                                  FROM dnptxkind_dbt 
                                                 WHERE (    t_Code LIKE 'PlusG_%'
                                                         OR t_Code LIKE 'BaseG_%'
                                                         OR t_Code LIKE 'MinusG_%'
                                                       )                               
                                               )
                   )
        AND nptxop.t_Client = q2.t_Client
        AND nptxop.t_OperDate > q2.t_EarlyDate
        AND nptxop.t_OperDate <= q2.t_LateDate
        and nptxop.t_Client = p_ClientID
        AND nptxop.t_OperDate between p_BeginDate and p_EndDate;
   END;
   
   
   PROCEDURE InsertDataInNPTX6Calc1(p_TypeOper IN NUMBER, p_RateType IN NUMBER, p_ClientID IN NUMBER, p_ClientCode IN VARCHAR2, p_DateTransfer IN DATE, p_DatePaid IN DATE, p_Rate IN NUMBER, p_TotalPlus IN NUMBER, p_Plus2 IN NUMBER,
                                    p_Plus3 IN NUMBER, p_Plus5 IN NUMBER, p_Plus9 IN NUMBER, p_PlusCode IN NUMBER, p_PlusDate IN DATE, p_DivPlus IN NUMBER, p_TotalMinus IN NUMBER, p_Minus2 IN NUMBER,
                                    p_Minus3 IN NUMBER, p_Minus5 IN NUMBER, p_Minus9 IN NUMBER, p_MinusCode IN NUMBER, p_TaxCalculated IN NUMBER, p_TaxCalculatedDiv IN NUMBER, p_TaxPaid IN NUMBER,
                                    p_TaxDue IN NUMBER, p_TaxOver IN NUMBER, p_TaxToReturnDue IN NUMBER, p_Znp NUMBER, p_IsHighRate CHAR, p_PaidObjID NUMBER, p_PlusObjID NUMBER, p_MinusObjID NUMBER)
   IS
   BEGIN
      IF p_TotalPlus <> 0 or p_Plus2 <> 0 or p_Plus3 <> 0 or p_Plus5 <> 0  or p_Plus9 <> 0  or p_TotalMinus <> 0  or p_Minus2 <> 0  or p_Minus3 <> 0  or p_Minus5 <> 0  or p_Minus9 <> 0 or
         p_TaxCalculated <> 0  or p_TaxPaid <> 0  or p_TaxToReturnDue <> 0
      THEN
         INSERT INTO DNPTX6CALC1_TMP (
                                        t_TypeOper,
                                        t_RateType,
                                        t_ClientID,
                                        t_ClientCode,
                                        t_DateTransfer,
                                        t_DatePaid,
                                        t_Rate,
                                        t_TotalPlus,
                                        t_Plus2,
                                        t_Plus3,
                                        t_Plus5,
                                        t_Plus9,
                                        t_PlusCode,
                                        t_PlusDate,
                                        t_DivPlus,
                                        t_TotalMinus,
                                        t_Minus2,
                                        t_Minus3,
                                        t_Minus5,
                                        t_Minus9,
                                        t_MinusCode,
                                        t_TaxCalculated,
                                        t_TaxCalculatedDiv,
                                        t_TaxPaid,
                                        t_TaxDue,
                                        t_TaxOver,
                                        t_TaxToReturnDue,
                                        t_Znp,
                                        t_IsHighRate,
                                        t_PaidObjID,
                                        t_PlusObjID,
                                        t_MinusObjID
                                     )
                              VALUES (
                                        p_TypeOper,
                                        p_RateType,
                                        p_ClientID,
                                        p_ClientCode,
                                        p_DateTransfer,
                                        p_DatePaid,
                                        p_Rate,
                                        p_TotalPlus,
                                        p_Plus2,
                                        p_Plus3,
                                        p_Plus5,
                                        p_Plus9,
                                        p_PlusCode,
                                        p_PlusDate,
                                        p_DivPlus,
                                        p_TotalMinus,
                                        p_Minus2,
                                        p_Minus3,
                                        p_Minus5,
                                        p_Minus9,
                                        p_MinusCode,
                                        p_TaxCalculated,
                                        p_TaxCalculatedDiv,
                                        p_TaxPaid,
                                        p_TaxDue,
                                        p_TaxOver,
                                        p_TaxToReturnDue,
                                        p_Znp,
                                        p_IsHighRate,
                                        p_PaidObjID,
                                        p_PlusObjID,
                                        p_MinusObjID
                                     );
      END IF;
   END;

   PROCEDURE CalcDataForPart2_Client(p_ClientID IN NUMBER, p_ClientID_2 IN NUMBER, p_GUID IN VARCHAR2, p_BeginDate IN DATE, p_EndDate IN DATE, p_ByCB IN CHAR, p_ByDepo IN CHAR, p_ByVS IN CHAR, p_RepDate DATE)
   IS
      v_TypeOper NUMBER := 0;
      v_DatePaid DATE := to_date('01.01.0001', 'dd.mm.yyyy');
      v_ClientID NUMBER := 0;
      v_DocKind  NUMBER := 0;
      v_DocID    NUMBER := 0;
      
      v_TypeOper15 NUMBER := 0;
      v_DatePaid15 DATE := to_date('01.01.0001', 'dd.mm.yyyy');
      v_ClientID15 NUMBER := 0;
      v_DocKind15  NUMBER := 0;
      v_DocID15    NUMBER := 0;
      
      v_BaseDepo_2 NUMBER := 0;
      v_BaseDepo_3 NUMBER := 0;
      v_BaseDepo_5 NUMBER := 0;
      v_BaseDepo_9 NUMBER := 0;
      
      v_TaxCalculated NUMBER := 0;
      v_TaxCalculatedDiv NUMBER := 0;

      v_FindTotalPlus  NUMBER := 0;
      v_FindPlus2  NUMBER := 0;
      v_FindPlus3  NUMBER := 0;
      v_FindPlus5  NUMBER := 0;
      v_FindPlus9  NUMBER := 0;

      v_FindTotalMinus NUMBER := 0;
      v_FindMinus2 NUMBER := 0;
      v_FindMinus3 NUMBER := 0;
      v_FindMinus5 NUMBER := 0;
      v_FindMinus9 NUMBER := 0;

      v_CurTotalPlus   NUMBER := 0;
      v_CurPlus2   NUMBER := 0;
      v_CurPlus3   NUMBER := 0;
      v_CurPlus5   NUMBER := 0;
      v_CurPlus9   NUMBER := 0;
                               
      v_CurTotalMinus  NUMBER := 0;
      v_CurMinus2  NUMBER := 0;
      v_CurMinus3  NUMBER := 0;
      v_CurMinus5  NUMBER := 0;
      v_CurMinus9  NUMBER := 0;


      v_BaseTotal NUMBER := 0;
      v_Base2 NUMBER := 0;
      v_Base3 NUMBER := 0;
      v_Base5 NUMBER := 0;
      v_Base9 NUMBER := 0;
      v_Base NUMBER := 0;

      v_PrevTotalBase     NUMBER := 0;
      v_PrevBase2         NUMBER := 0;
      v_PrevBase3         NUMBER := 0;
      v_PrevBase5         NUMBER := 0;
      v_PrevBase9         NUMBER := 0;
      v_PrevTaxCalculated NUMBER := 0;
      v_FindTaxPaid       NUMBER := 0;
      v_CurTaxPaid        NUMBER := 0;

      
      v_MaxOperDate DATE := to_date('01.01.0001', 'dd.mm.yyyy');
      v_IsBaseDepo BOOLEAN := false;
      
      v_FirstDateIIS DATE := to_date('01.01.0001', 'dd.mm.yyyy');
      v_CloseDateIIS DATE := to_date('01.01.0001', 'dd.mm.yyyy');
      v_LastWorkDayYear DATE := to_date('01.01.0001', 'dd.mm.yyyy');

      v_ClientCode DOBJCODE_DBT.T_CODE%TYPE;
      
      CURSOR CDataPart2(p_ClientID IN NUMBER, p_FirstDateIIS DATE, p_CloseDateIIS DATE, p_MaxOperDate DATE, p_LastWorkDayYear DATE) IS
      select q.t_TypeOper,
             q.t_DocID,
             q.t_DocKind,
             q.t_RateType,
             q.t_DateTransfer,
             q.t_DatePaid,
             q.t_Rate,
             (
                case
                   when q.t_PlusKind in (
                                           RSI_NPTXC.TXOBJ_PLUSG_1010,
                                           RSI_NPTXC.TXOBJ_PLUS15_1010,
                                           RSI_NPTXC.TXOBJ_PLUS9_1110,
                                           RSI_NPTXC.TXOBJ_PLUSG_1110,
                                           RSI_NPTXC.TXOBJ_PLUS9_1110_IIS,
                                           RSI_NPTXC.TXOBJ_BASEG1_13,
                                           RSI_NPTXC.TXOBJ_BASEG1_15,
                                           RSI_NPTXC.TXOBJ_PLUS35_3023
                                        )
                   then q.t_PlusSum
                   else 0
                end
             ) as t_TotalPlus,
             (
                case
                   when q.t_PlusKind in (
                                           RSI_NPTXC.TXOBJ_PLUSG_1011,
                                           RSI_NPTXC.TXOBJ_PLUSG_1530,
                                           RSI_NPTXC.TXOBJ_PLUSG_1531,
                                           RSI_NPTXC.TXOBJ_PLUSG_1536,
                                           RSI_NPTXC.TXOBJ_PLUSG_1532,
                                           RSI_NPTXC.TXOBJ_PLUSG_1533,
                                           RSI_NPTXC.TXOBJ_PLUSG_1535,
                                           RSI_NPTXC.TXOBJ_PLUSG_2640,
                                           RSI_NPTXC.TXOBJ_PLUSG_2641
                                        )
                   then q.t_PlusSum
                   else 0
                end
             ) as t_Plus2,
             (
                case
                   when q.t_PlusKind in (
                                           RSI_NPTXC.TXOBJ_PLUSG_1537,
                                           RSI_NPTXC.TXOBJ_PLUSG_1539
                                        )
                   then q.t_PlusSum
                   else 0
                end
             ) as t_Plus3,
             (
                case
                   when q.t_PlusKind in (
                                           RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                           RSI_NPTXC.TXOBJ_PLUSG_1544,
                                           RSI_NPTXC.TXOBJ_PLUSG_1545,
                                           RSI_NPTXC.TXOBJ_PLUSG_1549,
                                           RSI_NPTXC.TXOBJ_PLUSG_1546,
                                           RSI_NPTXC.TXOBJ_PLUSG_1547,
                                           RSI_NPTXC.TXOBJ_PLUSG_1548,
                                           RSI_NPTXC.TXOBJ_PLUSG_1551,
                                           RSI_NPTXC.TXOBJ_PLUSG_1553,
                                           RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                           RSI_NPTXC.TXOBJ_PLUSG_2641_IIS
                                        )
                   then q.t_PlusSum
                   else 0
                end
             ) as t_Plus5,
             (
                case
                   when q.t_PlusKind in (
                                           RSI_NPTXC.TXOBJ_PLUSG_2800,
                                           RSI_NPTXC.TXOBJ_BASEBILL
                                        )
                   then q.t_PlusSum
                   else 0
                end
             ) as t_Plus9,
             (
                case q.t_PlusKind
                   when RSI_NPTXC.TXOBJ_PLUSG_1010
                   then '1010'
                   when RSI_NPTXC.TXOBJ_PLUS15_1010
                   then '1010'
                   when RSI_NPTXC.TXOBJ_PLUS9_1110
                   then '1110'
                   when RSI_NPTXC.TXOBJ_PLUSG_1110
                   then '1110'
                   when RSI_NPTXC.TXOBJ_PLUS9_1110_IIS
                   then '1110'
                   when RSI_NPTXC.TXOBJ_PLUSG_1530
                   then '1530'
                   when RSI_NPTXC.TXOBJ_PLUSG_1531
                   then '1531'
                   when RSI_NPTXC.TXOBJ_PLUSG_1536
                   then '1536'
                   when RSI_NPTXC.TXOBJ_PLUSG_1532
                   then '1532'
                   when RSI_NPTXC.TXOBJ_PLUSG_1533
                   then '1533'
                   when RSI_NPTXC.TXOBJ_PLUSG_1535
                   then '1535'
                   when RSI_NPTXC.TXOBJ_PLUSG_2640
                   then '2640'
                   when RSI_NPTXC.TXOBJ_PLUSG_2641
                   then '2641'
                   when RSI_NPTXC.TXOBJ_PLUSG_1537
                   then '1537'
                   when RSI_NPTXC.TXOBJ_PLUSG_1539
                   then '1539'
                   when RSI_NPTXC.TXOBJ_PLUSG_1011_IIS
                   then '1011'
                   when RSI_NPTXC.TXOBJ_PLUSG_1544
                   then '1544'
                   when RSI_NPTXC.TXOBJ_PLUSG_1545
                   then '1545'
                   when RSI_NPTXC.TXOBJ_PLUSG_1549
                   then '1549'
                   when RSI_NPTXC.TXOBJ_PLUSG_1546
                   then '1546'
                   when RSI_NPTXC.TXOBJ_PLUSG_1547
                   then '1547'
                   when RSI_NPTXC.TXOBJ_PLUSG_1548
                   then '1548'
                   when RSI_NPTXC.TXOBJ_PLUSG_1551
                   then '1551'
                   when RSI_NPTXC.TXOBJ_PLUSG_1553
                   then '1553'
                   when RSI_NPTXC.TXOBJ_PLUSG_2640_IIS
                   then '2640'
                   when RSI_NPTXC.TXOBJ_PLUSG_2641_IIS
                   then '2641'
                   when RSI_NPTXC.TXOBJ_PLUSG_2800
                   then '2800'
                   when RSI_NPTXC.TXOBJ_BASEBILL
                   then '2800'
                   when RSI_NPTXC.TXOBJ_BASEG1_13
                   then '1010'
                   when RSI_NPTXC.TXOBJ_BASEG1_15
                   then '1010'
                   when RSI_NPTXC.TXOBJ_PLUS35_3023
                   then '3023'
                end
             ) as t_PlusCode,
             q.t_PlusDate,
             (
                case 
                   when q.t_MinusKind in (
                                            RSI_NPTXC.TXOBJ_MINUSG_601,
                                            RSI_NPTXC.TXOBJ_MINUS15_601,
                                            RSI_NPTXC.TXOBJ_MINUSG_218_1,
                                            RSI_NPTXC.TXOBJ_MINUSG_219_1
                                         )
                   then q.t_MinusSum
                   else 0
                end
             ) as t_TotalMinus,
             (
                case
                   when q.t_MinusKind in (
                                            RSI_NPTXC.TXOBJ_MINUSG_201,
                                            RSI_NPTXC.TXOBJ_MINUSG_202,
                                            RSI_NPTXC.TXOBJ_MINUSG_203,
                                            RSI_NPTXC.TXOBJ_MINUSG_206,
                                            RSI_NPTXC.TXOBJ_MINUSG_207,
                                            RSI_NPTXC.TXOBJ_MINUSG_222,
                                            RSI_NPTXC.TXOBJ_MINUSG_223,
                                            RSI_NPTXC.TXOBJ_MINUSG_205,
                                            RSI_NPTXC.TXOBJ_MINUSG_208,
                                            RSI_NPTXC.TXOBJ_MINUSG_209,
                                            RSI_NPTXC.TXOBJ_MINUSG_210,
                                            RSI_NPTXC.TXOBJ_MINUSG_224,
                                            RSI_NPTXC.TXOBJ_MINUSG_220,
                                            RSI_NPTXC.TXOBJ_MINUSG_618
                                         )
                   then q.t_MinusSum
                   else 0
                end
             ) as t_Minus2,
             (
                case
                   when q.t_MinusKind in (
                                            RSI_NPTXC.TXOBJ_MINUSG_211,
                                            RSI_NPTXC.TXOBJ_MINUSG_213,
                                            RSI_NPTXC.TXOBJ_MINUSG_218,
                                            RSI_NPTXC.TXOBJ_MINUSG_219
                                         )
                   then q.t_MinusSum
                   else 0
                end
             ) as t_Minus3,
             (
                case
                   when q.t_MinusKind in (
                                            RSI_NPTXC.TXOBJ_MINUSG_225,
                                            RSI_NPTXC.TXOBJ_MINUSG_226,
                                            RSI_NPTXC.TXOBJ_MINUSG_227,
                                            RSI_NPTXC.TXOBJ_MINUSG_228,
                                            RSI_NPTXC.TXOBJ_MINUSG_229,
                                            RSI_NPTXC.TXOBJ_MINUSG_230,
                                            RSI_NPTXC.TXOBJ_MINUSG_231,
                                            RSI_NPTXC.TXOBJ_MINUSG_233,
                                            RSI_NPTXC.TXOBJ_MINUSG_234,
                                            RSI_NPTXC.TXOBJ_MINUSG_235,
                                            RSI_NPTXC.TXOBJ_MINUSG_239,
                                            RSI_NPTXC.TXOBJ_MINUSG_240,
                                            RSI_NPTXC.TXOBJ_MINUSG_250,
                                            RSI_NPTXC.TXOBJ_MINUSG_251,
                                            RSI_NPTXC.TXOBJ_MINUSG_252,
                                            RSI_NPTXC.TXOBJ_MINUSG_619 
                                         )
                   then q.t_MinusSum
                   else 0
                end
             ) as t_Minus5,
             0 as t_Minus9,
             (
                case q.t_MinusKind
                   when RSI_NPTXC.TXOBJ_MINUSG_601
                   then '601'
                   when RSI_NPTXC.TXOBJ_MINUS15_601
                   then '601'
                   when RSI_NPTXC.TXOBJ_MINUSG_218_1
                   then '218'
                   when RSI_NPTXC.TXOBJ_MINUSG_219_1
                   then '219'
                   when RSI_NPTXC.TXOBJ_MINUSG_618
                   then '618'
                   when RSI_NPTXC.TXOBJ_MINUSG_619
                   then '619'
                   when RSI_NPTXC.TXOBJ_MINUSG_201
                   then '201'
                   when RSI_NPTXC.TXOBJ_MINUSG_202
                   then '202'
                   when RSI_NPTXC.TXOBJ_MINUSG_203
                   then '203'
                   when RSI_NPTXC.TXOBJ_MINUSG_206
                   then '206'
                   when RSI_NPTXC.TXOBJ_MINUSG_207
                   then '207'
                   when RSI_NPTXC.TXOBJ_MINUSG_222
                   then '222'
                   when RSI_NPTXC.TXOBJ_MINUSG_223
                   then '223'
                   when RSI_NPTXC.TXOBJ_MINUSG_205
                   then '205'
                   when RSI_NPTXC.TXOBJ_MINUSG_208
                   then '208'
                   when RSI_NPTXC.TXOBJ_MINUSG_209
                   then '209'
                   when RSI_NPTXC.TXOBJ_MINUSG_210
                   then '210'
                   when RSI_NPTXC.TXOBJ_MINUSG_224
                   then '224'
                   when RSI_NPTXC.TXOBJ_MINUSG_220
                   then '220'
                   when RSI_NPTXC.TXOBJ_MINUSG_211
                   then '211'
                   when RSI_NPTXC.TXOBJ_MINUSG_213
                   then '213'
                   when RSI_NPTXC.TXOBJ_MINUSG_218
                   then '218'
                   when RSI_NPTXC.TXOBJ_MINUSG_219
                   then '219'
                   when RSI_NPTXC.TXOBJ_MINUSG_214
                   then '214'
                   when RSI_NPTXC.TXOBJ_MINUSG_225
                   then '225'
                   when RSI_NPTXC.TXOBJ_MINUSG_226
                   then '226'
                   when RSI_NPTXC.TXOBJ_MINUSG_227
                   then '227'
                   when RSI_NPTXC.TXOBJ_MINUSG_228
                   then '228'
                   when RSI_NPTXC.TXOBJ_MINUSG_229
                   then '229'
                   when RSI_NPTXC.TXOBJ_MINUSG_230
                   then '230'
                   when RSI_NPTXC.TXOBJ_MINUSG_231
                   then '231'
                   when RSI_NPTXC.TXOBJ_MINUSG_233
                   then '233'
                   when RSI_NPTXC.TXOBJ_MINUSG_234
                   then '234'
                   when RSI_NPTXC.TXOBJ_MINUSG_214_IIS
                   then '214'
                   when RSI_NPTXC.TXOBJ_MINUSG_250
                   then '250'
                   when RSI_NPTXC.TXOBJ_MINUSG_251
                   then '251'
                   when RSI_NPTXC.TXOBJ_MINUSG_252
                   then '252'
                   when RSI_NPTXC.TXOBJ_MINUSG_241
                   then '241'
                   when RSI_NPTXC.TXOBJ_MINUSG_235
                   then '235'
                   when RSI_NPTXC.TXOBJ_MINUSG_239
                   then '239'
                   when RSI_NPTXC.TXOBJ_MINUSG_240
                   then '240'
                end
             ) as t_MinusCode,
             q.t_TaxPaid as t_TaxPaid,
             to_date('01.01.0001', 'dd.mm.yyyy') as t_DateOper,
             '' as t_NumOper,
             q.t_TaxBaseDiv as t_TaxBaseDiv,
             q.t_DivPlus as t_DivPlus,
             q.t_DivPay_Sec_0 as t_DivPay_Sec_0,
             q.t_PaidGeneral_0 as t_PaidGeneral_0,
             q.t_TaxToReturnDue as t_TaxToReturnDue,
             to_date('01.01.0001', 'dd.mm.yyyy') as t_DateTaxReturn,
             (q.t_DivPlus - q.t_TaxBaseDiv) * to_number(q.t_Rate) / 100 as t_Znp,
             q.t_IsHighRate,
             q.t_PaidObjID,
             q.t_PlusObjID,
             q.t_MinusObjID
      from (
              --Погашение СЭБ + Погашение купона СЭБ
              select 1 as t_TypeOper,
                     tick.t_DealID as t_DocID,
                     tick.t_BOfficeKind as t_DocKind,
                     INFORATE_TOTAL as t_RateType,
                     (
                        case
                           when q1.t_TaxPaid > 0
                           then rsi_rsbcalendar.GetDateAfterWorkDay(
                                                                      case
                                                                         when p_BeginDate <= to_date('31.12.2022', 'dd.mm.yyyy')
                                                                         --До 31.12.2022
                                                                         then q1.t_DatePaid + 30
                                                                         --После 31.12.2022
                                                                         else (
                                                                                 case
                                                                                    when RSB_COMMON.GetRegIntValue('COMMON\НДФЛ\СРОК_ПЕРЕЧИСЛЕНИЯ_НДФЛ_ПО_ЦБ') = 0
                                                                                    then q1.t_DatePaid + 30
                                                                                    else (
                                                                                            case
                                                                                               when extract(day from q1.t_DatePaid) < 23
                                                                                               then to_date('28.' || to_char(q1.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                               when extract(day from q1.t_DatePaid) >= 23 and extract(month from q1.t_DatePaid) = 12
                                                                                               then p_LastWorkDayYear
                                                                                               else to_date('28.' || to_char(ADD_MONTHS(q1.t_DatePaid, 1), 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                            end
                                                                                         )
                                                                                 end
                                                                              )
                                                                      end, 0
                                                                   )
                           else to_date('01.01.0001', 'dd.mm.yyyy')
                        end
                     ) as t_DateTransfer,
                     q1.t_DatePaid,
                     (
                        case
                           when RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then (
                                   case
                                      when q1.t_IsHighRate = 'X'
                                      then '15'
                                      else '13'
                                   end
                                )
                           else
                              case
                                 when exists(
                                               select 1
                                               from dnotetext_dbt notetext
                                               where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                 and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                 and notetext.t_NoteKind = 77
                                                 and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                            )
                                 then TO_CHAR(RSB_STRUCT.getDouble(RSI_RSB_KERNEL.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                 else '30'
                              end
                        end
                     ) as t_Rate,
                     PlusObj.t_Sum0 as t_PlusSum,
                     nvl(PlusObj.t_Kind, 0) as t_PlusKind,
                     nvl(PlusObj.t_ObjID, 0) as t_PlusObjID,
                     (
                        case
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                           then nvl(PlusObj.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     0 as t_MinusSum,
                     0 as t_MinusKind,
                     0 as t_MinusObjID,
                     (
                        case
                           when q1.t_TaxPaid > 0
                           then q1.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxPaid,
                     q1.t_ObjID as t_PaidObjID,
                     0 as t_TaxBaseDiv,
                     0 as t_DivPlus,
                     0 as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     (
                        case
                           when q1.t_TaxPaid < 0
                           then q1.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxToReturnDue,
                     q1.t_IsHighRate
              from (
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObj.t_Sum0 as t_TaxPaid,
                             chr(88) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Client,
                             TaxPaidObj.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where (
                               (
                                  TaxPaidObj.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObj.t_Kind in (
                                                              RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                              RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                              RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9
                                                           )
                               )
                            or (
                                  TaxPaidObj.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObj.t_Kind =  RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                               )
                            )
                        and RSI_NPTX.ResidenceStatus(TaxPaidObj.t_Client, p_EndDate) = 1
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                      union all
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObj.t_Sum0 as t_TaxPaid,
                             chr(0) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Client,
                             TaxPaidObj.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where (
                               (
                                  TaxPaidObj.t_Date between p_BeginDate and p_EndDate
                                  and TaxPaidObj.t_Kind in (
                                                              RSI_NPTXC.TXOBJ_PAIDMATERIAL,
                                                              RSI_NPTXC.TXOBJ_PAIDSPECIAL,
                                                              RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                              RSI_NPTXC.TXOBJ_PAIDBILL
                                                           )
                               )
                            or (
                                  TaxPaidObj.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObj.t_Kind in (
                                                              RSI_NPTXC.TXOBJ_PAIDMATERIAL_IIS,
                                                              RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS,
                                                              RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                              RSI_NPTXC.TXOBJ_DIVPAY_SEC
                                                           )
                               )
                            )
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                   ) q1
                   inner join dnptxobdc_dbt obdc
                   on q1.t_AnaliticKind1 = RSI_NPTXC.TXOBJ_KIND1020
                      and obdc.t_ObjID = q1.t_ObjID
                   inner join doproper_dbt oproper
                   on oproper.t_ID_Operation = obdc.t_DocID
                   inner join ddl_tick_dbt tick
                   on tick.t_BOfficeKind = RSB_SECUR.DL_RETIREMENT_OWN
                      and tick.t_DealID = to_number(oproper.t_DocumentID)
                   left outer join
                   (
                      dnptxobdc_dbt PlusOBDC
                      inner join dnptxobj_dbt PlusObj
                      on PlusObj.t_ObjID = PlusOBDC.t_ObjID
                         and PlusObj.t_Kind in (
                                                  RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1011_IIS
                                               )
                   )
                   on PlusOBDC.t_DocID = oproper.t_ID_Operation
                      and PlusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
              where p_ByCB = 'X'
                and q1.t_DatePaid between p_FirstDateIIS and p_EndDate
              union all
              --Возврат дивидендов
              select 2 as t_TypeOper,
                     tick.t_DealID as t_DocID,
                     tick.t_BOfficeKind as t_DocKind,
                     (
                        case
                           when q2.t_PaidSpecialSum <> 0
                              and q2.t_PaidGeneralSum = 0
                           then INFORATE9_15
                           else INFORATE_TOTAL
                        end
                     ) as t_RateType,
                     (
                        case
                           when q2.t_PaidGeneralSum >= 0 and q2.t_PaidSpecialSum >= 0
                           then rsi_rsbcalendar.GetDateAfterWorkDay(
                                                                      case 
                                                                         when p_BeginDate <= to_date('31.12.2022', 'dd.mm.yyyy')
                                                                         --До 31.12.2022
                                                                         then q2.t_DatePaid + 30
                                                                         --После 31.12.2022
                                                                         else (
                                                                                 case
                                                                                    when RSB_COMMON.GetRegIntValue('COMMON\НДФЛ\СРОК_ПЕРЕЧИСЛЕНИЯ_НДФЛ_ПО_ЦБ') = 0
                                                                                    then q2.t_DatePaid + 30
                                                                                    else (
                                                                                            case
                                                                                               when extract(day from q2.t_DatePaid) < 23
                                                                                               then to_date('28.' || to_char(q2.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                               when extract(day from q2.t_DatePaid) >= 23 and extract(month from q2.t_DatePaid) = 12
                                                                                               then p_LastWorkDayYear
                                                                                               else to_date('28.' || to_char(ADD_MONTHS(q2.t_DatePaid, 1), 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                            end
                                                                                         )
                                                                                 end
                                                                              )
                                                                      end, 0
                                                                   )
                           else to_date('01.01.0001', 'dd.mm.yyyy')
                        end
                     ) as t_DateTransfer,
                     q2.t_DatePaid,
                     (
                        case
                           when RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then
                              case
                                 when q2.t_PaidSpecialSum <> 0
                                  and q2.t_PaidGeneralSum = 0
                                 then '9'
                                 when q2.t_PaidSpecialSum = 0
                                  and q2.t_PaidGeneralSum <> 0
                                 then
                                    case
                                       when q2.t_IsHighRate = 'X'
                                       then '15'
                                       else '13'
                                    end
                                 else '13'
                              end
                           else
                              case
                                 when q2.t_PaidSpecialSum <> 0
                                  and q2.t_PaidGeneralSum = 0
                                 then '15'
                                 else
                                    case
                                       when exists(
                                                     select 1
                                                     from dnotetext_dbt notetext
                                                     where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                       and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                       and notetext.t_NoteKind = 77
                                                       and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                                  )
                                       then TO_CHAR(RSB_STRUCT.getDouble(RSI_RSB_KERNEL.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                       else '30'
                                    end
                              end
                        end
                     ) as t_Rate,
                     (
                        case
                           when PlusObj.t_Kind is not NULL
                           then (
                                   case
                                      when (
                                              RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) = 1
                                              and PlusObj.t_Kind = RSI_NPTXC.TXOBJ_PLUSG_1010
                                           )
                                        or (
                                              RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) <> 1
                                              and PlusObj.t_Kind = RSI_NPTXC.TXOBJ_PLUS15_1010
                                           )
                                      then PlusObj.t_Sum0
                                      else 0
                                   end
                                )
                           else 0
                        end
                     ) as t_PlusSum,
                     nvl(PlusObj.t_Kind, 0) as t_PlusKind,
                     nvl(PlusObj.t_ObjID, 0) as t_PlusObjID,
                     (
                        case
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                           then nvl(PlusObj.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     (
                        case
                           when MinusObj.t_Kind is not NULL
                           then (
                                   case
                                      when (
                                              RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) = 1
                                              and MinusObj.t_Kind = RSI_NPTXC.TXOBJ_MINUSG_601
                                           )
                                        or (
                                              RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) <> 1
                                              and MinusObj.t_Kind = RSI_NPTXC.TXOBJ_MINUS15_601
                                           )
                                      then MinusObj.t_Sum0
                                      else 0
                                   end
                                )
                           else 0
                        end
                     ) as t_MinusSum,
                     nvl(MinusObj.t_Kind, 0) as t_MinusKind,
                     nvl(MinusObj.t_ObjID, 0) as t_MinusObjID,
                     (
                        case
                           when q2.t_PaidGeneralSum > 0 and q2.t_PaidSpecialSum <= 0
                           then q2.t_PaidGeneralSum
                           when q2.t_PaidGeneralSum <= 0 and q2.t_PaidSpecialSum > 0
                           then q2.t_PaidSpecialSum
                           else 0
                        end
                     ) as t_TaxPaid,
                     q2.t_ObjID as t_PaidObjID,
                     (
                        select nvl(sum(TaxBaseDivObj.t_Sum0), 0)
                        from dnptxobj_dbt TaxBaseDivObj
                        where TaxBaseDivObj.t_AnaliticKind1 = RSI_NPTXC.TXOBJ_KIND1100
                          and TaxBaseDivObj.t_Analitic1 = tick.t_DealID
                          and (
                                 (
                                    q2.t_IsHighRate = 'X'
                                    and TaxBaseDivObj.t_Kind in (
                                                                   RSI_NPTXC.TXOBJ_BASESPECIAL_DIV_15
                                                                )
                                 )
                              or (
                                    q2.t_IsHighRate <> 'X'
                                    and TaxBaseDivObj.t_Kind in (
                                                                   RSI_NPTXC.TXOBJ_BASESPECIAL_DIV
                                                                )
                                 )
                              )
                          and TaxBaseDivObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                     ) as t_TaxBaseDiv,
                     (
                        select nvl(sum(DivPlusObj.t_Sum0), 0)
                        from dnptxobj_dbt DivPlusObj
                        where DivPlusObj.t_AnaliticKind1 = RSI_NPTXC.TXOBJ_KIND1100
                          and DivPlusObj.t_Analitic1 = tick.t_DealID
                          and (
                                 (
                                    RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) = 1
                                    and (
                                           (
                                              q2.t_IsHighRate <> 'X'
                                              and DivPlusObj.t_Kind = RSI_NPTXC.TXOBJ_BASEG1_13
                                           )
                                        or (
                                              q2.t_IsHighRate = 'X'
                                              and DivPlusObj.t_Kind = RSI_NPTXC.TXOBJ_BASEG1_15
                                           )
                                        )
                                 )
                              or (
                                    RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) <> 1
                                    and DivPlusObj.t_Kind in (
                                                                RSI_NPTXC.TXOBJ_PLUS15_1010
                                                             )
                                 )
                              )
                          and DivPlusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                     ) as t_DivPlus,
                     (
                        select nvl(sum(DivPaySec0_Obj.t_Sum0), 0)
                        from dnptxobj_dbt DivPaySec0_Obj
                        where DivPaySec0_Obj.t_AnaliticKind1 = RSI_NPTXC.TXOBJ_KIND1100
                          and DivPaySec0_Obj.t_Analitic1 = tick.t_DealID
                          and DivPaySec0_Obj.t_Kind in (
                                                          RSI_NPTXC.TXOBJ_DIVPAY_SEC_0
                                                       )
                          and DivPaySec0_Obj.t_Date between p_FirstDateIIS and p_MaxOperDate
                     ) as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     (
                        case
                           when q2.t_PaidGeneralSum < 0 and q2.t_PaidSpecialSum = 0
                           then q2.t_PaidGeneralSum
                           when q2.t_PaidGeneralSum = 0 and q2.t_PaidSpecialSum < 0
                           then q2.t_PaidSpecialSum
                           else 0
                        end
                     ) as t_TaxToReturnDue,
                     q2.t_IsHighRate
              from (
                      select /*+ index(PaidGeneralObjHigh DNPTXOBJ_DBT_IDXA)*/
                             PaidGeneralObjHigh.t_Sum0 as t_PaidGeneralSum,
                             0 as t_PaidSpecialSum,
                             chr(88) as t_IsHighRate,
                             PaidGeneralObjHigh.t_ObjID,
                             PaidGeneralObjHigh.t_AnaliticKind1,
                             PaidGeneralObjHigh.t_Analitic1,
                             PaidGeneralObjHigh.t_Client,
                             PaidGeneralObjHigh.t_Date as t_DatePaid
                      from dnptxobj_dbt PaidGeneralObjHigh
                      where (
                               (
                                  PaidGeneralObjHigh.t_Date between p_BeginDate and p_EndDate
                                  and PaidGeneralObjHigh.t_Kind in (
                                                                      RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                                      RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                                      RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9
                                                                   )
                               )
                            or (
                                  PaidGeneralObjHigh.t_Date between p_FirstDateIIS and p_EndDate
                                  and PaidGeneralObjHigh.t_Kind = RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                               )
                            )
                        and PaidGeneralObjHigh.t_User <> chr(88)
                        and PaidGeneralObjHigh.t_FromOutSyst <> chr(88)
                        and PaidGeneralObjHigh.t_Client = p_ClientID
                      union all
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             (
                                case
                                   when TaxPaidObj.t_Kind in (
                                                                RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                                RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                                RSI_NPTXC.TXOBJ_DIVPAY_SEC
                                                             )
                                   then TaxPaidObj.t_Sum0
                                   else 0
                                end
                             ) as t_PaidGeneralSum,
                             (
                                case
                                   when TaxPaidObj.t_Kind in (
                                                                RSI_NPTXC.TXOBJ_PAIDSPECIAL,
                                                                RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS
                                                             )
                                   then TaxPaidObj.t_Sum0
                                   else 0
                                end
                             ) as t_PaidSpecialSum,
                             chr(0) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Client,
                             TaxPaidObj.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where (
                               (
                                  TaxPaidObj.t_Date between p_BeginDate and p_EndDate
                                  and TaxPaidObj.t_Kind in (
                                                              RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                              RSI_NPTXC.TXOBJ_PAIDSPECIAL
                                                           )
                               )
                            or (
                                  TaxPaidObj.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObj.t_Kind in (
                                                              RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                              RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS,
                                                              RSI_NPTXC.TXOBJ_DIVPAY_SEC
                                                           )
                               )
                            )
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                   ) q2
                   inner join dnptxobdc_dbt obdc
                   on q2.t_AnaliticKind1 = RSI_NPTXC.TXOBJ_KIND1100
                      and obdc.t_ObjID = q2.t_ObjID
                   inner join doproper_dbt oproper
                   on oproper.t_ID_Operation = obdc.t_DocID
                   inner join ddl_tick_dbt tick
                   on tick.t_BOfficeKind = RSB_SECUR.DL_RETURN_DIVIDEND
                      and rsb_secur.IsDividendReturnRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType, tick.t_BOfficeKind))) = 1
                      and tick.t_DealID = to_number(oproper.t_DocumentID)
                   inner join ddl_leg_dbt leg
                   on leg.t_DealID = tick.t_DealID
                      and leg.t_LegKind = 0
                      and leg.t_LegID = 0
                   left outer join dnptxobj_dbt PlusObj
                   on PlusObj.t_AnaliticKind1 = 1100
                      and PlusObj.t_Analitic1 = tick.t_DealID
                      and (
                             (
                                RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) = 1
                                and PlusObj.t_Kind in (
                                                         RSI_NPTXC.TXOBJ_PLUSG_1010
                                                      )
                             )
                          or (
                                RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) <> 1
                                and PlusObj.t_Kind in (
                                                         RSI_NPTXC.TXOBJ_PLUS15_1010
                                                      )
                             )
                          )
                       and PlusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                   left outer join dnptxobj_dbt MinusObj
                   on MinusObj.t_AnaliticKind1 = RSI_NPTXC.TXOBJ_KIND1100
                      and MinusObj.t_Analitic1 = tick.t_DealID
                      and (
                             (
                                RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) = 1
                                and MinusObj.t_Kind in (
                                                          RSI_NPTXC.TXOBJ_MINUSG_601
                                                       )
                             )
                          or (
                                RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) <> 1
                                and MinusObj.t_Kind in (
                                                          RSI_NPTXC.TXOBJ_MINUS15_601
                                                       )
                             )
                          )
                      and MinusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
              where p_ByCB = 'X'
                and q2.t_DatePaid between p_FirstDateIIS and p_EndDate
              union all
              --СО БУ при выплате дохода ЮЛН, для которых указан фактический получатеель дохода - физ. лицо
              select 2 as t_TypeOper,
                     tick.t_DealID as t_DocID,
                     tick.t_BOfficeKind as t_DocKind,
                     (
                        case
                           when q3.t_PaidSpecialSum <> 0 and q3.t_PaidGeneralSum = 0
                           then INFORATE9_15
                           else INFORATE_TOTAL
                        end
                     ) as t_RateType,
                     (
                        case
                           when q3.t_PaidGeneralSum > 0 or q3.t_PaidSpecialSum > 0
                           then rsi_rsbcalendar.GetDateAfterWorkDay(
                                                                      case
                                                                         when p_BeginDate <= to_date('31.12.2022', 'dd.mm.yyyy')
                                                                         --До 31.12.2022
                                                                         then q3.t_DatePaid + 30
                                                                         --После 31.12.2022
                                                                         else (
                                                                                 case
                                                                                    when RSB_COMMON.GetRegIntValue('COMMON\НДФЛ\СРОК_ПЕРЕЧИСЛЕНИЯ_НДФЛ_ПО_ЦБ') = 0
                                                                                    then q3.t_DatePaid + 30
                                                                                    else (
                                                                                            case
                                                                                               when extract(day from q3.t_DatePaid) < 23
                                                                                               then to_date('28.' || to_char(q3.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                               when extract(day from q3.t_DatePaid) >= 23 and extract(month from q3.t_DatePaid) = 12
                                                                                               then p_LastWorkDayYear
                                                                                               else to_date('28.' || to_char(ADD_MONTHS(q3.t_DatePaid, 1), 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                            end
                                                                                         )
                                                                                 end
                                                                              )
                                                                      end, 0
                                                                   )
                           else to_date('01.01.0001', 'dd.mm.yyyy')
                        end
                     ) as t_DateTransfer,
                     q3.t_DatePaid,
                     (
                        case
                           when rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then
                              case
                                 when q3.t_PaidSpecialSum <> 0 and q3.t_PaidGeneralSum = 0
                                 then '9'
                                 when q3.t_PaidSpecialSum = 0 and q3.t_PaidGeneralSum <> 0
                                 then
                                    case
                                       when q3.t_IsHighRate = 'X'
                                       then '15'
                                       else '13'
                                    end
                                 else '13\9'
                              end
                           else
                              case
                                 when exists(
                                               select 1
                                               from dnotetext_dbt notetext
                                               where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                  and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                  and notetext.t_NoteKind = 77
                                                  and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                            )
                                 then TO_CHAR(RSB_STRUCT.getDouble(RSI_RSB_KERNEL.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                 else '30'
                              end
                        end
                     ) as t_Rate,
                     nvl(PlusObj.t_Sum0, 0) as t_PlusSum,
                     nvl(PlusObj.t_Kind, 0) as t_PlusKind,
                     nvl(PlusObj.t_ObjID, 0) as t_PlusObjID,
                     (
                        case
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                           then nvl(PlusObj.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     0 as t_MinusSum,
                     0 as t_MinusKind,
                     0 as t_MinusObjID,
                     (
                        case
                           when q3.t_PaidGeneralSum > 0 and q3.t_PaidSpecialSum = 0
                           then q3.t_PaidGeneralSum
                           when q3.t_PaidGeneralSum = 0 and q3.t_PaidSpecialSum > 0
                           then q3.t_PaidSpecialSum
                           else 0
                        end
                     ) as t_TaxPaid,
                     q3.t_ObjID as t_PaidObjID,
                     0 as t_TaxBaseDiv,
                     0 as t_DivPlus,
                     0 as t_DivPay_Sec_0,
                     (
                        select nvl(sum(PaidGeneral0_Obj.t_Sum0), 0)
                        from dnptxobj_dbt PaidGeneral0_Obj
                        where PaidGeneral0_Obj.t_AnaliticKind1 = RSI_NPTXC.TXOBJ_KIND1010
                          and PaidGeneral0_Obj.t_Analitic1 = tick.t_DealID
                          and (
                                 (
                                    q3.t_IsHighRate <> 'X'
                                    and PaidGeneral0_Obj.t_Kind = RSI_NPTXC.TXOBJ_PAIDGENERAL_0
                                 )
                              or (
                                    q3.t_IsHighRate = 'X'
                                    and PaidGeneral0_Obj.t_Kind = RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1_0
                                 )
                              )
                          and PaidGeneral0_Obj.t_Date between p_FirstDateIIS and p_EndDate
                     ) as t_PaidGeneral_0,
                     (
                        case
                           when q3.t_PaidGeneralSum < 0 and q3.t_PaidSpecialSum = 0
                           then q3.t_PaidGeneralSum
                           when q3.t_PaidGeneralSum = 0 and q3.t_PaidSpecialSum < 0
                           then q3.t_PaidSpecialSum
                           else 0
                        end
                     ) as t_TaxToReturnDue,
                     q3.t_IsHighRate
              from (
                      select /*+ index(TaxPaidObjHigh DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObjHigh.t_Sum0 as t_PaidGeneralSum,
                             0 as t_PaidSpecialSum,
                             chr(88) as t_IsHighRate,
                             TaxPaidObjHigh.t_ObjID,
                             TaxPaidObjHigh.t_AnaliticKind1,
                             TaxPaidObjHigh.t_Analitic1,
                             TaxPaidObjHigh.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObjHigh
                      where (
                               (
                                  TaxPaidObjHigh.t_Date between p_BeginDate and p_EndDate
                                  and TaxPaidObjHigh.t_Kind in (
                                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9
                                                               )
                               )
                            or (
                                  TaxPaidObjHigh.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObjHigh.t_Kind = RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                               )
                            )
                        and TaxPaidObjHigh.t_FromOutSyst <> chr(88)
                        and TaxPaidObjHigh.t_User <> chr(88)
                        and TaxPaidObjHigh.t_Client = p_ClientID
                      union all
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             (
                                case
                                   when TaxPaidObj.t_Kind in (
                                                                RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                                RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS
                                                             )
                                   then TaxPaidObj.t_Sum0
                                   else 0
                                end
                             ) as t_PaidGeneralSum,
                             (
                                case
                                   when TaxPaidObj.t_Kind in (
                                                                RSI_NPTXC.TXOBJ_PAIDSPECIAL,
                                                                RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS
                                                             )
                                   then TaxPaidObj.t_Sum0
                                   else 0
                                end
                             ) as t_PaidSpecialSum,
                             chr(0) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where (
                               (
                                  TaxPaidObj.t_Date between p_BeginDate and p_EndDate
                                  and TaxPaidObj.t_Kind in(
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL
                                                          )
                               )
                            or (
                                  TaxPaidObj.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObj.t_Kind in(
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS
                                                          )
                               )
                            )
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                   ) q3
                   inner join ddl_tick_dbt tick
                   on q3.t_AnaliticKind1 = RSI_NPTXC.TXOBJ_KIND1010
                      and tick.t_BOfficeKind = RSB_SECUR.DL_SECURITYDOC
                      and tick.t_DealID = q3.t_Analitic1
                      and rsi_nutx.IsNonResidentPayerNUTX(tick.t_ClientID, p_EndDate) = 1
                   left outer join dnptxobj_dbt PlusObj
                   on PlusObj.t_AnaliticKind1 = RSI_NPTXC.TXOBJ_KIND1010
                      and PlusObj.t_Analitic1 = tick.t_DealID
                      and PlusObj.t_Kind in (
                                               RSI_NPTXC.TXOBJ_PLUS9_1110,
                                               RSI_NPTXC.TXOBJ_PLUSG_1110,
                                               RSI_NPTXC.TXOBJ_PLUS9_1110_IIS,
                                               RSI_NPTXC.TXOBJ_PLUSG_1011,
                                               RSI_NPTXC.TXOBJ_PLUSG_1011_IIS
                                            )
                      and PlusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
              where p_ByCB = 'X'
                and q3.t_DatePaid between p_FirstDateIIS and p_EndDate
              union all
              --Списание денежных средств
              select 3 as t_TypeOper,
                     nptxop.t_ID as t_DocID,
                     nptxop.t_DocKind,
                     INFORATE_TOTAL as t_RateType,
                     (
                        case
                           when q4.t_TaxPaid > 0
                           then rsi_rsbcalendar.GetDateAfterWorkDay(
                                                                      case
                                                                         when p_BeginDate <= to_date('31.12.2022', 'dd.mm.yyyy')
                                                                         --До 31.12.2022
                                                                         then q4.t_DatePaid + 30
                                                                         --После 31.12.2022
                                                                         else (
                                                                                 case
                                                                                    when RSB_COMMON.GetRegIntValue('COMMON\НДФЛ\СРОК_ПЕРЕЧИСЛЕНИЯ_НДФЛ_ПО_ЦБ') = 0
                                                                                    then q4.t_DatePaid + 30
                                                                                    else (
                                                                                            case
                                                                                               when extract(day from q4.t_DatePaid) < 23
                                                                                               then to_date('28.' || to_char(q4.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                               when extract(day from q4.t_DatePaid) >= 23 and extract(month from q4.t_DatePaid) = 12
                                                                                               then p_LastWorkDayYear
                                                                                               else to_date('28.' || to_char(ADD_MONTHS(q4.t_DatePaid, 1), 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                            end
                                                                                         )
                                                                                 end
                                                                              )
                                                                      end, 0
                                                                   )
                           else to_date('01.01.0001', 'dd.mm.yyyy')
                        end
                     ) as t_DateTransfer,
                     q4.t_DatePaid,
                     (
                        case
                           when rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then
                              case
                                 when q4.t_IsHighRate = 'X'
                                 then '15'
                                 else '13'
                              end
                           else
                              case
                                 when exists(
                                               select 1
                                               from dnotetext_dbt notetext
                                               where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                 and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                 and notetext.t_NoteKind = 77
                                                 and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                            )
                                 then TO_CHAR(RSB_STRUCT.getDouble(RSI_RSB_KERNEL.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                 else '30'
                              end
                        end
                     ) as t_Rate,
                     (
                        case
                           when q4.t_IsHighRate <> 'X'
                              and PlusObj.t_Kind in (
                                                       RSI_NPTXC.TXOBJ_BASEG1_13,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1110,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1530,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1531,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1536,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1532,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1533,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2640,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2641,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1537,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1539,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1544,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1545,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1546,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1547,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1548,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1549,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1551,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1553,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2641_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2800
                                                    )
                           then PlusObj.t_Sum0
                           when q4.t_IsHighRate = 'X'
                              and PlusObj.t_Kind in (
                                                       RSI_NPTXC.TXOBJ_BASEG1_15,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1110,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1530,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1531,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1536,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1532,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1533,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2640,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2641,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1537,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1539,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1544,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1545,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1546,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1547,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1548,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1549,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1551,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1553,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2641_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2800
                                                    )
                           then PlusObj.t_Sum0
                           else 0
                        end
                     ) as t_PlusSum,
                     nvl(PlusObj.t_Kind, 0) as t_PlusKind,
                     nvl(PlusObj.t_ObjID, 0) as t_PlusObjID,
                     (
                        case
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                           then nvl(PlusObj.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     nvl(MinusObj.t_Sum0, 0) as t_MinusSum,
                     nvl(MinusObj.t_Kind, 0) as t_MinusKind,
                     nvl(MinusObj.t_ObjID, 0) as t_MinusObjID,
                     (
                        case
                           when q4.t_TaxPaid > 0
                           then q4.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxPaid,
                     q4.t_ObjID as t_PaidObjID,
                     0 as t_TaxBaseDiv,
                     0 as t_DivPlus,
                     0 as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     (
                        case
                           when q4.t_TaxPaid < 0
                           then q4.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxToReturnDue,
                     q4.t_IsHighRate
              from (
                      select /*+ index(TaxPaidObjHigh DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObjHigh.t_Sum0 as t_TaxPaid,
                             chr(88) as t_IsHighRate,
                             TaxPaidObjHigh.t_ObjID,
                             TaxPaidObjHigh.t_AnaliticKind1,
                             TaxPaidObjHigh.t_Analitic1,
                             TaxPaidObjHigh.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObjHigh
                      where (
                               (
                                  TaxPaidObjHigh.t_Date between p_BeginDate and p_EndDate
                                  and TaxPaidObjHigh.t_Kind in(
                                                                 RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                                 RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                                 RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9
                                                              )
                               )
                            or (
                                  TaxPaidObjHigh.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObjHigh.t_Kind = RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                               )
                            )
                        and TaxPaidObjHigh.t_User <> chr(88)
                        and TaxPaidObjHigh.t_FromOutSyst <> chr(88)
                        and TaxPaidObjHigh.t_Client = p_ClientID
                      union all
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObj.t_Sum0 as t_TaxPaid,
                             chr(0) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where (
                               (
                                  TaxPaidObj.t_Date between p_BeginDate and p_EndDate
                                  and TaxPaidObj.t_Kind in(
                                                             RSI_NPTXC.TXOBJ_PAIDMATERIAL,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL,
                                                             RSI_NPTXC.TXOBJ_PAIDBILL
                                                          )
                               )
                            or (
                                  TaxPaidObj.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObj.t_Kind in(
                                                             RSI_NPTXC.TXOBJ_PAIDMATERIAL_IIS,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS,
                                                             RSI_NPTXC.TXOBJ_DIVPAY_SEC
                                                          )
                               )
                            )
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                   ) q4
                   inner join dnptxobdc_dbt nptxobdc
                   on nptxobdc.t_ObjID = q4.t_ObjID
                   inner join dnptxop_dbt nptxop
                   on nptxop.t_ID = nptxobdc.t_DocID
                   left outer join dnptxop2oplnk_tmp lnk
                   on lnk.t_Parent_DocID = nptxop.t_ID
                      and lnk.t_PartyID = nptxop.t_Client
                   left outer join
                   (
                      dnptxobdc_dbt PlusOBDC
                      inner join dnptxobj_dbt PlusObj
                      on PlusObj.t_ObjID = PlusOBDC.t_ObjID
                         and PlusObj.t_Kind in (
                                                     RSI_NPTXC.TXOBJ_PLUSG_1110,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1530,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1531,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1536,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1532,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1533,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                     RSI_NPTXC.TXOBJ_PLUSG_2640,
                                                     RSI_NPTXC.TXOBJ_PLUSG_2641,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1537,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1539,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1544,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1545,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1549,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1546,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1547,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1548,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1551,
                                                     RSI_NPTXC.TXOBJ_PLUSG_1553,
                                                     RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                                     RSI_NPTXC.TXOBJ_PLUSG_2641_IIS,
                                                     RSI_NPTXC.TXOBJ_PLUSG_2800,
                                                     RSI_NPTXC.TXOBJ_BASEG1_13,
                                                     RSI_NPTXC.TXOBJ_BASEG1_15
                                                  )
                   )
                   on PlusOBDC.t_DocID = lnk.t_DocID
                      and PlusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                   left outer join
                   (
                      dnptxobdc_dbt MinusOBDC
                      inner join dnptxobj_dbt MinusObj
                      on MinusObj.t_ObjID = MinusOBDC.t_ObjID
                         and MinusObj.t_Kind in (
                                                   RSI_NPTXC.TXOBJ_MINUSG_218_1,
                                                   RSI_NPTXC.TXOBJ_MINUSG_219_1,
                                                   RSI_NPTXC.TXOBJ_MINUSG_618,
                                                   RSI_NPTXC.TXOBJ_MINUSG_619,
                                                   RSI_NPTXC.TXOBJ_MINUSG_201,
                                                   RSI_NPTXC.TXOBJ_MINUSG_202,
                                                   RSI_NPTXC.TXOBJ_MINUSG_203,
                                                   RSI_NPTXC.TXOBJ_MINUSG_206,
                                                   RSI_NPTXC.TXOBJ_MINUSG_207,
                                                   RSI_NPTXC.TXOBJ_MINUSG_222,
                                                   RSI_NPTXC.TXOBJ_MINUSG_223,
                                                   RSI_NPTXC.TXOBJ_MINUSG_205,
                                                   RSI_NPTXC.TXOBJ_MINUSG_208,
                                                   RSI_NPTXC.TXOBJ_MINUSG_209,
                                                   RSI_NPTXC.TXOBJ_MINUSG_210,
                                                   RSI_NPTXC.TXOBJ_MINUSG_224,
                                                   RSI_NPTXC.TXOBJ_MINUSG_220,
                                                   RSI_NPTXC.TXOBJ_MINUSG_211,
                                                   RSI_NPTXC.TXOBJ_MINUSG_213,
                                                   RSI_NPTXC.TXOBJ_MINUSG_218,
                                                   RSI_NPTXC.TXOBJ_MINUSG_219,
                                                   RSI_NPTXC.TXOBJ_MINUSG_214,
                                                   RSI_NPTXC.TXOBJ_MINUSG_225,
                                                   RSI_NPTXC.TXOBJ_MINUSG_226,
                                                   RSI_NPTXC.TXOBJ_MINUSG_227,
                                                   RSI_NPTXC.TXOBJ_MINUSG_228,
                                                   RSI_NPTXC.TXOBJ_MINUSG_229,
                                                   RSI_NPTXC.TXOBJ_MINUSG_230,
                                                   RSI_NPTXC.TXOBJ_MINUSG_231,
                                                   RSI_NPTXC.TXOBJ_MINUSG_233,
                                                   RSI_NPTXC.TXOBJ_MINUSG_234,
                                                   RSI_NPTXC.TXOBJ_MINUSG_214_IIS,
                                                   RSI_NPTXC.TXOBJ_MINUSG_250,
                                                   RSI_NPTXC.TXOBJ_MINUSG_251,
                                                   RSI_NPTXC.TXOBJ_MINUSG_252,
                                                   RSI_NPTXC.TXOBJ_MINUSG_241,
                                                   RSI_NPTXC.TXOBJ_MINUSG_236,
                                                   RSI_NPTXC.TXOBJ_MINUSG_235,
                                                   RSI_NPTXC.TXOBJ_MINUSG_239,
                                                   RSI_NPTXC.TXOBJ_MINUSG_240
                                                )
                   )
                   on MinusOBDC.t_DocID = lnk.t_DocID
                      and MinusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
              where p_ByCB = 'X'
                and nptxop.t_DocKind = RSB_SECUR.DL_WRTMONEY
                and nptxop.t_Client = p_ClientID
                and nptxop.t_SubKind_Operation = 20
                and nptxop.t_FlagTax = 'X'
                and q4.t_DatePaid between p_FirstDateIIS and p_EndDate

              union all
              --Списание денежных средств, в которых не удерживали налог
              select 3 as t_TypeOper,
                     nptxop.t_ID as t_DocID,
                     nptxop.t_DocKind,
                     INFORATE_TOTAL as t_RateType,
                     to_date('01.01.0001', 'dd.mm.yyyy') as t_DateTransfer,
                     nptxop.t_OperDate as t_DatePaid,
                     (
                        case
                           when rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then
                              case
                                 when l1.t_IsHighRate = 'X'
                                 then '15'
                                 else '13'
                              end
                           else
                              case
                                 when exists(
                                               select 1
                                               from dnotetext_dbt notetext
                                               where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                 and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                 and notetext.t_NoteKind = 77
                                                 and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                            )
                                 then TO_CHAR(RSB_STRUCT.getDouble(RSI_RSB_KERNEL.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                 else '30'
                              end
                        end
                     ) as t_Rate,
                     (
                        case
                           when l1.t_IsHighRate <> 'X'
                              and PlusObj.t_Kind in (
                                                       RSI_NPTXC.TXOBJ_BASEG1_13,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1110,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1530,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1531,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1536,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1532,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1533,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2640,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2641,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1537,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1539,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1544,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1545,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1546,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1547,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1548,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1549,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1551,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1553,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2641_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2800
                                                    )
                           then PlusObj.t_Sum0
                           when l1.t_IsHighRate = 'X'
                              and PlusObj.t_Kind in (
                                                       RSI_NPTXC.TXOBJ_BASEG1_15,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1110,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1530,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1531,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1536,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1532,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1533,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2640,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2641,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1537,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1539,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1544,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1545,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1546,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1547,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1548,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1549,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1551,
                                                       RSI_NPTXC.TXOBJ_PLUSG_1553,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2641_IIS,
                                                       RSI_NPTXC.TXOBJ_PLUSG_2800
                                                    )
                           then PlusObj.t_Sum0
                           else 0
                        end
                     ) as t_PlusSum,
                     nvl(PlusObj.t_Kind, 0) as t_PlusKind,
                     nvl(PlusObj.t_ObjID, 0) as t_PlusObjID,
                     (
                        case
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                           then nvl(PlusObj.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     nvl(MinusObj.t_Sum0, 0) as t_MinusSum,
                     nvl(MinusObj.t_Kind, 0) as t_MinusKind,
                     nvl(MinusObj.t_ObjID, 0) as t_MinusObjID,
                     0 as t_TaxPaid,
                     0 as t_PaidObjID,
                     0 as t_TaxBaseDiv,
                     0 as t_DivPlus,
                     0 as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     0 as t_TaxToReturnDue,
                     l1.t_ishighrate
              from   (select l.*
                        from (select (case when level = 1 then chr(0) else chr(88) end) as t_ishighrate from dual connect by level <= 2) l 
                       where (rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1 or (l.t_ishighrate = chr(0)))
                     ) l1,
                     dnptxop_dbt nptxop
                     inner join dnptxop2oplnk_tmp lnk on lnk.t_Parent_DocID = nptxop.t_ID and lnk.t_PartyID = nptxop.t_Client
                     left outer join
                     (
                        dnptxobdc_dbt PlusOBDC
                        inner join dnptxobj_dbt PlusObj
                        on PlusObj.t_ObjID = PlusOBDC.t_ObjID
                           and PlusObj.t_Kind in (
                                                    RSI_NPTXC.TXOBJ_PLUSG_1110,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1530,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1531,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1536,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1532,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1533,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                    RSI_NPTXC.TXOBJ_PLUSG_2640,
                                                    RSI_NPTXC.TXOBJ_PLUSG_2641,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1537,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1539,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1544,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1545,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1549,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1546,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1547,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1548,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1551,
                                                    RSI_NPTXC.TXOBJ_PLUSG_1553,
                                                    RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                                    RSI_NPTXC.TXOBJ_PLUSG_2641_IIS,
                                                    RSI_NPTXC.TXOBJ_PLUSG_2800,
                                                    RSI_NPTXC.TXOBJ_BASEG1_13,
                                                    RSI_NPTXC.TXOBJ_BASEG1_15
                                                 )
                     )
                     on PlusOBDC.t_DocID = lnk.t_DocID
                        and PlusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                        and PlusObj.t_Sum0 <> 0
                     left outer join
                     (
                        dnptxobdc_dbt MinusOBDC
                        inner join dnptxobj_dbt MinusObj
                        on MinusObj.t_ObjID = MinusOBDC.t_ObjID
                           and MinusObj.t_Kind in (
                                                     RSI_NPTXC.TXOBJ_MINUSG_218_1,
                                                     RSI_NPTXC.TXOBJ_MINUSG_219_1,
                                                     RSI_NPTXC.TXOBJ_MINUSG_618,
                                                     RSI_NPTXC.TXOBJ_MINUSG_619,
                                                     RSI_NPTXC.TXOBJ_MINUSG_201,
                                                     RSI_NPTXC.TXOBJ_MINUSG_202,
                                                     RSI_NPTXC.TXOBJ_MINUSG_203,
                                                     RSI_NPTXC.TXOBJ_MINUSG_206,
                                                     RSI_NPTXC.TXOBJ_MINUSG_207,
                                                     RSI_NPTXC.TXOBJ_MINUSG_222,
                                                     RSI_NPTXC.TXOBJ_MINUSG_223,
                                                     RSI_NPTXC.TXOBJ_MINUSG_205,
                                                     RSI_NPTXC.TXOBJ_MINUSG_208,
                                                     RSI_NPTXC.TXOBJ_MINUSG_209,
                                                     RSI_NPTXC.TXOBJ_MINUSG_210,
                                                     RSI_NPTXC.TXOBJ_MINUSG_224,
                                                     RSI_NPTXC.TXOBJ_MINUSG_220,
                                                     RSI_NPTXC.TXOBJ_MINUSG_211,
                                                     RSI_NPTXC.TXOBJ_MINUSG_213,
                                                     RSI_NPTXC.TXOBJ_MINUSG_218,
                                                     RSI_NPTXC.TXOBJ_MINUSG_219,
                                                     RSI_NPTXC.TXOBJ_MINUSG_214,
                                                     RSI_NPTXC.TXOBJ_MINUSG_225,
                                                     RSI_NPTXC.TXOBJ_MINUSG_226,
                                                     RSI_NPTXC.TXOBJ_MINUSG_227,
                                                     RSI_NPTXC.TXOBJ_MINUSG_228,
                                                     RSI_NPTXC.TXOBJ_MINUSG_229,
                                                     RSI_NPTXC.TXOBJ_MINUSG_230,
                                                     RSI_NPTXC.TXOBJ_MINUSG_231,
                                                     RSI_NPTXC.TXOBJ_MINUSG_233,
                                                     RSI_NPTXC.TXOBJ_MINUSG_234,
                                                     RSI_NPTXC.TXOBJ_MINUSG_214_IIS,
                                                     RSI_NPTXC.TXOBJ_MINUSG_250,
                                                     RSI_NPTXC.TXOBJ_MINUSG_251,
                                                     RSI_NPTXC.TXOBJ_MINUSG_252,
                                                     RSI_NPTXC.TXOBJ_MINUSG_241,
                                                     RSI_NPTXC.TXOBJ_MINUSG_236,
                                                     RSI_NPTXC.TXOBJ_MINUSG_235,
                                                     RSI_NPTXC.TXOBJ_MINUSG_239,
                                                     RSI_NPTXC.TXOBJ_MINUSG_240
                                                  )
                     )
                     on MinusOBDC.t_DocID = lnk.t_DocID
                        and MinusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                        and MinusObj.t_Sum0 <> 0 
              where p_ByCB = 'X'
                and nptxop.t_DocKind = RSB_SECUR.DL_WRTMONEY
                and nptxop.t_Client = p_ClientID
                and nptxop.t_SubKind_Operation = 20
                and nptxop.t_FlagTax = 'X'
                and nptxop.t_Client = p_ClientID
                and nptxop.t_OperDate BETWEEN p_BeginDate AND p_EndDate
                and Not Exists(select 1 
                                 from dnptxobdc_dbt nptxobdc, dnptxobj_dbt TaxPaidObj
                                where nptxobdc.t_DocID = nptxop.t_ID
                                  and TaxPaidObj.t_ObjID = nptxobdc.t_ObjID
                                  and TaxPaidObj.t_Kind in(
                                                             RSI_NPTXC.TXOBJ_PAIDMATERIAL,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL,
                                                             RSI_NPTXC.TXOBJ_PAIDBILL,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9,
                                                             RSI_NPTXC.TXOBJ_PAIDMATERIAL_IIS,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS,
                                                             RSI_NPTXC.TXOBJ_DIVPAY_SEC,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                                                          )
                                  and TaxPaidObj.t_User <> chr(88)
                                  and TaxPaidObj.t_FromOutSyst <> chr(88)
                                  and TaxPaidObj.t_Client = p_ClientID 
                              )



              -- Операции удержания НДФЛ (в т. ч. из созданные безынтерфейсно в Депозитарии)
              union all
              select (
                        case nptxop.t_SubKind_Operation
                           when DL_TXHOLD_OPTYPE_OUTMONEY
                           then 3
                           when DL_TXHOLD_OPTYPE_OUTAVOIR
                           then 4
                           when DL_TXHOLD_OPTYPE_LUCRE
                           then 5
                           when DL_TXHOLD_OPTYPE_ENDYEAR
                           then 6
                           when DL_TXHOLD_OPTYPE_DIVIDEND
                           then 7
                           else 8  --DL_TXHOLD_OPTYPE_CLOSE
                        end
                     ) as t_TypeOper,
                     nptxop.t_ID as t_DocID,
                     nptxop.t_DocKind as t_DocKind,
                     INFORATE_TOTAL as t_RateType,
                     (
                        case
                           when q6.t_TaxPaid > 0
                           then rsi_rsbcalendar.GetDateAfterWorkDay(
                                                                      (
                                                                         case
                                                                            when RSB_COMMON.GetRegIntValue('COMMON\НДФЛ\СРОК_ПЕРЕЧИСЛЕНИЯ_НДФЛ_ПО_ЦБ') = 0
                                                                            then (
                                                                                    case nptxop.t_SubKind_Operation
                                                                                       when DL_TXHOLD_OPTYPE_ENDYEAR
                                                                                       then (
                                                                                               case
                                                                                                  when p_BeginDate <= to_date('31.12.2022', 'dd.mm.yyyy')
                                                                                                  -- До 01.01.2023
                                                                                                  then to_date('30.01.' || to_char(extract(year from nptxop.t_PrevDate) + 1), 'dd.mm.yyyy')
                                                                                                  -- После 01.01.2023
                                                                                                  else to_date('28.01.' || to_char(extract(year from nptxop.t_PrevDate) + 1), 'dd.mm.yyyy')
                                                                                               end
                                                                                            )
                                                                                       when DL_TXHOLD_OPTYPE_CLOSE
                                                                                       then (
                                                                                               select q_contr.t_DateClose
                                                                                               from (
                                                                                                       select (
                                                                                                                 case
                                                                                                                    when p_BeginDate <= to_date('31.12.2022', 'dd.mm.yyyy')
                                                                                                                    -- До 01.01.2023
                                                                                                                    then (
                                                                                                                            case
                                                                                                                               when sfcontr.t_DateClose != to_date('01.01.0001', 'dd.mm.yyyy') and sfcontr.t_DateClose is not NULL
                                                                                                                               then sfcontr.t_DateClose
                                                                                                                               else q6.t_DatePaid + 30
                                                                                                                            end
                                                                                                                         )
                                                                                                                         -- После 01.01.2023
                                                                                                                    else (
                                                                                                                            case
                                                                                                                               when sfcontr.t_DateClose != to_date('01.01.0001', 'dd.mm.yyyy') and sfcontr.t_DateClose is not NULL
                                                                                                                               then to_date('28.' || case
                                                                                                                                                        when extract(month from sfcontr.t_DateClose) < 12
                                                                                                                                                        then to_char(extract(month from sfcontr.t_DateClose) + 1, '00') || '.' || to_char(sfcontr.t_DateClose, 'yyyy')
                                                                                                                                                        else '01.' || to_char(extract(year from sfcontr.t_DateClose) + 1, '0000')
                                                                                                                                                     end,
                                                                                                                                                     'dd.mm.yyyy'
                                                                                                                                           )
                                                                                                                               else to_date('28.' || case
                                                                                                                                                        when extract(month from q6.t_DatePaid) < 12
                                                                                                                                                        then to_char(extract(month from q6.t_DatePaid) + 1, '00') || '.' || to_char(q6.t_DatePaid, 'yyyy')
                                                                                                                                                        else '01.' || to_char(extract(year from q6.t_DatePaid) + 1, '0000')
                                                                                                                                                     end,
                                                                                                                                                     'dd.mm.yyyy'
                                                                                                                                           )
                                                                                                                            end
                                                                                                                         )
                                                                                                                 end
                                                                                                              ) as t_DateClose
                                                                                                       from dsfcontr_dbt sfcontr, ddlcontr_dbt dlcontr
                                                                                                       where sfcontr.t_PartyID = nptxop.t_Client
                                                                                                         and (
                                                                                                                (
                                                                                                                   exists(
                                                                                                                            select 1
                                                                                                                            from ddlcontr_dbt dlcontrsub, dsfcontr_dbt sfcontrsub
                                                                                                                            where sfcontrsub.t_ID = dlcontrsub.t_SfcontrID
                                                                                                                              and sfcontrsub.t_PartyID = sfcontr.t_PartyID
                                                                                                                         )
                                                                                                                   and dlcontr.t_IIS = nptxop.t_IIS
                                                                                                                   and sfcontr.t_ID = dlcontr.t_SfContrID
                                                                                                                )
                                                                                                             or (
                                                                                                                   not exists(
                                                                                                                                select 1
                                                                                                                                from ddlcontr_dbt dlcontrsub, dsfcontr_dbt sfcontrsub
                                                                                                                                where sfcontrsub.t_ID = dlcontrsub.t_SfContrID
                                                                                                                                  and sfcontrsub.t_PartyID = sfcontr.t_PartyID
                                                                                                                             )
                                                                                                                   and (
                                                                                                                          (
                                                                                                                             nptxop.t_IIS = chr(88)
                                                                                                                             and rsi_npto.CheckContrIIS(sfcontr.t_ID) = 1
                                                                                                                          )
                                                                                                                       or (
                                                                                                                             nptxop.t_IIS = chr(0)
                                                                                                                             and rsi_npto.CheckContrIIS(sfcontr.t_ID) = 0
                                                                                                                          )
                                                                                                                       )
                                                                                                                )
                                                                                                             )
                                                                                                       order by sfcontr.t_DateBegin DESC, sfcontr.t_DateClose DESC
                                                                                                    ) q_contr
                                                                                               where ROWNUM = 1
                                                                                            )
                                                                                       else q6.t_DatePaid + 30
                                                                                    end
                                                                                 )
                                                                            else (
                                                                                    case
                                                                                       when extract(day from q6.t_DatePaid) < 23
                                                                                       then to_date('28.' || to_char(q6.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                       when extract(day from q6.t_DatePaid) >= 23 and extract(month from q6.t_DatePaid) = 12
                                                                                       then p_LastWorkDayYear
                                                                                       else to_date('28.' || to_char(ADD_MONTHS(q6.t_DatePaid, 1), 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                    end
                                                                                 )
                                                                         end
                                                                      ), 0
                                                                   )
                           else to_date('01.01.0001', 'dd.mm.yyyy')
                        end
                     ) as t_DateTransfer,
                     q6.t_DatePaid,
                     (
                        case
                           when rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then
                              case
                                 when q6.t_IsHighRate = 'X'
                                 then '15'
                                 else '13'
                              end
                           else
                              case
                                 when exists(
                                               select 1
                                               from dnotetext_dbt notetext
                                               where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                 and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                 and notetext.t_NoteKind = 77
                                                 and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                            )
                                 then TO_CHAR(RSB_STRUCT.getDouble(RSI_RSB_KERNEL.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                 else '30'
                              end
                        end
                     ) as t_Rate,
                     nvl(PlusObj.t_Sum0, 0) as t_PlusSum,
                     nvl(PlusObj.t_Kind, 0) as t_PlusKind,
                     nvl(PlusObj.t_ObjID, 0) as t_PlusObjID,
                     (
                        case
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                           then nvl(PlusObj.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     nvl(MinusObj.t_Sum0, 0) as t_MinusSum,
                     nvl(MinusObj.t_Kind, 0) as t_MinusKind,
                     nvl(MinusObj.t_ObjID, 0) as t_MinusObjID,
                     (
                        case
                           when q6.t_TaxPaid > 0
                           then q6.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxPaid,
                     q6.t_ObjID as t_PaidObjID,
                     0 as t_TaxBaseDiv,
                     0 as t_DivPlus,
                     0 as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     (
                        case
                           when q6.t_TaxPaid < 0
                           then q6.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxToReturnDue,
                     q6.t_IsHighRate
              from (
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObj.t_Sum0 as t_TaxPaid,
                             chr(88) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where TaxPaidObj.t_Kind in(
                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9,
                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                                                )
                            
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                      union all
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObj.t_Sum0 as t_TaxPaid,
                             chr(0) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where TaxPaidObj.t_Kind in(
                                                  RSI_NPTXC.TXOBJ_PAIDMATERIAL,
                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                  RSI_NPTXC.TXOBJ_PAIDSPECIAL,
                                                  RSI_NPTXC.TXOBJ_PAIDBILL,
                                                  RSI_NPTXC.TXOBJ_PAIDMATERIAL_IIS,
                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                  RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS,
                                                  RSI_NPTXC.TXOBJ_DIVPAY_SEC
                                                )
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                   ) q6
                   inner join dnptxobdc_dbt nptxobdc
                   on nptxobdc.t_ObjID = q6.t_ObjID
                   inner join dnptxop_dbt nptxop
                   on nptxop.t_ID = nptxobdc.t_DocID
                   left outer join ddppmobj_dbt dppmobj
                   on dppmobj.t_ObjKind = nptxop.t_DocKind
                      and dppmobj.t_ObjID = nptxop.t_ID
                      and (
                             p_ByCB = 'X'
                             and dppmobj.t_ID IS NULL
                             or p_ByDepo = 'X'
                             and not dppmobj.t_ID IS NULL
                          )
                   left outer join dnptxop2oplnk_tmp lnk
                   on lnk.t_Parent_DocID = nptxop.t_ID
                      and lnk.t_PartyID = nptxop.t_Client
                   left outer join
                   (
                      dnptxobdc_dbt PlusOBDC
                      inner join dnptxobj_dbt PlusObj
                      on PlusObj.t_ObjID = PlusOBDC.t_ObjID
                         and PlusObj.t_Kind in (
                                                  RSI_NPTXC.TXOBJ_BASEG1_13,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1530,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1531,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1536,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1532,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1533,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2640,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2641,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1537,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1539,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1544,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1545,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1549,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1546,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1547,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1548,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1551,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1553,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2641_IIS,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2800
                                               )
                   )
                   on PlusOBDC.t_DocID = lnk.t_DocID
                      and PlusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                      and PlusObj.t_Sum0 <> 0
                   left outer join
                   (
                      dnptxobdc_dbt MinusOBDC
                      inner join dnptxobj_dbt MinusObj
                      on MinusObj.t_ObjID = MinusOBDC.t_ObjID
                         and MinusObj.t_Kind in (
                                                   RSI_NPTXC.TXOBJ_MINUSG_218_1,
                                                   RSI_NPTXC.TXOBJ_MINUSG_219_1,
                                                   RSI_NPTXC.TXOBJ_MINUSG_618,
                                                   RSI_NPTXC.TXOBJ_MINUSG_201,
                                                   RSI_NPTXC.TXOBJ_MINUSG_202,
                                                   RSI_NPTXC.TXOBJ_MINUSG_203,
                                                   RSI_NPTXC.TXOBJ_MINUSG_206,
                                                   RSI_NPTXC.TXOBJ_MINUSG_207,
                                                   RSI_NPTXC.TXOBJ_MINUSG_222,
                                                   RSI_NPTXC.TXOBJ_MINUSG_223,
                                                   RSI_NPTXC.TXOBJ_MINUSG_205,
                                                   RSI_NPTXC.TXOBJ_MINUSG_208,
                                                   RSI_NPTXC.TXOBJ_MINUSG_209,
                                                   RSI_NPTXC.TXOBJ_MINUSG_210,
                                                   RSI_NPTXC.TXOBJ_MINUSG_224,
                                                   RSI_NPTXC.TXOBJ_MINUSG_220,
                                                   RSI_NPTXC.TXOBJ_MINUSG_211,
                                                   RSI_NPTXC.TXOBJ_MINUSG_213,
                                                   RSI_NPTXC.TXOBJ_MINUSG_218,
                                                   RSI_NPTXC.TXOBJ_MINUSG_219,
                                                   RSI_NPTXC.TXOBJ_MINUSG_214,
                                                   RSI_NPTXC.TXOBJ_MINUSG_225,
                                                   RSI_NPTXC.TXOBJ_MINUSG_226,
                                                   RSI_NPTXC.TXOBJ_MINUSG_227,
                                                   RSI_NPTXC.TXOBJ_MINUSG_228,
                                                   RSI_NPTXC.TXOBJ_MINUSG_229,
                                                   RSI_NPTXC.TXOBJ_MINUSG_230,
                                                   RSI_NPTXC.TXOBJ_MINUSG_239,
                                                   RSI_NPTXC.TXOBJ_MINUSG_231,
                                                   RSI_NPTXC.TXOBJ_MINUSG_233,
                                                   RSI_NPTXC.TXOBJ_MINUSG_234,
                                                   RSI_NPTXC.TXOBJ_MINUSG_240,
                                                   RSI_NPTXC.TXOBJ_MINUSG_214_IIS,
                                                   RSI_NPTXC.TXOBJ_MINUSG_235,
                                                   RSI_NPTXC.TXOBJ_MINUSG_236,
                                                   RSI_NPTXC.TXOBJ_MINUSG_619
                                                )
                   )
                   on MinusOBDC.t_DocID = lnk.t_DocID
                      and MinusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                      and MinusObj.t_Sum0 <> 0
              where nptxop.t_DocKind = RSB_SECUR.DL_HOLDNDFL
                and nptxop.t_Subkind_Operation <> DL_TXHOLD_OPTYPE_TAXREF
                and (   (nptxop.t_SubKind_Operation <> DL_TXHOLD_OPTYPE_ENDYEAR and nptxop.t_OperDate between p_BeginDate and p_EndDate)
                     OR (nptxop.t_SubKind_Operation = DL_TXHOLD_OPTYPE_ENDYEAR AND EXTRACT(YEAR FROM nptxop.t_PrevDate) = EXTRACT(YEAR FROM p_EndDate))
                    )
                and nptxop.t_Client = p_ClientID

              -- Операции удержания НДФЛ (в т. ч. из созданные безынтерфейсно в Депозитарии), в которых ничего не удерживали
              union all
              select (
                        case nptxop.t_SubKind_Operation
                           when DL_TXHOLD_OPTYPE_OUTMONEY
                           then 3
                           when DL_TXHOLD_OPTYPE_OUTAVOIR
                           then 4
                           when DL_TXHOLD_OPTYPE_LUCRE
                           then 5
                           when DL_TXHOLD_OPTYPE_ENDYEAR
                           then 6
                           when DL_TXHOLD_OPTYPE_DIVIDEND
                           then 7
                           else 8  --DL_TXHOLD_OPTYPE_CLOSE
                        end
                     ) as t_TypeOper,
                     nptxop.t_ID as t_DocID,
                     nptxop.t_DocKind as t_DocKind,
                     INFORATE_TOTAL as t_RateType,
                     to_date('01.01.0001', 'dd.mm.yyyy') as t_DateTransfer,
                     nptxop.t_OperDate as t_DatePaid,
                     (
                        case
                           when rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then
                              case
                                 when l1.t_ishighrate = 'X'
                                 then '15'
                                 else '13'
                              end
                           else
                              case
                                 when exists(
                                               select 1
                                               from dnotetext_dbt notetext
                                               where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                 and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                 and notetext.t_NoteKind = 77
                                                 and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                            )
                                 then TO_CHAR(RSB_STRUCT.getDouble(RSI_RSB_KERNEL.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                 else '30'
                              end
                        end
                     ) as t_Rate,
                     nvl(PlusObj.t_Sum0, 0) as t_PlusSum,
                     nvl(PlusObj.t_Kind, 0) as t_PlusKind,
                     nvl(PlusObj.t_ObjID, 0) as t_PlusObjID,
                     (
                        case
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                           then nvl(PlusObj.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     nvl(MinusObj.t_Sum0, 0) as t_MinusSum,
                     nvl(MinusObj.t_Kind, 0) as t_MinusKind,
                     nvl(MinusObj.t_ObjID, 0) as t_MinusObjID,
                     0 as t_TaxPaid,
                     0 as t_PaidObjID,
                     0 as t_TaxBaseDiv,
                     0 as t_DivPlus,
                     0 as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     0 as t_TaxToReturnDue,
                     l1.t_ishighrate 
              from (select l.*
                      from (select (case when level = 1 then chr(0) else chr(88) end) as t_ishighrate from dual connect by level <= 2) l 
                     where (rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1 or (l.t_ishighrate = chr(0)))
                   ) l1,
                   dnptxop_dbt nptxop
                   inner join dnptxop2oplnk_tmp lnk on lnk.t_Parent_DocID = nptxop.t_ID and lnk.t_PartyID = nptxop.t_Client
                   left outer join
                   (
                      dnptxobdc_dbt PlusOBDC
                      inner join dnptxobj_dbt PlusObj
                      on PlusObj.t_ObjID = PlusOBDC.t_ObjID
                         and PlusObj.t_Kind in (
                                                  RSI_NPTXC.TXOBJ_BASEG1_13,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1530,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1531,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1536,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1532,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1533,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2640,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2641,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1537,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1539,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1544,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1545,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1549,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1546,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1547,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1548,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1551,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1553,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2641_IIS,
                                                  RSI_NPTXC.TXOBJ_PLUSG_2800
                                               )
                   )
                   on PlusOBDC.t_DocID = lnk.t_DocID
                      and PlusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                      and PlusObj.t_Sum0 <> 0
                   left outer join
                   (
                      dnptxobdc_dbt MinusOBDC
                      inner join dnptxobj_dbt MinusObj
                      on MinusObj.t_ObjID = MinusOBDC.t_ObjID
                         and MinusObj.t_Kind in (
                                                   RSI_NPTXC.TXOBJ_MINUSG_218_1,
                                                   RSI_NPTXC.TXOBJ_MINUSG_219_1,
                                                   RSI_NPTXC.TXOBJ_MINUSG_618,
                                                   RSI_NPTXC.TXOBJ_MINUSG_201,
                                                   RSI_NPTXC.TXOBJ_MINUSG_202,
                                                   RSI_NPTXC.TXOBJ_MINUSG_203,
                                                   RSI_NPTXC.TXOBJ_MINUSG_206,
                                                   RSI_NPTXC.TXOBJ_MINUSG_207,
                                                   RSI_NPTXC.TXOBJ_MINUSG_222,
                                                   RSI_NPTXC.TXOBJ_MINUSG_223,
                                                   RSI_NPTXC.TXOBJ_MINUSG_205,
                                                   RSI_NPTXC.TXOBJ_MINUSG_208,
                                                   RSI_NPTXC.TXOBJ_MINUSG_209,
                                                   RSI_NPTXC.TXOBJ_MINUSG_210,
                                                   RSI_NPTXC.TXOBJ_MINUSG_224,
                                                   RSI_NPTXC.TXOBJ_MINUSG_220,
                                                   RSI_NPTXC.TXOBJ_MINUSG_211,
                                                   RSI_NPTXC.TXOBJ_MINUSG_213,
                                                   RSI_NPTXC.TXOBJ_MINUSG_218,
                                                   RSI_NPTXC.TXOBJ_MINUSG_219,
                                                   RSI_NPTXC.TXOBJ_MINUSG_214,
                                                   RSI_NPTXC.TXOBJ_MINUSG_225,
                                                   RSI_NPTXC.TXOBJ_MINUSG_226,
                                                   RSI_NPTXC.TXOBJ_MINUSG_227,
                                                   RSI_NPTXC.TXOBJ_MINUSG_228,
                                                   RSI_NPTXC.TXOBJ_MINUSG_229,
                                                   RSI_NPTXC.TXOBJ_MINUSG_230,
                                                   RSI_NPTXC.TXOBJ_MINUSG_239,
                                                   RSI_NPTXC.TXOBJ_MINUSG_231,
                                                   RSI_NPTXC.TXOBJ_MINUSG_233,
                                                   RSI_NPTXC.TXOBJ_MINUSG_234,
                                                   RSI_NPTXC.TXOBJ_MINUSG_240,
                                                   RSI_NPTXC.TXOBJ_MINUSG_214_IIS,
                                                   RSI_NPTXC.TXOBJ_MINUSG_235,
                                                   RSI_NPTXC.TXOBJ_MINUSG_236,
                                                   RSI_NPTXC.TXOBJ_MINUSG_619
                                                )
                   )
                   on MinusOBDC.t_DocID = lnk.t_DocID
                      and MinusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                      and MinusObj.t_Sum0 <> 0
              where nptxop.t_DocKind = RSB_SECUR.DL_HOLDNDFL
                and nptxop.t_SubKind_Operation = DL_TXHOLD_OPTYPE_ENDYEAR
                and EXTRACT(YEAR FROM nptxop.t_PrevDate) = EXTRACT(YEAR FROM p_EndDate)
                and nptxop.t_Client = p_ClientID
                and Not Exists(select 1 
                                 from dnptxobdc_dbt nptxobdc, dnptxobj_dbt TaxPaidObj
                                where nptxobdc.t_DocID = nptxop.t_ID
                                  and TaxPaidObj.t_ObjID = nptxobdc.t_ObjID
                                  and TaxPaidObj.t_Kind in(
                                                             RSI_NPTXC.TXOBJ_PAIDMATERIAL,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL,
                                                             RSI_NPTXC.TXOBJ_PAIDBILL,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9,
                                                             RSI_NPTXC.TXOBJ_PAIDMATERIAL_IIS,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS,
                                                             RSI_NPTXC.TXOBJ_DIVPAY_SEC,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                                                          )
                                  and TaxPaidObj.t_User <> chr(88)
                                  and TaxPaidObj.t_FromOutSyst <> chr(88)
                                  and TaxPaidObj.t_Client = p_ClientID 
                              )
              
              
              union all

              --Операции возврата налога
              select 10 as t_TypeOper,
                     nptxop.t_ID as t_DocID,
                     nptxop.t_DocKind as t_DocKind,
                     INFORATE_TOTAL as t_RateType,
                     nptxop.t_OperDate as t_DateTransfer,
                     to_date('01.01.0001', 'dd.mm.yyyy') as t_DatePaid,
                     (
                        case
                           when rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then
                              case
                                 when q10.t_IsHighRate = 'X'
                                 then '15'
                                 else '13'
                              end
                           else
                              case
                                 when exists(
                                               select 1
                                               from dnotetext_dbt notetext
                                               where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                 and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                 and notetext.t_NoteKind = 77
                                                 and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                            )
                                 then TO_CHAR(RSB_STRUCT.getDouble(RSI_RSB_KERNEL.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                 else '30'
                              end
                        end
                     ) as t_Rate,
                     0 as t_PlusSum,
                     0 as t_PlusKind,
                     0 as t_PlusObjID,
                     to_date('01.01.0001', 'dd.mm.yyyy') as t_PlusDate,
                     0 as t_MinusSum,
                     0 as t_MinusKind,
                     0 as t_MinusObjID,
                     0 as t_TaxPaid,
                     q10.t_ObjID as t_PaidObjID,
                     0 as t_TaxBaseDiv,
                     0 as t_DivPlus,
                     0 as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     q10.t_TaxPaid as t_TaxToReturnDue,
                     q10.t_IsHighRate
              from (
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObj.t_Sum0 as t_TaxPaid,
                             chr(88) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where TaxPaidObj.t_Kind IN (RSI_NPTXC.TXOBJ_PAIDGENERAL_15,
                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS)
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                      union all
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObj.t_Sum0 as t_TaxPaid,
                             chr(0) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Date as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where TaxPaidObj.t_Kind IN (RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                  RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS)
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                   ) q10
                   inner join dnptxobdc_dbt nptxobdc
                   on nptxobdc.t_ObjID = q10.t_ObjID
                   inner join dnptxop_dbt nptxop
                   on nptxop.t_ID = nptxobdc.t_DocID
                   left outer join dnptxop2oplnk_tmp lnk
                   on lnk.t_Parent_DocID = nptxop.t_ID
                      and lnk.t_PartyID = nptxop.t_Client
              where nptxop.t_DocKind = RSB_SECUR.DL_HOLDNDFL
                and nptxop.t_Subkind_Operation = DL_TXHOLD_OPTYPE_TAXREF
                and EXTRACT(YEAR FROM nptxop.t_PrevDate) = EXTRACT(YEAR FROM p_EndDate)
              union all

              --Операции расчета выплат в депозитарии
              select (
                        case dpcorpop.t_PaymentType
                           when RSB_DEPO.DP_CRPPAYMENTTYPE_DIVIDENDS
                           then 7
                           when RSB_DEPO.DP_CRPPAYMENTTYPE_COUPON
                           then 1
                           else 3
                        end
                     ) as t_TypeOper,
                     dpcorpop.t_DocumentID as t_DocID,
                     dpcorpop.t_DocKind as t_DocKind,
                     (
                        case
                           when q7.t_PaidGeneralSum > 0 and q7.t_PaidSpecialSum = 0
                           then INFORATE_TOTAL
                           else INFORATE9_15
                        end
                     ) as t_RateType,
                     (
                        case
                           when q7.t_PaidGeneralSum >= 0 and q7.t_PaidSpecialSum >= 0
                           then rsi_rsbcalendar.GetDateAfterWorkDay(
                                                                      case
                                                                         when p_BeginDate <= to_date('31.12.2022', 'dd.mm.yyyy')
                                                                         --До 31.12.2022
                                                                         then q7.t_DatePaid + 30
                                                                         --После 31.12.2022
                                                                         else (
                                                                                 case
                                                                                    when RSB_COMMON.GetRegIntValue('COMMON\НДФЛ\СРОК_ПЕРЕЧИСЛЕНИЯ_НДФЛ_ПО_ЦБ') = 0
                                                                                    then q7.t_DatePaid + 30
                                                                                    else (
                                                                                            case
                                                                                               when extract(day from q7.t_DatePaid) < 23
                                                                                               then to_date('28.' || to_char(q7.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                               when extract(day from q7.t_DatePaid) >= 23 and extract(month from q7.t_DatePaid) = 12
                                                                                               then p_LastWorkDayYear
                                                                                               else to_date('28.' || to_char(ADD_MONTHS(q7.t_DatePaid, 1), 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                            end
                                                                                         )
                                                                                 end
                                                                              )
                                                                      end, 0
                                                                   )
                           else to_date('01.01.0001', 'dd.mm.yyyy')
                        end
                     ) as t_DateTransfer,
                     q7.t_DatePaid,
                     (
                        case
                           when rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then (
                                   case
                                      when q7.t_PaidGeneralSum > 0 and q7.t_PaidSpecialSum = 0
                                      then (
                                              case
                                                 when q7.t_IsHighRate = 'X'
                                                 then '15'
                                                 else '13'
                                              end
                                           )
                                      else '9'
                                   end
                                )
                           else
                              case
                                 when exists(
                                               select 1
                                               from dnotetext_dbt notetext
                                               where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                 and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                 and notetext.t_NoteKind = 77
                                                 and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                            )
                                 then TO_CHAR(RSB_STRUCT.getDouble(RSI_RSB_KERNEL.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                 else (
                                         case
                                            when q7.t_PaidGeneralSum > 0 and q7.t_PaidSpecialSum = 0
                                            then '30'
                                            else '15'
                                         end
                                      )
                              end
                        end
                     ) as t_Rate,
                     (
                        case
                           when (
                                   (
                                      q7.t_PaidGeneralSum = 0 and q7.t_PaidSpecialSum > 0
                                      and (
                                             (
                                                rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                                                and PlusObj.t_Kind in (
                                                                         RSI_NPTXC.TXOBJ_PLUS9_1110,
                                                                         RSI_NPTXC.TXOBJ_PLUS9_1110_IIS
                                                                      )
                                             )
                                          or (
                                                rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) <> 1
                                                and PlusObj.t_Kind = RSI_NPTXC.TXOBJ_PLUS15_1010
                                             )
                                          )
                                   )
                                or (
                                      q7.t_PaidGeneralSum > 0 and q7.t_PaidSpecialSum = 0
                                      and PlusObj.t_Kind in (
                                                               RSI_NPTXC.TXOBJ_PLUSG_1110,
                                                               RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                               RSI_NPTXC.TXOBJ_PLUSG_1011_IIS
                                                            )
                                   )
                                or PlusObj.t_Kind in (
                                                        RSI_NPTXC.TXOBJ_PLUS35_3023
                                                     )
                                )
                           then PlusObj.t_Sum0
                           else 0
                        end
                     ) as t_PlusSum,
                     nvl(PlusObj.t_Kind, 0) as t_PlusKind,
                     nvl(PlusObj.t_ObjID, 0) as t_PlusObjID,
                     (
                        case
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                           then nvl(PlusObj.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     (
                        case
                           when MinusObj.t_Kind is not NULL
                           then (
                                   case
                                      when (
                                              q7.t_PaidGeneralSum = 0 and q7.t_PaidSpecialSum > 0
                                              and MinusObj.t_Kind in (
                                                                        RSI_NPTXC.TXOBJ_MINUS15_601
                                                                     )
                                           )
                                           or MinusObj.t_Kind in (
                                                                    RSI_NPTXC.TXOBJ_MINUSG_601
                                                                 )
                                      then MinusObj.t_Sum0
                                      else 0
                                   end
                                )
                           else 0
                        end
                     ) as t_MinusSum,
                     nvl(MinusObj.t_Kind, 0) as t_MinusKind,
                     nvl(MinusObj.t_ObjID, 0) as t_MinusObjID,
                     (
                        case
                           when q7.t_PaidGeneralSum > 0 and q7.t_PaidSpecialSum = 0
                           then q7.t_PaidGeneralSum
                           when q7.t_PaidGeneralSum = 0 and q7.t_PaidSpecialSum > 0
                           then q7.t_PaidSpecialSum
                           else 0
                        end
                     ) as t_TaxPaid,
                     q7.t_ObjID as t_PaidObjID,
                     (
                        select nvl(sum(BaseSpecialDivObj.t_Sum0), 0)
                        from dnptxobj_dbt BaseSpecialDivObj,
                             ddppmobj_dbt dppmobjBaseSpecialDiv
                        where dppmobjBaseSpecialDiv.t_PmAccID = dppmacc.t_ID
                          and dppmobjBaseSpecialDiv.t_ObjKind = RSB_SECUR.OBJTYPE_NPTXOBJ
                          and BaseSpecialDivObj.t_ObjID = dppmobjBaseSpecialDiv.t_ObjID
                          and (
                                 (
                                    q7.t_IsHighRate <> 'X'
                                    and BaseSpecialDivObj.t_Kind in (
                                                                       RSI_NPTXC.TXOBJ_BASESPECIAL_DIV
                                                                    )
                                 )
                              or (
                                    q7.t_IsHighRate = 'X'
                                    and BaseSpecialDivObj.t_Kind in (
                                                                       RSI_NPTXC.TXOBJ_BASESPECIAL_DIV_15
                                                                    )
                                 )
                              )
                          and BaseSpecialDivObj.t_Date between p_FirstDateIIS and p_MaxOperDate
                     ) as t_TaxBaseDiv,
                     (
                        select nvl(sum(DivPlusObj.t_Sum0), 0)
                        from dnptxobj_dbt DivPlusObj,
                             ddppmobj_dbt dppmobjDivPlus
                        where dppmobjDivPlus.t_PmAccID = dppmacc.t_ID
                          and dppmobjDivPlus.t_ObjKind = RSB_SECUR.OBJTYPE_NPTXOBJ
                          and DivPlusObj.t_ObjID = dppmobjDivPlus.t_ObjID
                          and (
                                 (
                                    q7.t_IsHighRate = 'X'
                                    and DivPlusObj.t_Kind in (
                                                                RSI_NPTXC.TXOBJ_BASEG1_15
                                                             )
                                 )
                              or (
                                    q7.t_IsHighRate <> 'X'
                                    and DivPlusObj.t_Kind in (
                                                                RSI_NPTXC.TXOBJ_BASEG1_13
                                                             )
                                 )
                              )
                     ) as t_DivPlus,
                     (
                        select nvl(sum(DivPaySec0_Obj.t_Sum0), 0)
                        from dnptxobj_dbt DivPaySec0_Obj,
                             ddppmobj_dbt dppmobjDivPaySec0
                        where dppmobjDivPaySec0.t_PmAccID = dppmacc.t_ID
                          and dppmobjDivPaySec0.t_ObjKind = RSB_SECUR.OBJTYPE_NPTXOBJ
                          and DivPaySec0_Obj.t_ObjID = dppmobjDivPaySec0.t_ObjID
                          and DivPaySec0_Obj.t_Kind in (
                                                          RSI_NPTXC.TXOBJ_DIVPAY_SEC_0
                                                       )
                          and DivPaySec0_Obj.t_Date between p_FirstDateIIS and p_MaxOperDate
                     ) as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     (
                        case
                           when q7.t_PaidGeneralSum < 0 and q7.t_PaidSpecialSum = 0
                           then q7.t_PaidGeneralSum
                           when q7.t_PaidGeneralSum = 0 and q7.t_PaidSpecialSum < 0
                           then q7.t_PaidSpecialSum
                           else 0
                        end
                     ) as t_TaxToReturnDue,
                     q7.t_IsHighRate
              from (
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             PaidGeneralObj.t_Sum0 as t_PaidGeneralSum,
                             0 as t_PaidSpecialSum,
                             chr(88) as t_IsHighRate,
                             PaidGeneralObj.t_ObjID,
                             PaidGeneralObj.t_AnaliticKind1,
                             PaidGeneralObj.t_Analitic1,
                             PaidGeneralObj.t_Date as t_DatePaid
                      from dnptxobj_dbt PaidGeneralObj
                      where (
                               (
                                  PaidGeneralObj.t_Date between p_BeginDate and p_EndDate
                                  and PaidGeneralObj.t_Kind in(
                                                                 RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                                 RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                                 RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9
                                                              )
                               )
                            or (
                                  PaidGeneralObj.t_Date between p_FirstDateIIS and p_EndDate
                                  and PaidGeneralObj.t_Kind = RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                               )
                            )
                        and PaidGeneralObj.t_User <> chr(88)
                        and PaidGeneralObj.t_FromOutSyst <> chr(88)
                        and PaidGeneralObj.t_Client = p_ClientID
                      union all
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             (
                                case
                                   when TaxPaidObj.t_Kind in(
                                                               RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                               RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                               RSI_NPTXC.TXOBJ_DIVPAY_SEC
                                                            )
                                   then TaxPaidObj.t_Sum0
                                   else 0
                                end
                             ) as t_PaidGeneralSum,
                             (
                                case
                                   when TaxPaidObj.t_Kind in(
                                                               RSI_NPTXC.TXOBJ_PAIDSPECIAL,
                                                               RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS
                                                            )
                                   then TaxPaidObj.t_Sum0
                                   else 0
                                end
                             ) as t_PaidSpecialSum,
                             chr(0) as t_IsHighRate,
                             TaxPaidObj.t_ObjID,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             (
                                case
                                   when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                                   then TaxPaidObj.t_Date
                                   else p_CloseDateIIS
                                end
                             ) as t_DatePaid
                      from dnptxobj_dbt TaxPaidObj
                      where (
                               (
                                  TaxPaidObj.t_Date between p_BeginDate and p_EndDate
                                  and TaxPaidObj.t_Kind in(
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL
                                                          )
                               )
                            or (
                                  TaxPaidObj.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObj.t_Kind in(
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                             RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS,
                                                             RSI_NPTXC.TXOBJ_DIVPAY_SEC
                                                          )
                               )
                            )
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                   ) q7
                   inner join ddppmobj_dbt dppmobj
                   on dppmobj.t_ObjKind = RSB_SECUR.OBJTYPE_NPTXOBJ
                      and dppmobj.t_ObjID = q7.t_ObjID
                   inner join ddppmacc_dbt dppmacc
                   on dppmacc.t_ID = dppmobj.t_PmAccID
                   inner join ddpcorpop_dbt dpcorpop
                   on dpcorpop.t_DocumentID = dppmacc.t_DocumentID
                   inner join dspground_dbt spground
                   on spground.t_SpGroundID = dpcorpop.t_SpGroundID
                   left outer join
                   (
                      ddppmobj_dbt dppmobjPlus
                      inner join dnptxobj_dbt PlusObj
                      on PlusObj.t_ObjID = dppmobjPlus.t_ObjID
                         and PlusObj.t_Kind in (
                                                  RSI_NPTXC.TXOBJ_PLUS15_1010,
                                                  RSI_NPTXC.TXOBJ_PLUS9_1110,
                                                  RSI_NPTXC.TXOBJ_PLUS9_1110_IIS,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1110,
                                                  RSI_NPTXC.TXOBJ_PLUS35_3023,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                  RSI_NPTXC.TXOBJ_PLUSG_1011_IIS
                                               )
                   )
                   on dppmobjPlus.t_PmAccID = dppmacc.t_ID
                      and dppmobjPlus.t_ObjKind = RSB_SECUR.OBJTYPE_NPTXOBJ
                      and PlusObj.t_Date between p_FirstDateIIS and p_EndDate
                   left outer join
                   (
                      ddppmobj_dbt dppmobjMinus
                      inner join dnptxobj_dbt MinusObj
                      on MinusObj.t_ObjID = dppmobjMinus.t_ObjID
                         and MinusObj.t_Kind in (
                                                   RSI_NPTXC.TXOBJ_MINUS15_601,
                                                   RSI_NPTXC.TXOBJ_MINUSG_601
                                                )
                   )
                   on dppmobjMinus.t_PmAccID = dppmacc.t_ID
                      and dppmobjMinus.t_ObjKind = RSB_SECUR.OBJTYPE_NPTXOBJ
                      and MinusObj.t_Date between p_FirstDateIIS and p_EndDate
              where p_ByDepo = 'X'
                and q7.t_AnaliticKind1 in(
                                            RSI_NPTXC.TXOBJ_KIND1095,
                                            RSI_NPTXC.TXOBJ_KIND1098
                                         )
                and dpcorpop.t_DocKind = RSB_SECUR.SP_DEPOPER_DIVIDEND
                and q7.t_DatePaid between p_FirstDateIIS and p_EndDate
              -- Операции погашения собственных векселей банка, погашение собственных векселей банка, выданных другим фииалом, выкупа собственных векселей,
              -- зачет взаимных требований, мена векселей
              union all
              select 9 as t_TypeOper,
                     q8.t_ContractID as t_DocID,
                     q8.t_DocKind,
                     INFORATE_TOTAL as t_RateType,
                     (
                        case
                           when q8.t_TaxPaid > 0
                           then rsi_rsbcalendar.GetDateAfterWorkDay(
                                                                      case
                                                                         when p_BeginDate <= to_date('31.12.2022', 'dd.mm.yyyy')
                                                                         --До 31.12.2022
                                                                         then q8.t_DatePaid + 1
                                                                         --После 31.12.2022
                                                                         else (
                                                                                 case
                                                                                    when q8.t_Kind in(
                                                                                                        RSI_NPTXC.TXOBJ_PAIDBILL,
                                                                                                        RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9
                                                                                                     )
                                                                                    then (
                                                                                            case
                                                                                               when q8.t_DatePaid < to_date('23.' || to_char(q8.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                               then to_date('28.01.' || to_char(q8.t_DatePaid, 'yyyy'), 'dd.mm.yyyy')
                                                                                               when q8.t_DatePaid between to_date('23.12.' || to_char(q8.t_DatePaid, 'yyyy'), 'dd.mm.yyyy') and to_date('31.12.' || to_char(q8.t_DatePaid, 'yyyy'), 'dd.mm.yyyy')
                                                                                               then p_LastWorkDayYear
                                                                                               else ADD_MONTHS(to_date('28.' || to_char(q8.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy'), 1)
                                                                                            end
                                                                                         )
                                                                                    else ADD_MONTHS(to_date('28.' || to_char(q8.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy'), 1)
                                                                                 end
                                                                              )
                                                                      end, 0
                                                                   )
                           else to_date('01.01.0001', 'dd.mm.yyyy')
                        end
                     ) as t_DateTransfer,
                     q8.t_DatePaid,
                     (
                        case
                           when rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then (
                                   case
                                      when q8.t_IsHighRate = 'X'
                                      then '15'
                                      else '13'
                                   end
                                )
                           else (
                                   case
                                      when exists(
                                                    select 1
                                                    from dnotetext_dbt notetext
                                                    where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                      and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                      and notetext.t_NoteKind = 77
                                                      and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                                 )
                                      then to_char(rsb_struct.getDouble(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                      else '30'
                                   end
                                )
                        end
                     ) as t_Rate,
                     nvl(PlusObj.t_Sum0, 0) as t_PlusSum,
                     nvl(PlusObj.t_Kind, 0) as t_PlusKind,
                     nvl(PlusObj.t_ObjID, 0) as t_PlusObjID,
                     (
                        case
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy')
                           then nvl(PlusObj.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     0 as t_MinusSum,
                     0 as t_MinusKind,
                     0 as t_MinusObjID,
                     (
                        case
                           when q8.t_TaxPaid > 0
                           then q8.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxPaid,
                     q8.t_ObjID as t_PaidObjID,
                     0 as t_TaxBaseDiv,
                     0 as t_DivPlus,
                     0 as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     (
                        case
                           when q8.t_TaxPaid < 0
                           then q8.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxToReturnDue,
                     q8.t_IsHighRate
              from (
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObj.t_Sum0 as t_TaxPaid,
                             chr(88) as t_IsHighRate,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Date as t_DatePaid,
                             TaxPaidObj.t_Kind,
                             oproper.t_ID_Operation,
                             dl_order.t_ContractID,
                             dl_order.t_DocKind,
                             TaxPaidObj.t_ObjID
                      from dnptxobj_dbt TaxPaidObj
                           inner join dnptxobdc_dbt nptxobdc
                           on nptxobdc.t_ObjID = TaxPaidObj.t_ObjID
                           inner join doproper_dbt oproper
                           on oproper.t_ID_Operation = nptxobdc.t_DocID
                           inner join doprkoper_dbt oprkoper
                           on oprkoper.t_Kind_Operation = oproper.t_Kind_Operation
                           inner join ddl_order_dbt dl_order
                           on dl_order.t_DocKind = oproper.t_DocKind
                           and LPAD(dl_order.t_ContractID, 10, '0') = oproper.t_DocumentID
                           and (
                                  dl_order.t_DocKind = DL_VEKSELDRAWORDER
                                  and INSTR(oprkoper.t_SysTypes, 'Ф') = 0
                                  or dl_order.t_DocKind = RSB_SECUR.DL_VSBARTERORDER
                                  or dl_order.t_DocKind = DL_VSINTERCHANGE
                               )
                      where (
                               (
                                  TaxPaidObj.t_Date between p_BeginDate and p_EndDate
                                  and TaxPaidObj.t_Kind in(
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                             RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9
                                                          )
                               )
                            or (
                                  TaxPaidObj.t_Date between p_FirstDateIIS and p_EndDate
                                  and TaxPaidObj.t_Kind = RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                               )
                            )
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                      union all
                      select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                             TaxPaidObj.t_Sum0 as t_TaxPaid,
                             chr(0) as t_IsHighRate,
                             TaxPaidObj.t_AnaliticKind1,
                             TaxPaidObj.t_Analitic1,
                             TaxPaidObj.t_Date as t_DatePaid,
                             TaxPaidObj.t_Kind,
                             oproper.t_ID_Operation,
                             dl_order.t_ContractID,
                             dl_order.t_DocKind,
                             TaxPaidObj.t_ObjID
                      from dnptxobj_dbt TaxPaidObj
                           inner join dnptxobdc_dbt nptxobdc
                           on nptxobdc.t_ObjID = TaxPaidObj.t_ObjID
                           inner join doproper_dbt oproper
                           on oproper.t_ID_Operation = nptxobdc.t_DocID
                           inner join doprkoper_dbt oprkoper
                           on oprkoper.t_Kind_Operation = oproper.t_Kind_Operation
                           inner join ddl_order_dbt dl_order
                           on dl_order.t_DocKind = oproper.t_DocKind
                           and LPAD(dl_order.t_ContractID, 10, '0') = oproper.t_DocumentID
                           and (
                                  dl_order.t_DocKind = DL_VEKSELDRAWORDER
                                  and INSTR(oprkoper.t_SysTypes, 'Ф') = 0
                                  or dl_order.t_DocKind = RSB_SECUR.DL_VSBARTERORDER
                                  or dl_order.t_DocKind = DL_VSINTERCHANGE
                               )
                      where TaxPaidObj.t_Date between p_FirstDateIIS AND p_EndDate
                        AND TaxPaidObj.t_Kind in(
                                                   RSI_NPTXC.TXOBJ_PAIDBILL
                                                )
                        and TaxPaidObj.t_User <> chr(88)
                        and TaxPaidObj.t_FromOutSyst <> chr(88)
                        and TaxPaidObj.t_Client = p_ClientID
                   ) q8
                   left outer join
                   (
                      dnptxobdc_dbt PlusOBDC
                      inner join dnptxobj_dbt PlusObj
                      on PlusObj.t_ObjID = PlusOBDC.t_ObjID
                         and ((PlusObj.t_Kind = RSI_NPTXC.TXOBJ_PLUSG_2800 and rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1) or
                              (PlusObj.t_Kind = RSI_NPTXC.TXOBJ_BASEBILL and rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) <> 1)
                             )
                                               
                   )
                   on PlusOBDC.t_DocID = q8.t_ID_Operation
                      and PlusObj.t_Date between p_FirstDateIIS and p_MaxOperDate
              where p_ByVS = 'X'
                and q8.t_DatePaid between p_FirstDateIIS and p_EndDate
              union all
              --Пользовательские объекты НДР
              select 0 as t_TypeOper,
                     0 as t_DocID,
                     0 as t_DocKind,
                     (
                        case
                           when q9.t_Kind = RSI_NPTXC.TXOBJ_PAIDSPECIAL
                           then INFORATE9_15
                           else INFORATE_TOTAL
                        end
                     ) as t_RateType,
                     (
                        case
                           when q9.t_TaxPaid > 0
                           then rsi_rsbcalendar.GetDateAfterWorkDay(
                                                                       case
                                                                          --До 31.12.2022
                                                                          when p_BeginDate <= to_date('31.12.2022', 'dd.mm.yyyy')
                                                                          then (
                                                                                   case
                                                                                      when q9.t_DatePaid <= to_date('30.01.' || to_char(q9.t_DatePaid, 'yyyy'), 'dd.mm.yyyy')
                                                                                      then to_date('30.01.' || to_char(q9.t_DatePaid, 'yyyy'), 'dd.mm.yyyy')
                                                                                      else q9.t_DatePaid + 30
                                                                                   end
                                                                               )
                                                                          --После 31.12.2022
                                                                          else (
                                                                                   case
                                                                                      when RSB_COMMON.GetRegIntValue('COMMON\НДФЛ\СРОК_ПЕРЕЧИСЛЕНИЯ_НДФЛ_ПО_ЦБ') = 0
                                                                                      then (
                                                                                               case
                                                                                                  when q9.t_DatePaid <= to_date('28.01.' || to_char(q9.t_DatePaid, 'yyyy'), 'dd.mm.yyyy')
                                                                                                  then to_date('28.01.' || to_char(q9.t_DatePaid, 'yyyy'), 'dd.mm.yyyy')
                                                                                                  else to_date('28.' || to_char(ADD_MONTHS(q9.t_DatePaid, 1), 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                               end
                                                                                           )
                                                                                      else (
                                                                                               case
                                                                                                  when extract(day from q9.t_DatePaid) < 23
                                                                                                  then to_date('28.' || to_char(q9.t_DatePaid, 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                                  when extract(day from q9.t_DatePaid) >= 23 and extract(month from q9.t_DatePaid) = 12
                                                                                                  then GetLastWorkDayYear(p_BeginDate)
                                                                                                  else to_date('28.' || to_char(ADD_MONTHS(q9.t_DatePaid, 1), 'mm.yyyy'), 'dd.mm.yyyy')
                                                                                               end
                                                                                            )
                                                                                   end
                                                                                )
                                                                       end, 0
                                                                )
                           else to_date('01.01.2021', 'dd.mm.yyyy')
                        end
                     ) as t_DateTransfer,
                     q9.t_DatePaid,
                     (
                        case
                           when rsi_nptx.ResidenceStatus(p_ClientID, p_EndDate) = 1
                           then (
                                    case
                                       when q9.t_IsHighRate = chr(88)
                                       then '15'
                                       else (   
                                               case
                                                  when q9.t_Kind = RSI_NPTXC.TXOBJ_PAIDSPECIAL
                                                  then '9'
                                                  else '13'
                                               end
                                            )
                                    end
                                 )
                           else (
                                   case
                                      when q9.t_Kind = RSI_NPTXC.TXOBJ_PAIDSPECIAL
                                      then '9'
                                      else (
                                              case
                                                 when exists(
                                                               select 1
                                                               from dnotetext_dbt notetext
                                                               where notetext.t_ObjectType = RSB_SECUR.OBJTYPE_PARTY
                                                                 and notetext.t_DocumentID = LPAD(p_ClientID, 10, '0')
                                                                 and notetext.t_NoteKind = 77
                                                                 and p_EndDate between notetext.t_Date and notetext.t_ValidToDate
                                                            )
                                                 then to_char(rsb_struct.getDouble(rsi_rsb_kernel.GetNote(RSB_SECUR.OBJTYPE_PARTY, LPAD(p_ClientID, 10, '0'), 77, p_EndDate)))
                                                 else '30'
                                              end
                                           )
                                   end
                                )
                        end
                     ) as t_Rate,
                     ( case when pm.t_Direction = RSI_NPTXC.TXOBJ_DIR_IN then pm.t_Sum0 else 0 end ) as t_PlusSum,
                     ( case when pm.t_Direction = RSI_NPTXC.TXOBJ_DIR_IN then pm.t_Kind else 0 end ) as t_PlusKind,
                     ( case when pm.t_Direction = RSI_NPTXC.TXOBJ_DIR_IN then pm.t_ObjID else 0 end ) as t_PlusObjID,
                     (
                        case
                           when pm.t_Direction != RSI_NPTXC.TXOBJ_DIR_IN then to_date('01.01.0001', 'dd.mm.yyyy')
                           when p_CloseDateIIS = to_date('01.01.0001', 'dd.mm.yyyy') then nvl(pm.t_Date, to_date('01.01.0001', 'dd.mm.yyyy'))
                           else p_CloseDateIIS
                        end
                     ) as t_PlusDate,
                     ( case when pm.t_Direction = RSI_NPTXC.TXOBJ_DIR_OUT then pm.t_Sum0 else 0 end ) as t_MinusSum,
                     ( case when pm.t_Direction = RSI_NPTXC.TXOBJ_DIR_OUT then pm.t_Kind else 0 end )  as t_MinusKind,
                     ( case when pm.t_Direction = RSI_NPTXC.TXOBJ_DIR_OUT then pm.t_ObjID else 0 end )  as t_MinusObjID,
                     (
                        case
                           when q9.t_TaxPaid > 0
                           then q9.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxPaid,
                     q9.t_ObjID as t_PaidObjID,
                     0 as t_TaxBaseDiv,
                     0 as t_DivPlus,
                     0 as t_DivPay_Sec_0,
                     0 as t_PaidGeneral_0,
                     (
                        case
                           when q9.t_TaxPaid < 0
                           then q9.t_TaxPaid
                           else 0
                        end
                     ) as t_TaxToReturnDue,
                     q9.t_IsHighRate
              from (
                         select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                                TaxPaidObj.t_Sum0 as t_TaxPaid,
                                chr(88) as t_IsHighRate,
                                TaxPaidObj.t_ObjID,
                                TaxPaidObj.t_AnaliticKind1,
                                TaxPaidObj.t_Analitic1,
                                TaxPaidObj.t_Date as t_DatePaid,
                                TaxPaidObj.t_Kind,
                                TaxPaidObj.t_Client
                         from dnptxobj_dbt TaxPaidObj
                         where (
                                  (
                                     TaxPaidObj.t_Date between p_BeginDate and p_EndDate
                                     and TaxPaidObj.t_Kind in(
                                                                RSI_NPTXC.TXOBJ_PAIDGENERAL_15_1,
                                                                RSI_NPTXC.TXOBJ_PAIDGENERAL_15_2,
                                                                RSI_NPTXC.TXOBJ_PAIDGENERAL_15_9
                                                             )
                                  )
                               or (
                                     TaxPaidObj.t_Date between p_FirstDateIIS and p_EndDate
                                     and TaxPaidObj.t_Kind = RSI_NPTXC.TXOBJ_PAIDGENERAL_15_IIS
                                  )
                               )
                           and TaxPaidObj.t_Client = p_ClientID
                           and TaxPaidObj.t_FromOutSyst <> chr(88)
                           and TaxPaidObj.t_User = chr(88)
                         union all
                         select /*+ index(TaxPaidObj DNPTXOBJ_DBT_IDXA)*/
                               TaxPaidObj.t_Sum0 as t_TaxPaid,
                               chr(0) as t_IsHighRate,
                               TaxPaidObj.t_ObjID,
                               TaxPaidObj.t_AnaliticKind1,
                               TaxPaidObj.t_Analitic1,
                               TaxPaidObj.t_Date as t_DatePaid,
                               TaxPaidObj.t_Kind,
                               TaxPaidObj.t_Client
                         from dnptxobj_dbt TaxPaidObj
                         where TaxPaidObj.t_Date between p_BeginDate and p_EndDate
                           and TaxPaidObj.t_Kind in(
                                                      RSI_NPTXC.TXOBJ_PAIDMATERIAL,
                                                      RSI_NPTXC.TXOBJ_PAIDGENERAL,
                                                      RSI_NPTXC.TXOBJ_PAIDSPECIAL,
                                                      RSI_NPTXC.TXOBJ_PAIDMATERIAL_IIS,
                                                      RSI_NPTXC.TXOBJ_PAIDGENERAL_IIS,
                                                      RSI_NPTXC.TXOBJ_PAIDSPECIAL_IIS,
                                                      RSI_NPTXC.TXOBJ_PAIDBILL,
                                                      RSI_NPTXC.TXOBJ_DIVPAY_SEC
                                                   )
                           and TaxPaidObj.t_Client = p_ClientID
                           and TaxPaidObj.t_FromOutSyst <> chr(88)
                           and TaxPaidObj.t_User = chr(88)
                      ) q9
                      left join dnptxobj_dbt pm on     pm.t_Client = q9.t_Client
                                                   and pm.t_Date between p_FirstDateIIS and p_MaxOperDate
                                                   and pm.t_User = 'X'
                                                   and pm.t_FromOutSyst <> 'X'
                                                   and pm.t_Kind IN (--Доходы
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1010,
                                                                     RSI_NPTXC.TXOBJ_PLUS15_1010,
                                                                     RSI_NPTXC.TXOBJ_PLUS9_1110,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1110,
                                                                     RSI_NPTXC.TXOBJ_PLUS9_1110_IIS,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1011,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1530,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1531,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1536,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1532,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1535,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_2640,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_2641,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1537,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1539,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1011_IIS,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1544,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1545,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1549,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1546,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1547,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1548,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1551,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_1553,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_2640_IIS,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_2641_IIS,
                                                                     RSI_NPTXC.TXOBJ_PLUSG_2800,
                                                                     RSI_NPTXC.TXOBJ_BASEG1_13,
                                                                     RSI_NPTXC.TXOBJ_BASEG1_15,
                                                                     RSI_NPTXC.TXOBJ_PLUS35_3023,
                                                                     RSI_NPTXC.TXOBJ_BASEBILL,
                                                                     --Расходы
                                                                     RSI_NPTXC.TXOBJ_MINUSG_601,
                                                                     RSI_NPTXC.TXOBJ_MINUS15_601,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_218_1,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_219_1,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_618,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_619,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_201,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_202,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_203,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_206,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_207,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_222,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_223,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_205,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_208,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_209,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_210,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_224,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_220,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_211,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_213,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_218,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_219,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_214,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_225,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_226,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_227,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_228,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_229,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_230,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_231,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_233,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_234,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_214_IIS,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_250,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_251,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_252,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_241,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_236,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_235,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_239,
                                                                     RSI_NPTXC.TXOBJ_MINUSG_240
                                                                   )
           ) q,
           dpersn_dbt persn,
           dparty_dbt party
      where persn.t_PersonID = p_ClientID
        and party.t_PartyID = p_ClientID
        and (
               q.t_PlusSum >= 0.01
               or q.t_DivPlus >= 0.01
               or q.t_MinusSum >= 0.01
               or q.t_TaxPaid >= 0.01
               or abs(q.t_TaxToReturnDue) >= 0.01
            )
      order by (case when q.t_DatePaid > TO_DATE('01.01.0001','DD.MM.YYYY') then q.t_DatePaid else q.t_DateTransfer end), 
               (case when q.t_DateTransfer > TO_DATE('01.01.0001','DD.MM.YYYY') then q.t_DateTransfer else q.t_DatePaid end), 
               q.t_RateType, q.t_Rate, q.t_IsHighRate, q.t_TypeOper, q.t_DocKind, q.t_DocID, q.t_PaidObjID, q.t_PlusObjID, q.t_MinusObjID;
   BEGIN
      DELETE FROM DNPTX6CALC1_TMP;
   
      v_LastWorkDayYear := GetLastWorkDayYear(p_BeginDate);

  
      InsertDataInNPTXOP2OPLNK(p_ClientID, p_BeginDate, p_EndDate);

      v_ClientCode := RSB_SECUR.SC_GetObjCodeOnDate(RSB_SECUR.OBJTYPE_PARTY, 1, p_ClientID, p_RepDate);

      SELECT MAX(q.t_OperDate) INTO v_MaxOperDate
        FROM (
                SELECT nptxop.t_OperDate AS t_OperDate
                  FROM dnptxop_dbt nptxop
                 WHERE nptxop.t_OperDate BETWEEN p_BeginDate AND p_EndDate
                   AND nptxop.t_Status > 0
                   AND nptxop.t_Client = p_ClientID
                   AND nptxop.t_DocKind = RSB_SECUR.DL_WRTMONEY
                   AND nptxop.t_SubKind_Operation = DL_NPTXOP_WRTKIND_WRTOFF
                UNION
                SELECT nptxop.t_OperDate AS t_OperDate
                  FROM dnptxop_dbt nptxop
                 WHERE nptxop.t_OperDate BETWEEN p_BeginDate AND p_EndDate
                   AND nptxop.t_Status > 0
                   AND nptxop.t_Client = p_ClientID
                   AND nptxop.t_DocKind = RSB_SECUR.DL_CALCNDFL
                   AND nptxop.t_SubKind_Operation IN(
                                                       DL_TXBASECALC_OPTYPE_ENDYEAR,
                                                       DL_TXBASECALC_OPTYPE_CLOSE_IIS
                                                    )
                UNION
                SELECT tick.t_DealDate AS t_OperDate
                  FROM ddl_tick_dbt tick
                 WHERE tick.t_DealDate BETWEEN p_BeginDate AND p_EndDate
                   AND tick.t_DealStatus > 0
                   AND tick.t_ClientID = p_ClientID
                   AND (
                         (
                            tick.t_BOfficeKind = RSB_SECUR.DL_AVRWRT
                            AND tick.t_DealType = 2010
                         )
                      OR (
                            tick.t_BOfficeKind = RSB_SECUR.DL_RETIREMENT_OWN
                            AND tick.t_DealType = 2052
                         )
                      OR (
                            tick.t_BOfficeKind = RSB_SECUR.DL_RETURN_DIVIDEND
                            AND tick.t_DealType = 12108
                         )
                      )
                UNION
                SELECT dl_order.t_SignDate AS t_OperDate
                  FROM ddl_order_dbt dl_order
                 WHERE dl_order.t_SignDate BETWEEN p_BeginDate AND p_EndDate
                   AND dl_order.t_DocKind = DL_VEKSELDRAWORDER
                   AND dl_order.t_ContractStatus > 0
                   AND dl_order.t_Contractor = p_ClientID
             ) q;

      
      v_IsBaseDepo := false;
      v_TaxCalculated := 0;
      v_TaxCalculatedDiv := 0;

      v_Base2 := 0;
      v_Base3 := 0;
      v_Base5 := 0;
      v_Base9 := 0;
      
      v_FirstDateIIS := GetFirstDateIIS(p_ClientID, p_BeginDate, p_EndDate);
      v_CloseDateIIS := GetCloseDateIIS(p_ClientID, p_BeginDate, p_EndDate);
   
      FOR CurData IN CDataPart2(p_ClientID, v_FirstDateIIS, v_CloseDateIIS, v_MaxOperDate, v_LastWorkDayYear)
      LOOP
      
         IF CurData.t_RateType = INFORATE9_15
         THEN
            IF RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) = 1
            THEN
               v_TaxCalculated := (
                                     GetBase(RSI_NPTXC.TXOBJ_BASESPECIAL, p_ClientID, p_BeginDate, v_MaxOperDate) +
                                     GetBase(RSI_NPTXC.TXOBJ_BASESPECIAL_IIS, p_ClientID, p_BeginDate, v_MaxOperDate)
                                  ) * to_number(CurData.t_Rate) / 100 -
                                  GetBase(RSI_NPTXC.TXOBJ_PAIDSPECIAL_0, p_ClientID, p_BeginDate, p_EndDate);
            ELSE
               v_TaxCalculated := CurData.t_TaxPaid - GetBaseDepo(RSI_NPTXC.TXOBJ_DIVPAY_SEC_0, p_ClientID, p_BeginDate, p_EndDate);
            END IF;
            
            IF v_ClientID <> p_ClientID or v_DatePaid <> CurData.t_DatePaid or v_TypeOper <> CurData.t_TypeOper 
               or (CurData.t_TypeOper = 9 and (CurData.t_DocKind <> v_DocKind or CurData.t_DocID <> v_DocID)) 
            THEN
               InsertDataInNPTX6Calc1(CurData.t_TypeOper, CurData.t_RateType, p_ClientID, v_ClientCode, CurData.t_DateTransfer, CurData.t_DatePaid, CurData.t_Rate, CurData.t_TotalPlus, CurData.t_Plus2,
                                      CurData.t_Plus3, CurData.t_Plus5, CurData.t_Plus9, CurData.t_PlusCode, CurData.t_PlusDate, CurData.t_DivPlus, CurData.t_TotalMinus, CurData.t_Minus2,
                                      CurData.t_Minus3, CurData.t_Minus5, CurData.t_Minus9, CurData.t_MinusCode, ROUND(v_TaxCalculated, 0), 0, ROUND(CurData.t_TaxPaid, 0),
                                      0, 0, ROUND(CurData.t_TaxToReturnDue, 0), ROUND(CurData.t_Znp, 0), CurData.t_IsHighRate, CurData.t_PaidObjID, CurData.t_PlusObjID, CurData.t_MinusObjID);
                                         
               v_ClientID := p_ClientID;
               v_DatePaid := CurData.t_DatePaid;
               v_TypeOper := CurData.t_TypeOper;
               v_DocKind  := CurData.t_DocKind;
               v_DocID    := CurData.t_DocID;
            ELSE
               InsertDataInNPTX6Calc1(CurData.t_TypeOper, CurData.t_RateType, p_ClientID, v_ClientCode, CurData.t_DateTransfer, CurData.t_DatePaid, CurData.t_Rate, CurData.t_TotalPlus, CurData.t_Plus2,
                                      CurData.t_Plus3, CurData.t_Plus5, CurData.t_Plus9, CurData.t_PlusCode, CurData.t_PlusDate, CurData.t_DivPlus, CurData.t_TotalMinus, CurData.t_Minus2,
                                      CurData.t_Minus3, CurData.t_Minus5, CurData.t_Minus9, CurData.t_MinusCode, 0, 0, 0, 0, 0, 0, ROUND(CurData.t_Znp, 0), CurData.t_IsHighRate, CurData.t_PaidObjID, CurData.t_PlusObjID, CurData.t_MinusObjID);
            END IF;
         ELSIF CurData.t_RateType = INFORATE_TOTAL
         THEN
            IF RSI_NPTX.ResidenceStatus(p_ClientID, p_EndDate) <> 1
            THEN
               IF v_ClientID <> p_ClientID or v_DatePaid <> CurData.t_DatePaid or v_TypeOper <> CurData.t_TypeOper
                  or (CurData.t_TypeOper = 9 and (CurData.t_DocKind <> v_DocKind or CurData.t_DocID <> v_DocID))
               THEN
                  v_TaxCalculated := 0;
                  IF v_ClientID <> p_ClientID or v_DatePaid <> CurData.t_DatePaid or v_TypeOper <> CurData.t_TypeOper THEN
                    v_TaxCalculated := (
                                          GetBase(RSI_NPTXC.TXOBJ_BASEGENERAL, p_ClientID, p_BeginDate, CurData.t_DatePaid) +
                                          GetBase(RSI_NPTXC.TXOBJ_BASEMATERIAL, p_ClientID, p_BeginDate, CurData.t_DatePaid) +
                                          GetBase(RSI_NPTXC.TXOBJ_BASEBILL, p_ClientID, p_BeginDate, CurData.t_DatePaid) +
                                          GetBase(RSI_NPTXC.TXOBJ_BASEGENERAL_IIS, p_ClientID, p_BeginDate, CurData.t_DatePaid) +
                                          GetBase(RSI_NPTXC.TXOBJ_BASEMATERIAL_IIS, p_ClientID, p_BeginDate, CurData.t_DatePaid)
                                       ) * to_number(CurData.t_Rate) / 100 -
                                       GetBase(RSI_NPTXC.TXOBJ_PAIDGENERAL_0, p_ClientID, p_BeginDate, CurData.t_DatePaid);
                  END IF;

                  InsertDataInNPTX6Calc1(CurData.t_TypeOper, CurData.t_RateType, p_ClientID, v_ClientCode, CurData.t_DateTransfer, CurData.t_DatePaid, CurData.t_Rate, CurData.t_TotalPlus, CurData.t_Plus2,
                                         CurData.t_Plus3, CurData.t_Plus5, CurData.t_Plus9, CurData.t_PlusCode, CurData.t_PlusDate, CurData.t_DivPlus, CurData.t_TotalMinus, CurData.t_Minus2,
                                         CurData.t_Minus3, CurData.t_Minus5, CurData.t_Minus9, CurData.t_MinusCode, ROUND(v_TaxCalculated, 0), 0, ROUND(CurData.t_TaxPaid, 0), 0, 0, ROUND(CurData.t_TaxToReturnDue, 0), ROUND(CurData.t_Znp, 0),
                                         CurData.t_IsHighRate, CurData.t_PaidObjID, CurData.t_PlusObjID, CurData.t_MinusObjID);

                   v_ClientID := p_ClientID;
                   v_TypeOper := CurData.t_TypeOper;
                   v_DatePaid := CurData.t_DatePaid;
                   v_DocKind  := CurData.t_DocKind;
                   v_DocID    := CurData.t_DocID;
               ELSE
                  InsertDataInNPTX6Calc1(CurData.t_TypeOper, CurData.t_RateType, p_ClientID, v_ClientCode, CurData.t_DateTransfer, CurData.t_DatePaid, CurData.t_Rate, CurData.t_TotalPlus, CurData.t_Plus2,
                                         CurData.t_Plus3, CurData.t_Plus5, CurData.t_Plus9, CurData.t_PlusCode, CurData.t_PlusDate, CurData.t_DivPlus, CurData.t_TotalMinus, CurData.t_Minus2,
                                         CurData.t_Minus3, CurData.t_Minus5, CurData.t_Minus9, CurData.t_MinusCode, 0, 0, 0, 0, 0, 0, ROUND(CurData.t_Znp, 0), CurData.t_IsHighRate, CurData.t_PaidObjID, CurData.t_PlusObjID, CurData.t_MinusObjID);
               END IF;
            ELSE

               v_FindTotalPlus := 0;
               v_FindPlus2 := 0; 
               v_FindPlus3 := 0;
               v_FindPlus5 := 0;
               v_FindPlus9 := 0;

               v_FindTotalMinus := 0;
               v_FindMinus2 := 0;
               v_FindMinus3 := 0;
               v_FindMinus5 := 0;
               v_FindMinus9 := 0;

               v_FindTaxPaid := 0;

               v_BaseTotal := 0;
               v_Base2 := 0;
               v_Base3 := 0;
               v_Base5 := 0;
               v_Base9 := 0;

               IF v_IsBaseDepo = false
               THEN
                  v_BaseDepo_2 := GetBaseDepo(RSI_NPTXC.TXOBJ_BASEG2, p_ClientID, p_BeginDate, v_MaxOperDate);
                  v_BaseDepo_2 := CASE WHEN v_BaseDepo_2 < RSI_NPTXC.BASE_MAX_15 THEN v_BaseDepo_2 ELSE RSI_NPTXC.BASE_MAX_15 END;
                  v_Base2      := v_BaseDepo_2;

                  v_BaseDepo_3 := GetBaseDepo(RSI_NPTXC.TXOBJ_BASEG3, p_ClientID, p_BeginDate, v_MaxOperDate);
                  v_BaseDepo_3 := CASE WHEN v_BaseDepo_3 < RSI_NPTXC.BASE_MAX_15 THEN v_BaseDepo_3 ELSE RSI_NPTXC.BASE_MAX_15 END;
                  v_Base3      := v_BaseDepo_3;

                  v_BaseDepo_5 := GetBaseDepo(RSI_NPTXC.TXOBJ_BASEG5, p_ClientID, p_BeginDate, v_MaxOperDate);
                  v_BaseDepo_5 := CASE WHEN v_BaseDepo_5 < RSI_NPTXC.BASE_MAX_15 THEN v_BaseDepo_5 ELSE RSI_NPTXC.BASE_MAX_15 END;
                  v_Base5      := v_BaseDepo_5;

                  v_BaseDepo_9 := GetBaseDepo(RSI_NPTXC.TXOBJ_BASEG9, p_ClientID, p_BeginDate, v_MaxOperDate);
                  v_BaseDepo_9 := CASE WHEN v_BaseDepo_9 < RSI_NPTXC.BASE_MAX_15 THEN v_BaseDepo_9 ELSE RSI_NPTXC.BASE_MAX_15 END;
                  v_Base9      := v_BaseDepo_9;

                  v_IsBaseDepo := true;
               ELSE
                  v_BaseDepo_2 := 0;
                  v_BaseDepo_3 := 0;
                  v_BaseDepo_5 := 0;
                  v_BaseDepo_9 := 0;
               END IF;

               v_PrevTotalBase := 0;
               v_PrevBase2 := 0;
               v_PrevBase3 := 0;
               v_PrevBase5 := 0;
               v_PrevBase9 := 0;
               
               IF CurData.t_PlusObjID > 0 OR CurData.t_MinusObjID > 0 THEN
                 SELECT NVL(SUM(t_TotalPlus-t_TotalMinus), 0),
                        NVL(SUM(t_Plus2-t_Minus2), 0),
                        NVL(SUM(t_Plus3-t_Minus3), 0),
                        NVL(SUM(t_Plus5-t_Minus5), 0),
                        NVL(SUM(t_Plus9-t_Minus9), 0)
                   INTO v_PrevTotalBase,
                        v_PrevBase2,
                        v_PrevBase3,
                        v_PrevBase5,
                        v_PrevBase9
                   FROM DNPTX6CALC1_TMP
                  WHERE T_CLIENTID = p_ClientID
                    AND T_ISHIGHRATE = CurData.t_IsHighRate;
               END IF;


               --Чтобы не учитывать одни и те же доходы и вычеты дважды, определим, сколько по каждому объекту уже учли
               IF CurData.t_PaidObjID > 0 THEN
                 SELECT NVL(SUM(t_TaxPaid), 0) 
                   INTO v_FindTaxPaid
                   FROM DNPTX6CALC1_TMP
                  WHERE T_CLIENTID = p_ClientID
                    AND T_PAIDOBJID = CurData.t_PaidObjID;
               END IF;

               IF CurData.t_PlusObjID > 0 THEN
                 SELECT NVL(SUM(t_TotalPlus), 0), 
                        NVL(SUM(t_Plus2), 0),
                        NVL(SUM(t_Plus3), 0), 
                        NVL(SUM(t_Plus5), 0), 
                        NVL(SUM(t_Plus9), 0) 
                   INTO v_FindTotalPlus,
                        v_FindPlus2, 
                        v_FindPlus3, 
                        v_FindPlus5, 
                        v_FindPlus9 
                   FROM DNPTX6CALC1_TMP
                  WHERE T_CLIENTID = p_ClientID
                    AND T_PLUSOBJID = CurData.t_PlusObjID;
               END IF;

               IF CurData.t_MinusObjID > 0 THEN
                 SELECT NVL(SUM(t_TotalMinus), 0),
                        NVL(SUM(t_Minus2), 0),
                        NVL(SUM(t_Minus3), 0),
                        NVL(SUM(t_Minus5), 0),
                        NVL(SUM(t_Minus9), 0)
                   INTO v_FindTotalMinus, v_FindMinus2, v_FindMinus3, v_FindMinus5, v_FindMinus9
                   FROM DNPTX6CALC1_TMP
                  WHERE T_CLIENTID = p_ClientID
                    AND T_MINUSOBJID = CurData.t_MinusObjID;
               END IF;

               --Уплаченный налог по текущей записи с учетом ранее учтенного уплаченного налога по текущему объекты удержания
               v_CurTaxPaid := CurData.t_TaxPaid - v_FindTaxPaid;

               --Доход по текущей записи с учетом ранее учтенного дохода по текущему объекту дохода
               v_CurTotalPlus := CurData.t_TotalPlus-v_FindTotalPlus;
               v_CurPlus2 := CurData.t_Plus2-v_FindPlus2;
               v_CurPlus3 := CurData.t_Plus3-v_FindPlus3;
               v_CurPlus5 := CurData.t_Plus5-v_FindPlus5;
               v_CurPlus9 := CurData.t_Plus9-v_FindPlus9;

               --Вычет по текущей записи с учетом ранее учтенного вычета по текущему объекту вычета
               v_CurTotalMinus := CurData.t_TotalMinus-v_FindTotalMinus;
               v_CurMinus2 := CurData.t_Minus2-v_FindMinus2;
               v_CurMinus3 := CurData.t_Minus3-v_FindMinus3;
               v_CurMinus5 := CurData.t_Minus5-v_FindMinus5;
               v_CurMinus9 := CurData.t_Minus9-v_FindMinus9;

               --Накопленный сумарный НОБ по текущей ставке 
               v_BaseTotal := v_BaseTotal + v_CurTotalPlus - v_CurTotalMinus + v_PrevTotalBase;
               v_Base2 := v_Base2 + v_CurPlus2 - v_CurMinus2 + v_PrevBase2;
               v_Base3 := v_Base3 + v_CurPlus3 - v_CurMinus3 + v_PrevBase3;
               v_Base5 := v_Base5 + v_CurPlus5 - v_CurMinus5 + v_PrevBase5;
               v_Base9 := v_Base9 + v_CurPlus9 - v_CurMinus9 + v_PrevBase9;
               
               --Ранее рассчитанный налог определим по тем же записям, в которых есть доход или вычет в тех же полях, что и в текущей записи
               --То есть это будет ранее рассчитанный налог по той же самой налоговой базе
               SELECT NVL(SUM(CASE WHEN (v_CurTotalPlus <> 0 OR v_CurTotalMinus <> 0) AND (t_TotalPlus <> 0 OR t_TotalMinus <> 0) THEN t_TaxCalculated
                                   WHEN (v_CurPlus2 <> 0 OR v_CurMinus2 <> 0) AND (t_Plus2 <> 0 OR t_Minus2 <> 0) THEN t_TaxCalculated
                                   WHEN (v_CurPlus3 <> 0 OR v_CurMinus3 <> 0) AND (t_Plus3 <> 0 OR t_Minus3 <> 0) THEN t_TaxCalculated
                                   WHEN (v_CurPlus5 <> 0 OR v_CurMinus5 <> 0) AND (t_Plus5 <> 0 OR t_Minus5 <> 0) THEN t_TaxCalculated
                                   WHEN (v_CurPlus9 <> 0 OR v_CurMinus9 <> 0) AND (t_Plus9 <> 0 OR t_Minus9 <> 0) THEN t_TaxCalculated
                                   ELSE 0 END
                             ), 0)
                 INTO v_PrevTaxCalculated 
                 FROM DNPTX6CALC1_TMP
                WHERE T_CLIENTID = p_ClientID
                  AND T_ISHIGHRATE = CurData.t_IsHighRate;
                                    
               IF CurData.t_IsHighRate <> 'X'
               THEN

                 v_BaseTotal := CASE WHEN v_BaseTotal < RSI_NPTXC.BASE_MAX_15 THEN v_BaseTotal ELSE RSI_NPTXC.BASE_MAX_15 END;
                 v_Base2 := CASE WHEN v_Base2 < RSI_NPTXC.BASE_MAX_15 THEN v_Base2 ELSE RSI_NPTXC.BASE_MAX_15 END - v_BaseDepo_2;
                 v_Base3 := CASE WHEN v_Base3 < RSI_NPTXC.BASE_MAX_15 THEN v_Base3 ELSE RSI_NPTXC.BASE_MAX_15 END - v_BaseDepo_3;
                 v_Base5 := CASE WHEN v_Base5 < RSI_NPTXC.BASE_MAX_15 THEN v_Base5 ELSE RSI_NPTXC.BASE_MAX_15 END - v_BaseDepo_5;
                 v_Base9 := CASE WHEN v_Base9 < RSI_NPTXC.BASE_MAX_15 THEN v_Base9 ELSE RSI_NPTXC.BASE_MAX_15 END - v_BaseDepo_9;

                 
                 v_CurTotalPlus := v_CurTotalMinus + v_BaseTotal - v_PrevTotalBase;
                 v_CurPlus2 := v_CurMinus2 + v_Base2 - v_PrevBase2;
                 v_CurPlus3 := v_CurMinus3 + v_Base3 - v_PrevBase3;
                 v_CurPlus5 := v_CurMinus5 + v_Base5 - v_PrevBase5;
                 v_CurPlus9 := v_CurMinus9 + v_Base9 - v_PrevBase9;

               END IF;
               
               IF(v_CurTotalPlus != 0 OR
                  v_CurPlus2 != 0 OR
                  v_CurPlus3 != 0 OR
                  v_CurPlus5 != 0 OR
                  v_CurPlus9 != 0 OR
                  v_CurTotalMinus != 0 OR
                  v_CurMinus2 != 0 OR
                  v_CurMinus3 != 0 OR
                  v_CurMinus5 != 0 OR
                  v_CurMinus9 != 0 OR
                  CurData.t_TaxBaseDiv != 0 OR
                  v_CurTaxPaid != 0 OR
                  CurData.t_TaxToReturnDue != 0 OR
                  CurData.t_Znp != 0
                 ) THEN  --Считаем налог и добавляем запись только в том случае, если есть доход или вычет

                 v_Base := 0;
                 
                 --Налог рассчиываем только от конкретной базы
                 IF v_CurTotalPlus !=0 OR v_CurTotalMinus != 0 THEN
                   v_Base := v_BaseTotal;
                 ELSIF  v_CurPlus2 !=0 OR v_CurMinus2 != 0 THEN
                   v_Base := v_Base2; 
                 ELSIF  v_CurPlus3 !=0 OR v_CurMinus3 != 0 THEN
                   v_Base := v_Base3;
                 ELSIF  v_CurPlus5 !=0 OR v_CurMinus5 != 0 THEN
                   v_Base := v_Base5;
                 ELSIF  v_CurPlus9 !=0 OR v_CurMinus9 != 0 THEN
                   v_Base := v_Base9;
                 END IF;  
                 
                 v_TaxCalculated := v_Base * to_number(CurData.t_Rate) / 100.0 
                                      - CurData.t_DivPay_Sec_0 - CurData.t_PaidGeneral_0 - v_PrevTaxCalculated;

                 v_TaxCalculatedDiv := CurData.t_TaxBaseDiv * to_number(CurData.t_Rate) / 100.0;

                 InsertDataInNPTX6Calc1(CurData.t_TypeOper, CurData.t_RateType, p_ClientID, v_ClientCode, CurData.t_DateTransfer, CurData.t_DatePaid, CurData.t_Rate, v_CurTotalPlus,
                                        v_CurPlus2, v_CurPlus3, v_CurPlus5, v_CurPlus9,
                                        CurData.t_PlusCode, CurData.t_PlusDate, CurData.t_DivPlus, v_CurTotalMinus, v_CurMinus2,
                                        v_CurMinus3, v_CurMinus5, v_CurMinus9, CurData.t_MinusCode, ROUND(v_TaxCalculated, 0), ROUND(v_TaxCalculatedDiv, 0), ROUND(v_CurTaxPaid, 0), 0, 0, ROUND(CurData.t_TaxToReturnDue, 0),
                                        ROUND(CurData.t_Znp, 0), CurData.t_IsHighRate, CurData.t_PaidObjID, CurData.t_PlusObjID, CurData.t_MinusObjID);

               END IF;
            END IF;
         END IF;
      END LOOP;


      INSERT INTO DNPTX6CALC1_DBT (     t_GUID,
                                        t_TypeOper,
                                        t_RateType,
                                        t_ClientID,
                                        t_ClientCode,
                                        t_DateTransfer,
                                        t_DatePaid,
                                        t_Rate,
                                        t_TotalPlus,
                                        t_Plus2,
                                        t_Plus3,
                                        t_Plus5,
                                        t_Plus9,
                                        t_PlusCode,
                                        t_PlusDate,
                                        t_DivPlus,
                                        t_TotalMinus,
                                        t_Minus2,
                                        t_Minus3,
                                        t_Minus5,
                                        t_Minus9,
                                        t_MinusCode,
                                        t_TaxCalculated,
                                        t_TaxCalculatedDiv,
                                        t_TaxPaid,
                                        t_TaxDue,
                                        t_TaxOver,
                                        t_TaxToReturnDue,
                                        t_Znp,
                                        t_IsHighRate,
                                        t_PaidObjID,
                                        t_PlusObjID,
                                        t_MinusObjID
                                 )
                                 SELECT p_GUID,
                                        t_TypeOper,
                                        t_RateType,
                                        t_ClientID,
                                        t_ClientCode,
                                        t_DateTransfer,
                                        t_DatePaid,
                                        t_Rate,
                                        t_TotalPlus,
                                        t_Plus2,
                                        t_Plus3,
                                        t_Plus5,
                                        t_Plus9,
                                        t_PlusCode,
                                        t_PlusDate,
                                        t_DivPlus,
                                        t_TotalMinus,
                                        t_Minus2,
                                        t_Minus3,
                                        t_Minus5,
                                        t_Minus9,
                                        t_MinusCode,
                                        t_TaxCalculated,
                                        t_TaxCalculatedDiv,
                                        t_TaxPaid,
                                        t_TaxDue,
                                        t_TaxOver,
                                        t_TaxToReturnDue,
                                        t_Znp,
                                        t_IsHighRate,
                                        t_PaidObjID,
                                        t_PlusObjID,
                                        t_MinusObjID
                                   FROM DNPTX6CALC1_TMP;

          DELETE FROM DNPTX6CALC1_TMP;

  END;


  PROCEDURE CalcDataForPart2(p_BeginDate IN DATE, p_EndDate IN DATE, p_ByCB IN CHAR, p_ByDepo IN CHAR, p_ByVS IN CHAR, p_RepDate DATE)
  IS
    v_task_name VARCHAR2(30);
    v_sql_chunks CLOB;
    v_sql_process VARCHAR2(400);
    v_try NUMBER(5) := 0;
    v_status NUMBER;

    PARALLEL_LEVEL CONSTANT NUMBER(5) := 10; --количество потоков

    v_GUID VARCHAR(32);

  BEGIN

    SELECT CAST(SYS_GUID() AS VARCHAR2(32)) INTO v_GUID FROM dual;

    IF PARALLEL_LEVEL <= 1 THEN
      FOR one_pt IN (SELECT p.t_PartyID
                       FROM DNPTX6CLIENT_TMP p
                    )
      LOOP
        RSI_NPTX6CALC.CalcDataForPart2_Client(one_pt.t_PartyID, one_pt.t_PartyID, v_GUID, p_BeginDate, p_EndDate, p_ByCB, p_ByDepo, p_ByVS, p_RepDate);
      END LOOP;
    ELSE

      v_task_name := DBMS_PARALLEL_EXECUTE.generate_task_name;   
      DBMS_PARALLEL_EXECUTE.create_task (task_name => v_task_name);      

      v_sql_chunks := 'SELECT p.t_PartyID, p.t_PartyID ' ||
                      '  FROM DNPTX6CLIENT_TMP p ';

      DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(task_name => v_task_name,
                                                 sql_stmt  => v_sql_chunks, 
                                                 by_rowid  => FALSE);      

      v_sql_process := 'CALL RSI_NPTX6CALC.CalcDataForPart2_Client(:start_id, :end_id, '||
                                                                   '''' || v_GUID || ''', ' ||
                                                                   'TO_DATE('''||TO_CHAR(p_BeginDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY''), ' ||
                                                                   'TO_DATE('''||TO_CHAR(p_EndDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY''), ' ||
                                                                   '''' || p_ByCB || ''', ' ||
                                                                   '''' || p_ByDepo || ''', ' ||
                                                                   '''' || p_ByVS || ''', ' ||
                                                                   'TO_DATE('''||TO_CHAR(p_RepDate, 'DD.MM.YYYY')||''',''DD.MM.YYYY'') ' || ')';

      DBMS_PARALLEL_EXECUTE.run_task(task_name => v_task_name,
                                     sql_stmt => v_sql_process,
                                     language_flag => DBMS_SQL.NATIVE,
                                     parallel_level => PARALLEL_LEVEL);

      v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
      WHILE(v_try < 2 and v_status != DBMS_PARALLEL_EXECUTE.FINISHED)
      LOOP
        v_try := v_try + 1;
        DBMS_PARALLEL_EXECUTE.resume_task(v_task_name);
        v_status := DBMS_PARALLEL_EXECUTE.task_status(v_task_name);
      END LOOP;

      DBMS_PARALLEL_EXECUTE.drop_task(v_task_name);
    END IF;

    DELETE FROM DNPTX6CALC1_TMP;

    INSERT INTO DNPTX6CALC1_TMP (
                                      t_TypeOper,
                                      t_RateType,
                                      t_ClientID,
                                      t_ClientCode,
                                      t_DateTransfer,
                                      t_DatePaid,
                                      t_Rate,
                                      t_TotalPlus,
                                      t_Plus2,
                                      t_Plus3,
                                      t_Plus5,
                                      t_Plus9,
                                      t_PlusCode,
                                      t_PlusDate,
                                      t_DivPlus,
                                      t_TotalMinus,
                                      t_Minus2,
                                      t_Minus3,
                                      t_Minus5,
                                      t_Minus9,
                                      t_MinusCode,
                                      t_TaxCalculated,
                                      t_TaxCalculatedDiv,
                                      t_TaxPaid,
                                      t_TaxDue,
                                      t_TaxOver,
                                      t_TaxToReturnDue,
                                      t_Znp,
                                      t_IsHighRate,
                                      t_PaidObjID,
                                      t_PlusObjID,
                                      t_MinusObjID
                                   )
                               SELECT 
                                      t_TypeOper,
                                      t_RateType,
                                      t_ClientID,
                                      t_ClientCode,
                                      t_DateTransfer,
                                      t_DatePaid,
                                      t_Rate,
                                      t_TotalPlus,
                                      t_Plus2,
                                      t_Plus3,
                                      t_Plus5,
                                      t_Plus9,
                                      t_PlusCode,
                                      t_PlusDate,
                                      t_DivPlus,
                                      t_TotalMinus,
                                      t_Minus2,
                                      t_Minus3,
                                      t_Minus5,
                                      t_Minus9,
                                      t_MinusCode,
                                      t_TaxCalculated,
                                      t_TaxCalculatedDiv,
                                      t_TaxPaid,
                                      t_TaxDue,
                                      t_TaxOver,
                                      t_TaxToReturnDue,
                                      t_Znp,
                                      t_IsHighRate,
                                      t_PaidObjID,
                                      t_PlusObjID,
                                      t_MinusObjID
                                 FROM DNPTX6CALC1_DBT WHERE t_GUID = v_GUID;

    DELETE FROM DNPTX6CALC1_DBT WHERE t_GUID = v_GUID;

  END;


END RSI_NPTX6CALC;
/
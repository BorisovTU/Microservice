CREATE OR REPLACE FORCE VIEW DV_SCTOTALDEALS
(
   T_DOCKIND,
   T_DOCID,
   T_DEALCODE,
   T_DEALDATE,
   T_KIND_OPERATION,
   T_CLIENTID,
   T_CLIENTCONTRID,
   T_PARTYID,
   T_PARTYCONTRID,
   T_ISPARTYCLIENT,
   T_AMOUNT,
   T_PFI,
   T_TOTALCOST,
   T_CFI,
   T_STATUS,
   T_DEPARTMENT,
   T_MARKETSCHEMEID,
   T_DEPSETID,
   T_PAYFI,
   T_PROGNOS
)
AS
(SELECT CAST(q.t_DocKind AS NUMBER(5)),
        q.t_DocID ,
        CAST(q.t_DealCode AS VARCHAR2(30)),
        q.t_DealDate,
        q.t_Kind_Operation ,
        q.t_ClientID ,
        q.t_ClientContrID,
        CAST(q.t_PartyID AS NUMBER(10)),
        CAST(q.t_PartyContrID AS NUMBER(10)),
        CAST(q.t_IsPartyClient AS CHAR(1)),
        CAST(q.t_Amount AS NUMBER(32,12)),
        CAST(q.t_PFI AS NUMBER(10)),
        CAST(q.t_TotalCost AS NUMBER(32,12)),
        CAST(q.t_CFI AS NUMBER(10)),
        CAST(q.t_Status AS NUMBER(5)),
        CAST(q.t_Department AS NUMBER(10)),
        CAST(q.t_MarketSchemeID AS NUMBER(10)),
        CAST(q.t_DepSetID AS NUMBER(10)),
        CAST(q.t_PayFI AS NUMBER(10)),
        CAST(q.t_Prognos AS CHAR)
 FROM ( SELECT tk.t_BOfficeKind AS t_DocKind,
               tk.t_DealID AS t_DocID,
               tk.t_DealCode AS t_DealCode,
               tk.t_DealDate AS t_DealDate,
               tk.t_DealType AS t_Kind_Operation,
               leg.t_Principal AS t_Amount,
               leg.t_PFI AS t_PFI,
               leg.t_TotalCost AS t_TotalCost,
               leg.t_CFI AS t_CFI,
               tk.t_PartyID AS t_PartyID,
               tk.t_PartyContrID AS t_PartyContrID,
               tk.t_IsPartyClient AS t_IsPartyClient,
               tk.t_ClientID AS t_ClientID,
               tk.t_ClientContrID AS t_ClientContrID,
               tk.t_DealStatus AS t_Status,
               tk.t_Department AS t_Department,
               tk.t_MarketSchemeID AS t_MarketSchemeID,
               tk.t_DepSetID AS t_DepSetID,
               leg.t_PayFIID AS t_PayFI,
               tk.t_Prognos AS t_Prognos
          FROM ddl_tick_dbt tk, ddl_leg_dbt leg, ddp_dep_dbt dep
         WHERE leg.t_LegKind = 0
           AND leg.t_DealID = tk.t_DealID
           AND leg.t_LegID = 0
           AND tk.t_BOfficeKind IN (101, 117, 127, 4830, 4831)
           AND dep.t_Code = tk.t_Department
        UNION ALL
        SELECT comm.t_DocKind AS t_DocKind,
               comm.t_DocumentID AS t_DocID,
               comm.t_CommCode AS t_DealCode,
               comm.t_CommDate AS t_DealDate,
               comm.t_OperationKind AS t_Kind_Operation,
               comm.t_Hidden_Sum AS t_Amount,
               comm.t_FIID AS t_PFI,
               0 AS t_TotalCost,
               -1 AS t_CFI,
               comm.t_PartyID AS t_PartyID,
               0 AS t_PartyContrID,
               CHR(0) AS t_IsPartyClient,
               comm.t_ClientID AS t_ClientID,
               comm.t_ContractID AS t_ClientContrID,
               (comm.t_CommStatus*10) AS t_Status,
               comm.t_Division AS t_Department,
               comm.t_MarketSchemeID AS t_MarketSchemeID,
               comm.t_DepSetID AS t_DepSetID,
               -1 AS t_PayFI,
               chr(0) AS t_Prognos
          FROM ddl_comm_dbt comm
         WHERE comm.t_DocKind IN (4619)
        UNION ALL
        SELECT ntg.t_DocKind AS t_DocKind,
               ntg.t_NettingID AS t_DocID,
               ntg.t_DealNumber AS t_DealCode,
               ntg.t_SigningDate AS t_DealDate,
               ntg.t_Kind_Operation AS t_Kind_Operation,
               NVL((SELECT rq.t_Amount
                      FROM ddlrq_dbt rq
                     WHERE rq.t_DocKind = ntg.t_DocKind
                       AND rq.t_DocID = ntg.t_NettingID
                       AND rq.t_DealPart = 1
                       AND rq.t_Type = 8 --RSI_DLRQ.DLRQ_TYPE_DELIVERY
                       AND rq.t_FIID = ntg.t_BaseFIID
                       AND rq.t_Num = 0
                   ),0),
               (CASE WHEN ntg.t_FI_KIND = 2 THEN ntg.t_BaseFIID ELSE -1 END) AS t_PFI,
               NVL((SELECT rq.t_Amount
                      FROM ddlrq_dbt rq
                     WHERE rq.t_DocKind = ntg.t_DocKind
                       AND rq.t_DocID = ntg.t_NettingID
                       AND rq.t_DealPart = 1
                       AND rq.t_Type = 2 --RSI_DLRQ.DLRQ_TYPE_PAYMENT
                       AND rq.t_FIID = ntg.t_BaseFIID
                       AND rq.t_Num = 0
                   ),0),
               (CASE WHEN ntg.t_FI_KIND = 1 THEN ntg.t_BaseFIID ELSE -1 END) AS t_CFI,
               ntg.t_Contractor AS t_PartyID,
               0 AS t_PartyContrID,
               CHR(0) AS t_IsPartyClient,
               NTG.T_CLIENTID as  T_CLIENTID,
               NTG.T_CLIENTCONTRID as T_CLIENTCONTRID ,
               ntg.t_Status AS t_Status,
               ntg.t_Department AS t_Department,
               0 AS t_MarketSchemeID,
               0 AS t_DepSetID,
               (CASE WHEN ntg.t_FI_KIND = 1 THEN ntg.t_BaseFIID ELSE -1 END) AS t_PayFI,
               chr(0) AS t_Prognos
          FROM ddl_nett_dbt ntg
         WHERE ntg.t_DocKind = 154
        UNION ALL
        SELECT comm.t_DocKind AS t_DocKind,
               comm.t_DocumentID AS t_DocID,
               comm.t_CommCode AS t_DealCode,
               comm.t_CommDate AS t_DealDate,
               comm.t_OperationKind AS t_Kind_Operation,
               comm.t_Hidden_Sum AS t_Amount,
               comm.t_FIID AS t_PFI,
               0 AS t_TotalCost,
               -1 AS t_CFI,
               comm.t_PartyID AS t_PartyID,
               0 AS t_PartyContrID,
               CHR(0) AS t_IsPartyClient,
               comm.t_ClientID AS t_ClientID,
               comm.t_ContractID AS t_ClientContrID,
               (comm.t_CommStatus*10) AS t_Status,
               comm.t_Division AS t_Department,
               comm.t_MarketSchemeID AS t_MarketSchemeID,
               0 AS t_DepSetID,
               -1 AS t_PayFI,
               chr(0) AS t_Prognos
          FROM ddl_comm_dbt comm
         WHERE comm.t_DocKind IN (4621) /*Перевод остатков в КДУ*/
      ) q)
/

/* Массовое исполнение сделок. В дистрибутиве это операции 
  2143 - Покупка ц/б биржевая
  2144 - Покупка ц/б биржевая today
  2153 - Продажа ц/б биржевая
  2154 - Продажа ц/б биржевая today
  2122 - Обратное РЕПО на бирже без признания ц/б
  2127 - Прямое РЕПО на бирже без признания ц/б
  2010 - Списание ценных бумаг
  2011 - Зачисление ценных бумаг
*/
CREATE OR REPLACE FORCE VIEW DV_MKDEAL_MASS_EXEC
(           
   T_BofficeKind,
   T_DealID,
   T_LegID,
   T_ClientID,
   T_Kind_Operation,
   t_ID_Operation,
   t_ID_Step,
   t_DealDate,
   t_DealSetAvoiriss,   
   t_DealPay,           
   t_DealBeginExec,
   t_PFI,
   t_NKD,
   t_CFI,
   t_DealComiss,
   t_DealEnd
)
AS
(SELECT tick.t_BofficeKind,
        tick.t_DealID,
        leg.t_ID,
        tick.T_ClientID,
        opr.T_KIND_OPERATION,
        opr.t_ID_Operation,
        opr.t_ID_Step,                       
        RSI_RsbCalendar.GetDateAfterWorkDay( tick.t_DealDate, 0),
        ( CASE WHEN leg.T_MATURITYISPRINCIPAL = chr(88) THEN RSI_RsbCalendar.GetDateAfterWorkDay( leg.T_Maturity, 0)
               ELSE RSI_RsbCalendar.GetDateAfterWorkDay( leg.T_EXPIRY, 0 ) 
          END 
        ),
        ( CASE WHEN leg.T_MATURITYISPRINCIPAL = chr(88) THEN RSI_RsbCalendar.GetDateAfterWorkDay( leg.T_Expiry, 0)
               ELSE RSI_RsbCalendar.GetDateAfterWorkDay( leg.T_Maturity, 0 ) 
          END 
        ),
        RSI_RsbCalendar.GetDateAfterWorkDay( least( leg.t_Expiry, leg.t_Maturity ), 0 ),
        tick.t_PFI,
        leg.t_NKD,
        leg.t_CFI,
        CASE WHEN tick.t_CommDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN tick.t_CommDate ELSE RSI_RsbCalendar.GetDateAfterWorkDay( tick.t_CommDate, 0) END,
        RSI_RsbCalendar.GetDateAfterWorkDay( GREATEST( leg.t_Expiry, leg.t_Maturity, tick.t_CommDate ) , 0 )
   FROM doprtemp_view opr, 
        ddl_tick_dbt tick, 
        ddl_leg_dbt leg
  WHERE opr.t_DocKind IN (101, 127, 4830, 4831)
    AND TO_NUMBER(opr.t_DocumentID) = tick.t_DealID
    AND leg.t_DealID  = tick.t_DealID
    AND leg.t_LegKind = 0 
    AND leg.t_LegId   = 0
)
/


CREATE OR REPLACE FORCE VIEW DV_MKDEAL_MASS_EXEC_CALEND
(           
   T_BofficeKind,
   T_DealID,
   T_LegID,
   T_ClientID,
   T_Kind_Operation,
   t_ID_Operation,
   t_ID_Step,
   t_DealDate,
   t_DealSetAvoiriss,   
   t_DealPay,           
   t_DealBeginExec,
   t_PFI,
   t_NKD,
   t_CFI,
   t_DealComiss,
   t_DealEnd
)
AS
(SELECT tick.t_BofficeKind,
        tick.t_DealID,
        leg.t_ID,
        tick.T_ClientID,
        opr.T_KIND_OPERATION,
        opr.t_ID_Operation,
        opr.t_ID_Step,                       
        RSI_DlCalendars.SP_GetDateWorkDay(tick.t_DealDate, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID),
        ( CASE WHEN leg.T_MATURITYISPRINCIPAL = chr(88) THEN RSI_DlCalendars.SP_GetDateWorkDay(leg.T_Maturity, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID)
               ELSE RSI_DlCalendars.SP_GetDateWorkDay(leg.T_EXPIRY, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID)
          END 
        ),
        ( CASE WHEN leg.T_MATURITYISPRINCIPAL = chr(88) THEN RSI_DlCalendars.SP_GetDateWorkDay(leg.T_Expiry, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID) 
               ELSE RSI_DlCalendars.SP_GetDateWorkDay(leg.T_Maturity, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID) 
          END 
        ),
        RSI_DlCalendars.SP_GetDateWorkDay(least( leg.t_Expiry, leg.t_Maturity ), tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID),
        tick.t_PFI,
        leg.t_NKD,
        leg.t_CFI,
        CASE WHEN tick.t_CommDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') THEN tick.t_CommDate ELSE  RSI_DlCalendars.SP_GetDateWorkDay(tick.t_CommDate, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID) END,
        RSI_DlCalendars.SP_GetDateWorkDay(GREATEST( leg.t_Expiry, leg.t_Maturity, tick.t_CommDate ), tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID)
   FROM doprtemp_view opr, 
        ddl_tick_dbt tick, 
        ddl_leg_dbt leg
  WHERE opr.t_DocKind IN (101, 127, 4830, 4831)
    AND TO_NUMBER(opr.t_DocumentID) = tick.t_DealID
    AND leg.t_DealID  = tick.t_DealID
    AND leg.t_LegKind = 0 
    AND leg.t_LegId   = 0
)
/


/* Массовое исполнение БИРЖЕВЫХ сделок, вторая часть сделки. В дистрибутиве это операции 
  2122 - Обратное РЕПО на бирже без признания ц/б
  2127 - Прямое РЕПО на бирже без признания ц/б
*/
CREATE OR REPLACE FORCE VIEW DV_MKDEAL_MASS_EXEC_2
(           
   T_BofficeKind,
   T_DealID,
   T_LegID,
   T_Kind_Operation,
   t_ID_Operation,
   t_ID_Step,
   t_DealSetAvoiriss_2,   
   t_DealPay_2,           
   t_DealBeginExec_2,
   t_DealBegin_2,
   t_DealEnd_2
)
AS
(SELECT tick.t_BofficeKind,
        tick.t_DealID,
        leg.t_ID,
        opr.T_KIND_OPERATION,
        opr.t_ID_Operation,
        opr.t_ID_Step,                       
        ( CASE WHEN leg.T_MATURITYISPRINCIPAL = chr(88) THEN RSI_RsbCalendar.GetDateAfterWorkDay( leg.T_Maturity, 0)
               ELSE RSI_RsbCalendar.GetDateAfterWorkDay( leg.T_EXPIRY, 0 ) 
          END 
        ),
        ( CASE WHEN leg.T_MATURITYISPRINCIPAL = chr(88) THEN RSI_RsbCalendar.GetDateAfterWorkDay( leg.T_Expiry, 0)
               ELSE RSI_RsbCalendar.GetDateAfterWorkDay( leg.T_Maturity, 0 ) 
          END 
        ),
        RSI_RsbCalendar.GetDateAfterWorkDay( least( leg.t_Expiry, leg.t_Maturity ), 0 ),
        RSI_RsbCalendar.GetDateAfterWorkDay( least( leg.t_Expiry, leg.t_Maturity ), 0 ),
        RSI_RsbCalendar.GetDateAfterWorkDay( GREATEST( leg.t_Expiry, leg.t_Maturity, tick.t_CommDate ) , 0 )
   FROM doprtemp_view opr, 
        ddl_tick_dbt tick, 
        ddl_leg_dbt leg
  WHERE     opr.t_DocKind = 101
        AND TO_NUMBER(opr.t_DocumentID) = tick.t_DealID
        AND leg.t_DealID  = tick.t_DealID
        AND leg.t_LegKind = 2 /*LEG_KIND_DL_TICK_BACK*/
        AND leg.t_LegId   = 0
)
/

CREATE OR REPLACE FORCE VIEW DV_MKDEAL_MASS_EXEC_2_CALEND
(           
   T_BofficeKind,
   T_DealID,
   T_LegID,
   T_Kind_Operation,
   t_ID_Operation,
   t_ID_Step,
   t_DealSetAvoiriss_2,   
   t_DealPay_2,           
   t_DealBeginExec_2,
   t_DealBegin_2,
   t_DealEnd_2
)
AS
(SELECT tick.t_BofficeKind,
        tick.t_DealID,
        leg.t_ID,
        opr.T_KIND_OPERATION,
        opr.t_ID_Operation,
        opr.t_ID_Step,                       
        ( CASE WHEN leg.T_MATURITYISPRINCIPAL = chr(88) THEN RSI_DlCalendars.SP_GetDateWorkDay(leg.T_Maturity, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID)
               ELSE RSI_DlCalendars.SP_GetDateWorkDay(leg.T_EXPIRY, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID)
          END 
        ),
        ( CASE WHEN leg.T_MATURITYISPRINCIPAL = chr(88) THEN RSI_DlCalendars.SP_GetDateWorkDay(leg.T_Expiry, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID) 
               ELSE RSI_DlCalendars.SP_GetDateWorkDay(leg.T_Maturity, tick.t_BofficeKind, tick.t_DealID,tick.T_MARKETID) 
          END 
        ),
        RSI_DlCalendars.SP_GetDateWorkDay( least( leg.t_Expiry, leg.t_Maturity ), tick.t_BofficeKind,tick.t_DealID ,tick.T_MARKETID),
        RSI_DlCalendars.SP_GetDateWorkDay( least( leg.t_Expiry, leg.t_Maturity ), tick.t_BofficeKind,tick.t_DealID ,tick.T_MARKETID),
        RSI_DlCalendars.SP_GetDateWorkDay( GREATEST( leg.t_Expiry, leg.t_Maturity, tick.t_CommDate ), tick.t_BofficeKind,tick.t_DealID ,tick.T_MARKETID)
   FROM doprtemp_view opr, 
        ddl_tick_dbt tick, 
        ddl_leg_dbt leg
  WHERE     opr.t_DocKind = 101
        AND TO_NUMBER(opr.t_DocumentID) = tick.t_DealID
        AND leg.t_DealID  = tick.t_DealID
        AND leg.t_LegKind = 2 /*LEG_KIND_DL_TICK_BACK*/
        AND leg.t_LegId   = 0
)
/

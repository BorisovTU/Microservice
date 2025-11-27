declare
   CURSOR cNPTXOP is 
      SELECT 0 as t_ID,
             4605 as t_DocKind,
             2035 as t_Kind_Operation,
             CASE WHEN q2.t_Year < 2023 THEN 10 ELSE 20 END as t_SubKind_Operation,
             q2.t_Code as t_Code,
             CASE WHEN q2.t_Year < 2023 THEN q2.t_OperDate ELSE q2.t_PrevDate END as t_OperDate,
             q2.t_Client as t_Client,
             0 as t_Contract,
             q2.t_PrevDate as t_PrevDate,
             0 as t_PlaceKind,
             -1 as t_Place,
             0 as t_TaxBase,
             0 as t_OutSum,
             0 as t_OutCost,
             0 as t_TOut,
             0 as t_TotalTaxSum,
             0 as t_PrevTaxSum,
             0 as t_TaxSum,
             0 as t_Tax,
             0 as t_Method,
             CHR (1) as t_Account,
             0 as t_Currency,
             0 as t_Status,
             NULL as t_Oper,
             NULL as t_Department,
             CHR (0) as t_IIS,
             0 as t_TaxToPay,
             CHR (88) as t_CalcNDFL,
             CHR (88) as t_Recalc,
             TO_DATE ('01.01.' || q2.t_Year, 'dd.mm.yyyy') as t_BegRecalcDate,
             q2.t_PrevDate as t_EndRecalcDate,
             TO_DATE ('01.01.0001', 'dd.mm.yyyy') as t_Time,
             0 as t_CurrentYear_Sum,
             1 as t_CurrencySum,
             CHR (0) as t_FlagTax,
             CHR (0) as t_Partial,
             CHR (1) as t_AccountTax,
             0 as t_TaxSum2,
             -1 as t_FIID,
             CHR (0) as t_CloseContr,
             0 as t_LimitStatus,
             0 as t_PlaceKind2,
             0 as t_Place2,
             0 as t_MarketPlace,
             0 as t_MarketPlace2,
             0 as t_MarketSector,
             0 as t_MarketSector2,
             0 as t_TaxDP
        FROM (SELECT q1.*,
                     q1.t_Client || '_' || q1.t_Year || '_Recalc_NKD' AS t_Code,
                     NVL (
                        (SELECT MAX (PrevNptxop.t_OperDate)
                           FROM dnptxop_dbt PrevNptxop
                          WHERE     PrevNptxop.t_DocKind = 4605
                                AND PrevNptxop.t_Recalc = CHR (0)
                                AND PrevNptxop.t_Client = q1.t_Client
                                AND PrevNptxop.t_IIS = CHR (0)
                                AND PrevNptxop.t_OperDate <
                                       TO_DATE ('31.12.' || q1.t_Year,
                                                'dd.mm.yyyy')
                                AND PrevNptxop.t_Status <> 0
                                AND PrevNptxop.t_SubKind_Operation <> 50),
                        TO_DATE ('31.12.' || q1.t_Year, 'dd.mm.yyyy'))
                        AS t_PrevDate
                FROM (SELECT DISTINCT
                             obj.t_Client,
                             EXTRACT (YEAR FROM obj.t_Date) AS t_Year,
                             TO_DATE (
                                '31.12.'
                                || TO_CHAR (EXTRACT (YEAR FROM obj.t_Date), '9999'),
                                'dd.mm.yyyy')
                                AS t_OperDate
                        FROM dnptxobj_dbt obj,
                             ddl_tick_dbt tick,
                             ddl_leg_dbt leg,
                             dfininstr_dbt fininstr
                       WHERE     obj.t_Date >= TO_DATE ('01.01.2019', 'dd.mm.yyyy')
                             AND obj.t_Kind = 20                               --NKD
                             AND tick.t_DealID = obj.t_Analitic1
                             AND leg.t_DealID = obj.t_Analitic1
                             AND leg.t_LegID = 0
                             AND leg.t_LegKind = 0
                             AND fininstr.t_FIID = leg.t_PFI
                             AND fininstr.t_FaceValueFI <> LEG.T_NKDFIID
                             AND fininstr.t_FaceValueFI = obj.t_Cur
                             AND leg.t_NKD > 0) q1) q2
       WHERE NOT EXISTS
                (SELECT 1
                   FROM dnptxop_dbt op
                  WHERE op.t_DocKind = 4605 AND op.t_Code = q2.t_code);
begin
   for CurNPTXOP in cNPTXOP
   loop
      insert into dnptxop_dbt (
                                 t_ID,
                                 t_DocKind,
                                 t_Kind_Operation,
                                 t_SubKind_Operation,
                                 t_Code,
                                 t_OperDate,
                                 t_Client,
                                 t_Contract,
                                 t_PrevDate,
                                 t_PlaceKind,
                                 t_Place,
                                 t_TaxBase,
                                 t_OutSum,
                                 t_OutCost,
                                 t_TOut,
                                 t_TotalTaxSum,
                                 t_PrevTaxSum,
                                 t_TaxSum,
                                 t_Tax,
                                 t_Method,
                                 t_Account,
                                 t_Currency,
                                 t_Status,
                                 t_Oper,
                                 t_Department,
                                 t_IIS,
                                 t_TaxToPay,
                                 t_CalcNDFL,
                                 t_Recalc,
                                 t_BegRecalcDate,
                                 t_EndRecalcDate,
                                 t_Time,
                                 t_CurrentYear_Sum,
                                 t_CurrencySum,
                                 t_FlagTax,
                                 t_Partial,
                                 t_AccountTax,
                                 t_TaxSum2,
                                 t_FIID,
                                 t_CloseContr,
                                 t_LimitStatus,
                                 t_PlaceKind2,
                                 t_Place2,
                                 t_MarketPlace,
                                 t_MarketPlace2,
                                 t_MarketSector,
                                 t_MarketSector2,
                                 t_TaxDP
                              )
                       values (
                               CurNPTXOP.t_ID,
                               CurNPTXOP.t_DocKind,
                               CurNPTXOP.t_Kind_Operation,
                               CurNPTXOP.t_SubKind_Operation,
                               CurNPTXOP.t_Code,
                               CurNPTXOP.t_OperDate,
                               CurNPTXOP.t_Client,
                               CurNPTXOP.t_Contract,
                               CurNPTXOP.t_PrevDate,
                               CurNPTXOP.t_PlaceKind,
                               CurNPTXOP.t_Place,
                               CurNPTXOP.t_TaxBase,
                               CurNPTXOP.t_OutSum,
                               CurNPTXOP.t_OutCost,
                               CurNPTXOP.t_TOut,
                               CurNPTXOP.t_TotalTaxSum,
                               CurNPTXOP.t_PrevTaxSum,
                               CurNPTXOP.t_TaxSum,
                               CurNPTXOP.t_Tax,
                               CurNPTXOP.t_Method,
                               CurNPTXOP.t_Account,
                               CurNPTXOP.t_Currency,
                               CurNPTXOP.t_Status,
                               CurNPTXOP.t_Oper,
                               CurNPTXOP.t_Department,
                               CurNPTXOP.t_IIS,
                               CurNPTXOP.t_TaxToPay,
                               CurNPTXOP.t_CalcNDFL,
                               CurNPTXOP.t_Recalc,
                               CurNPTXOP.t_BegRecalcDate,
                               CurNPTXOP.t_EndRecalcDate,
                               CurNPTXOP.t_Time,
                               CurNPTXOP.t_CurrentYear_Sum,
                               CurNPTXOP.t_CurrencySum,
                               CurNPTXOP.t_FlagTax,
                               CurNPTXOP.t_Partial,
                               CurNPTXOP.t_AccountTax,
                               CurNPTXOP.t_TaxSum2,
                               CurNPTXOP.t_FIID,
                               CurNPTXOP.t_CloseContr,
                               CurNPTXOP.t_LimitStatus,
                               CurNPTXOP.t_PlaceKind2,
                               CurNPTXOP.t_Place2,
                               CurNPTXOP.t_MarketPlace,
                               CurNPTXOP.t_MarketPlace2,
                               CurNPTXOP.t_MarketSector,
                               CurNPTXOP.t_MarketSector2,
                               CurNPTXOP.t_TaxDP
                              );
      it_log.log('DEF-56843: Создана операция пересчета НОБ с кодом ' || CurNPTXOP.t_Code || ', клиент ' || CurNPTXOP.t_Client || ', дата ' || to_char(CurNPTXOP.t_OperDate, 'dd.mm.yyyy'));
   end loop;
end;
/
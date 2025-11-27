declare
   v_CalcNKD NUMBER := 0;
   v_CalcTotalCost NUMBER := 0;
   v_RQAmount NUMBER := 0;
   v_OperCode VARCHAR2(32);
   
   CURSOR cNPTXOP is select q.t_Client, q.t_Year, to_date('31.12.' || to_char(q.t_Year, '9999'), 'dd.mm.yyyy') as t_OperDate,
                            nvl (
                                   (
                                      select max(PrevNptxop.t_OperDate)
                                      from dnptxop_dbt PrevNptxop
                                      where PrevNptxop.t_DocKind = 4605
                                        and PrevNptxop.t_Recalc = chr(0)
                                        and PrevNptxop.t_Client = q.t_Client
                                        and PrevNptxop.t_IIS = chr(0)
                                        and PrevNptxop.t_OperDate < to_date('31.12.' || q.t_Year, 'dd.mm.yyyy')
                                        and PrevNptxop.t_Status <> 0
                                        and PrevNptxop.t_SubKind_Operation <> 50
                                     ), to_date('31.12.' || q.t_Year, 'dd.mm.yyyy')
                                ) as t_PrevDate
                         from (
                                  select obj.t_Client, extract(year from obj.t_Date) as t_Year, obj.t_Date
                                  from dnptxobj_dbt obj, ddl_tick_dbt tick, ddl_leg_dbt leg, dfininstr_dbt fininstr
                                  where obj.t_Date >= to_date('01.01.2019', 'dd.mm.yyyy')
                                    and obj.t_Kind = 20 --NKD
                                    --and obj.t_AnaliticKind1 = 1010 --TЇї√·Ё
                                    and tick.t_DealID = obj.t_Analitic1
                                    and leg.t_DealID = obj.t_Analitic1
                                    and fininstr.t_FIID = leg.t_PFI
                                    --and leg.t_CFI = 0
                                    and fininstr.t_FaceValueFI <> 0
                                    and fininstr.t_FaceValueFI = obj.t_Cur
                                    and leg.t_NKD > 0
                                    and obj.t_Sum / leg.t_NKD >= 50
                                    and obj.t_Sum / leg.t_NKD < 100
                                    and not exists(
                                                     select 1
                                                     from ddl_leg_dbt leg1
                                                     where leg1.t_DealID = obj.t_Analitic1
                                                       and leg1.t_ID <> leg.t_ID
                                                       and obj.t_Sum / leg1.t_NKD < 1.1
                                                       and leg1.t_NKD > 0
                                                  )
                                  union
                                  select objbc.t_Client, extract(year from objbc.t_Date) as t_Year, objbc.t_Date
                                  from dnptxobjbc_dbt objbc, ddl_tick_dbt tick, ddl_leg_dbt leg, dfininstr_dbt fininstr
                                  where objbc.t_Date >= to_date('01.01.2019', 'dd.mm.yyyy')
                                    and objbc.t_Kind = 20 --NKD
                                    --and obj.t_AnaliticKind1 = 1010 --TЇї√·Ё
                                    and tick.t_DealID = objbc.t_Analitic1
                                    and leg.t_DealID = objbc.t_Analitic1
                                    and fininstr.t_FIID = leg.t_PFI
                                    --and leg.t_CFI = 0
                                    and fininstr.t_FaceValueFI <> 0
                                    and fininstr.t_FaceValueFI = objbc.t_Cur
                                    and leg.t_NKD > 0
                                    and objbc.t_Sum / leg.t_NKD >= 50
                                    and objbc.t_Sum / leg.t_NKD < 100
                                    and not exists(
                                                     select 1
                                                     from ddl_leg_dbt leg1
                                                     where leg1.t_DealID = objbc.t_Analitic1
                                                       and leg1.t_ID <> leg.t_ID
                                                       and objbc.t_Sum / leg1.t_NKD < 1.1
                                                       and leg1.t_NKD > 0
                                                  )
                              ) q
                     group by q.t_Client, q.t_Year
                     order by q.t_Year;

   CURSOR cDLRQ is select q.t_RQID, q.t_DealID, q.t_LegID, q.t_Cost,
                          (
                             select nvl(sum(avrq.t_Amount), 0)
                             from ddlrq_dbt avrq
                             where avrq.t_DocKind = q.t_DocKind
                               and avrq.t_DocID = q.t_DocID
                               and avrq.t_Type = 0 --DLRQ_TYPE_AVANCE
                               and avrq.t_DealPart = q.t_DealPart
                          ) as t_AvanceSum,
                          (
                             select nvl(sum(avrq.t_Amount), 0)
                             from ddlrq_dbt avrq
                             where avrq.t_DocKind = q.t_DocKind
                               and avrq.t_DocID = q.t_DocID
                               and avrq.t_Type = 1 --DLRQ_TYPE_DEPOSIT
                               and avrq.t_DealPart = q.t_DealPart
                          ) as t_DepositSum,
                          q.t_PFI,
                          q.t_CFI,
                          q.t_FaceValueFI,
                          q.t_Principal,
                          q.t_DealCode,
                          q.t_FactDate
                   from (
                           select rq.t_ID as t_RQID, tick.t_DealID, leg.t_ID as t_LegID, leg.t_Cost, rq.t_DocKind, rq.t_DocID, rq.t_DealPart, leg.t_PFI, leg.t_CFI, fininstr.t_FaceValueFI,
                                  leg.t_Principal, tick.t_DealCOde, rq.t_FactDate
                           from dnptxobj_dbt obj, ddl_tick_dbt tick, ddl_leg_dbt leg, dfininstr_dbt fininstr, ddlrq_dbt rq
                           where obj.t_Date >= to_date('01.01.2019', 'dd.mm.yyyy')
                             and obj.t_Kind = 20 --NKD
                             and tick.t_DealID = obj.t_Analitic1
                             and leg.t_DealID = obj.t_Analitic1
                             and fininstr.t_FIID = leg.t_PFI
                             and fininstr.t_FaceValueFI <> 0
                             and fininstr.t_FaceValueFI = obj.t_Cur
                             and leg.t_NKD > 0
                             and obj.t_Sum / leg.t_NKD >= 50
                             and obj.t_Sum / leg.t_NKD < 100
                             and not exists(
                                              select 1
                                              from ddl_leg_dbt leg1
                                              where leg1.t_DealID = obj.t_Analitic1
                                                and leg1.t_ID <> leg.t_ID
                                                and obj.t_Sum / leg1.t_NKD < 1.1
                                                and leg1.t_NKD > 0
                                           )
                             and rq.t_DocKind = tick.t_BOfficeKind
                             and rq.t_DocID = tick.t_DealID
                             and rq.t_Type = 2 --DLRQ_TYPE_PAYMENT
                             and (
                                    (
                                       leg.t_LegKind = 0
                                       and rq.t_DealPart = 1
                                    )
                                    or (
                                          leg.t_LegKind = 2
                                          and rq.t_DealPart = 2
                                       )
                                 )
                           union
                           select rq.t_ID as t_RQID, tick.t_DealID, leg.t_ID as t_LegID, leg.t_Cost, rq.t_DocKind, rq.t_DocID, rq.t_DealPart, leg.t_PFI, leg.t_CFI, fininstr.t_FaceValueFI,
                                  leg.t_Principal, tick.t_DealCOde, rq.t_FactDate
                           from dnptxobjbc_dbt objbc, ddl_tick_dbt tick, ddl_leg_dbt leg, dfininstr_dbt fininstr, ddlrq_dbt rq
                           where objbc.t_Date >= to_date('01.01.2019', 'dd.mm.yyyy')
                             and objbc.t_Kind = 20 --NKD
                             and tick.t_DealID = objbc.t_Analitic1
                             and leg.t_DealID = objbc.t_Analitic1
                             and fininstr.t_FIID = leg.t_PFI
                             and fininstr.t_FaceValueFI <> 0
                             and fininstr.t_FaceValueFI = objbc.t_Cur
                             and leg.t_NKD > 0
                             and objbc.t_Sum / leg.t_NKD >= 50
                             and objbc.t_Sum / leg.t_NKD < 100
                             and not exists(
                                              select 1
                                              from ddl_leg_dbt leg1
                                              where leg1.t_DealID = objbc.t_Analitic1
                                                and leg1.t_ID <> leg.t_ID
                                                and objbc.t_Sum / leg1.t_NKD < 1.1
                                                and leg1.t_NKD > 0
                                           )
                             and rq.t_DocKind = tick.t_BOfficeKind
                             and rq.t_DocID = tick.t_DealID
                             and rq.t_Type = 2 --DLRQ_TYPE_PAYMENT
                             and (
                                    (
                                       leg.t_LegKind = 0
                                       and rq.t_DealPart = 1
                                    )
                                    or (
                                          leg.t_LegKind = 2
                                          and rq.t_DealPart = 2
                                       )
                                 )
                        ) q;
begin
   for CurDLRQ in cDLRQ
   loop
      v_CalcNKD := rsi_rsb_fiinstr.FI_CalcNKD(CurDLRQ.t_PFI, CurDLRQ.t_FactDate, CurDLRQ.t_Principal, 0);
      v_CalcTotalCost := CurDLRQ.t_Cost + RSB_FIINSTR.ConvSum(v_CalcNKD, CurDLRQ.t_FaceValueFI, CurDLRQ.t_CFI, CurDLRQ.t_FactDate);
   
      update ddl_leg_dbt leg
      set leg.t_NKD = v_CalcNKD,
          leg.t_TotalCost = v_CalcTotalCost,
          leg.t_NKDFIID = CurDLRQ.t_FaceValueFI
      where leg.t_ID = CurDLRQ.t_LegID;
      
      it_log.log('DEF-50166: Обновлена запись DDL_LEG_DBT по сделке ' || CurDLRQ.t_DealCode || '. t_NKD = ' || v_CalcNKD || ', t_TotalCost = ' || v_CalcTotalCost);
      
      v_RQAmount := v_CalcTotalCost + CurDLRQ.t_AvanceSum + CurDLRQ.t_DepositSum;
      update ddlrq_dbt rq
      set rq.t_Amount = v_RQAmount
      where rq.t_ID = CurDLRQ.t_RQID;
      
      it_log.log('DEF-50166: Обновлено ТО по сделке ' || CurDLRQ.t_DealCode || '. t_Amount = ' || v_RQAmount);
   end loop;
  
   for CurNPTXOP in cNPTXOP
   loop
      v_OperCode := CurNPTXOP.t_Client || '_' || extract(year from CurNPTXOP.t_OperDate) || '_Recalc_NKD';
      
      begin
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
                                    0, -- t_ID
                                    4605, --t_DocKind
                                    2035, --t_Kind_Operation
                                    case when CurNPTXOP.t_Year < 2023 then 10 else 20 end, --t_SubKind_Operation
                                    CurNPTXOP.t_Client || '_' || extract(year from CurNPTXOP.t_OperDate) || '_Recalc_NKD', --t_Code
                                    case when CurNPTXOP.t_Year < 2023 then CurNPTXOP.t_OperDate else CurNPTXOP.t_PrevDate end, --t_OperDate
                                    CurNPTXOP.t_Client, --t_Client
                                    0,
                                    CurNPTXOP.t_PrevDate, --t_PrevDate
                                    0, --t_PlaceKind
                                    -1, --t_Place
                                    0, --t_TaxBase
                                    0, --t_OutSum
                                    0, --t_OutCost
                                    0, --t_TOut
                                    0, --t_TotalTaxSum
                                    0, --t_PrevTaxSum
                                    0, --t_TaxSum
                                    0, --t_Tax
                                    0, --t_Method
                                    chr(1),--t_Account
                                    0, --t_Currency
                                    0, --t_Status
                                    NULL, --t_Oper
                                    NULL, --t_Department
                                    chr(0),--t_IIS
                                    0, --t_TaxToPay
                                    chr(88), --t_CalcNDFL
                                    chr(88), --t_Recalc
                                    to_date('01.01.' || CurNPTXOP.t_Year, 'dd.mm.yyyy'), --t_BegRecalcDate,
                                    CurNPTXOP.t_PrevDate, --t_EndRecalcDate,
                                    to_date('01.01.0001', 'dd.mm.yyyy'), --t_Time
                                    0,--t_CurrentYear_Sum
                                    1, --t_CurrencySum
                                    chr(0), --t_FlagTax
                                    chr(0), --t_Partial
                                    chr(1), --t_AccountTax
                                    0, --t_TaxSum2
                                    -1, --t_FIID
                                    chr(0), --t_CloseContr
                                    0, --t_LimitStatus
                                    0, --t_PlaceKind2
                                    0, --t_Place2
                                    0, --t_MarketPlace
                                    0, --t_MarketPlace2
                                    0, --t_MarketSector
                                    0, --t_MarketSector2
                                    0  --t_TaxDP
                                 );

         it_log.log('DEF-50166: Создана операция пересчета НОБ с кодом ' || CurNPTXOP.t_Client || '_' || extract(year from CurNPTXOP.t_OperDate) || '_Recalc_NKD' || ', клиент ' || CurNPTXOP.t_Client || ', дата ' || to_char(CurNPTXOP.t_OperDate, 'dd.mm.yyyy'));
      exception
         when DUP_VAL_ON_INDEX
         then it_log.log('DEF-50166: Уже существует операция с кодом ' || CurNPTXOP.t_Client || '_' || extract(year from CurNPTXOP.t_OperDate) || '_Recalc_NKD');
      end;
   end loop;

   commit;
end;
/
--Корректировка сумм НКД в сделках, загруженных через Payments, в которых участвуют валютные облигации

declare
   v_CalcNKD  NUMBER := 0;
   v_msg_clob CLOB;
   v_totalCost NUMBER := 0;

   CURSOR CDeals IS select tick.t_DealID, fininstr.t_FaceValueFI, rq.t_FactDate, leg.t_PFI/*, tick.t_DealDate - warn.t_FirstDate as t_CoupDays, warn.t_IncomeRate*/, leg.t_NKDFIID,
                                         tick.t_DealDate,
                                         tick.t_DealTime,
                                         tick.t_DealCode,
                                         (
                                            select q.t_Code
                                            from (
                                                      select objcode.t_Code
                                                      from dobjcode_dbt objcode
                                                      where objcode.t_ObjectType = 3
                                                        and objcode.t_CodeKind = 1
                                                        and objcode.t_ObjectID = tick.t_ClientID
                                                        and objcode.t_BankDate <= sysdate
                                                        and (
                                                                 objcode.t_BankCloseDate > sysdate
                                                                 or objcode.t_BankCloseDate = to_date('01.01.0001', 'dd.mm.yyyy')
                                                              )
                                                     order by objcode.t_BankDate desc
                                                  ) q
                                            where ROWNUM = 1
                                         ) as t_ClientCode,
                                         fininstr.t_FI_Code,
                                         leg.t_NKD,
                                         FaceValueFI.t_Ccy as t_CurCalcNkd,
                                         FaceValueFI.t_Ccy as t_CurNKD,
                                         leg.t_Principal,
                                         leg.t_ID as t_LegID,
                                         leg.t_Cost,
                                         leg.t_CFI
                               from ddl_tick_dbt tick, ddl_leg_dbt leg, dfininstr_dbt fininstr, ddlrq_dbt rq, /*dfiwarnts_dbt warn, */dfininstr_dbt FaceValueFI, dfiwarnts_dbt warn
                               where tick.t_DealDate >= to_date('01.01.2019', 'dd.mm.yyyy')
                                   and tick.t_ClientID > 0
                                   and leg.t_DealID = tick.t_DealID
                                   and fininstr.t_FIID = leg.t_PFI
                                   and WARN.T_FIID = leg.t_PFI
                                   and WARN.T_ISPARTIAL <> 'X'
                                   and WARN.T_FIRSTDATE <= tick.t_DealDate
                                   and WARN.T_DRAWINGDATE > tick.t_DealDate
                                   and LEG.T_CFI <> fininstr.t_FaceValueFI
                                   and LEG.T_LEGID = 0
                                   and leg.t_NKD > 0
                                   and RQ.T_DOCKIND = tick.t_BOfficeKind
                                   and rq.t_DocID = tick.t_DealID
                                   and RQ.T_TYPE = 8
                                   and (
                                            (
                                               leg.t_LegKind <> 2
                                               and RQ.T_DEALPART = 1
                                            )
                                         or (
                                                leg.t_LegKind = 2
                                                and rq.t_DealPart = 2
                                             )
                                         )
                                   and FACEVALUEFI.T_FIID = fininstr.t_FaceValueFI;
begin
      FOR CurDeal IN CDeals
      LOOP  
        v_msg_clob := null;      
        v_CalcNKD  := RSI_RSB_FIInstr.FI_CalcNKD(CurDeal.t_PFI, CurDeal.t_DealDate, CurDeal.t_Principal, 0);

         IF v_CalcNKD <> 0
         THEN
            IF CurDeal.t_NKD / v_CalcNKD >= 50 and CurDeal.t_NKD / v_CalcNKD < 100 
            THEN
               v_msg_clob := 't_ID = ' || CurDeal.t_LegID || ' t_NKD_old = '||CurDeal.t_NKD || ' t_NKD_new = ' || v_CalcNKD;
               it_log.log(v_msg_clob); 
               
               v_TotalCost := CurDeal.t_Cost + RSB_FIINSTR.ConvSum(v_CalcNKD, CurDeal.t_FaceValueFI, CurDeal.t_CFI, CurDeal.t_DealDate);
               execute immediate ' update ddl_leg_dbt ' ||
                                              ' set t_NKD = :1' ||
                                              '    ,t_TotalCost = :2' ||
                                              ' where t_ID = :3' USING v_CalcNKD, v_TotalCost, CurDeal.t_LegID;
            END IF;
         END IF;
      END LOOP;

      COMMIT;       
end;
/
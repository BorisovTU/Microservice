CREATE OR REPLACE PACKAGE BODY rsb_dlwreporep
IS /* Тело пакета rsb_dlwreporep */ 
 
  FUNCTION GetCatID (p_DealType IN NUMBER, p_DocKind IN NUMBER)
   RETURN NUMBER
  IS
    CatID NUMBER;
  BEGIN
    CatID := DL_CATID_MAINRESTP;
    
    IF Rsb_Secur.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(p_DealType, p_DocKind))) = 1 THEN
      CatID := DL_CATID_MAINRESTR;
    END IF;
    
    RETURN CatID;
  END GetCatID;

  PROCEDURE CreateData(DepartmentID IN NUMBER, BegDate IN DATE, EndDate IN DATE, Oper IN NUMBER, RepID OUT INTEGER, Err OUT INTEGER)
  IS
     TYPE wreporep_t IS TABLE OF DWREPOREP_TMP%ROWTYPE;
     g_wreporep_ins wreporep_t := wreporep_t();
     wreporep DWREPOREP_TMP%rowtype;
  BEGIN
     DELETE FROM DWREPOREP_TMP;
     --FOR one_rec IN (select 1 from dual)
     FOR one_rec IN (
       SELECT deal.*, 
              nvl(rsi_rsb_fiinstr.convsum(t_begdealsum,
                                      deal.t_acccur,
                                      0,
                                      BegDate), 0) t_begdealsumrur,
              nvl(rsi_rsb_fiinstr.convsum(t_enddealsum,
                                      deal.t_acccur,
                                      0,
                                      EndDate), 0) t_enddealsumrur          
       FROM (
           SELECT tick.t_dealcode,
                  issuer.t_name t_issuername,
                  party.t_name t_partyname,
                  fin.t_ccy t_ccy,
                  (CASE WHEN Rsb_Secur.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType,
                                                                                           tick.t_BofficeKind)))=1 THEN 'Обратное РЕПО' ELSE 'Прямое РЕПО' END) t_dealtype,
                  nvl(abs(rsb_account.restall(nvl(accdoc1.t_account, accdoc2.t_account),
                                nvl(accdoc1.t_chapter, accdoc2.t_chapter),
                                nvl(accdoc1.t_currency, accdoc2.t_currency),
                                BegDate-1)),
                  0) t_begdealsum,
                  nvl(abs(rsb_account.restall(nvl(accdoc1.t_account, accdoc2.t_account),
                                nvl(accdoc1.t_chapter, accdoc2.t_chapter),
                                nvl(accdoc1.t_currency, accdoc2.t_currency),
                                EndDate-1)),
                  0) t_enddealsum,
                  nvl(accdoc1.t_currency, accdoc2.t_currency) t_acccur
             FROM ddl_tick_dbt tick
             JOIN ddl_leg_dbt leg1 ON leg1.t_dealid = tick.t_dealid
                                  AND leg1.t_legid = 0
                                  AND leg1.t_legkind = 0
        LEFT JOIN ddl_leg_dbt leg2 ON leg2.t_dealid = tick.t_dealid
                                  AND leg2.t_legid = 0
                                  AND leg2.t_legkind = 2
        LEFT JOIN dmcaccdoc_dbt accdoc1 ON  accdoc1.t_dockind=176 and accdoc1.t_docid = leg1.t_id and accdoc1.t_catid  = rsb_dlwreporep.GetCatID (tick.t_DealType,tick.t_BofficeKind)
        LEFT JOIN dmcaccdoc_dbt accdoc2 ON  accdoc2.t_dockind=176 and accdoc2.t_docid = leg2.t_id and accdoc2.t_catid  = rsb_dlwreporep.GetCatID (tick.t_DealType,tick.t_BofficeKind)
             JOIN dfininstr_dbt fin ON fin.t_FIID = leg1.t_CFI
        LEFT JOIN dfininstr_dbt pfi ON pfi.t_FIID = leg1.t_PFI
        LEFT JOIN dparty_dbt issuer ON issuer.t_PartyID = pfi.t_Issuer
        LEFT JOIN dparty_dbt party  ON party.t_PartyID = tick.t_PartyID
            WHERE --tick.t_bofficekind in (101, 117, 127) 
                tick.t_bofficekind = 101 
            AND tick.t_clientid = -1 
            AND tick.t_dealstatus > 0
            AND tick.t_dealdate <= EndDate
            AND RSB_SECUR.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(tick.t_DealType,
                                                                                           tick.t_BofficeKind))) = 1
            AND ((rsb_brkrep_u.GetExecDate (tick.t_bofficekind, tick.t_dealid, 1) BETWEEN BegDate and EndDate) OR
                 (rsb_brkrep_u.GetExecDate (tick.t_bofficekind, tick.t_dealid, 2) BETWEEN BegDate and EndDate))
            AND (leg1.t_RejectDate = to_date('01.01.0001','DD.MM.YYYY') or leg1.t_RejectDate >= BegDate)
            AND TICK.T_DEPARTMENT = (case when DepartmentID > 0 then DepartmentID else TICK.T_DEPARTMENT end)
       
       ) deal)
     LOOP
      wreporep.t_dealcode      := one_rec.t_dealcode;
      wreporep.t_partyname     := one_rec.t_partyname;
      wreporep.t_issuername    := one_rec.t_issuername;
      wreporep.t_dealcur       := one_rec.t_ccy;
      wreporep.t_dealtype      := one_rec.t_dealtype;
      wreporep.t_begdealsum    := one_rec.t_begdealsum;
      wreporep.t_begdealsumrur := one_rec.t_begdealsumrur;
      wreporep.t_enddealsum    := one_rec.t_enddealsum;
      wreporep.t_enddealsumrur := one_rec.t_enddealsumrur;
      
      g_wreporep_ins.extend;
      g_wreporep_ins(g_wreporep_ins.LAST) := wreporep;
     END LOOP;

     IF g_wreporep_ins IS NOT EMPTY THEN
         FORALL i IN g_wreporep_ins.FIRST .. g_wreporep_ins.LAST
              INSERT INTO DWREPOREP_TMP
                   VALUES g_wreporep_ins(i);
         g_wreporep_ins.delete;
     END IF;
     
     Err := 0;
     INSERT INTO DDL_REPORT_REPO_DBT (T_PERIOD, T_OPER, T_OPERDATE, T_SYSDATE, T_SYSTIME, T_ERR) 
          VALUES ((to_char(BegDate, 'DD.MM.YYYY') || '-' || to_char(EndDate, 'DD.MM.YYYY')), Oper, RsbSessionData.curdate, sysdate, sysdate, 0)
       RETURNING T_ID INTO RepID;
     --INSERT INTO DDL_REPORT_REPO_DBT (T_PERIOD, T_OPERDATE, T_ERR) VALUES ((to_char(BegDate, 'DD.MM.YYYY') || '-' || to_char(EndDate, 'DD.MM.YYYY')), RsbSessionData.curdate, 0);

  END CreateData;

END rsb_dlwreporep;
/

create or replace package body rshb_limit_util is

  C_MICEX_CODE_CURMARKET varchar2(128);

  C_MICEX_CODE_STOCKMARKET varchar2(128);

  RegVal_RepoWithCCPTradingModes VARCHAR2(200)
     := ','||REPLACE(RSB_COMMON.GetRegStrValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\QUIK\РЕЖ_ТОРГОВ_РЕПО_С_ЦК'),' ','')||',';

  RegVal_AllowedDif NUMBER(32,12)
     := COALESCE(TO_NUMBER(RSB_COMMON.GetRegDoubleValue('РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\СВЕРКА С ДОП ДАННЫМИ ПОГРЕШ')),0.02);

  FUNCTION to_chardatesql(p_Date date) return varchar2 as
  BEGIN
    RETURN ' to_date('''||to_char(p_Date,'ddmmyyyy')||''',''ddmmyyyy'')';
  END;

  function getQueryWhere(p_alias         varchar2
                        ,p_secur_checked integer
                        ,p_cur_checked   integer
                        ,p_first_cond    boolean default false) return varchar2 as
    v_query_where varchar2(32000);
    v_alias varchar2(32) := case
                              when p_alias is not null then
                               p_alias || '.'
                            end;
  begin
    if p_secur_checked = 1
       and p_cur_checked = 1
    then
      v_query_where := ' (' || v_alias || 't_firm_id = ''' || C_MICEX_CODE_CURMARKET || ''' or ' || v_alias || 't_firm_id = ''' ||
                       C_MICEX_CODE_STOCKMARKET || ''') ';
    elsif p_secur_checked = 1
    then
      v_query_where := ' ' || v_alias || 't_firm_id = ''' || C_MICEX_CODE_STOCKMARKET || ''' ';
    elsif p_cur_checked = 1
    then
      v_query_where := ' ' || v_alias || 't_firm_id = ''' || C_MICEX_CODE_CURMARKET || ''' ';
    else
      v_query_where := '  1 = 1  ';
    end if;
    return case when p_first_cond then ' where ' else ' and ' end || v_query_where;
  end;

  procedure AddMessageToBegin(p_str varchar2) as
    v_str varchar2(32676) := replace(p_str, '"', '""');
  begin
    v_str := translate(v_str, chr(0) || chr(13), ' ');
    if instr(v_str, '"') > 0
       or instr(v_str, ';') > 0
    then
      v_str := '"' || v_str || '"';
    end if;
    it_rsl_string.insert_before_varchar(v_str || chr(13) || chr(10));
  end;

  function GetDifBrokerComisByQuikData(p_basedate date, p_tradedate date, p_limkind NUMBER,
                                       p_currency VARCHAR2, p_clientcode VARCHAR2, p_quikdataexists integer) return NUMBER deterministic is
     pragma udf;
     v_retSum NUMBER(32,12) := 0;
  begin
    if p_quikdataexists = 1 THEN
     SELECT NVL(SUM (NVL (qdeal.T_BROKERCOMMISSION, 0)),0) AS t_sum INTO v_retSum
       FROM DQUIK_DEALS_DBT qdeal, DLIMIT_BSDATE_DBT limdate
      WHERE t_tradedate = p_tradedate
            AND QDEAL.T_SETTLEDATE BETWEEN limdate.T_SETTLEDATEFROM
                                       AND limdate.T_SETTLEDATETO
            AND qdeal.T_ClientCode = p_clientcode
            and LIMDATE.T_BASEDATE = p_basedate
            AND limdate.T_LIMITKIND <= p_limkind
            AND qdeal.T_TRADECURRENCY IN ( (CASE WHEN p_currency = 'SUR' THEN 'RUB' ELSE p_currency END), p_currency);
    END IF;

    return v_retSum;
  end;

  FUNCTION GetCursorRepoByQuikData (p_basedate date,
                                    p_tradedate date,
                                    p_clientcode    VARCHAR2,
                                    p_currency      VARCHAR2 DEFAULT NULL,
                                    p_seccode       VARCHAR2 DEFAULT NULL)
     RETURN SYS_REFCURSOR
  IS
     v_curAlias          VARCHAR2(10);
     v_Cursor            SYS_REFCURSOR;
     v_sqlBuf            VARCHAR2(10000);
  BEGIN
     v_sqlBuf :=
      q'[SELECT CASE
                  WHEN LOWER (repo1.T_OPERATION) = 'продажа' THEN 1
                  ELSE -1
               END
                  AS T_SELL,
               NVL (repo1.T_TSCOMMMISSION, 0) AS T_TSCOMMMISSION,
               NVL (repo1.T_BROKERCOMMISSION, 0) AS T_BROKERCOMMISSION,
               NVL (repo1.T_REPOVALUE, 0) AS T_REPOVALUE,
               NVL (repo1.T_REPO2VALUE, 0) AS T_REPO2VALUE,
               NVL (repo1.T_QTY, 0) AS T_QTY,
               NVL (repo1.T_VALUE, 0) AS T_VALUE,
               T_LIMITKIND
          FROM DQUIK_DEALS_DBT repo1
               INNER JOIN DQUIK_DEALS_DBT repo2
                  ON     REPO2.T_ORDERNUM = repo1.T_ORDERNUM
                     AND REPO2.T_CLASSCODE = REPO1.T_CLASSCODE
                     AND REPO2.T_OPERATION != REPO1.T_OPERATION
                     AND REPO2.T_TRADENUM != REPO1.T_TRADENUM
               INNER JOIN DLIMIT_BSDATE_DBT limdate
                  ON repo2.T_SETTLEDATE BETWEEN limdate.T_SETTLEDATEFROM
                                            AND limdate.T_SETTLEDATETO
                     AND :basedate = LIMDATE.T_BASEDATE
         WHERE repo1.t_tradedate = :tradedate
               AND REPO2.T_CLASSCODE IN ('EQRP', 'EQRP_BND', 'PSRP')
               AND repo1.t_tradedate = repo1.T_SETTLEDATE
               AND repo1.T_ClientCode = :clientcode
               AND repo1.T_TSCOMMMISSION IS NOT NULL
               AND repo1.T_TSCOMMMISSION > 0]';

     if (p_currency is not null) THEN
        IF (p_currency = 'SUR') THEN
           v_curAlias := 'RUB';
        END IF;
        v_sqlBuf := v_sqlBuf || ' AND repo1.T_TRADECURRENCY IN ('''||p_currency||''', '''||v_curAlias||''') ';
     end if;

     if (p_seccode is not null) THEN
        v_sqlBuf := v_sqlBuf || ' AND repo1.T_SECCODE = '''||p_seccode||''' ';
     end if;

     OPEN v_Cursor FOR v_sqlBuf USING p_basedate, p_tradedate, p_clientcode;

     return v_Cursor;
  END;

  FUNCTION GetDifSecurRepoByQuikData (p_basedate      DATE,
                                      p_tradedate     DATE,
                                      p_limkind       NUMBER,
                                      p_seccode       VARCHAR2,
                                      p_clientcode    VARCHAR2,
                                      p_quikdataexists integer)
     RETURN NUMBER
     DETERMINISTIC
  IS
     v_diffQnty          NUMBER (10) := 0;
     v_quikRepoDealArr   quikRepoDealArr;
     v_Cursor            SYS_REFCURSOR;
  BEGIN

     if p_quikdataexists = 1 THEN 

        v_Cursor := GetCursorRepoByQuikData (p_basedate, p_tradedate, p_clientcode, null, p_seccode);
        LOOP
           FETCH v_Cursor
           BULK COLLECT INTO v_quikRepoDealArr
           LIMIT 1000;
     
           FOR indx IN 1 .. v_quikRepoDealArr.COUNT
           LOOP
              IF (((p_limkind = 1) AND (v_quikRepoDealArr (indx).T_LIMITKIND = 1)) OR
                  ((p_limkind = 2) AND (v_quikRepoDealArr (indx).T_LIMITKIND = 2)) OR
                  ((p_limkind > 2) AND (v_quikRepoDealArr (indx).T_LIMITKIND > 2)))
              THEN
                 v_diffQnty := v_diffQnty - (v_quikRepoDealArr (indx).T_SELL * v_quikRepoDealArr (indx).T_QTY);
              END IF;
           END LOOP;
     
           EXIT WHEN v_Cursor%NOTFOUND;
        END LOOP;
   
        CLOSE v_Cursor;

     END IF;
  
     RETURN v_diffQnty;
  END;

  FUNCTION GetDifSecurAvrWrtBySofrData (p_tradedate     DATE,
                                        p_seccode       VARCHAR2,
                                        p_clientcode    VARCHAR2,
                                        p_marketid      NUMBER,
                                        p_market_kind   VARCHAR2,
                                        p_quikdataexists integer)
     RETURN NUMBER
     DETERMINISTIC
  IS
     pragma udf;
     v_diffQnty          NUMBER (10) := 0;
  BEGIN

     SELECT NVL(SUM(q1.t_principal),0) INTO v_diffQnty
       FROM (SELECT (  SELECT t_Code
                         FROM DOBJCODE_DBT
                        WHERE t_ObjectType = Rsb_Secur.OBJTYPE_FININSTR
                              AND t_CodeKind =
                                     RSHB_RSI_SCLIMIT.
                                      GetKindMarketCodeOrNote (mp.T_MARKETID, 1, 0)
                              AND t_ObjectID = leg.T_PFI
                              AND t_BankDate <= p_tradedate
                              AND t_BankCloseDate =
                                     TO_DATE ('01.01.0001', 'dd.mm.yyyy')
                     ORDER BY t_BankDate DESC
                     FETCH NEXT 1 ROWS ONLY)
                       AS t_MarketCode,
                    leg.*
               FROM ddl_tick_dbt tk, ddl_leg_dbt leg, DDLCONTRMP_DBT mp
              WHERE     T_DEALTYPE = 2011
                    AND t_dealstatus <> 0
                    AND leg.t_dealid = tk.t_dealid
                    AND leg.t_legid = 0
                    AND leg.t_legkind = 0
                    and MP.T_SFCONTRID = tk.t_clientcontrid
                    AND tk.t_clientcontrid IN 
                   (SELECT DISTINCT SF.T_ID
                      FROM DDLCONTRMP_DBT mp,
                           DSFCONTR_DBT sf
                     WHERE     mp.T_MPCODE = p_clientcode
                           AND sf.t_ServKindSub = 8
                           AND MP.T_MARKETID = CASE WHEN p_market_kind = 'ЕДП' THEN MP.T_MARKETID ELSE p_marketid END
                           AND SF.T_ID = MP.T_SFCONTRID)
                    AND TK.T_DEALDATE >= p_tradedate
                    AND EXISTS
                           (SELECT 1
                              FROM ddlgrdeal_dbt grdeal, ddlgracc_dbt gracc
                             WHERE     GRDEAL.T_DOCID = TK.T_DEALID
                                   AND t_dockind = TK.T_BOFFICEKIND
                                   AND GRACC.T_GRDEALID = GRDEAL.T_ID
                                   AND GRDEAL.T_TEMPLNUM = 17
                                   AND GRACC.T_ACCNUM = 2
                                   AND GRACC.T_STATE = 2
                                   AND GRACC.T_FACTDATE <= p_tradedate)
                    AND NOT EXISTS
                               (SELECT 1
                                  FROM dobjatcor_dbt c
                                 WHERE c.t_objecttype = 101 AND c.t_groupid = 210
                                       AND c.t_object =
                                              LPAD (tk.T_DEALID, 34, '0')
                                       AND (c.t_validtodate >= p_tradedate
                                            OR c.t_validtodate =
                                                  TO_DATE ('01.01.0001',
                                                           'dd.mm.yyyy')))) q1
      WHERE q1.t_marketcode = p_seccode;
  
     RETURN v_diffQnty;
  END;


  FUNCTION GetDifMoneyComissSofr (p_basedate      DATE,
                                  p_tradedate     DATE,
                                  p_limkind       NUMBER,
                                  p_currency      VARCHAR2,
                                  p_clientcode    VARCHAR2,
                                  p_marketid      NUMBER,
                                  p_market_kind   VARCHAR2,
                                  p_isBankComis   NUMBER,
                                  p_quikdataexists integer,
                                  p_isBrokerComis NUMBER)
     RETURN NUMBER
     DETERMINISTIC
  IS
     pragma udf;
     v_diffSum           NUMBER (32, 12) := 0;
     v_bufSum           NUMBER (32, 12) := 0;
  BEGIN
     IF p_quikdataexists = 1 THEN
        WITH subq
                AS (SELECT DISTINCT SF.T_ID, SF.T_PARTYID, SF.T_SERVKIND
                      FROM DDLCONTRMP_DBT mp,
                           DSFCONTR_DBT sf
                     WHERE     mp.T_MPCODE = p_clientcode
                           AND sf.t_ServKindSub = 8
                           AND SF.T_SERVKIND = 21
                           AND MP.T_MARKETID = CASE WHEN p_market_kind = 'ЕДП' THEN MP.T_MARKETID ELSE p_marketid END
                           AND SF.T_ID = MP.T_SFCONTRID)
          SELECT NVL(SUM (
                    NVL (SUM (q1.t_sum), 0)
                    - NVL (
                         (SELECT SUM (PM.T_AMOUNT)
                            FROM dpmpaym_dbt pm
                           WHERE     pm.t_DocKind = q1.t_DocKind
                                 AND PM.T_FIID = q1.T_FIID_COMM
                                 AND pm.t_DocumentID = q1.T_DOCID
                                 AND pm.t_purpose = q1.t_PayPurp),
                         0)),0)
                    AS t_dvsum
                  INTO v_bufSum
          FROM (  SELECT comm.T_DOCKIND,
                         comm.t_docid,
                         sfc.T_FIID_COMM,
                         comm.t_sum,
                         CASE
                            WHEN (sfc.t_receiverid IN (SELECT d.t_PartyID
                                                         FROM ddp_dep_dbt d)) THEN 72
                            WHEN NVL (
                                    (SELECT 1
                                       FROM DPARTYOWN_DBT ow
                                      WHERE OW.T_PARTYID = sfc.t_receiverid
                                            AND OW.T_PARTYKIND = 3),
                                    0) = 1 THEN 40
                            ELSE 0
                         END
                            AS t_PayPurp
                    FROM ddlcomis_dbt comm,
                         ddvndeal_dbt ndeal,
                         ddvnfi_dbt nfi,
                         dfininstr_dbt bafi,
                         dsfcomiss_dbt sfc,
                         DLIMIT_BSDATE_DBT limdate,
                         subq subq2
                   WHERE     ndeal.t_Client = subq2.T_PARTYID
                         AND ndeal.t_ClientContr = subq2.t_id
                         AND ndeal.t_DocKind IN (4813, 199)             -- Конверсионн
                         AND ndeal.t_Date = p_tradedate
                         AND ndeal.t_Sector = CHR (88)
                         AND ndeal.t_MarketKind IN (2)
                         AND nfi.t_dealID = ndeal.t_ID
                         AND nfi.t_Type = 0
                         AND bafi.t_FIID = nfi.t_FIID
                         AND bafi.t_fi_kind = 1                              -- валюта
                         AND COMM.T_DOCID = ndeal.t_id
                         AND COMM.T_DOCKIND IN (4813, 199)
                         AND sfc.t_number = comm.t_comnumber
                         AND sfc.T_FEETYPE = comm.T_FEETYPE
                         AND limdate.T_BASEDATE = p_basedate
                         AND limdate.T_LIMITKIND <= p_limkind
                         AND ((p_isBrokerComis <> 1) OR (sfc.t_receiverid IN (SELECT d.t_PartyID
                                                    FROM ddp_dep_dbt d)))
                         AND ((p_isBankComis = 1) OR (comm.T_ISBANKEXPENSES <> CHR (88)))
                         AND ((p_isBankComis <> 1) OR (comm.T_ISBANKEXPENSES = CHR (88)))
                         AND nfi.t_paydate BETWEEN limdate.T_SETTLEDATEFROM
                                               AND limdate.T_SETTLEDATETO
                         AND (SELECT sfc.t_fiid_comm
                                FROM dsfcomiss_dbt sfc
                               WHERE sfc.t_number = comm.t_comnumber
                                     AND sfc.t_servicekind <> 1) =
                                (SELECT T_FIID
                                   FROM DFININSTR_DBT
                                  WHERE T_CCY =
                                           CASE
                                              WHEN p_currency = 'SUR' THEN 'RUB'
                                              ELSE p_currency
                                           END)) q1
                GROUP BY T_DOCKIND,
                         t_docid,
                         T_FIID_COMM,
                         T_PAYPURP;
        v_diffSum := v_diffSum + v_bufSum;
   
        WITH subq
                AS (SELECT DISTINCT SF.T_ID, SF.T_PARTYID, SF.T_SERVKIND
                      FROM DDLCONTRMP_DBT mp,
                           DSFCONTR_DBT sf
                     WHERE     mp.T_MPCODE = p_clientcode
                           AND sf.t_ServKindSub = 8
                           AND SF.T_SERVKIND = 1
                           AND MP.T_MARKETID = CASE WHEN p_market_kind = 'ЕДП' THEN MP.T_MARKETID ELSE p_marketid END
                           AND SF.T_ID = MP.T_SFCONTRID)
        SELECT NVL (SUM (rq.t_Amount), 0) as t_scsum
          INTO v_bufSum
          FROM ddlrq_dbt rq,
               ddl_tick_dbt tk,
               ddlcomis_dbt comm,
               dsfcomiss_dbt sfc,
               DLIMIT_BSDATE_DBT limdate,
               subq subq1
         WHERE     tk.t_ClientID = subq1.t_partyid
               AND tk.t_bofficekind <> RSB_SECUR.DL_RETIREMENT
               AND tk.t_DealDate = p_tradedate
               AND limdate.T_BASEDATE = p_basedate
               AND rq.t_DocKind = tk.t_BOfficeKind
               AND rq.t_DocID = tk.t_DealID
               AND COMM.T_ID = RQ.T_SOURCEOBJID
               AND COMM.T_DOCKIND = TK.t_BOfficeKind
               AND ((p_isBankComis = 1) OR (comm.T_ISBANKEXPENSES <> CHR (88)))
               AND ((p_isBankComis <> 1) OR (comm.T_ISBANKEXPENSES = CHR (88)))
               AND RQ.T_SOURCEOBJKIND =  RSB_SECUR.DL_SECURITYCOM
               AND rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_CURRENCY
               AND sfc.t_number = comm.t_comnumber
               AND sfc.T_FEETYPE = comm.T_FEETYPE
               AND ((p_isBrokerComis <> 1) OR (sfc.t_receiverid IN (SELECT d.t_PartyID
                                          FROM ddp_dep_dbt d)))
               AND sfc.t_fiid_comm =
                      (SELECT T_FIID
                         FROM DFININSTR_DBT
                        WHERE T_CCY =
                                 CASE WHEN p_currency = 'SUR' THEN 'RUB' ELSE p_currency END)
               AND rq.t_PlanDate BETWEEN limdate.T_SETTLEDATEFROM
                                     AND limdate.T_SETTLEDATETO
               AND RQ.T_FIID = sfc.t_fiid_comm
               AND limdate.T_LIMITKIND <= p_limkind
               AND ( (rq.t_Kind = 1 AND tk.t_ClientID = subq1.t_partyid)
                    OR (rq.t_Kind = 0 AND tk.t_PartyID = subq1.t_partyid))
               AND subq1.t_partyid = tk.t_ClientID
               AND subq1.t_id = tk.t_ClientContrID;
        v_diffSum := v_diffSum + v_bufSum;
     END IF;

     return v_diffSum;
  END;

  FUNCTION GetDifMoneyPlanComissSofr (p_basedate       DATE,
                                      p_tradedate      DATE,
                                      p_currency       VARCHAR2,
                                      p_clientcode     VARCHAR2,
                                      p_marketid       NUMBER,
                                      p_market_kind    VARCHAR2)
     RETURN NUMBER
     DETERMINISTIC
  IS
     pragma udf;
     v_diffSum   NUMBER (32, 12) := 0;
     v_bufSum    NUMBER (32, 12) := 0;
  BEGIN
     /*
    По фондовому рынку неоплаченная комиссия - на дату Т-1 по сделке исполнен график Фиксация комиссии, 
    но не исполнен график Оплата комиссии, либо исполнен в дату больше чем >= Даты расчета лимитов (p_basedate)
     */
     SELECT  NVL (SUM (t_sum), 0) INTO v_bufSum
       FROM  ddl_tick_dbt t , ddlcomis_dbt c , ddlrq_dbt r
      WHERE     c.t_date <> c.t_planpaydate
            AND c.t_planpaydate >= p_tradedate
            AND c.t_dockind = t.t_bofficekind
            AND C.T_DOCID = t.t_dealid
            /*AND t.t_dealstatus <> 20*/
            AND C.T_ID = R.T_SOURCEOBJID
            AND C.T_DOCKIND = T.t_BOfficeKind
            AND C.T_ISBANKEXPENSES <> CHR (88)
            AND C.t_date < p_basedate
            and r.t_fiid = 
            (SELECT T_FIID
               FROM DFININSTR_DBT
              WHERE T_CCY =
                       CASE WHEN p_currency = 'SUR' THEN 'RUB' ELSE p_currency END)
            AND t.t_clientcontrid IN
                   (SELECT DISTINCT SF.T_ID
                      FROM DDLCONTRMP_DBT mp, DSFCONTR_DBT sf
                     WHERE     mp.T_MPCODE = p_clientcode
                           AND sf.t_ServKindSub = 8
                           AND SF.T_SERVKIND = 1
                           AND MP.T_MARKETID =
                                  CASE
                                     WHEN p_market_kind = 'ЕДП' THEN MP.T_MARKETID
                                     ELSE p_marketid
                                  END
                           AND SF.T_ID = MP.T_SFCONTRID)
            AND R.T_SOURCEOBJKIND = 4721                 --RSB_SECUR.DL_SECURITYCOM
            AND  EXISTS
                       (SELECT 1
                          FROM ddlgrdeal_dbt gr
                         WHERE gr.t_DocKind = r.t_DocKind
                               AND gr.t_DocID = r.t_DocID
                               --AND gr.t_PlanDate <= p_CheckDate
                               AND ( (r.t_Type IN (6   -- RSI_DLRQ.DLRQ_TYPE_COMISS
                                                    ) AND gr.t_TemplNum IN (9 --RSI_DLGR.DLGR_TEMPL_PAYCOM
                                                                             ))
                                    AND EXISTS
                                           (SELECT 1
                                              FROM ddlgracc_dbt gracc
                                             WHERE     gracc.t_GrDealID = gr.t_ID
                                                   AND gracc.t_AccNum = 2
                                                   --RSI_DLGR.DLGR_ACCKIND_ACCOUNTING
                                                   AND (   gracc.t_State = 1
                                                        OR (    gracc.t_State = 2
                                                            AND gracc.t_FactDate >= p_basedate) ))))
            AND EXISTS
                   (SELECT 1
                      FROM ddlgrdeal_dbt gr
                     WHERE gr.t_DocKind = r.t_DocKind AND gr.t_DocID = r.t_DocID
                           --AND gr.t_PlanDate <= p_CheckDate
                           AND ( (r.t_Type IN (6       -- RSI_DLRQ.DLRQ_TYPE_COMISS
                                                ) AND gr.t_TemplNum IN (100))
                                AND EXISTS
                                       (SELECT 1
                                          FROM ddlgracc_dbt gracc
                                         WHERE     gracc.t_GrDealID = gr.t_ID
                                               AND gracc.t_AccNum = 2 --RSI_DLGR.DLGR_ACCKIND_ACCOUNTING
                                               AND (gracc.t_State = 2 --RSI_DLGR.DLGRACC_STATE_FACTEXEC
                                                                     ))));

     v_diffSum := v_diffSum + v_bufSum;

     /*
       По валютному рынку неоплаченная комиссия. По сделке не существует исполненного платежа соответствующего комиссии.
       А также проверяется существует ли шаг оплаты комиссии для покупки продажи, или шаг исполнения требований первой части для СВОПа
       Получатель комиссии - биржа. (OW.T_PARTYKIND = 3)
     */
       WITH subcontr
               AS (select distinct sf.t_id,
                          sf.t_partyid,
                          sf.t_servkind,
                          mp_v.t_mpcode
                     from ddlcontrmp_dbt mp
                     join ddlcontrmp_dbt mp_v on mp_v.t_dlcontrid = mp.t_dlcontrid
                     join dsfcontr_dbt sf on sf.t_id = mp_v.t_sfcontrid
                    where mp.t_mpcode = p_clientcode
                      and sf.t_servkind = 21
                      and sf.t_servkindsub = 8
                      and mp_v.t_marketid = case when p_market_kind = 'ЕДП' then mp_v.t_marketid else p_marketid end)
     SELECT NVL(SUM(comm.t_sum),0) INTO v_bufSum
       FROM ddlcomis_dbt comm,
            ddvndeal_dbt ndeal,
            ddvnfi_dbt nfi,
            dsfcomiss_dbt sfc,
            subcontr subq1
      WHERE ndeal.t_Client = subq1.T_PARTYID
            AND ndeal.t_ClientContr = subq1.t_id
            AND ndeal.t_DocKind IN (4813, 199) -- Конверсионная сделка ФИСС и КО /*bpv и плюс СВОПы*/
            and  NFI.T_PAYDATE >= p_tradedate
            and comm.t_date < p_basedate
            AND ndeal.t_date <> NFI.T_PAYDATE
            /*AND NDEAL.T_STATE <> 2*/
            AND ndeal.t_Sector = CHR (88)
            AND ndeal.t_MarketKind IN (2)
            AND nfi.t_dealID = ndeal.t_ID
            AND nfi.t_Type = 0
            AND comm.T_ISBANKEXPENSES <> CHR (88)
            AND sfc.t_number = comm.t_comnumber
            AND sfc.T_FEETYPE = comm.T_FEETYPE
            AND SFC.T_FIID_COMM = (SELECT T_FIID
                                        FROM DFININSTR_DBT
                                       WHERE T_CCY =
                                                CASE
                                                   WHEN p_currency = 'SUR' THEN 'RUB'
                                                   ELSE p_currency
                                                END)
            AND EXISTS
                   (SELECT 1
                      FROM DPARTYOWN_DBT ow
                     WHERE OW.T_PARTYID = sfc.t_receiverid AND OW.T_PARTYKIND = 3)
            AND COMM.T_DOCID = ndeal.t_id
            AND COMM.T_DOCKIND IN (4813, 199)
            AND (not EXISTS
                       (SELECT 1
                          FROM dpmpaym_dbt p
                         WHERE     P.T_DOCKIND = NDEAL.T_docKIND
                               AND P.T_DOCUMENTID = ndeal.t_id
                               AND P.T_PURPOSE IN (72)
                               AND P.T_PAYMSTATUS = 32000)
                               or  EXISTS
                       (SELECT 1
                          FROM dpmpaym_dbt p
                         WHERE     P.T_DOCKIND = NDEAL.T_docKIND
                               AND P.T_DOCUMENTID = ndeal.t_id
                               AND P.T_PURPOSE IN (72)
                               AND (P.T_PAYMSTATUS <> 32000 or P.T_VALUEDATE >= p_basedate)))
            AND EXISTS
                (SELECT 1
                   FROM DOPRSTEP_DBT step, doproper_dbt oper
                  WHERE (step.T_KIND_OPERATION IN
                            (SELECT T_KIND_OPERATION
                               FROM DOPRKOPER_DBT koper
                              WHERE koper.T_DOCKIND = 199
                                    AND koper.T_KIND_OPERATION > 0
                                    AND (koper.T_NOTINUSE <> CHR (88)
                                         OR koper.T_NOTINUSE IS NULL)
                                    AND INSTR (T_SYSTYPES, 'N') > 0
                                    AND INSTR (T_SYSTYPES, 'H') > 0
                                    AND INSTR (T_SYSTYPES, 'K') > 0)
                         AND step.T_SYMBOL = CHR (226)
                         AND oper.t_DocumentID = LPAD (ndeal.t_ID, 34, '0')
                         AND oper.t_DocKind = ndeal.T_DOCKIND
                         AND oper.t_DocKind = step.t_dockind
                         AND oper.t_id_operation = step.t_id_operation
                         AND step.T_ISEXECUTE = CHR (88))
                        OR (step.T_KIND_OPERATION IN
                               (SELECT T_KIND_OPERATION
                                  FROM DOPRKOPER_DBT koper
                                 WHERE koper.T_DOCKIND = 4813
                                       AND koper.T_KIND_OPERATION > 0
                                       AND (koper.T_NOTINUSE <> CHR (88)
                                            OR koper.T_NOTINUSE IS NULL)
                                       AND INSTR (T_SYSTYPES, 'D') > 0
                                       AND INSTR (T_SYSTYPES, 'K') > 0)
                            AND step.T_SYMBOL = CHR (170)
                            AND oper.t_DocumentID = LPAD (ndeal.t_ID, 34, '0')
                            AND oper.t_DocKind = ndeal.T_DOCKIND
                            AND oper.t_DocKind = step.t_dockind
                            AND oper.t_id_operation = step.t_id_operation
                            AND step.T_ISEXECUTE = CHR (88)))
                               ;

     v_diffSum := v_diffSum + v_bufSum;


     /*
       По валютному рынку неоплаченная комиссия. По сделке не существует исполненного платежа соответствующего комиссии.
       А также проверяется существует ли шаг оплаты комиссии для покупки продажи, или шаг исполнения требований первой части для СВОПа
       Получатель комиссии - наш банк.
            AND sfc.t_receiverid IN (SELECT d.t_PartyID
                                       FROM ddp_dep_dbt d)
     */
       WITH subcontr
               AS (select distinct sf.t_id,
                          sf.t_partyid,
                          sf.t_servkind,
                          mp_v.t_mpcode
                     from ddlcontrmp_dbt mp
                     join ddlcontrmp_dbt mp_v on mp_v.t_dlcontrid = mp.t_dlcontrid
                     join dsfcontr_dbt sf on sf.t_id = mp_v.t_sfcontrid
                    where mp.t_mpcode = p_clientcode
                      and sf.t_servkind = 21
                      and sf.t_servkindsub = 8
                      and mp_v.t_marketid = case when p_market_kind = 'ЕДП' then mp_v.t_marketid else p_marketid end)
     SELECT NVL(SUM(comm.t_sum),0) INTO v_bufSum
       FROM ddlcomis_dbt comm,
            ddvndeal_dbt ndeal,
            ddvnfi_dbt nfi,
            dsfcomiss_dbt sfc,
            subcontr subq1
      WHERE ndeal.t_Client = subq1.T_PARTYID
            AND ndeal.t_ClientContr = subq1.t_id
            and comm.t_date < p_basedate
            AND ndeal.t_DocKind IN (4813, 199) -- Конверсионная сделка ФИСС и КО /*bpv и плюс СВОПы*/
            AND ndeal.t_date <> NFI.T_PAYDATE
            /*AND NDEAL.T_STATE <> 2*/
            AND ndeal.t_Sector = CHR (88)
            AND ndeal.t_MarketKind IN (2)
            AND nfi.t_dealID = ndeal.t_ID
            AND nfi.t_Type = 0
            AND NFI.T_PAYDATE >= p_tradedate
            AND sfc.t_number = comm.t_comnumber
            AND sfc.T_FEETYPE = comm.T_FEETYPE
            AND SFC.T_FIID_COMM = (SELECT T_FIID
                                        FROM DFININSTR_DBT
                                       WHERE T_CCY =
                                                CASE
                                                   WHEN p_currency = 'SUR' THEN 'RUB'
                                                   ELSE p_currency
                                                END)
            AND sfc.t_receiverid IN (SELECT d.t_PartyID
                                       FROM ddp_dep_dbt d)
            AND COMM.T_DOCID = ndeal.t_id
            AND COMM.T_DOCKIND IN (4813, 199)
            AND (NOT EXISTS
                       (SELECT 1
                          FROM dpmpaym_dbt p
                         WHERE     P.T_DOCKIND = NDEAL.T_docKIND
                               AND P.T_DOCUMENTID = ndeal.t_id
                               AND P.T_PURPOSE IN (72))
                               or 
                               EXISTS
                       (SELECT 1
                          FROM dpmpaym_dbt p
                         WHERE     P.T_DOCKIND = NDEAL.T_docKIND
                               AND P.T_DOCUMENTID = ndeal.t_id
                               AND P.T_PURPOSE IN (72)
                               AND (P.T_PAYMSTATUS <> 32000 or p.t_valuedate >= p_basedate)))
            AND EXISTS
                (SELECT 1
                   FROM DOPRSTEP_DBT step, doproper_dbt oper
                  WHERE (step.T_KIND_OPERATION IN
                            (SELECT T_KIND_OPERATION
                               FROM DOPRKOPER_DBT koper
                              WHERE koper.T_DOCKIND = 199
                                    AND koper.T_KIND_OPERATION > 0
                                    AND (koper.T_NOTINUSE <> CHR (88)
                                         OR koper.T_NOTINUSE IS NULL)
                                    AND INSTR (T_SYSTYPES, 'N') > 0
                                    AND INSTR (T_SYSTYPES, 'H') > 0
                                    AND INSTR (T_SYSTYPES, 'K') > 0)
                         AND step.T_SYMBOL = CHR (226)
                         AND oper.t_DocumentID = LPAD (ndeal.t_ID, 34, '0')
                         AND oper.t_DocKind = ndeal.T_DOCKIND
                         AND oper.t_DocKind = step.t_dockind
                         AND oper.t_id_operation = step.t_id_operation
                         AND step.T_ISEXECUTE = CHR (88))
                        OR (step.T_KIND_OPERATION IN
                               (SELECT T_KIND_OPERATION
                                  FROM DOPRKOPER_DBT koper
                                 WHERE koper.T_DOCKIND = 4813
                                       AND koper.T_KIND_OPERATION > 0
                                       AND (koper.T_NOTINUSE <> CHR (88)
                                            OR koper.T_NOTINUSE IS NULL)
                                       AND INSTR (T_SYSTYPES, 'D') > 0
                                       AND INSTR (T_SYSTYPES, 'K') > 0)
                            AND step.T_SYMBOL = CHR (170)
                            AND oper.t_DocumentID = LPAD (ndeal.t_ID, 34, '0')
                            AND oper.t_DocKind = ndeal.T_DOCKIND
                            AND oper.t_DocKind = step.t_dockind
                            AND oper.t_id_operation = step.t_id_operation
                            AND step.T_ISEXECUTE = CHR (88)))
                               ;

     v_diffSum := v_diffSum + v_bufSum;


     /*
       По валютному рынку неоплаченная комиссия. По сделке существует исполненный платеж.
       Поиск частичных оплат комиссии. От суммы комиссии отнимается сумма платежа.
       Базовый актив - валюта
       Получатель комиссии - наш банк.
            AND sfc.t_receiverid IN (SELECT d.t_PartyID
                                       FROM ddp_dep_dbt d)
     */
     SELECT NVL (
               SUM (
                  NVL (q1.t_sum, 0)
                  - NVL (
                       (SELECT SUM (PM.T_AMOUNT)
                          FROM dpmpaym_dbt pm
                         WHERE     pm.t_DocKind = q1.t_DocKind
                               AND PM.T_FIID = q1.T_FIID_COMM
                               AND pm.t_DocumentID = q1.T_DOCID
                               AND pm.t_purpose IN (72)
                               AND PM.T_PAYER = q1.t_partyid),
                       0)),
               0) 
               AS v_sum INTO v_bufSum
       FROM (WITH subcontr
                     AS (select distinct sf.t_id,
                                sf.t_partyid,
                                sf.t_servkind,
                                mp_v.t_mpcode
                           from ddlcontrmp_dbt mp
                           join ddlcontrmp_dbt mp_v on mp_v.t_dlcontrid = mp.t_dlcontrid
                           join dsfcontr_dbt sf on sf.t_id = mp_v.t_sfcontrid
                          where mp.t_mpcode = p_clientcode
                            and sf.t_servkind = 21
                            and sf.t_servkindsub = 8
                            and mp_v.t_marketid = case when p_market_kind = 'ЕДП' then mp_v.t_marketid else p_marketid end)
               SELECT comm.T_DOCKIND,
                      comm.t_docid,
                      sfc.T_FIID_COMM,
                      subq1.t_partyid,
                      SUM (comm.t_sum) AS t_sum
                 FROM ddlcomis_dbt comm,
                      ddvndeal_dbt ndeal,
                      ddvnfi_dbt nfi,
                      dfininstr_dbt bafi,
                      dsfcomiss_dbt sfc,
                      subcontr subq1
                WHERE     ndeal.t_Client = subq1.t_partyid
                      AND ndeal.t_ClientContr = subq1.t_id
                      AND ndeal.t_DocKind IN (4813, 199) -- Конверсионная сделка ФИСС и КО /*bpv и плюс СВОПы*/
                      AND ndeal.t_Date < p_tradedate
                      AND ndeal.t_Sector = CHR (88)
                      AND ndeal.t_MarketKind IN (2)
                      AND nfi.t_dealID = ndeal.t_ID
                      AND nfi.t_Type = 0
                      AND bafi.t_FIID = nfi.t_FIID
                      AND bafi.t_fi_kind = 1 -- валюта
                      AND COMM.T_DOCID = ndeal.t_id
                      AND COMM.T_DOCKIND IN (4813, 199)
                      AND sfc.t_number = comm.t_comnumber
                      AND sfc.T_FEETYPE = comm.T_FEETYPE
                      AND sfc.t_receiverid IN (SELECT d.t_PartyID
                                                 FROM ddp_dep_dbt d)
                      AND comm.T_ISBANKEXPENSES <> CHR (88)
                      AND nfi.t_paydate < p_tradedate
                      AND sfc.t_fiid_comm =
                             (SELECT T_FIID
                                FROM DFININSTR_DBT
                               WHERE T_CCY =
                                        CASE
                                           WHEN p_currency = 'SUR' THEN 'RUB'
                                           ELSE p_currency
                                        END)
             GROUP BY comm.T_DOCKIND,
                      comm.t_docid,
                      sfc.T_FIID_COMM,
                      subq1.t_partyid) q1;

     v_diffSum := v_diffSum + v_bufSum;

     return v_diffSum;
  END GetDifMoneyPlanComissSofr;

  FUNCTION GetDifMoneyBrokerFixComissSofr (p_tradedate     DATE,
                                           p_basedate      DATE,
                                           p_currency      VARCHAR2,
                                           p_clientcode    VARCHAR2,
                                           p_marketid      NUMBER,
                                           p_market_kind   VARCHAR2,
                                           p_quikdataexists integer)
     RETURN NUMBER   
     DETERMINISTIC
  IS
     pragma udf;
     v_diffSum           NUMBER (32, 12) := 0;
     v_bufSum           NUMBER (32, 12) := 0;
  BEGIN

     if p_quikdataexists = 1 then

        SELECT NVL (SUM (c.t_sum), 0) INTO v_bufSum
          FROM dsfdef_dbt c
               JOIN doproper_dbt o
                  ON o.t_dockind = 51                 /*Периодическая комиссия (нов)*/
                                     AND o.t_documentid = LPAD (c.t_id, 34, '0')
               JOIN doprstep_dbt s
                  ON O.T_ID_OPERATION = S.T_ID_OPERATION AND s.T_SYMBOL = 'О' /*оплата*/
         WHERE c.t_status = 40                                              --оплачена
               AND t_sfcontrid IN
                       (SELECT DISTINCT SF.T_ID
                      FROM DDLCONTRMP_DBT mp,
                           DSFCONTR_DBT sf
                     WHERE     mp.T_MPCODE = p_clientcode
                           AND sf.t_ServKindSub = 8
                           AND MP.T_MARKETID = CASE WHEN p_market_kind = 'ЕДП' THEN MP.T_MARKETID ELSE p_marketid END
                           AND SF.T_ID = MP.T_SFCONTRID)
               AND c.t_fiid_sum =
                      (SELECT T_FIID
                         FROM DFININSTR_DBT
                        WHERE T_CCY =
                                 CASE WHEN p_currency = 'SUR' THEN 'RUB' ELSE p_currency END)
               AND S.T_SYST_DATE >= p_tradedate
               AND S.T_SYST_DATE < p_basedate;
   
        v_diffSum := v_diffSum + v_bufSum;
     end if;

     return v_diffSum;
  END;

  FUNCTION GetDifMoneyNoUnloadedNptx (p_tradedate     DATE,
                                      p_currency      VARCHAR2,
                                      p_clientcode    VARCHAR2,
                                      p_marketid      NUMBER,
                                      p_market_kind   VARCHAR2,
                                      p_quikdataexists integer)
     RETURN NUMBER
     DETERMINISTIC
  IS
     pragma udf;
     v_diffSum           NUMBER (32, 12) := 0;
  BEGIN
     SELECT NVL (SUM (NPTXOP.T_OUTSUM ), 0) INTO v_diffSum
       FROM dnptxop_dbt nptxop
      WHERE     NPTXOP.T_STATUS = 2
            AND NPTXOP.T_DOCKIND = 4607
            AND NPTXOP.T_OPERDATE = p_tradedate
            AND NPTXOP.T_CURRENCY = (SELECT T_FIID FROM DFININSTR_DBT WHERE T_CCY = CASE WHEN p_currency = 'SUR' THEN 'RUB' ELSE p_currency END)
            AND NPTXOP.T_CONTRACT IN
                   (SELECT SF.T_ID
                      FROM DDLCONTRMP_DBT mp,
                           DSFCONTR_DBT sf
                     WHERE     mp.T_MPCODE = p_clientcode
                           AND sf.t_ServKindSub = 8
                           AND SF.T_ID = MP.T_SFCONTRID
                           AND MP.T_MARKETID = CASE WHEN p_market_kind = 'ЕДП' THEN MP.T_MARKETID ELSE p_marketid END
                           )
            AND NPTXOP.T_SUBKIND_OPERATION = 10
            AND NOT EXISTS
                       (SELECT 1
                          FROM dnotetext_dbt
                         WHERE     t_objecttype = 131
                               AND t_documentid = LPAD (nptxop.t_id, 34, '0')
                               AND t_notekind = 101);
     return v_diffSum * -1;
  END;

  FUNCTION GetDifMoneyDueSofr (p_basedate      DATE,
                               p_tradedate     DATE,
                               p_accountid     NUMBER,
                               p_currency      VARCHAR2,
                               p_client_code   VARCHAR2,
                               p_market_kind   VARCHAR2,
                               p_market        NUMBER,
                               p_currid        NUMBER,
                               p_client_id     NUMBER)
     RETURN NUMBER
     DETERMINISTIC
  IS
     pragma udf;
     v_diffSum  NUMBER (32, 12) := 0;
     v_curdue   NUMBER (32, 12) := 0;
  BEGIN
    v_curdue := rshb_limit_util.GetDifDueRestSofr  (p_tradedate,
                                                    p_client_code,
                                                    p_market_kind,
                                                    p_market,
                                                    p_currid,
                                                    p_client_id);

      WITH a458
              AS (SELECT /*+ use_concat*/ *
                    FROM daccount_dbt
                   WHERE     t_chapter = 1
                         AND (T_ACCOUNT LIKE '45815%' OR T_ACCOUNT LIKE '45817%')
                         AND t_open_close <> CHR (135))
      SELECT NVL(SUM(rsb_account.restac (a458.t_Account, a458.t_code_Currency, p_basedate - 1, a458.t_Chapter, NULL)), 0) INTO v_diffSum
        FROM daccount_dbt a306
        JOIN a458 ON a458.t_client = a306.t_client and
                     SUBSTR (a306.t_account, 15) = SUBSTR (a458.t_account, 15) and
                     A306.T_CODE_CURRENCY = a458.t_code_currency
       WHERE a306.t_accountid = p_accountid
         and A306.T_CODE_CURRENCY = (SELECT T_FIID FROM DFININSTR_DBT WHERE T_CCY = CASE WHEN p_currency = 'SUR' THEN 'RUB' ELSE p_currency END);

    return (v_diffSum * -1) + v_curdue;
  END;

  FUNCTION GetDifDueRestSofr  (p_tradedate     DATE,
                               p_client_code   VARCHAR2,
                               p_market_kind   VARCHAR2,
                               p_market        NUMBER,
                               p_currid        NUMBER,
                               p_client_id     NUMBER)
     RETURN NUMBER
     DETERMINISTIC
  IS
     pragma udf;
     v_DueRestSum NUMBER (32, 12) := 0;
  BEGIN

    SELECT NVL (SUM (ABS(rsb_account.
                      restac (q1.t_Account,
                              q1.T_CODE_CURRENCY,
                              p_tradedate,
                              q1.t_Chapter,
                              NULL))),
                0)
              INTO v_DueRestSum
      FROM (SELECT DISTINCT a.T_ACCOUNT, a.T_CODE_CURRENCY, a.T_CHAPTER
              FROM dmcaccdoc_dbt mc, daccount_dbt a
             WHERE mc.t_clientcontrid IN
                      (SELECT SF.T_ID
                         FROM DDLCONTRMP_DBT mp, DSFCONTR_DBT sf
                        WHERE mp.T_MPCODE = p_client_code AND sf.t_ServKindSub = 8
                              AND sf.T_SERVKIND =
                                     CASE
                                        WHEN p_market_kind = 'фондовый'
                                        THEN
                                           1
                                        WHEN p_market_kind = 'валютный'
                                        THEN
                                           21
                                        ELSE
                                           sf.T_SERVKIND
                                     END
                              AND SF.T_ID = MP.T_SFCONTRID
                              AND MP.T_MARKETID = p_market)
                   AND mc.t_currency = p_currid
                   AND mc.t_owner = p_client_id
                   AND mc.t_catid = 818
                   AND mc.t_iscommon = CHR (88)
                   AND mc.t_chapter = a.t_chapter
                   AND mc.t_account = a.t_account
                   AND MC.T_CURRENCY = A.T_CODE_CURRENCY) q1;

    return v_DueRestSum;
  END;

  FUNCTION GetDifMoneyOrSecurRepoSofr (p_basedate      DATE,
                                       p_tradedate     DATE,
                                       p_ismoney       NUMBER,
                                       p_currency      VARCHAR2,
                                       p_fiid          NUMBER,
                                       p_clientcode    VARCHAR2,
                                       p_limkind       NUMBER,
                                       p_marketid      NUMBER,
                                       p_market_kind   VARCHAR2,
                                       p_quikdataexists integer)
     RETURN NUMBER
     DETERMINISTIC
  IS
     pragma udf;
     v_diffSum           NUMBER (32, 12) := 0;
  BEGIN

     if (p_limkind = 1) then
        SELECT NVL (SUM (q1.T_AMOUNT), 0) INTO v_diffSum
          FROM (SELECT NVL ( (SELECT t_numinlist
                                FROM DOBJATTR_DBT
                               WHERE     t_objecttype = 101
                                     AND t_groupid = 106
                                     AND t_attrid = RSB_SECUR.
                                                     GetMainObjAttr (
                                                       101,
                                                       LPAD (tk.T_DEALID, 34, '0'),
                                                       106,
                                                       p_tradedate)),
                            '0')
                          AS t_trademode,
                       CASE
                          WHEN rq.t_kind = 1 THEN rq.t_Amount * -1
                          ELSE rq.t_Amount
                       END
                          AS T_AMOUNT,
                       tk.*
                  FROM ddl_tick_dbt tk, ddlrq_dbt rq
                 WHERE tk.t_BOfficeKind = 101
                       AND tk.t_ClientContrID IN
                              (SELECT SF.T_ID
                                 FROM DDLCONTRMP_DBT mp, DSFCONTR_DBT sf
                                WHERE     mp.T_MPCODE = p_clientcode
                                      AND sf.t_ServKindSub = 8
                                      AND SF.T_ID = MP.T_SFCONTRID
                                      AND MP.T_MARKETID =
                                             CASE
                                                WHEN p_market_kind = 'ЕДП'
                                                THEN
                                                   MP.T_MARKETID
                                                ELSE
                                                   p_marketid
                                             END)
                       AND tk.t_DealDate = p_tradedate
                       AND rq.t_DocKind = tk.t_BOfficeKind
                       AND rq.t_plandate = p_basedate
                       AND rq.t_DocID = tk.t_DealID
                       AND RQ.T_SUBKIND = CASE WHEN p_ismoney = 1 THEN 0 /* RSI_DLRQ.DLRQ_SUBKIND_CURRENCY*/ ELSE 1 /*RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS*/ END
                       AND rq.T_DEALPART = 2
                       and rq.T_FIID = CASE WHEN p_ismoney = 1 THEN (SELECT T_FIID FROM DFININSTR_DBT WHERE T_CCY = CASE WHEN p_currency = 'SUR' THEN 'RUB' ELSE p_currency END)
                                                               ELSE p_fiid
                                       END
                       AND RSB_SECUR.
                            GetMainObjAttr (101,
                                            LPAD (tk.T_DEALID, 34, '0'),
                                            210,
                                            p_tradedate) <>
                              1) q1
         WHERE (EXISTS
                   (SELECT 1
                      FROM DOBJATTR_DBT
                     WHERE     t_objecttype = 101
                           AND t_groupid = 103
                           AND t_attrid = RSB_SECUR.
                                           GetMainObjAttr (
                                             101,
                                             LPAD (q1.T_DEALID, 34, '0'),
                                             103,
                                             p_tradedate))
                OR INSTR (
                      RegVal_RepoWithCCPTradingModes,
                      ',' || q1.t_trademode || ',') > 0
                OR SUBSTR (q1.t_trademode, 1, 1) = 'P');
    end if;

    return v_diffSum;
  END;


  FUNCTION GetDifMoneyRepoByQuikData (p_basedate      DATE,
                                      p_tradedate     DATE,
                                      p_limkind       NUMBER,
                                      p_currency      VARCHAR2,
                                      p_clientcode    VARCHAR2,
                                      p_quikdataexists integer)
     RETURN NUMBER
     DETERMINISTIC
  IS
     v_diffSum           NUMBER (32, 12) := 0;
     v_quikRepoDealArr   quikRepoDealArr;
     v_Cursor            SYS_REFCURSOR;
  BEGIN
     IF p_quikdataexists = 1 THEN

        v_Cursor := GetCursorRepoByQuikData (p_basedate, p_tradedate, p_clientcode, p_currency);
        LOOP
           FETCH v_Cursor
           BULK COLLECT INTO v_quikRepoDealArr
           LIMIT 1000;
     
           FOR indx IN 1 .. v_quikRepoDealArr.COUNT
           LOOP
              v_diffSum := v_diffSum + v_quikRepoDealArr (indx).T_TSCOMMMISSION;
              IF (p_limkind = 1)
              THEN
                 IF (v_quikRepoDealArr (indx).T_LIMITKIND = 1)
                 THEN
                    v_diffSum :=
                       v_diffSum
                       + (v_quikRepoDealArr (indx).T_SELL
                          * v_quikRepoDealArr (indx).T_REPOVALUE)
                       - (v_quikRepoDealArr (indx).T_BROKERCOMMISSION);
                 END IF;
              ELSIF (p_limkind = 2)
              THEN
                 IF (v_quikRepoDealArr (indx).T_LIMITKIND = 2)
                 THEN
                    v_diffSum :=
                       v_diffSum
                       + (v_quikRepoDealArr (indx).T_SELL
                          * v_quikRepoDealArr (indx).T_REPOVALUE)
                       - (v_quikRepoDealArr (indx).T_BROKERCOMMISSION);
                 ELSIF (v_quikRepoDealArr (indx).T_LIMITKIND = 1)
                 THEN
                    v_diffSum :=
                       v_diffSum
                       - (v_quikRepoDealArr (indx).T_SELL
                          * (v_quikRepoDealArr (indx).T_REPO2VALUE
                             - v_quikRepoDealArr (indx).T_REPOVALUE))
                       + (v_quikRepoDealArr (indx).T_BROKERCOMMISSION);
                 END IF;
              ELSIF (p_limkind > 2)
              THEN
                 IF (v_quikRepoDealArr (indx).T_LIMITKIND > 2)
                 THEN
                    v_diffSum :=
                       v_diffSum
                       + (v_quikRepoDealArr (indx).T_SELL
                          * v_quikRepoDealArr (indx).T_REPOVALUE)
                       - (v_quikRepoDealArr (indx).T_BROKERCOMMISSION);
                 ELSIF (v_quikRepoDealArr (indx).T_LIMITKIND IN (1, 2))
                 THEN
                    v_diffSum :=
                       v_diffSum
                       - (v_quikRepoDealArr (indx).T_SELL
                          * (v_quikRepoDealArr (indx).T_REPO2VALUE
                             - v_quikRepoDealArr (indx).T_REPOVALUE))
                       + (v_quikRepoDealArr (indx).T_BROKERCOMMISSION);
                 END IF;
              END IF;
           END LOOP;
     
           EXIT WHEN v_Cursor%NOTFOUND;
        END LOOP;
   
        CLOSE v_Cursor;
     END IF;
  
     RETURN v_diffSum;
  END;

  procedure FillBaseDateForLimit(p_basedate DATE)
  as
    PRAGMA AUTONOMOUS_TRANSACTION;
  begin
    MERGE INTO DLIMIT_BSDATE_DBT c
         USING (SELECT T_BASEDATE,
                       T_LIMITKIND,
                       CASE
                          WHEN T_LIMITKIND > 2 THEN T_SETTLEDATEFROM + 1
                          ELSE T_SETTLEDATEFROM
                       END
                          AS T_SETTLEDATEFROM,
                       T_SETTLEDATETO
                  FROM (SELECT p_basedate AS T_BASEDATE,
                               limKnd.T_LIMITKINDTO AS T_LIMITKIND,
                               RSHB_RSI_SCLIMIT.
                                GetCheckDateByParams (
                                  limKnd.T_LIMITKINDFROM,
                                  p_basedate,
                                  -1,
                                  1)
                                  AS T_SETTLEDATEFROM,
                               RSHB_RSI_SCLIMIT.
                                GetCheckDateByParams (
                                  limKnd.T_LIMITKINDTO,
                                  p_basedate,
                                  -1,
                                  1)
                                  AS T_SETTLEDATETO
                          FROM (SELECT -1 AS T_LIMITKINDFROM, -1 AS T_LIMITKINDTO
                                  FROM DUAL
                                UNION ALL
                                SELECT 0 AS T_LIMITKINDFROM, 0 AS T_LIMITKINDTO
                                  FROM DUAL
                                UNION ALL
                                SELECT 1 AS T_LIMITKINDFROM, 1 AS T_LIMITKINDTO
                                  FROM DUAL
                                UNION ALL
                                SELECT 2 AS T_LIMITKINDFROM, 2 AS T_LIMITKINDTO
                                  FROM DUAL
                                UNION ALL
                                SELECT 2 AS T_LIMITKINDFROM, 365 AS T_LIMITKINDTO
                                  FROM DUAL) limKnd) q1) d
            ON (c.T_BASEDATE = d.T_BASEDATE AND c.T_LIMITKIND = d.T_LIMITKIND)
    WHEN MATCHED
    THEN
       UPDATE SET
          c.T_SETTLEDATEFROM = d.T_SETTLEDATEFROM,
          c.T_SETTLEDATETO = d.T_SETTLEDATETO
    WHEN NOT MATCHED
    THEN
       INSERT     (c.T_BASEDATE,
                   c.T_LIMITKIND,
                   c.T_SETTLEDATEFROM,
                   c.T_SETTLEDATETO)
           VALUES (d.T_BASEDATE,
                   d.T_LIMITKIND,
                   d.T_SETTLEDATEFROM,
                   d.T_SETTLEDATETO);
    COMMIT;
  end;

  procedure FillBaseDateCurrForLimit
  as
    PRAGMA AUTONOMOUS_TRANSACTION;

    p_calparamarrTrd     RSI_DlCalendars.calparamarr_t;
    p_calparamarrSettl   RSI_DlCalendars.calparamarr_t;
    p_SettlDate          DATE;
    p_dateDiff           NUMBER(10);
  begin
    p_calparamarrTrd ('ObjectType') := RSI_DlCalendars.DL_CALLNK_MARKET;
    p_calparamarrTrd ('MarketPlace') := RSI_DlCalendars.DL_CALLNK_MARKETPLACE_CUR;
    p_calparamarrTrd ('DayType') := RSI_DlCalendars.DL_CALLNK_MRKTDAY_TRADE;
 
    p_calparamarrSettl ('ObjectType') := RSI_DlCalendars.DL_CALLNK_MARKET;
    p_calparamarrSettl ('MarketPlace') := RSI_DlCalendars.DL_CALLNK_MARKETPLACE_CUR;
    p_calparamarrSettl ('DayType') := RSI_DlCalendars.DL_CALLNK_MRKTDAY_SETTL;
 
    FOR rec
       IN (SELECT DISTINCT T_DATE, T_CURRID, T_MARKET
             FROM DDL_LIMITCASHSTOCK_dbt)
    LOOP
       p_calparamarrTrd ('Market') := rec.t_market;
       p_calparamarrSettl ('Market') := rec.t_market;
       p_calparamarrSettl ('Currency') := rec.T_CURRID;
       p_SettlDate :=
          RSI_DlCalendars.
           GetBankDateAfterWorkDayByCalendar (
             rec.t_date,
             0,
             RSI_DlCalendars.DL_GetCalendByDynParam (158, p_calparamarrSettl));
       p_dateDiff := RSI_DlCalendars.CalcNumWorkDaysForPeriodAtLeastOne(rec.t_date, p_SettlDate, RSI_DlCalendars.DL_GetCalendarArrByParam (p_calparamarrTrd));
 
       MERGE INTO DLIMIT_BSDATECUR_DBT c
            USING (SELECT rec.t_date AS t_date,
                          rec.T_CURRID AS t_currid,
                          rec.T_MARKET AS t_marketid,
                          p_SettlDate AS T_SETTLEDATE,
                          p_dateDiff AS T_DATEDIFF,
                          LEAST ( (-1 + p_dateDiff), 2)
                             AS T_T1T2SHIFT
                     FROM DUAL) d
               ON (    c.T_BASEDATE = d.t_date
                   AND c.T_SETTL_CURRID = d.t_currid
                   AND c.T_MARKETID = d.t_marketid)
       WHEN MATCHED
       THEN
          UPDATE SET c.T_SETTLEDATE = d.T_SETTLEDATE,
                     c.T_DATEDIFF = d.T_DATEDIFF,
                     c.T_T1T2SHIFT = d.T_T1T2SHIFT
       WHEN NOT MATCHED
       THEN
          INSERT     (c.T_BASEDATE,
                      c.T_SETTL_CURRID,
                      c.T_MARKETID,
                      c.T_SETTLEDATE,
                      c.T_DATEDIFF,
                      c.T_T1T2SHIFT)
              VALUES (d.t_date,
                      d.T_CURRID,
                      d.t_marketid,
                      d.T_SETTLEDATE,
                      d.T_DATEDIFF,
                      d.T_T1T2SHIFT);
    END LOOP;
    COMMIT;
  end;

  procedure DelOldTempLimit  (p_sessionid integer
                             ,p_calcid    number
                             ,p_basedate  date)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    DELETE FROM DLIMIT_MONEY_REVISE_DBT WHERE (t_sessionid = p_sessionid and t_calcid <= p_calcid) or (T_DATE <> p_basedate);
    DELETE FROM DLIMIT_SECUR_REVISE_DBT WHERE (t_sessionid = p_sessionid and t_calcid <= p_calcid)  or (T_DATE <> p_basedate);
    COMMIT;
  END;

  procedure FillLimitSecur(p_sessionid      integer
                          ,p_calcid         number
                          ,p_secur_checked  integer
                          ,p_cur_checked    integer)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    EXECUTE IMMEDIATE
    q'[
       INSERT INTO DLIMIT_SECUR_REVISE_DBT (T_SESSIONID,
                                            T_CALCID,
                                            T_DATE,
                                            T_CLIENT_CODE,
                                            T_CLIENT_NAME,
                                            T_CLIENT_ID,
                                            T_LIMIT_KIND,
                                            T_SECCODE,
                                            T_SECID,
                                            T_SOFR_OPEN_BALANCE,
                                            T_QUIK_OPEN_BALANCE,
                                            T_ISBLOCKED,
                                            t_trdaccid,
                                            T_MARKET,
                                            T_MARKET_KIND)
          SELECT ]' || TO_CHAR(p_sessionid) ||q'[,
                 ]' || TO_CHAR(p_calcid) ||q'[,
                 lim.t_date,
                 lim.t_client_code,
                 PARTY.T_NAME,
                 lim.t_client,
                 lim.t_limit_kind,
                 lim.T_SECCODE,
                 lim.T_SECURITY,
                 trunc(lim.t_open_balance),
                 qlim.t_open_balance,
                 lim.t_isblocked,
                 lim.t_trdaccid,
                 lim.T_MARKET,
                 lim.T_MARKET_KIND
            FROM    (SELECT t_SECCODE,             
                            t_client_code,         
                            t_limit_kind,          
                              case when t_limit_kind = 0 then t_quantity + t_open_limit  
                                   when t_limit_kind = 365 then t_open_balance
                                else lag(t_open_balance,1,0) over (partition by t_trdaccid, t_seccode, t_client_code order by t_trdaccid, t_seccode, t_client_code, t_limit_kind)   end 
                               as t_open_balance,
                            t_wa_position_price,       
                            t_TRDACCID,                           
                            t_isblocked,                          
                            t_client,
                            t_date,
                            t_security,
                            T_MARKET,
                            T_MARKET_KIND
                       FROM DDL_LIMITSECURITES_DBT c
                      WHERE 1 = 1 ]' || getQueryWhere(null, p_secur_checked, p_cur_checked) || q'[
                                 ) lim
                 INNER JOIN 
                    DPARTY_DBT PARTY
                 ON     PARTY.T_PARTYID = LIM.T_CLIENT
                 INNER JOIN 
                    DDL_LIMITSECURITES qlim
                 ON     lim.t_client_code = qlim.t_client_code
                    AND lim.t_SECCODE = qlim.t_SECCODE        
                    AND lim.t_limit_kind = qlim.t_limit_kind  
                    AND lim.t_trdaccid = qlim.t_trdaccid      
                    and QLIM.t_open_balance IS NOT NULL
                  ]' || getQueryWhere('qlim', p_secur_checked, p_cur_checked) || q'[
           WHERE lim.t_open_balance <> QLIM.t_open_balance
       ]';
       COMMIT;
  END;

  procedure FillLimitMoney(p_sessionid      integer
                          ,p_calcid         number
                          ,p_secur_checked  integer
                          ,p_cur_checked    integer)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
   v_sel varchar2(32000);
  BEGIN
    v_sel :=
    q'[
       INSERT INTO DLIMIT_MONEY_REVISE_DBT (t_sessionid,
                                            t_calcid,
                                            t_date,
                                            t_client_code,
                                            t_limit_kind,
                                            t_curcode,
                                            t_sofr_open_balance,
                                            t_quik_open_balance,
                                            T_ISBLOCKED,
                                            t_sofr_rest,
                                            t_client_name,
                                            t_client_id,
                                            t_curid,
                                            t_market,
                                            T_MARKET_KIND,
                                            T_SOFR_DUE,
                                            T_LIMACCOUNTID,
                                            t_isdel)
          SELECT ]' || TO_CHAR(p_sessionid) ||q'[,
                 ]' || TO_CHAR(p_calcid) ||q'[,
                 t_date
                ,t_client_code
                ,t_limit_kind
                ,t_curr_code
                ,lim_open_balance
                ,qlim_open_balance
                ,t_isblocked
                ,rest
                ,T_NAME
                ,t_client
                ,T_CURRID
                ,t_market
                ,T_MARKET_KIND
                ,0
                ,t_internalaccount
                ,chr(0)
            from (select lim.t_date,
                 lim.t_client_code,
                 lim.t_limit_kind,
                 lim.t_curr_code,
                 NVL (lim.t_open_balance, 0) lim_open_balance ,
                 NVL (qlim.t_open_balance, 0) qlim_open_balance ,
                 lim.t_isblocked,
                 (lim.t_money306 + lim.t_open_limit) as rest,
                 PARTY.T_NAME,
                 lim.t_client,
                 lim.T_CURRID,
                 lim.t_market,
                 lim.T_MARKET_KIND,
                 lim.t_internalaccount,
                 DENSE_RANK() over(order by a.t_open_date,lim.t_client) ra,
                 DENSE_RANK() over(order by a.t_open_date desc,lim.t_client) rd
                 FROM (SELECT t_id,
                            t_firm_id,
                            t_tag,
                            t_curr_code,
                            t_client_code,
                            t_limit_kind,
                            CASE
                               WHEN t_limit_kind = 0
                               THEN
                                  /*для случая когда смещение стандартное (-1), то Т0 от СОФРа - это money306, в остальных случаях это Т0 (t_open_balance)*/
                                  DECODE(dtcur.T_T1T2SHIFT, -1, (t_money306 - T_COMPREVIOUS - T_COMPREVIOUS_1 + t_open_limit), t_open_balance)
                               WHEN t_limit_kind = 365
                               THEN
                                  /*Т365 это всегда Т365*/
                                  t_open_balance
                               WHEN dtcur.T_T1T2SHIFT > 0
                               THEN
                                  /*Если смещение положительное, то функцию lag заменяем на функцию lead*/
                                  /*выполняем её через COALESCE два раза, чтобы когда смещение максимальное, значение ушедшее за предел, подвинулось на одно смещение назад*/
                                  COALESCE (
                                     LEAD (
                                        t_open_balance,
                                        ABS (dtcur.T_T1T2SHIFT),
                                        NULL)
                                     OVER (PARTITION BY t_curr_code, t_client_code, t_tag
                                           ORDER BY t_tag, t_limit_kind),
                                     LEAD (
                                        t_open_balance,
                                        ABS (dtcur.T_T1T2SHIFT - 1),
                                        0)
                                     OVER (PARTITION BY t_curr_code, t_client_code, t_tag
                                           ORDER BY t_tag, t_limit_kind))
                               ELSE
                                  /*для отрицательного или нулевого смещения выполняем как обычно, передавая смещение как параметр для LAG*/
                                  /*два раза чисто для единообразия, но фактически оно никогда не сработает*/
                                  COALESCE (
                                     LAG (
                                        t_open_balance,
                                        ABS (dtcur.T_T1T2SHIFT),
                                        NULL)
                                     OVER (PARTITION BY t_curr_code, t_client_code, t_tag
                                           ORDER BY t_tag, t_limit_kind),
                                     LAG (
                                        t_open_balance,
                                        ABS (dtcur.T_T1T2SHIFT - 1),
                                        0)
                                     OVER (PARTITION BY t_curr_code, t_client_code, t_tag
                                           ORDER BY t_tag, t_limit_kind))
                            END
                               AS t_open_balance,
                            t_leverage,
                            T_ISBLOCKED,
                            c.t_client,
                            c.t_date,
                            c.T_CURRID,
                            c.t_internalaccount, 
                            c.t_open_limit,
                            c.t_market,
                            c.T_MARKET_KIND,
                            c.T_DUE474,
                            c.t_money306
                       FROM DDL_LIMITCASHSTOCK_dbt c, DLIMIT_BSDATECUR_DBT dtcur
                      WHERE dtcur.T_BASEDATE = c.t_date AND c.T_CURRID = dtcur.T_SETTL_CURRID AND dtcur.t_marketid = c.t_market ]'
                       || getQueryWhere(null, p_secur_checked, p_cur_checked) || q'[
                                 ) lim
                 INNER JOIN 
                    daccount_dbt a on 
                     lim.t_internalaccount = a.t_accountid
                 INNER JOIN 
                    DPARTY_DBT PARTY
                 ON     PARTY.T_PARTYID = LIM.T_CLIENT
                 INNER JOIN
                    DDL_LIMITCASHSTOCK qlim
                 ON     lim.t_client_code = qlim.t_client_code
                    AND lim.t_curr_code = qlim.t_curr_code
                    AND lim.t_limit_kind = qlim.t_limit_kind
                    AND lim.t_firm_id = qlim.t_firm_id
                    AND lim.t_tag = TRIM(qlim.t_tag)
                    and QLIM.t_open_balance IS NOT NULL
                  ]' || getQueryWhere('qlim', p_secur_checked, p_cur_checked) || q'[
           WHERE lim.t_open_balance <> 0 or QLIM.t_open_balance <> 0
       ) order by case
              when mod(ra, 2) = 0 then
               ra
              else
               rd
            end , t_client ]';
  --  dbms_output.put_line(v_sel);   
    execute immediate v_sel;
    it_log.log(p_msg => 'End. p_calcid = ' || p_calcid);
    COMMIT;
  END FillLimitMoney;
  
  
  --временное решение. Для целевого необходимо исправить заполнение open_limit при расчете лимитов и sofr_plan_comis. В них не должны учитываться архивные неоплаченные комиссии.
  procedure FixComDue(p_sessionid integer
                     ,p_calcid    number)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE DLIMIT_MONEY_REVISE_DBT lim
       SET lim.t_sofr_rest = lim.t_sofr_rest + (select SUM (t_reqSum) - t_payed_tr from u_compay_dbt where T_CALCID = (SELECT MAX (t_calcid) FROM U_COMPAY_DBT) AND t_sysdate is not null and t_ekk = lim.t_client_code group by t_payed_tr)
     WHERE t_sofr_rest < 0
       AND EXISTS
              (SELECT 1
                 FROM u_compay_dbt
                WHERE     T_CALCID = (SELECT MAX (t_calcid) FROM U_COMPAY_DBT)
                      AND t_sysdate IS NOT NULL
                      AND t_ekk = lim.t_client_code)
                      AND t_isdel <> chr(88)
                      AND t_sessionid = p_sessionid 
                      and t_calcid = p_calcid;

    it_log.log(p_msg => 'FixComDue End. p_calcid = ' || p_calcid);
    COMMIT;
  END FixComDue;
  
  procedure DropEqualsRecsZero(p_sessionid      integer
                              ,p_calcid         number)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    EXECUTE IMMEDIATE
    q'[
        UPDATE DLIMIT_MONEY_REVISE_DBT lim
        SET    lim.t_isdel = chr(88)
              WHERE   lim.t_sofr_rest >= 0
                     AND ABS(lim.t_sofr_open_balance - lim.t_quik_open_balance) <= ]' || TO_CHAR(RegVal_AllowedDif) || q'[
                     AND t_isdel <> chr(88)
                     AND t_sessionid = ]' 
     || TO_CHAR(p_sessionid) || q'[ and t_calcid = ]' || TO_CHAR(p_calcid);

    it_log.log(p_msg => 'End. p_calcid = ' || p_calcid);
    COMMIT;
  END DropEqualsRecsZero;

  procedure DropEqualsRecsFirst(p_sessionid      integer
                               ,p_calcid         number)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    EXECUTE IMMEDIATE
    q'[
        UPDATE DLIMIT_MONEY_REVISE_DBT lim
        SET    lim.t_isdel = chr(88)
              WHERE lim.t_sofr_rest >= 0
                     AND ABS( ( (  lim.t_sofr_open_balance
                            + lim.t_sofr_broker_comis_for_rev
                            + lim.t_sofr_NoUnloadedNptx
                            + lim.t_sofr_fixcomis
                            - (lim.t_sofr_due - lim.t_sofr_plan_comis)
                            + lim.t_sofr_repo_dif) -
                             (  lim.t_quik_open_balance
                              + lim.t_quik_repo_dif
                              + lim.t_quik_broker_comis ) ) ) <=  ]' || TO_CHAR(RegVal_AllowedDif) || q'[
                     AND t_isdel <> chr(88)
                     AND t_sessionid = ]' 
     || TO_CHAR(p_sessionid) || q'[ and t_calcid = ]' || TO_CHAR(p_calcid);

    it_log.log(p_msg => 'End. p_calcid = ' || p_calcid);
    COMMIT;
  END DropEqualsRecsFirst;

  procedure DropEqualsRecsSecond(p_sessionid      integer
                                ,p_calcid         number)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    EXECUTE IMMEDIATE
    q'[
        UPDATE DLIMIT_MONEY_REVISE_DBT lim
        SET    lim.t_isdel = chr(88)
              WHERE lim.t_sofr_rest >= 0
                     AND ABS( ( (  lim.t_sofr_open_balance
                            + lim.t_sofr_broker_comis_for_rev
                            + lim.t_sofr_NoUnloadedNptx
                            + lim.t_sofr_fixcomis
                            - (lim.t_sofr_due - lim.t_sofr_plan_comis)
                            + lim.t_sofr_repo_dif
                            - lim.t_sofr_bankexp_comis) -
                             (  lim.t_quik_open_balance
                              + lim.t_quik_repo_dif
                              + lim.t_quik_broker_comis))) <= ]' || TO_CHAR(RegVal_AllowedDif) || q'[
                     AND t_isdel <> chr(88)
                     AND t_sessionid = ]' 
     || TO_CHAR(p_sessionid) || q'[ and t_calcid = ]' || TO_CHAR(p_calcid);
    
    it_log.log(p_msg => 'End. p_calcid = ' || p_calcid);
    COMMIT;
  END DropEqualsRecsSecond;

  procedure DropEqualsRecsIncludingComis(p_sessionid      integer
                                        ,p_calcid         number)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    UPDATE DLIMIT_MONEY_REVISE_DBT lim
    SET   lim.t_isdel = chr(88)
    WHERE lim.t_sofr_rest >= 0
      AND (lim.t_sofr_open_balance - lim.t_quik_open_balance) > 0
      AND (lim.t_sofr_open_balance - lim.t_quik_open_balance) < lim.t_sofr_broker_comis
      AND t_isdel <> chr(88)
      AND t_sessionid = p_sessionid
      and t_calcid = p_calcid;
     
    it_log.log(p_msg => 'End. p_calcid = ' || p_calcid);
    COMMIT;
  END DropEqualsRecsIncludingComis;

  procedure UpdateLimitMoney(p_sessionid integer, p_calcid number, p_basedate date, p_tradedate date, p_quikdataexists integer)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    RSB_DLUTILS.ParallelUpdateExecute (
       'DLIMIT_MONEY_REVISE_DBT',
       'UPDATE DLIMIT_MONEY_REVISE_DBT
             SET t_sofr_broker_comis =
           rshb_limit_util.GetDifMoneyComissSofr ('||to_chardatesql(p_basedate)||',
                                                  '||to_chardatesql(p_tradedate)||',
                                                  t_limit_kind,
                                                  T_CURCODE,
                                                  t_client_code,
                                                  t_market, 
                                                  t_market_kind,
                                                  0,
                                                  1,
                                                  1),
        t_sofr_broker_comis_for_rev =
           rshb_limit_util.GetDifMoneyComissSofr ('||to_chardatesql(p_basedate)||',
                                                  '||to_chardatesql(p_tradedate)||',
                                                  t_limit_kind,
                                                  T_CURCODE,
                                                  t_client_code,
                                                  t_market, 
                                                  t_market_kind,
                                                  0,
                                                  '|| TO_CHAR(p_quikdataexists) || ',
                                                  1),
        t_sofr_bankexp_comis =
           rshb_limit_util.GetDifMoneyComissSofr ('||to_chardatesql(p_basedate)||',
                                                  '||to_chardatesql(p_tradedate)||',
                                                  t_limit_kind,
                                                  T_CURCODE,
                                                  t_client_code,
                                                  t_market, 
                                                  t_market_kind,
                                                  1,
                                                  1,
                                                  0),
        t_sofr_plan_comis =
           rshb_limit_util.GetDifMoneyPlanComissSofr ('||to_chardatesql(p_basedate)||',
                                                      '||to_chardatesql(p_tradedate)||',
                                                      T_CURCODE,
                                                      t_client_code,
                                                      t_market, 
                                                      t_market_kind),
        t_quik_broker_comis =
           rshb_limit_util.GetDifBrokerComisByQuikData ('||to_chardatesql(p_basedate)||',
                                                        '||to_chardatesql(p_tradedate)||',
                                                        t_limit_kind,
                                                        T_CURCODE,
                                                        t_client_code,
                                                        '|| TO_CHAR(p_quikdataexists) || '),
        t_quik_repo_dif =
           rshb_limit_util.GetDifMoneyRepoByQuikData ('||to_chardatesql(p_basedate)||',
                                                      '||to_chardatesql(p_tradedate)||',
                                                      t_limit_kind,
                                                      T_CURCODE,
                                                      t_client_code,
                                                      '|| TO_CHAR(p_quikdataexists) || '),
        t_sofr_due =
           rshb_limit_util.GetDifMoneyDueSofr ('||to_chardatesql(p_basedate)||',
                                               '||to_chardatesql(p_tradedate)||',
                                               t_limaccountid,
                                               T_CURCODE,
                                               T_CLIENT_CODE,
                                               T_MARKET_KIND,
                                               T_MARKET,
                                               T_CURID,
                                               T_CLIENT_ID),
        t_sofr_repo_dif =
           rshb_limit_util.GetDifMoneyOrSecurRepoSofr ('||to_chardatesql(p_basedate)||',
                                                       '||to_chardatesql(p_tradedate)||',
                                                       1,
                                                       T_CURCODE,
                                                       0,
                                                       t_client_code,
                                                       t_limit_kind,
                                                       t_market, 
                                                       t_market_kind,
                                                       '|| TO_CHAR(p_quikdataexists) || '),
        t_sofr_NoUnloadedNptx =
           rshb_limit_util.GetDifMoneyNoUnloadedNptx ('||to_chardatesql(p_tradedate)||', T_CURCODE, t_client_code, t_market, t_market_kind, '|| TO_CHAR(p_quikdataexists) || '),
        t_sofr_fixcomis =
           rshb_limit_util.GetDifMoneyBrokerFixComissSofr ('||to_chardatesql(p_basedate)||',
                                                           '||to_chardatesql(p_tradedate)||',
                                                           T_CURCODE,
                                                           t_client_code,
                                                           t_market, 
                                                           t_market_kind,
                                                           '|| TO_CHAR(p_quikdataexists) || ')',
       't_sessionid = ' || TO_CHAR(p_sessionid) || ' and t_calcid = ' || TO_CHAR(p_calcid),
       16);
    
    it_log.log(p_msg => 'End. p_calcid = ' || p_calcid);
    COMMIT;
  END UpdateLimitMoney;

  procedure UpdateLimitSecur(p_sessionid integer, p_calcid number, p_basedate date, p_tradedate date, p_quikdataexists integer)
  AS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    RSB_DLUTILS.
     ParallelUpdateExecute (
       'DLIMIT_SECUR_REVISE_DBT',
       'UPDATE DLIMIT_SECUR_REVISE_DBT
             SET T_QUIK_REPO_DIF =
           rshb_limit_util.GetDifSecurRepoByQuikData ('||to_chardatesql(p_basedate)||',
                                                      '||to_chardatesql(p_tradedate)||',
                                                        t_limit_kind,
                                                        T_SECCODE,
                                                        t_client_code,
                                                        '|| TO_CHAR(p_quikdataexists) || '),
                 T_SOFR_REPO_DIF =
           rshb_limit_util.GetDifMoneyOrSecurRepoSofr ('||to_chardatesql(p_basedate)||',
                                                       '||to_chardatesql(p_tradedate)||',
                                                       0,
                                                       T_SECCODE,
                                                       T_SECID,
                                                       t_client_code,
                                                       t_limit_kind,
                                                       t_market, 
                                                       t_market_kind,
                                                       '|| TO_CHAR(p_quikdataexists) || '),
                T_SOFR_AVRWRT =
           rshb_limit_util.GetDifSecurAvrWrtBySofrData ('||to_chardatesql(p_tradedate)||',
                                                        T_SECCODE,
                                                        t_client_code,
                                                        t_market, 
                                                        t_market_kind,
                                                        '|| TO_CHAR(p_quikdataexists) || ')',
       't_sessionid = ' || TO_CHAR(p_sessionid) || ' and t_calcid = ' || TO_CHAR(p_calcid),
       8);
       COMMIT;
  END;

  --Позиции, по которым имеется расхождение
  procedure CheckDataSecur_ds(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer
                             ,p_is_advanced_check      integer) as
    cur_select    sys_refcursor;
    vc_cur_select varchar2(32000);
    v_f00         ddl_limitsecurites_dbt.t_client_code%type;
    v_f01         varchar2(1000);
    v_f02         ddl_limitsecurites_dbt.t_seccode%type;
    v_f03         ddl_limitsecurites_dbt.t_open_balance%type;
    v_f04         ddl_limitsecurites.t_open_balance%type;
    v_f05         ddl_limitsecurites_dbt.t_trdaccid%type;
    v_f06         ddl_limitsecurites.t_trdaccid%type;
    v_f07         ddl_limitsecurites_dbt.t_isblocked%type;
    v_f08         number;
    v_f09         DPARTY_DBT.T_NAME%type;
    v_f10         varchar2(3);
    v_f11         number(32,12);
    v_f12         number(32,12);
    v_start       boolean := true;
    p_tradeDate   DATE;
    p_baseDate    DATE;
    v_sessionid   integer;
    v_calcid      NUMBER(20);
    v_servKindString varchar2(20);
    v_quikDataExists integer := 0;
  begin
    C_MICEX_CODE_CURMARKET   := p_micex_code_curmarket;
    C_MICEX_CODE_STOCKMARKET := p_micex_code_stockmarket;
    it_rsl_string.clear;

    if p_secur_checked = 1 then
      v_servKindString := v_servKindString || CASE WHEN v_servKindString IS NOT NULL THEN ',' ELSE '' END || '1';
    end if;
    if p_cur_checked = 1 then
      v_servKindString := v_servKindString || CASE WHEN v_servKindString IS NOT NULL THEN ',' ELSE '' END || '21';
    end if;

    if (p_is_advanced_check = 1) then
      v_sessionid := RSB_DLUTILS.GETSESSIONID;
      v_calcid := dlimit_revise_seq.NEXTVAL;
      
      execute immediate 'SELECT max(t_date) FROM DDL_LIMITSECURITES_DBT ' || getQueryWhere(null, p_secur_checked, p_cur_checked, TRUE ) || ' AND ROWNUM = 1' INTO p_baseDate;
      if p_baseDate is null then
        return ;
      end if;
      FillBaseDateForLimit (p_baseDate);
      FillBaseDateCurrForLimit ();
      SELECT T_SETTLEDATEFROM
        INTO p_tradeDate
        FROM DLIMIT_BSDATE_DBT
       WHERE T_BASEDATE = p_baseDate AND T_LIMITKIND = -1;
      select NVL((SELECT 1 FROM DQUIK_DEALS_DBT WHERE t_tradedate = p_tradeDate AND rownum = 1),0) INTO v_quikDataExists from dual;
      DelOldTempLimit(v_sessionid, v_calcid, p_baseDate);
    end if;

    if (p_is_advanced_check = 1) then
       FillLimitSecur(v_sessionid, v_calcid, p_secur_checked, p_cur_checked);
       UpdateLimitSecur(v_sessionid, v_calcid, p_baseDate, p_tradeDate, v_quikDataExists);

       vc_cur_select := ' SELECT lim.t_client_code as f00,                               
                          ''T'' || lim.t_limit_kind as f01,          
                          lim.t_SECCODE as f02,
                          nvl(lim.T_SOFR_OPEN_BALANCE,0) as f03,        -- (lim.T_SOFR_OPEN_BALANCE - lim.T_SOFR_AVRWRT + lim.t_sofr_repo_dif) as f03, DEF-66187
                          nvl(lim.T_QUIK_OPEN_BALANCE,0) as f04,        -- (lim.T_QUIK_OPEN_BALANCE + lim.T_QUIK_REPO_DIF) as f04,  DEF-66187
                          lim.t_trdaccid as f05,
                          lim.t_trdaccid as f06,                  
                          lim.T_ISBLOCKED as f07,                 
                          lim.T_CLIENT_NAME as f09,
                          CASE
                              WHEN EXISTS
                                       (SELECT 1
                                          FROM dobjatcor_dbt c
                                         WHERE     c.t_objecttype = 12
                                               AND c.t_groupid = 35
                                               AND c.t_object = LPAD (lim.T_SECID, 10, ''0'')
                                               AND T_ATTRID = 2
                                               AND (   c.t_validtodate >= lim.t_date
                                                    OR c.t_validtodate = TO_DATE (''01.01.0001'', ''dd.mm.yyyy'')))
                              THEN
                                  ''Да''
                              ELSE
                                  ''''
                          END    AS f10,
                          0 AS f12
                     FROM  DLIMIT_SECUR_REVISE_DBT lim
                          WHERE ((lim.T_SOFR_OPEN_BALANCE - lim.T_SOFR_AVRWRT + lim.t_sofr_repo_dif) <> (lim.T_QUIK_OPEN_BALANCE + lim.T_QUIK_REPO_DIF))
                                and t_sessionid = ' || TO_CHAR(v_sessionid) || ' and t_calcid = ' || TO_CHAR(v_calcid) || '
                        order by  lim.t_client_code, lim.t_seccode, lim.t_limit_kind ';

    ELSE
      vc_cur_select := ' SELECT lim.t_client_code as f00,                               
                         ''T'' || lim.t_limit_kind as f01,          
                         lim.t_SECCODE as f02,                    
                         trunc(lim.t_open_balance) as f03,               
                         qlim.t_open_balance as f04,              
                         lim.t_trdaccid as f05,                   
                         qlim.t_trdaccid as f06,                  
                         lim.T_ISBLOCKED as f07,                 
                         party.t_name as f09,
                         CASE
                             WHEN EXISTS
                                      (SELECT 1
                                         FROM dobjatcor_dbt c
                                        WHERE     c.t_objecttype = 12
                                              AND c.t_groupid = 35
                                              AND c.t_object = LPAD (lim.t_security, 10, ''0'')
                                              AND T_ATTRID = 2
                                              AND (   c.t_validtodate >= lim.t_date
                                                   OR c.t_validtodate = TO_DATE (''01.01.0001'', ''dd.mm.yyyy'')))
                             THEN
                                 ''Да''
                             ELSE
                                 ''''
                         END    AS f10,
                         0 AS f12
                    FROM    (SELECT t_firm_id,             
                                    t_SECCODE,             
                                    t_client_code,         
                                    t_limit_kind,          
                                      case when t_limit_kind = 0 then t_quantity + t_open_limit
                                           when t_limit_kind = 365 then t_open_balance
                                        else lag(t_open_balance,1,0) over (partition by t_trdaccid, t_seccode, t_client_code order by t_trdaccid, t_seccode, t_client_code, t_limit_kind)   end 
                                       as t_open_balance,
                                    t_wa_position_price,       
                                    t_TRDACCID,                           
                                    t_isblocked,                          
                                    t_client,
                                    t_date,
                                    t_security
                               FROM DDL_LIMITSECURITES_DBT                
                              WHERE 1 = 1 ' || getQueryWhere(null, p_secur_checked, p_cur_checked) ||
                       ' ) lim
                         LEFT JOIN 
                            DPARTY_DBT PARTY
                         ON
                            PARTY.T_PARTYID = LIM.T_CLIENT
                         LEFT JOIN 
                            DDL_LIMITSECURITES qlim
                         ON     lim.t_client_code = qlim.t_client_code
                            AND lim.t_SECCODE = qlim.t_SECCODE        
                            AND lim.t_limit_kind = qlim.t_limit_kind  
                            AND lim.t_trdaccid = qlim.t_trdaccid      
                            AND trunc(lim.t_open_balance) <> QLIM.t_open_balance ' ||
                       getQueryWhere('qlim', p_secur_checked, p_cur_checked) ||
                       '  WHERE QLIM.t_open_balance IS NOT NULL                 
                       order by  lim.t_client_code, lim.t_seccode, lim.t_limit_kind ';
    end if;


    it_log.log(p_msg => 'START Step1', p_msg_clob => vc_cur_select);
    open cur_select for vc_cur_select;
    loop
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07
            ,v_f09
            ,v_f10
            ,v_f12;
      if v_start
      then
        v_start := false;
        it_log.log('Calc Step1');
      end if;
      exit when cur_select%notfound;
      it_rsl_string.AddCell('DEPO');
      it_rsl_string.AddCell(v_f00);
      it_rsl_string.AddCell(v_f09);
      it_rsl_string.AddCell(v_f01);
      it_rsl_string.AddCell(v_f02);
      it_rsl_string.AddCell(v_f03);
      it_rsl_string.AddCell(v_f04);
      it_rsl_string.AddCell(v_f05);
      it_rsl_string.AddCell(v_f06);
      if v_f07 = 'X'
      then
        it_rsl_string.AddCell('Классификатор блокировки');
      else
        it_rsl_string.AddCell(' ');
      end if;
      if v_f01 = 'T0'
      then
        it_rsl_string.AddCell(v_f03 - v_f04);
      else
        it_rsl_string.AddCell(' ');
      end if;
      it_rsl_string.AddCell(v_f12); /*Сумма вечерних пополнений*/
      it_rsl_string.AddCell(v_f10); /*Дефолт*/
      it_rsl_string.AddCell(' ');
      it_rsl_string.AddCell(' ', true);
    end loop;
    close cur_select;

    if (p_is_advanced_check = 1) then

       FillLimitMoney(v_sessionid, v_calcid, p_secur_checked, p_cur_checked);
       UpdateLimitMoney(v_sessionid, v_calcid, p_baseDate, p_tradeDate, v_quikDataExists);
       FixComDue(v_sessionid, v_calcid);
       DropEqualsRecsZero(v_sessionid, v_calcid);
       DropEqualsRecsFirst(v_sessionid, v_calcid);
       DropEqualsRecsSecond(v_sessionid, v_calcid);
       DropEqualsRecsIncludingComis(v_sessionid, v_calcid);

       vc_cur_select := 'SELECT lim.t_client_code as f00,                                                              
                              ''T'' || lim.t_limit_kind as f01,                                            
                              lim.t_curcode as f02,
                              nvl(lim.t_sofr_open_balance,0) as f03,  -- (lim.t_sofr_open_balance + lim.t_sofr_broker_comis_for_rev + lim.t_sofr_NoUnloadedNptx +    
                                                                      -- lim.t_sofr_fixcomis - (lim.t_sofr_due - lim.t_sofr_plan_comis) + lim.t_sofr_repo_dif - lim.t_sofr_bankexp_comis)  as f03,
                              nvl(lim.t_quik_open_balance,0) as f04,  -- (lim.t_quik_open_balance + lim.t_quik_repo_dif + lim.t_quik_broker_comis) as f04,  DEF-66197
                              '' '' as f05,
                              '' '' as f06,                                                                
                              lim.t_isblocked as f07,                                                    
                              lim.t_sofr_rest as f08,                                                          
                              lim.t_client_name as f09,
                             (SELECT NVL (SUM (CASE WHEN T_SUBKIND_OPERATION = 20 THEN NPTXOP.T_OUTSUM * -1 ELSE NPTXOP.T_OUTSUM END), 0)
                                FROM dnptxop_dbt nptxop
                               WHERE     NPTXOP.T_STATUS = 2
                                     AND NPTXOP.T_DOCKIND = 4607
                                     AND NPTXOP.T_OPERDATE = '||to_chardatesql(p_tradeDate)||'
                                     AND NPTXOP.T_CURRENCY = lim.T_CURID
                                     AND NPTXOP.t_client = lim.t_client_id
                                     AND NPTXOP.T_CONTRACT IN
                                           (SELECT SF.T_ID
                                              FROM DDLCONTRMP_DBT mp,
                                                   DSFCONTR_DBT sf
                                             WHERE     mp.T_MPCODE = lim.T_CLIENT_CODE
                                                   AND sf.t_ServKindSub = 8
                                                   AND sf.T_SERVKIND IN ('||v_servKindString||')
                                                   AND SF.T_ID = MP.T_SFCONTRID
                                                   AND MP.T_MARKETID = CASE WHEN lim.T_MARKET_KIND = ''ЕДП'' THEN MP.T_MARKETID ELSE LIM.T_MARKET END
                                                   )
                                     AND NPTXOP.T_SUBKIND_OPERATION IN (10)
                                     AND NOT EXISTS
                                                (SELECT 1
                                                   FROM dnotetext_dbt
                                                  WHERE     t_objecttype = 131
                                                        AND t_documentid = LPAD (nptxop.t_id, 34, ''0'')
                                                        AND t_notekind = 101)
                                     )    AS f12
                         FROM DLIMIT_MONEY_REVISE_DBT lim
                        WHERE t_sessionid = ' || TO_CHAR(v_sessionid) || ' and t_calcid = ' || TO_CHAR(v_calcid) || ' and lim.t_isdel <> chr(88)
                        order by  lim.t_client_code, lim.t_curcode, lim.t_limit_kind ';
    ELSE
      vc_cur_select := 'SELECT lim.t_client_code as f00,                                                              
                             ''T'' || lim.t_limit_kind as f01,                                            
                             lim.t_curr_code as f02,                                                    
                             nvl(lim.t_open_balance,0) as f03,                                          
                             nvl(qlim.t_open_balance,0) as f04,                                         
                             '' '' as f05,                                                                
                             '' '' as f06,                                                                
                             lim.t_isblocked as f07,                                                    
                             lim.rest as f08,                                                          
                             party.t_name as f09,
                            (SELECT NVL (SUM (CASE WHEN T_SUBKIND_OPERATION = 20 THEN NPTXOP.T_OUTSUM * -1 ELSE NPTXOP.T_OUTSUM END), 0)
                               FROM dnptxop_dbt nptxop
                              WHERE     NPTXOP.T_STATUS = 2
                                    AND NPTXOP.T_DOCKIND = 4607
                                    AND NPTXOP.T_OPERDATE = RSI_RsbCalendar.GetDateAfterWorkDay (lim.t_date, -1)
                                    AND NPTXOP.t_client = lim.t_client
                                    AND NPTXOP.T_CURRENCY = lim.T_CURRID
                                    AND NPTXOP.T_CONTRACT IN
                                           (SELECT SF.T_ID
                                              FROM DDLCONTRMP_DBT mp,
                                                   DSFCONTR_DBT sf
                                             WHERE     mp.T_MPCODE = lim.T_CLIENT_CODE
                                                   AND sf.t_ServKindSub = 8
                                                   AND sf.T_SERVKIND IN ('||v_servKindString||')
                                                   AND SF.T_ID = MP.T_SFCONTRID
                                                   AND MP.T_MARKETID = CASE WHEN lim.T_MARKET_KIND = ''ЕДП'' THEN MP.T_MARKETID ELSE LIM.T_MARKET END
                                                   )
                                    AND NPTXOP.T_SUBKIND_OPERATION IN (10)
                                    AND NOT EXISTS
                                               (SELECT 1
                                                  FROM dnotetext_dbt
                                                 WHERE     t_objecttype = 131
                                                       AND t_documentid = LPAD (nptxop.t_id, 34, ''0'')
                                                       AND t_notekind = 101)
                                    )    AS f12
                        FROM    (SELECT t_firm_id,                                               
                                        t_tag,                                                   
                                        t_curr_code,                                             
                                        t_client_code,                                           
                                        t_limit_kind,                                            
                                        case when t_limit_kind = 0 then t_money306 - T_COMPREVIOUS - T_COMPREVIOUS_1 + t_open_limit
                                             when t_limit_kind = 365 then t_open_balance
                                          else lag(t_open_balance,1,0) over (partition by t_curr_code, t_client_code, t_tag order by t_tag, t_client_code, t_curr_code, t_limit_kind)   end 
                                         as t_open_balance, 
                                        t_leverage,          
                                        T_ISBLOCKED,         
                                        (c.T_MONEY306  - c.T_COMPREVIOUS - c.T_COMPREVIOUS_1 + c.t_open_limit) as rest,
                                        c.t_client,
                                        c.t_date,
                                        c.T_CURRID,
                                        c.T_MARKET,
                                        c.T_MARKET_KIND
                                   FROM DDL_LIMITCASHSTOCK_dbt c, daccount_dbt a        
                                  WHERE 1 = 1 and c.t_internalaccount = a.t_accountid ' ||
                       getQueryWhere(null, p_secur_checked, p_cur_checked) ||
                       ') lim  
                             LEFT JOIN 
                                DPARTY_DBT PARTY
                             ON
                                PARTY.T_PARTYID = LIM.T_CLIENT
                             LEFT JOIN                                                     
                                DDL_LIMITCASHSTOCK qlim                                    
                             ON     lim.t_client_code = qlim.t_client_code                 
                                AND lim.t_curr_code = qlim.t_curr_code                     
                                AND lim.t_limit_kind = qlim.t_limit_kind                   
                                AND lim.t_firm_id = qlim.t_firm_id                         
                                AND lim.t_tag = TRIM(qlim.t_tag)
                                AND (lim.t_open_balance <> QLIM.t_open_balance or lim.rest < 0) ' ||
                       getQueryWhere('qlim', p_secur_checked, p_cur_checked) || '  
                       WHERE QLIM.t_open_balance IS NOT NULL or lim.rest < 0                     
                       order by  lim.t_client_code, lim.t_curr_code, lim.t_limit_kind ';
    end if;


    it_log.log(p_msg => 'START Step2', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07
            ,v_f08
            ,v_f09
            ,v_f12;
      if v_start
      then
        v_start := false;
        it_log.log('Calc Step2');
      end if;
      exit when cur_select%notfound;
      it_rsl_string.AddCell('MONEY');
      it_rsl_string.AddCell(v_f00);
      it_rsl_string.AddCell(v_f09);
      it_rsl_string.AddCell(v_f01);
      it_rsl_string.AddCell(v_f02);
      it_rsl_string.AddCell(v_f03);
      it_rsl_string.AddCell(v_f04);
      it_rsl_string.AddCell(v_f05);
      it_rsl_string.AddCell(v_f06);
      if v_f07 = 'X'
      then
        it_rsl_string.AddCell('Классификатор блокировки');
      else
        it_rsl_string.AddCell(' ');
      end if;
      if v_f01 = 'T0'
      then
        it_rsl_string.AddCell(v_f03 - v_f04);
      else
        it_rsl_string.AddCell(' ');
      end if;
      it_rsl_string.AddCell(v_f12); /*Сумма вечерних пополнений*/
      it_rsl_string.AddCell(' '); /*Дефолт*/
      if v_f08 < 0
      then
        it_rsl_string.AddCell(v_f08);
      else
        it_rsl_string.AddCell(' ');
      end if;
      it_rsl_string.AddCell(' ', true);
    end loop;
    close cur_select;
    it_log.log('Store Step2');
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      it_error.clear_error_stack;
      raise;
  end CheckDataSecur_ds;

  -- Позиции в ИТС QUIK, которые не обнаружены в СОФР
  procedure CheckDataSecur_ss(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer
                             ,p_record_limit           integer default null) as
    cur_select    sys_refcursor;
    vc_cur_select varchar2(32000);
    v_f00         ddl_limitsecurites.t_client_code%type;
    v_f01         varchar2(1000);
    v_f02         ddl_limitsecurites.t_seccode%type;
    v_f03         ddl_limitsecurites.t_open_balance%type;
    v_f04         ddl_limitsecurites.t_trdaccid%type;
    v_f05         char(1);
    v_f06         DPARTY_DBT.T_NAME%type;
    v_f07         varchar2(3);
    v_start       boolean := true;
    v_recCount    number(15) := 0;
  begin
    C_MICEX_CODE_CURMARKET   := p_micex_code_curmarket;
    C_MICEX_CODE_STOCKMARKET := p_micex_code_stockmarket;
    it_rsl_string.clear;
    vc_cur_select := 'SELECT lim.t_client_code as f00,                          
                    ''T'' || lim.t_limit_kind as f01,                             
                    lim.t_SECCODE as f02,                                         
                    trunc(lim.t_open_balance) as f03,              
                    lim.t_trdaccid as f04,                                        
                    '' '' as f05,                                                   
                    (SELECT party.t_name
                       FROM DDL_LIMITPRM_DBT  limprm,
                            DDLCONTRMP_DBT    mp,
                            DSFCONTR_DBT      sfcontr,
                            dparty_dbt        party
                      WHERE     mp.T_MPCODE = lim.T_CLIENT_CODE
                            AND limprm.T_FIRMCODE = lim.T_FIRM_ID
                            AND limprm.T_MARKETID = mp.T_MARKETID
                            AND sfcontr.t_id = mp.T_SFCONTRID
                            AND party.T_PARTYID = sfcontr.T_PARTYID
                            AND ROWNUM = 1)    AS f06,
                    CASE
                        WHEN EXISTS
                                 (SELECT /*+ ordered use_nl(objc)*/ 1
                                    FROM DDL_LIMITPRM_DBT  limprm,
                                         DOBJCODE_DBT      objc,
                                         dobjatcor_dbt     c
                                   WHERE     limprm.T_FIRMCODE = lim.T_FIRM_ID
                                         AND objc.T_OBJECTTYPE = 9
                                         AND objc.T_CODEKIND =
                                             RSHB_RSI_SCLIMIT.GetKindMarketCodeOrNote (
                                                 limprm.T_MARKETID,
                                                 1,
                                                 0)
                                         AND objc.T_STATE = 0
                                         AND objc.t_code = lim.T_SECCODE
                                         AND c.t_objecttype = 12
                                         AND c.t_groupid = 35
                                         AND c.t_object = LPAD (objc.T_OBJECTID, 10, ''0'')
                                         AND T_ATTRID = 2)
                        THEN
                            ''Да''
                        ELSE
                            ''''
                    END    AS f07
               FROM DDL_LIMITSECURITES lim                                 
              LEFT JOIN  DDL_LIMITSECURITES_dbt lt                   
                             ON    LIM.t_client_code = LT.t_client_code   
                                   AND LIM.t_SECCODE = LT.t_SECCODE        
                                   AND LIM.t_limit_kind = LT.t_limit_kind  
                                    AND lim.t_firm_id = lt.t_firm_id       
                                    AND lim.t_trdaccid = lt.t_trdaccid     
                                   AND 1 = 1 ' || getQueryWhere('lt', p_secur_checked, p_cur_checked) || ' 
                 ' || getQueryWhere('lim', p_secur_checked, p_cur_checked, true) || '  
                       AND lt.t_id is null  
                       AND lim.t_open_balance is not null and lim.t_open_balance <> 0
                 order by  lim.t_client_code, lim.t_seccode, lim.t_limit_kind';
    it_log.log(p_msg => 'START Step1', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      v_recCount := v_recCount + 1;
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07;
      if v_start
      then
        v_start := false;
        it_log.log('Calc Step1');
      end if;
      exit when cur_select%notfound;
      if ((p_record_limit is null) or (v_recCount <= p_record_limit)) then
        it_rsl_string.AddCell('DEPO');
        it_rsl_string.AddCell(v_f00);
        it_rsl_string.AddCell(v_f06); /*имя клиента*/
        it_rsl_string.AddCell(v_f01);
        it_rsl_string.AddCell(v_f02);
        it_rsl_string.AddCell(v_f03);
        it_rsl_string.AddCell(v_f04);
        it_rsl_string.AddCell(v_f05);
        it_rsl_string.AddCell(v_f07);
        it_rsl_string.AddCell(' '); /*дефолт*/
        it_rsl_string.AddCell(' ', true);
      end if;
    end loop;
    close cur_select;
    vc_cur_select := 'SELECT lim.t_client_code as f00,                   
              ''T'' || lim.t_limit_kind as f01,                             
              lim.t_curr_code as f02,                                       
              lim.t_open_balance as f03,                                    
              '' ''as f04,                      
              '' ''as f05,                      
              (SELECT party.t_name
                 FROM DDL_LIMITPRM_DBT  limprm,
                      DDLCONTRMP_DBT    mp,
                      DSFCONTR_DBT      sfcontr,
                      dparty_dbt        party
                WHERE     mp.T_MPCODE = lim.T_CLIENT_CODE
                      AND limprm.T_FIRMCODE = lim.T_FIRM_ID
                      AND limprm.T_MARKETID = mp.T_MARKETID
                      AND sfcontr.t_id = mp.T_SFCONTRID
                      AND party.T_PARTYID = sfcontr.T_PARTYID
                      AND ROWNUM = 1)    AS f06
                    FROM DDL_LIMITCASHSTOCK lim                    
                    LEFT JOIN DDL_LIMITCASHSTOCK_dbt lt              
                        ON     LIM.t_client_code = LT.t_client_code  
                              AND LIM.t_curr_code = LT.t_curr_code   
                              AND LIM.t_limit_kind = LT.t_limit_kind 
                              AND lim.t_firm_id = lt.t_firm_id       
                              AND TRIM(lim.t_tag) = lt.t_tag
                        AND 1 = 1 ' || getQueryWhere('lt', p_secur_checked, p_cur_checked) || ' 
             ' || getQueryWhere('lim', p_secur_checked, p_cur_checked, true) || '
                      AND lim.t_open_balance is not null and lim.t_open_balance <> 0 AND lt.t_tag is null
           order by  lim.t_client_code, lim.t_curr_code, lim.t_limit_kind ';
    it_log.log(p_msg => 'START Step2', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      v_recCount := v_recCount + 1;
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06;
      if v_start
      then
        v_start := false;
        it_log.log('Calc Step2');
      end if;
      exit when cur_select%notfound;
      if ((p_record_limit is null) or (v_recCount <= p_record_limit)) then
        it_rsl_string.AddCell('MONEY');
        it_rsl_string.AddCell(v_f00);
        it_rsl_string.AddCell(v_f06); /*имя клиента*/
        it_rsl_string.AddCell(v_f01);
        it_rsl_string.AddCell(v_f02);
        it_rsl_string.AddCell(v_f03);
        it_rsl_string.AddCell(v_f04);
        it_rsl_string.AddCell(v_f05);
        it_rsl_string.AddCell(' ');/*дефолт*/
        it_rsl_string.AddCell(' '); 
        it_rsl_string.AddCell(' ', true);
      end if;
    end loop;
    close cur_select;

    if ((p_record_limit is not null) and (v_recCount > p_record_limit)) then
      AddMessageToBegin('Превышен лимит вывода строк на таблицу. Не выведено строк: ' || TO_CHAR((v_recCount - p_record_limit)));
    end if;
    it_log.log('Store Step2');
  end;

  -- Позиции в СОФР которые не обнаружены в ИТС QUIK
  procedure CheckDataSecur_qs(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer
                             ,p_record_limit           integer default null) as
    cur_select    sys_refcursor;
    vc_cur_select varchar2(32000);
    v_f00         ddl_limitsecurites.t_client_code%type;
    v_f01         varchar2(1000);
    v_f02         ddl_limitsecurites.t_seccode%type;
    v_f03         ddl_limitsecurites.t_open_balance%type;
    v_f04         ddl_limitsecurites.t_trdaccid%type;
    v_f05         char(1);
    v_f06         DPARTY_DBT.T_NAME%type;
    v_f07         varchar2(3);
    v_start       boolean := true;
    v_recCount    number(15) := 0;
  begin
    C_MICEX_CODE_CURMARKET   := p_micex_code_curmarket;
    C_MICEX_CODE_STOCKMARKET := p_micex_code_stockmarket;
    it_rsl_string.clear;
    vc_cur_select := 'SELECT lim.t_client_code as f00,                   
              ''T'' || lim.t_limit_kind as f01,                            
              lim.t_SECCODE as f02,                                      
              trunc(lim.t_open_balance) as f03,           
              lim.t_trdaccid as f04,                                     
              lim.T_ISBLOCKED as f05,                                   
              party.t_name as f06,
              CASE
                  WHEN EXISTS
                           (SELECT 1
                              FROM dobjatcor_dbt c
                             WHERE     c.t_objecttype = 12
                                   AND c.t_groupid = 35
                                   AND c.t_object = LPAD (lim.t_security, 10, ''0'')
                                   AND T_ATTRID = 2
                                   AND (   c.t_validtodate >= lim.t_date
                                        OR c.t_validtodate = TO_DATE (''01.01.0001'', ''dd.mm.yyyy'')))
                  THEN
                      ''Да''
                  ELSE
                      ''''
              END    AS f07
         FROM DDL_LIMITSECURITES_DBT lim                          
        LEFT JOIN 
           DPARTY_DBT PARTY
        ON
           PARTY.T_PARTYID = LIM.T_CLIENT
        LEFT JOIN DDL_LIMITSECURITES lt                   
                       ON     LIM.t_client_code = LT.t_client_code
                             AND LIM.t_SECCODE = LT.t_SECCODE     
                              AND lim.t_firm_id = lt.t_firm_id    
                              AND lim.t_trdaccid = lt.t_trdaccid  
                             AND LIM.t_limit_kind = LT.t_limit_kind ' || getQueryWhere('lt', p_secur_checked, p_cur_checked) || ' 
              ' || getQueryWhere('lim', p_secur_checked, p_cur_checked, true) || ' 
                      AND lt.t_seccode is null 
                      AND lim.t_open_balance is not null and lim.t_open_balance <> 0
           order by  lim.t_client_code, lim.t_seccode, lim.t_limit_kind   ';
    it_log.log(p_msg => 'START Step1', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      v_recCount := v_recCount + 1;
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07;
      if v_start
      then
        v_start := false;
        it_log.log('Calc Step1');
      end if;
      exit when cur_select%notfound;
      if ((p_record_limit is null) or (v_recCount <= p_record_limit)) then
        it_rsl_string.AddCell('DEPO');
        it_rsl_string.AddCell(v_f00);
        it_rsl_string.AddCell(v_f06);
        it_rsl_string.AddCell(v_f01);
        it_rsl_string.AddCell(v_f02);
        it_rsl_string.AddCell(v_f03);
        it_rsl_string.AddCell(v_f04);
        if v_f05 = 'X'
        then
          it_rsl_string.AddCell('Классификатор блокировки');
        else
          it_rsl_string.AddCell(' ');
        end if;
        it_rsl_string.AddCell(v_f07);
        it_rsl_string.AddCell(' ');
        it_rsl_string.AddCell(' ', true);
      end if;
    end loop;
    close cur_select;
    vc_cur_select := 'SELECT lim.t_client_code as f00,          
               ''T'' || lim.t_limit_kind as f01,                  
               lim.t_curr_code as f02,                          
               lim.t_open_balance as f03,   
    --           '' '' as f04,                                      
               lim.T_ISBLOCKED as f05,                          
               party.t_name as f06
          FROM DDL_LIMITCASHSTOCK_dbt lim                
         LEFT JOIN 
            DPARTY_DBT PARTY
         ON
            PARTY.T_PARTYID = LIM.T_CLIENT
         LEFT JOIN DDL_LIMITCASHSTOCK lt                 
                        ON     LIM.t_client_code = LT.t_client_code 
                              AND LIM.t_curr_code = LT.t_curr_code  
                              AND lim.t_firm_id = lt.t_firm_id      
                              AND lim.t_tag = TRIM(lt.t_tag)
                              AND LIM.t_limit_kind = LT.t_limit_kind ' || getQueryWhere('lt', p_secur_checked, p_cur_checked) || ' 
             ' || getQueryWhere('lim', p_secur_checked, p_cur_checked, true) || '  
                      AND lim.t_open_balance is not null and lim.t_open_balance <> 0 AND lt.t_tag is null
           order by  lim.t_client_code, lim.t_curr_code, lim.t_limit_kind  ';
    it_log.log(p_msg => 'START Step2', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      v_recCount := v_recCount + 1;
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
             --       ,v_f04
            ,v_f05
            ,v_f06;
      if v_start
      then
        v_start := false;
        it_log.log('Calc Step2');
      end if;
      exit when cur_select%notfound;
      if ((p_record_limit is null) or (v_recCount <= p_record_limit)) then
        it_rsl_string.AddCell('MONEY');
        it_rsl_string.AddCell(v_f00);
        it_rsl_string.AddCell(v_f06); /*Имя клиента*/
        it_rsl_string.AddCell(v_f01);
        it_rsl_string.AddCell(v_f02);
        it_rsl_string.AddCell(v_f03);
        it_rsl_string.AddCell(' '); --   it_rsl_string.AddCell(v_f04);
        if v_f05 = 'X'
        then
          it_rsl_string.AddCell('Классификатор блокировки');
        else
          it_rsl_string.AddCell(' ');
        end if;
        it_rsl_string.AddCell(' ');/*Дефолт*/
        it_rsl_string.AddCell(' '); 
        it_rsl_string.AddCell(' ', true);
      end if;
    end loop;
    close cur_select;

    if ((p_record_limit is not null) and (v_recCount > p_record_limit)) then
      AddMessageToBegin('Превышен лимит вывода строк на таблицу. Не выведено строк: ' || TO_CHAR((v_recCount - p_record_limit)));
    end if;
    it_log.log('Store Step2');
  end;

  --Позиции в ИТС QUIK, которые совпадают с СОФР
  procedure CheckDataSecur_is(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer) as
    cur_select    sys_refcursor;
    vc_cur_select varchar2(32000);
    v_f00         ddl_limitsecurites.t_client_code%type;
    v_f01         varchar2(1000);
    v_f02         ddl_limitsecurites.t_seccode%type;
    v_f03         ddl_limitsecurites.t_open_balance%type;
    v_f04         ddl_limitsecurites.t_open_balance%type;
    v_f05         ddl_limitsecurites.t_trdaccid%type;
    v_f06         ddl_limitsecurites.t_trdaccid%type;
    v_f07         char(1);
    v_f08         DPARTY_DBT.T_NAME%type;
    v_f09         varchar2(3);
    v_start       boolean := true;
  begin
    C_MICEX_CODE_CURMARKET   := p_micex_code_curmarket;
    C_MICEX_CODE_STOCKMARKET := p_micex_code_stockmarket;
    it_rsl_string.clear;
    vc_cur_select := ' SELECT /*+ index (lim ddl_limitsecurites_dbt_idx3) index (qlim DDL_LIMITSECURITES_idx1)*/ 
                         lim.t_client_code as f00, 
                          ''T'' || lim.t_limit_kind as f01, 
                          lim.t_SECCODE as f02, 
                          trunc(lim.t_open_balance) as f03, 
                          qlim.t_open_balance  as f04, 
                          lim.t_trdaccid  as f05, 
                          qlim.t_trdaccid  as f06, 
                          lim.T_ISBLOCKED  as f07,
                          party.t_name as f08,
                          CASE
                              WHEN EXISTS
                                       (SELECT 1
                                          FROM dobjatcor_dbt c
                                         WHERE     c.t_objecttype = 12
                                               AND c.t_groupid = 35
                                               AND c.t_object = LPAD (lim.t_security, 10, ''0'')
                                               AND T_ATTRID = 2
                                               AND (   c.t_validtodate >= lim.t_date
                                                    OR c.t_validtodate = TO_DATE (''01.01.0001'', ''dd.mm.yyyy'')))
                              THEN
                                  ''Да''
                              ELSE
                                  ''''
                          END    AS f09
                     FROM    (SELECT t_client_code, 
                                     t_seccode, 
                                     t_limit_kind, 
                                     t_quantity, 
                                     t_market, 
                                     t_isblocked, 
                                     t_trdaccid, 
                                     t_firm_id, 
                                     CASE 
                                        WHEN t_limit_kind = 0 
                                        THEN t_quantity + t_open_limit
                                        ELSE 
                                           LAG ( t_open_balance, 1, 0) 
                                           OVER ( partition by t_trdaccid, t_seccode, t_client_code ORDER BY t_trdaccid, t_seccode, t_client_code, t_limit_kind) 
                                     END t_open_balance,
                                     T_CLIENT,
                                     t_security
                                FROM DDL_LIMITSECURITES_DBT where 1=1 ' || getQueryWhere(null, p_secur_checked, p_cur_checked) ||
                     ') lim 
                          LEFT JOIN 
                             DPARTY_DBT PARTY
                          ON
                             PARTY.T_PARTYID = LIM.T_CLIENT
                          LEFT JOIN 
                             DDL_LIMITSECURITES qlim 
                          ON     lim.t_client_code = qlim.t_client_code 
                             AND lim.t_SECCODE = qlim.t_SECCODE 
                             AND lim.t_limit_kind = qlim.t_limit_kind 
                             AND lim.t_firm_id = qlim.t_firm_id 
                             AND lim.t_trdaccid = qlim.t_trdaccid                          
                             AND trunc(lim.t_open_balance) = QLIM.t_open_balance 
                             AND 1 = 1 ' || getQueryWhere('qlim', p_secur_checked, p_cur_checked) || '  
                    WHERE QLIM.t_open_balance IS NOT NULL 
                 ORDER BY lim.t_client_code, lim.t_seccode, lim.t_limit_kind   ';
    it_log.log(p_msg => 'START Step1', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07
            ,v_f08
            ,v_f09;
      if v_start
      then
        v_start := false;
        it_log.log('Calc Step1');
      end if;
      exit when cur_select%notfound;
      it_rsl_string.AddCell('DEPO');
      it_rsl_string.AddCell(v_f00);
      it_rsl_string.AddCell(v_f08);
      it_rsl_string.AddCell(v_f01);
      it_rsl_string.AddCell(v_f02);
      it_rsl_string.AddCell(v_f03);
      it_rsl_string.AddCell(v_f04);
      it_rsl_string.AddCell(v_f05);
      it_rsl_string.AddCell(v_f06);
      it_rsl_string.AddCell(v_f07);
      it_rsl_string.AddCell(v_f09, true);
    end loop;
    close cur_select;
    vc_cur_select := 'SELECT lim.t_client_code as f00,                  
                   ''T'' || lim.t_limit_kind as f01,                      
                   lim.t_curr_code as f02,                              
                   lim.t_open_balance as f03,                           
                   qlim.t_open_balance as f04,                          
                   '' '' as f05,                                           
                   '' '' as f06,                                          
                   lim.T_ISBLOCKED as f07,                              
                   party.T_NAME as f08
              FROM    (SELECT t_firm_id,                         
                              t_tag,                             
                              t_curr_code,                       
                              t_client_code,                     
                              t_limit_kind,                      
                          CASE 
                             WHEN t_limit_kind = 0 
                             THEN t_money306  - T_COMPREVIOUS - T_COMPREVIOUS_1 + t_open_limit
                             ELSE 
                                LAG ( t_open_balance, 1, 0) 
                                OVER ( partition by t_curr_code, t_client_code, t_tag ORDER BY t_tag, t_client_code, t_curr_code, t_limit_kind)
                              END as t_open_balance,
                              t_leverage,                                 
                              t_isblocked, t_due474, t_comprevious,       
                              T_CLIENT
                         FROM DDL_LIMITCASHSTOCK_dbt                      
                        WHERE 1 = 1 ' || getQueryWhere(null, p_secur_checked, p_cur_checked) ||
                     ') lim 
                   LEFT JOIN 
                      DPARTY_DBT PARTY
                   ON
                      PARTY.T_PARTYID = LIM.T_CLIENT
                   LEFT JOIN                                            
                      DDL_LIMITCASHSTOCK qlim                           
                   ON     lim.t_client_code = qlim.t_client_code        
                      AND lim.t_curr_code = qlim.t_curr_code            
                      AND lim.t_limit_kind = qlim.t_limit_kind          
                      AND lim.t_tag = TRIM(qlim.t_tag)
                  AND lim.t_firm_id = qlim.t_firm_id 
                      AND lim.t_open_balance = QLIM.t_open_balance ' || getQueryWhere('qlim', p_secur_checked, p_cur_checked) || ' 
             WHERE QLIM.t_open_balance IS NOT NULL  
             order by  lim.t_client_code, lim.t_curr_code, lim.t_limit_kind   ';
    it_log.log(p_msg => 'START Step2', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07
            ,v_f08;
      if v_start
      then
        v_start := false;
        it_log.log('Calc Step2');
      end if;
      exit when cur_select%notfound;
      it_rsl_string.AddCell('DEPO');
      it_rsl_string.AddCell(v_f00);
      it_rsl_string.AddCell(v_f08);
      it_rsl_string.AddCell(v_f01);
      it_rsl_string.AddCell(v_f02);
      it_rsl_string.AddCell(v_f03);
      it_rsl_string.AddCell(v_f04);
      it_rsl_string.AddCell(v_f05);
      it_rsl_string.AddCell(v_f06);
      it_rsl_string.AddCell(v_f07);
      it_rsl_string.AddCell(' ', true);
      
    end loop;
    close cur_select;
    it_log.log('Store Step2');
  end;

  --Позиции, по которым имеются расхождения в цене
  procedure CheckDataSecur_wp(p_micex_code_curmarket   varchar2
                             ,p_micex_code_stockmarket varchar2
                             ,p_secur_checked          integer
                             ,p_cur_checked            integer) as
    cur_select    sys_refcursor;
    vc_cur_select varchar2(32000);
    v_f00         ddl_limitsecurites.t_client_code%type;
    v_f01         varchar2(1000);
    v_f02         ddl_limitsecurites.t_seccode%type;
    v_f03         ddl_limitsecurites.t_open_balance%type;
    v_f04         ddl_limitsecurites.t_open_balance%type;
    v_f05         ddl_limitsecurites.t_trdaccid%type;
    v_f06         ddl_limitsecurites.t_trdaccid%type;
    v_f07         char(1);
    v_f08         ddl_limitsecurites.t_wa_position_price%type;
    v_f09         ddl_limitsecurites.t_wa_position_price%type;
    v_f10         DPARTY_DBT.T_NAME%type;
    v_start       boolean := true;
  begin
    C_MICEX_CODE_CURMARKET   := p_micex_code_curmarket;
    C_MICEX_CODE_STOCKMARKET := p_micex_code_stockmarket;
    it_rsl_string.clear;
    vc_cur_select := 'SELECT lim.t_client_code as f00,          
             ''T'' || lim.t_limit_kind as f01,                    
             lim.t_SECCODE as f02,                              
             trunc(lim.t_open_balance) as f03,   
             qlim.t_open_balance as f04,                        
             lim.t_trdaccid as f05,                             
             qlim.t_trdaccid as f06,                            
             lim.T_ISBLOCKED as f07,                            
             lim.t_wa_position_price as f08,                    
             qlim.t_wa_position_price as f09,                  
             party.t_name as f10
        FROM    (SELECT t_firm_id,                       
                        t_SECCODE,                       
                        t_client_code,                   
                        t_limit_kind,                    
                          case when t_limit_kind = 0 then t_quantity + t_open_limit 
                            else lag(t_open_balance,1,0) over (partition by t_trdaccid, t_seccode, t_client_code order by t_trdaccid, t_seccode, t_client_code, t_limit_kind)   end 
                           as t_open_balance, t_wa_position_price,      
                        t_TRDACCID,                           
                        t_isblocked,                          
                        T_CLIENT
                   FROM DDL_LIMITSECURITES_DBT                
                  WHERE 1 = 1 ' || getQueryWhere(null, p_secur_checked, p_cur_checked) ||
                     ') lim
             LEFT JOIN 
                DPARTY_DBT PARTY
             ON
                PARTY.T_PARTYID = LIM.T_CLIENT
             LEFT JOIN                                    
                DDL_LIMITSECURITES qlim                   
             ON     lim.t_client_code = qlim.t_client_code
                AND lim.t_SECCODE = qlim.t_SECCODE        
                AND lim.t_limit_kind = qlim.t_limit_kind  
                AND qlim.t_limit_kind in (2,365)          
                AND lim.t_firm_id = qlim.t_firm_id        
                AND lim.t_trdaccid = qlim.t_trdaccid          
                AND lim.t_wa_position_price <> qlim.t_wa_position_price ' ||
                     getQueryWhere('qlim', p_secur_checked, p_cur_checked) || '
       WHERE QLIM.t_open_balance IS NOT NULL       
           order by  lim.t_client_code, lim.t_seccode, lim.t_limit_kind';
    it_log.log(p_msg => 'START ', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07
            ,v_f08
            ,v_f09
            ,v_f10;
      if v_start
      then
        v_start := false;
        it_log.log('Calculated');
      end if;
      exit when cur_select%notfound;
      it_rsl_string.AddCell('DEPO');
      it_rsl_string.AddCell(v_f00);
      it_rsl_string.AddCell(v_f10);
      it_rsl_string.AddCell(v_f01);
      it_rsl_string.AddCell(v_f02);
      it_rsl_string.AddCell(v_f08);
      it_rsl_string.AddCell(v_f09);
      it_rsl_string.AddCell(v_f09 - v_f08, true);
    end loop;
    close cur_select;
    it_log.log('Stored');
  end;

  procedure CheckDataFutur_df as
    cur_select    sys_refcursor;
    vc_cur_select varchar2(32000);
    v_f00         ddl_limitfuturmark_dbt.t_client%type;
    v_f01         char(1);
    v_f02         char(1);
    v_f03         ddl_limitfuturmark_dbt.t_volumemn%type;
    v_f04         ddl_limitfuturmark.t_volumemn%type;
    v_f05         ddl_limitfuturmark_dbt.t_account%type;
    v_f06         ddl_limitfuturmark.t_account%type;
    v_f07         char(1);
    v_f08         number;
    v_f09         dparty_dbt.t_name%type;
    v_f10         ddlcontrmp_dbt.t_mpcode%type;
    v_start       boolean := true;
  begin
    it_rsl_string.clear;
    vc_cur_select := 'SELECT lim.t_client as f00,                       
                  '' '' as f01,                                   
                  '' '' as f02,                                   
                  lim.T_VOLUMEMN as f03,                        
                  qlim.T_VOLUMEMN as f04,                       
                  lim.T_ACCOUNT as f05,                         
                  qlim.t_account as f06,                        
                  lim.T_ISBLOCKED as f07,                       
                  lim.T_VOLUMEMN - qlim.T_VOLUMEMN as f08,      
                  party.t_name as f09,
                  (SELECT mp.t_mpcode
                     FROM daccount_dbt a,
                          dmcaccdoc_dbt d,
                          ddlcontrmp_dbt mp,
                          dsfcontr_dbt sf
                    WHERE     mp.t_sfcontrid = sf.t_id
                          AND sf.t_servkind = 15
                          AND sf.t_id = D.T_CLIENTCONTRID
                          AND SF.T_PARTYID = d.t_owner
                          AND d.t_catid = 70
                          AND d.t_account = a.t_account
                          AND d.t_chapter = a.t_chapter
                          AND D.T_CURRENCY = A.T_CODE_CURRENCY
                          AND lim.T_CLIENT = sf.t_partyid
                          AND lim.T_INTERNALACCOUNT = a.t_accountid
                          AND rownum = 1) as f10
             FROM     DDL_LIMITFUTURMARK_DBT lim         
                  LEFT JOIN                              
                     DDL_LIMITFUTURMARK qlim             
                  ON lim.T_ACCOUNT = qlim.T_ACCOUNT      
                     AND lim.T_VOLUMEMN <> QLIM.T_VOLUMEMN  
                  LEFT JOIN 
                     DPARTY_DBT PARTY
                  ON
                     PARTY.T_PARTYID = LIM.T_CLIENT
         WHERE QLIM.T_VOLUMEMN IS NOT NULL';
    it_log.log(p_msg => 'START ', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07
            ,v_f08
            ,v_f09
            ,v_f10;
      if v_start
      then
        v_start := false;
        it_log.log('Calculated');
      end if;
      exit when cur_select%notfound;
      it_rsl_string.AddCell('SPBFUT');
      it_rsl_string.AddCell(v_f10);
      it_rsl_string.AddCell(v_f09);
      it_rsl_string.AddCell(v_f01);
      it_rsl_string.AddCell(v_f02);
      it_rsl_string.AddCell(v_f03);
      it_rsl_string.AddCell(v_f04);
      it_rsl_string.AddCell(v_f05);
      it_rsl_string.AddCell(v_f06);
      if v_f07 = 'X'
      then
        it_rsl_string.AddCell('Классификатор блокировки');
      else
        it_rsl_string.AddCell(' ');
      end if;
      it_rsl_string.AddCell(v_f08, true);
    end loop;
    close cur_select;
    it_log.log('Stored');
  end;

  procedure CheckDataFutur_sf as
    cur_select    sys_refcursor;
    vc_cur_select varchar2(32000);
    v_f00         char(1);
    v_f01         char(1);
    v_f02         char(1);
    v_f03         ddl_limitfuturmark.t_volumemn%type;
    v_f04         ddl_limitfuturmark.t_account%type;
    v_f05         char(1);
    v_f10         DPARTY_DBT.T_NAME%type;
    v_start       boolean := true;
  begin
    it_rsl_string.clear;
    vc_cur_select := 'SELECT '' '',                       
                       '' '',                               
                       '' '',                               
                       lim.T_VOLUMEMN,                    
                       lim.T_ACCOUNT,                     
                       '' ''                                
                  FROM DDL_LIMITFUTURMARK lim             
                 WHERE NOT EXISTS                         
                          (SELECT 1                       
                             FROM DDL_LIMITFUTURMARK_dbt lt 
                            WHERE lim.T_ACCOUNT = lt.T_ACCOUNT)
                       and lim.T_VOLUMEMN != 0 ';
    it_log.log(p_msg => 'START ', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05;
      if v_start
      then
        v_start := false;
        it_log.log('Calculated');
      end if;
      exit when cur_select%notfound;
      it_rsl_string.AddCell('SPBFUT');
      it_rsl_string.AddCell(v_f00);
      it_rsl_string.AddCell(' ');
      it_rsl_string.AddCell(v_f01);
      it_rsl_string.AddCell(v_f02);
      it_rsl_string.AddCell(v_f03);
      it_rsl_string.AddCell(v_f04);
      it_rsl_string.AddCell(v_f05);
      it_rsl_string.AddCell(' ');
      it_rsl_string.AddCell(' ', true);
    end loop;
    close cur_select;
    it_log.log('Stored');
  end;

  procedure CheckDataFutur_qf as
    cur_select    sys_refcursor;
    vc_cur_select varchar2(32000);
    v_f00         ddl_limitfuturmark_dbt.t_client%type;
    v_f01         char(1);
    v_f02         char(1);
    v_f03         ddl_limitfuturmark_dbt.t_volumemn%type;
    v_f04         ddl_limitfuturmark_dbt.t_account%type;
    v_f05         char(1);
    v_f06         dparty_dbt.t_name%type;
    v_f07         ddlcontrmp_dbt.t_mpcode%type;
    v_start       boolean := true;
  begin
    it_rsl_string.clear;
    vc_cur_select := 'SELECT lim.t_client as f00,                        
                      '' '' as f01,                                    
                      '' '' as f02,                                    
                      lim.T_VOLUMEMN as f03,                         
                      lim.T_ACCOUNT as f04,                          
                      lim.T_ISBLOCKED as f05,                       
                      (SELECT party.t_name FROM dparty_dbt party WHERE party.t_partyid=lim.t_client ) as f06,
                      (SELECT mp.t_mpcode
                         FROM daccount_dbt a,
                              dmcaccdoc_dbt d,
                              ddlcontrmp_dbt mp,
                              dsfcontr_dbt sf
                        WHERE     mp.t_sfcontrid = sf.t_id
                              AND sf.t_servkind = 15
                              AND sf.t_id = D.T_CLIENTCONTRID
                              AND SF.T_PARTYID = d.t_owner
                              AND d.t_catid = 70
                              AND d.t_account = a.t_account
                              AND d.t_chapter = a.t_chapter
                              AND D.T_CURRENCY = A.T_CODE_CURRENCY
                              AND lim.T_CLIENT = sf.t_partyid
                              AND lim.T_INTERNALACCOUNT = a.t_accountid
                              AND rownum = 1) as f07
                 FROM DDL_LIMITFUTURMARK_DBT lim              
                WHERE NOT EXISTS                              
                         (SELECT 1                            
                            FROM DDL_LIMITFUTURMARK lt        
                           WHERE lim.T_ACCOUNT = lt.T_ACCOUNT)';
    it_log.log(p_msg => 'START ', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07;
      if v_start
      then
        v_start := false;
        it_log.log('Calculated');
      end if;
      exit when cur_select%notfound;
      it_rsl_string.AddCell('SPBFUT');
      it_rsl_string.AddCell(v_f07);
      it_rsl_string.AddCell(v_f06);
      it_rsl_string.AddCell(v_f01);
      it_rsl_string.AddCell(v_f02);
      it_rsl_string.AddCell(v_f03);
      it_rsl_string.AddCell(v_f04);
      if v_f05 = 'X'
      then
        it_rsl_string.AddCell('Классификатор блокировки');
      else
        it_rsl_string.AddCell(' ');
      end if;
      it_rsl_string.AddCell(' ');
      it_rsl_string.AddCell(' ', true);
    end loop;
    close cur_select;
    it_log.log('Stored');
  end;

  procedure CheckDataFutur_if as
    cur_select    sys_refcursor;
    vc_cur_select varchar2(32000);
    v_f00         ddl_limitfuturmark_dbt.t_client%type;
    v_f01         char(1);
    v_f02         char(1);
    v_f03         ddl_limitfuturmark_dbt.t_volumemn%type;
    v_f04         ddl_limitfuturmark.t_volumemn%type;
    v_f05         ddl_limitfuturmark_dbt.t_account%type;
    v_f06         ddl_limitfuturmark.t_account%type;
    v_f07         char(1);
    v_f08         DPARTY_DBT.T_NAME%type;
    v_f09         ddlcontrmp_dbt.t_mpcode%type;
    v_start       boolean := true;
  begin
    it_rsl_string.clear;
    vc_cur_select := 'SELECT lim.t_client as f00,                                        
                '' '' as f01,                                                              
                '' '' as f02,                                                              
                lim.T_VOLUMEMN as f03,                                                   
                qlim.T_VOLUMEMN as f04,                                                  
                lim.T_ACCOUNT as f05,                                                    
                qlim.t_account as f06,                                                   
                lim.T_ISBLOCKED as f07,                                                 
                (SELECT party.t_name FROM dparty_dbt party WHERE party.t_partyid=lim.t_client ) as f08,
                (SELECT mp.t_mpcode
                   FROM daccount_dbt a,
                        dmcaccdoc_dbt d,
                        ddlcontrmp_dbt mp,
                        dsfcontr_dbt sf
                  WHERE     mp.t_sfcontrid = sf.t_id
                        AND sf.t_servkind = 15
                        AND sf.t_id = D.T_CLIENTCONTRID
                        AND SF.T_PARTYID = d.t_owner
                        AND d.t_catid = 70
                        AND d.t_account = a.t_account
                        AND d.t_chapter = a.t_chapter
                        AND D.T_CURRENCY = A.T_CODE_CURRENCY
                        AND lim.T_CLIENT = sf.t_partyid
                        AND lim.T_INTERNALACCOUNT = a.t_accountid
                        AND rownum = 1) as f09
           FROM    (SELECT t_client,                                              
                           T_ACCOUNT,                                             
                           T_VOLUMEMN,                                            
                           t_isblocked,
                           T_INTERNALACCOUNT
                      FROM DDL_LIMITFUTURMARK_DBT) lim                            
                LEFT JOIN                                                         
                   DDL_LIMITFUTURMARK qlim                                        
                ON lim.T_ACCOUNT = qlim.T_ACCOUNT AND lim.T_VOLUMEMN = QLIM.T_VOLUMEMN  
          WHERE QLIM.T_VOLUMEMN IS NOT NULL ';
    it_log.log(p_msg => 'START ', p_msg_clob => vc_cur_select);
    v_start := true;
    open cur_select for vc_cur_select;
    loop
      fetch cur_select
        into v_f00
            ,v_f01
            ,v_f02
            ,v_f03
            ,v_f04
            ,v_f05
            ,v_f06
            ,v_f07
            ,v_f08
            ,v_f09;
      if v_start
      then
        v_start := false;
        it_log.log('Calculated');
      end if;
      exit when cur_select%notfound;
      it_rsl_string.AddCell('SPBFUT');
      it_rsl_string.AddCell(v_f09);
      it_rsl_string.AddCell(v_f08);
      it_rsl_string.AddCell(v_f01);
      it_rsl_string.AddCell(v_f02);
      it_rsl_string.AddCell(v_f03);
      it_rsl_string.AddCell(v_f04);
      it_rsl_string.AddCell(v_f05);
      it_rsl_string.AddCell(v_f06);
      if v_f07 = 'X'
      then
        it_rsl_string.AddCell('Классификатор блокировки', true);
      else
        it_rsl_string.AddCell(' ', true);
      end if;
    end loop;
    close cur_select;
    it_log.log('Stored');
  end;

  function GetParm(p_record varchar2
                  ,p_parm   varchar2
                  ,p_zndef  varchar2 default chr(1)) return varchar2 as
    v_record varchar2(32000) := translate(p_record, 'A ' || chr(13), 'A');
    v_parm   varchar2(2000) := upper(p_parm);
    v_pos    integer;
    v_retval varchar2(2000);
  begin
    v_pos := instr(v_record, v_parm || '=');
    if v_pos > 0
    then
      v_pos    := v_pos + length(v_parm) + 1;
      v_retval := SubStr(v_record, v_pos, instr(v_record, ';', v_pos) - v_pos);
      v_retval := translate(v_retval, ',', '.');
    end if;
    return nvl(v_retval, p_zndef);
  end;

  -- Возвращает кол-во строк при обработке уже загруженных данных
  function LoadDataGetLineCount(p_FILE_CODE itt_file.file_code%type) return number as
    vx_param   xmltype;
    vs_param   itt_file.note%type;
    v_precount integer;
  begin
    begin
      select f.note into vs_param from itt_file f where f.file_code = p_FILE_CODE;
    exception
      when no_data_found then
        vs_param := null;
    end;
    if vs_param is not null
    then
      vx_param := xmltype(vs_param);
      select EXTRACTVALUE(vx_param, '/XML/@LineCount') into v_precount from dual;
    else
      v_precount := 0;
    end if;
    return v_precount;
  end;

  -- Сохраняет метаданные файла (пока кол-во строк  при обработке данных )  возвращает XML
  function LoadDataSetMeta(p_FILE_CODE itt_file.file_code%type
                          ,p_LineCount number) return xmltype as
    vx_param  xmltype;
    v_id_file integer;
  begin
    select xmlelement("XML", xmlattributes(p_LineCount as "LineCount")) into vx_param from dual;
    delete itt_file f where f.file_code = p_FILE_CODE; -- Удаляем перед добавлением новой информации 
    v_id_file := it_file.insert_file(p_from_system => it_file.C_QUIK
                                    ,p_from_module => $$plsql_unit
                                    ,p_to_system => it_file.C_SOFR_DB
                                    ,p_to_module => null
                                    ,p_file_code => p_FILE_CODE
                                    ,p_note => vx_param.getStringVal);
    return vx_param;
  end;

  -- Загрузка данных QUIK возвращает кол-во строк с ошибками 0 - ок
  function LoadDataFutur(p_DataLimit clob
                        ,p_trunc_t   number default 1
                        ,o_mess      out varchar2) return number as
    v_pos   number := 1;
    v_dfstr varchar2(32676);
    C_DFLEN constant integer := 32676;
    v_dflen    integer;
    v_record   varchar2(32676);
    v_poschr10 integer;
    v_postmp   integer;
    v_last     boolean := false;
    type FUTUR_t is table of DDL_LIMITFUTURMARK%rowtype index by pls_integer;
    t_FUTUR FUTUR_t;
    v_bk_cnt constant integer := 50000;
    v_bk_nFUTUR  integer := 0;
    v_cnt_record integer := 0;
    v_ret        integer := 0;
    v_add_record integer := 0;
    vx_param     xmltype;
    v_precount   integer;
    c_file_code constant itt_file.file_code%type := it_file.C_FILE_CODE_QUIK_FUTUR;
  begin
    if nvl(dbms_lob.getlength(p_DataLimit), 0) = 0
    then
      o_mess := 'Нет данных';
      return - 1;
    end if;
    if nvl(p_trunc_t, 1) = 1
    then
      execute immediate 'truncate table DDL_LIMITFUTURMARK';
      delete itt_file f where f.file_code = c_file_code; -- Новая загрузка 
      v_precount := 0;
    else
      v_precount := LoadDataGetLineCount(c_file_code);
    end if;
    v_cnt_record := v_precount;
    loop
      v_dflen := C_DFLEN;
      dbms_lob.read(lob_loc => p_DataLimit, amount => v_dflen, offset => v_pos, buffer => v_dfstr);
      v_last     := nvl(v_dflen, 0) < C_DFLEN;
      v_poschr10 := 1;
      loop
        v_postmp := instr(v_dfstr, chr(10), v_poschr10);
        exit when nvl(v_postmp, 0) = 0 and not v_last;
        if v_postmp = 0
        then
          v_record := substr(v_dfstr, v_poschr10);
        else
          v_record   := substr(v_dfstr, v_poschr10, v_postmp - v_poschr10);
          v_poschr10 := v_postmp + 1;
        end if;
        v_record     := trim(translate(v_record, 'A' || chr(13) || chr(10), 'A'));
        v_cnt_record := v_cnt_record + 1;
        if v_record is not null
           and upper(substr(v_record, 1, 10)) = 'FUT_MONEY:'
        then
          v_bk_nFUTUR := v_bk_nFUTUR + 1;
          begin
            t_FUTUR(v_bk_nFUTUR).t_class_code := GetParm(v_record, 'CLASS_CODE');
            t_FUTUR(v_bk_nFUTUR).t_firm_id := GetParm(v_record, 'FIRM_ID');
            t_FUTUR(v_bk_nFUTUR).t_account := GetParm(v_record, 'ACCOUNT');
            t_FUTUR(v_bk_nFUTUR).t_volumemn := it_xml.char_to_number(GetParm(v_record, 'VOLUMEMN', '0'));
            t_FUTUR(v_bk_nFUTUR).t_volumepl := it_xml.char_to_number(GetParm(v_record, 'VOLUMEPL', '0'));
            t_FUTUR(v_bk_nFUTUR).t_kfl := GetParm(v_record, 'KFL', '0');
            t_FUTUR(v_bk_nFUTUR).t_kgo := GetParm(v_record, 'KGO');
            t_FUTUR(v_bk_nFUTUR).t_use_kgo := GetParm(v_record, 'USE_KGO');
          exception
            when others then
              if nvl(length(o_mess), 0) < 500
              then
                o_mess := o_mess || 'Стр.№ ' || v_cnt_record || ' Ошибка преобразования данных' || chr(10);
              end if;
              v_ret       := v_ret + 1;
              v_bk_nFUTUR := v_bk_nFUTUR - 1;
          end;
          if v_bk_nFUTUR >= v_bk_cnt
          then
            v_add_record := v_add_record + t_FUTUR.count;
            forall indx in t_FUTUR.first .. t_FUTUR.last
              insert into DDL_LIMITFUTURMARK
              values
                (t_FUTUR(indx).t_class_code
                ,t_FUTUR(indx).t_firm_id
                ,t_FUTUR(indx).t_account
                ,t_FUTUR(indx).t_volumemn
                ,t_FUTUR(indx).t_volumepl
                ,t_FUTUR(indx).t_kfl
                ,t_FUTUR(indx).t_kgo
                ,t_FUTUR(indx).t_use_kgo);
            t_FUTUR.delete;
            v_bk_nFUTUR := 0;
          end if;
        elsif v_record is not null
              and upper(substr(v_record, 1, 9)) = 'FUT_DEPO:'
        then
          null;
        elsif v_record is not null
        then
          if nvl(length(o_mess), 0) < 500
          then
            o_mess := o_mess || 'Стр.№' || v_cnt_record || ' Ошибка данных "' || substr(v_record, 1, 10) || '..."' || chr(10);
          end if;
          v_ret := v_ret + 1;
        end if;
        exit when v_postmp = 0;
      end loop;
      exit when v_last;
      v_pos := v_pos + v_poschr10 - 1;
    end loop;
    v_add_record := v_add_record + t_FUTUR.count;
    forall indx in t_FUTUR.first .. t_FUTUR.last
      insert into DDL_LIMITFUTURMARK
      values
        (t_FUTUR(indx).t_class_code
        ,t_FUTUR(indx).t_firm_id
        ,t_FUTUR(indx).t_account
        ,t_FUTUR(indx).t_volumemn
        ,t_FUTUR(indx).t_volumepl
        ,t_FUTUR(indx).t_kfl
        ,t_FUTUR(indx).t_kgo
        ,t_FUTUR(indx).t_use_kgo);
    /*     while ( next (Quik) )
       if (substr(Quik(0), 1, 8) != "FUT_DEPO") 
          CLASS_CODE = GetParm(Quik.str,"CLASS_CODE");
          FIRM_ID    = GetParm(Quik.str,"FIRM_ID");
          ACCOUNT    = GetParm(Quik.str,"ACCOUNT");
          VOLUMEMN   = GetParm(Quik.str,"VOLUMEMN","0");
          VOLUMEPL   = GetParm(Quik.str,"VOLUMEPL","0");
          KFL        = GetParm(Quik.str,"KFL", "0");
          KGO        = GetParm(Quik.str,"KGO");
          USE_KGO    = GetParm(Quik.str,"USE_KGO");
          execSql ("insert into DDL_LIMITFUTURMARK values (:0, :1, :2,TO_NUMBER(:3), TO_NUMBER(:4), :5, :6, :7)",
          makeArray (sqlParam ("0", CLASS_CODE), sqlParam ("1", FIRM_ID), sqlParam ("2", ACCOUNT), sqlParam ("3", VOLUMEMN), 
                     sqlParam ("4", VOLUMEPL),   sqlParam ("5", KFL), sqlParam ("6", KGO), sqlParam ("7", USE_KGO)));
       end;
    end;   */
    vx_param   := LoadDataSetMeta(c_file_code, v_cnt_record);
    commit;
    if v_add_record = 0
       and v_precount = 0
    then
      o_mess := o_mess || 'Обработано строк: ' || v_cnt_record || ' НЕТ ЗАПИСЕЙ для загрузки';
      v_ret  := -1;
    else
      o_mess := o_mess || 'Создано записей: ' || v_add_record || '  отбраковано строк: ' || v_ret || chr(10);
    end if;
    it_log.log(p_msg => o_mess || vx_param.getStringVal);
    return v_ret;
  exception
    when others then
      rollback;
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg => 'Обработано строк :' || v_cnt_record, p_msg_clob => p_DataLimit);
      raise;
  end;

  function LoadDataSecur(p_DataLimit clob
                        ,p_trunc_t   number default 1
                        ,o_mess      out varchar2) return number as
    v_pos   number := 1;
    v_dfstr varchar2(32676);
    C_DFLEN constant integer := 32676;
    v_dflen    integer;
    v_record   varchar2(32676);
    v_poschr10 integer;
    v_postmp   integer;
    v_last     boolean := false;
    type MONEY_t is table of DDL_LIMITCASHSTOCK%rowtype index by pls_integer;
    type DEPO_t is table of DDL_LIMITSECURITES%rowtype index by pls_integer;
    t_MONEY MONEY_t;
    t_DEPO  DEPO_t;
    v_bk_cnt constant integer := 50000;
    v_bk_nMONEY  integer := 0;
    v_bk_nDEPO   integer := 0;
    v_cnt_record integer := 0;
    v_ret        integer := 0;
    v_add_record integer := 0;
    vx_param     xmltype;
    v_precount   integer;
    c_file_code constant itt_file.file_code%type := it_file.C_FILE_CODE_QUIK_SECUR;
  begin
    if nvl(dbms_lob.getlength(p_DataLimit), 0) = 0
    then
      o_mess := 'Нет данных';
      return - 1;
    end if;
    if nvl(p_trunc_t, 1) = 1
    then
      execute immediate 'truncate table DDL_LIMITSECURITES';
      execute immediate 'truncate table DDL_LIMITCASHSTOCK';
      delete itt_file f where f.file_code = c_file_code; -- Новая загрузка 
      v_precount := 0;
    else
       v_precount := LoadDataGetLineCount(c_file_code);
    end if;
    v_cnt_record := v_precount ; 
    loop
      v_dflen := C_DFLEN;
      dbms_lob.read(lob_loc => p_DataLimit, amount => v_dflen, offset => v_pos, buffer => v_dfstr);
      v_last     := nvl(v_dflen, 0) < C_DFLEN;
      v_poschr10 := 1;
      loop
        v_postmp := instr(v_dfstr, chr(10), v_poschr10);
        exit when nvl(v_postmp, 0) = 0 and not v_last;
        if v_postmp = 0
        then
          v_record := substr(v_dfstr, v_poschr10);
        else
          v_record   := substr(v_dfstr, v_poschr10, v_postmp - v_poschr10);
          v_poschr10 := v_postmp + 1;
        end if;
        v_record     := trim(translate(v_record, 'A' || chr(13) || chr(10), 'A'));
        v_cnt_record := v_cnt_record + 1;
        if v_record is not null
           and upper(substr(v_record, 1, 6)) = 'MONEY:'
        then
          v_bk_nMONEY := v_bk_nMONEY + 1;
          begin
            t_MONEY(v_bk_nMONEY).t_firm_id := GetParm(v_record, 'FIRM_ID');
            t_MONEY(v_bk_nMONEY).t_tag := GetParm(v_record, 'TAG');
            t_MONEY(v_bk_nMONEY).t_curr_code := GetParm(v_record, 'CURR_CODE');
            t_MONEY(v_bk_nMONEY).t_client_code := GetParm(v_record, 'CLIENT_CODE');
            t_MONEY(v_bk_nMONEY).t_limit_kind := it_xml.char_to_number(GetParm(v_record, 'LIMIT_KIND'));
            t_MONEY(v_bk_nMONEY).t_open_balance := it_xml.char_to_number(GetParm(v_record, 'OPEN_BALANCE'));
            t_MONEY(v_bk_nMONEY).t_open_limit := it_xml.char_to_number(GetParm(v_record, 'OPEN_LIMIT'));
            t_MONEY(v_bk_nMONEY).t_leverage := it_xml.char_to_number(GetParm(v_record, 'LEVERAGE', '0'));
          exception
            when others then
              if nvl(length(o_mess), 0) < 500
              then
                o_mess := o_mess || 'Стр.№' || v_cnt_record || ' Ошибка преобразования данных' || chr(10);
              end if;
              v_ret       := v_ret + 1;
              v_bk_nMONEY := v_bk_nMONEY - 1;
          end;
          if v_bk_nMONEY >= v_bk_cnt
          then
            v_add_record := v_add_record + t_MONEY.count;
            forall indx in t_MONEY.first .. t_MONEY.last
              insert into DDL_LIMITCASHSTOCK
              values
                (t_MONEY(indx).t_firm_id
                ,t_MONEY(indx).t_tag
                ,t_MONEY(indx).t_curr_code
                ,t_MONEY(indx).t_client_code
                ,t_MONEY(indx).t_limit_kind
                ,t_MONEY(indx).t_open_balance
                ,t_MONEY(indx).t_open_limit
                ,t_MONEY(indx).t_leverage);
            t_MONEY.delete;
            v_bk_nMONEY := 0;
          end if;
        elsif v_record is not null
              and upper(substr(v_record, 1, 5)) = 'DEPO:'
        then
          v_bk_nDEPO := v_bk_nDEPO + 1;
          begin
            t_DEPO(v_bk_nDEPO).t_firm_id := GetParm(v_record, 'FIRM_ID');
            t_DEPO(v_bk_nDEPO).t_seccode := GetParm(v_record, 'SECCODE');
            t_DEPO(v_bk_nDEPO).t_client_code := GetParm(v_record, 'CLIENT_CODE');
            t_DEPO(v_bk_nDEPO).t_limit_kind := it_xml.char_to_number(GetParm(v_record, 'LIMIT_KIND'));
            t_DEPO(v_bk_nDEPO).t_open_balance := it_xml.char_to_number(GetParm(v_record, 'OPEN_BALANCE'));
            t_DEPO(v_bk_nDEPO).t_open_limit := it_xml.char_to_number(GetParm(v_record, 'OPEN_LIMIT'));
            t_DEPO(v_bk_nDEPO).t_trdaccid := GetParm(v_record, 'TRDACCID');
            t_DEPO(v_bk_nDEPO).t_wa_position_price := it_xml.char_to_number(GetParm(v_record, 'WA_POSITION_PRICE', '0.0'));
          exception
            when others then
              if nvl(length(o_mess), 0) < 500
              then
                o_mess := o_mess || 'Стр.№' || v_cnt_record || ' Ошибка преобразования данных' || chr(10);
              end if;
              v_ret      := v_ret + 1;
              v_bk_nDEPO := v_bk_nDEPO - 1;
          end;
          if v_bk_nDEPO >= v_bk_cnt
          then
            v_add_record := v_add_record + t_DEPO.count;
            forall indx in t_DEPO.first .. t_DEPO.last
              insert into DDL_LIMITSECURITES
              values
                (t_DEPO(indx).t_firm_id
                ,t_DEPO(indx).t_seccode
                ,t_DEPO(indx).t_client_code
                ,t_DEPO(indx).t_limit_kind
                ,t_DEPO(indx).t_open_balance
                ,t_DEPO(indx).t_open_limit
                ,t_DEPO(indx).t_trdaccid
                ,t_DEPO(indx).t_wa_position_price);
            t_DEPO.delete;
            v_bk_nDEPO := 0;
          end if;
        elsif v_record is not null
        then
          if nvl(length(o_mess), 0) < 500
          then
            o_mess := o_mess || 'Стр.№' || v_cnt_record || ' Ошибка данных "' || substr(v_record, 1, 10) || '..."' || chr(10);
          end if;
          v_ret := v_ret + 1;
        end if;
        exit when v_postmp = 0;
      end loop;
      exit when v_last;
      v_pos := v_pos + v_poschr10 - 1;
    end loop;
    v_add_record := v_add_record + t_MONEY.count;
    forall indx in t_MONEY.first .. t_MONEY.last
      insert into DDL_LIMITCASHSTOCK
      values
        (t_MONEY(indx).t_firm_id
        ,t_MONEY(indx).t_tag
        ,t_MONEY(indx).t_curr_code
        ,t_MONEY(indx).t_client_code
        ,t_MONEY(indx).t_limit_kind
        ,t_MONEY(indx).t_open_balance
        ,t_MONEY(indx).t_open_limit
        ,t_MONEY(indx).t_leverage);
    v_add_record := v_add_record + t_DEPO.count;
    forall indx in t_DEPO.first .. t_DEPO.last
      insert into DDL_LIMITSECURITES
      values
        (t_DEPO(indx).t_firm_id
        ,t_DEPO(indx).t_seccode
        ,t_DEPO(indx).t_client_code
        ,t_DEPO(indx).t_limit_kind
        ,t_DEPO(indx).t_open_balance
        ,t_DEPO(indx).t_open_limit
        ,t_DEPO(indx).t_trdaccid
        ,t_DEPO(indx).t_wa_position_price);
    /*while ( next (Quik) )
      // quik.str = StrSubst(Quik.str, " ","");
       if (substr(Quik(0), 1, 5) == "MONEY")
          //debugbreak;
          FIRM_ID      = GetParm(Quik.str,"FIRM_ID");
          TAG          = GetParm(Quik.str,"TAG");
          CURR_CODE    = GetParm(Quik.str,"CURR_CODE");
          CLIENT_CODE  = GetParm(Quik.str,"CLIENT_CODE");
          LIMIT_KIND   = GetParm(Quik.str,"LIMIT_KIND");
          OPEN_BALANCE = GetParm(Quik.str,"OPEN_BALANCE");
          OPEN_LIMIT   = GetParm(Quik.str,"OPEN_LIMIT");
          LEVERAGE     = GetParm(Quik.str,"LEVERAGE", "0");
    
          execSql ("insert into DDL_LIMITCASHSTOCK values (:0, :1, :2, :3, TO_NUMBER(:4), TO_NUMBER(:5), TO_NUMBER(:6), TO_NUMBER(:7))",
          makeArray (sqlParam ("0", FIRM_ID), sqlParam ("1", TAG), sqlParam ("2", CURR_CODE), sqlParam ("3", CLIENT_CODE), 
          sqlParam ("4", LIMIT_KIND),   sqlParam ("5", OPEN_BALANCE), sqlParam ("6", OPEN_LIMIT), sqlParam ("7", LEVERAGE)));
       elif (substr(Quik(0), 1, 4) == "DEPO")
          FIRM_ID      = GetParm(Quik.str,"FIRM_ID");
          SECCODE      = GetParm(Quik.str,"SECCODE");
          CLIENT_CODE  = GetParm(Quik.str,"CLIENT_CODE");
          LIMIT_KIND   = GetParm(Quik.str,"LIMIT_KIND");
          OPEN_BALANCE = GetParm(Quik.str,"OPEN_BALANCE");
          OPEN_LIMIT   = GetParm(Quik.str,"OPEN_LIMIT");
          TRDACCID     = GetParm(Quik.str,"TRDACCID");
          WA_POSITION_PRICE     = GetParm(Quik.str,"WA_POSITION_PRICE", "0.0");
    
          execSql ("insert into DDL_LIMITSECURITES values  (:0, :1, :2,TO_NUMBER(:3), TO_NUMBER(:4), TO_NUMBER(:5), :6, TO_NUMBER(:7))",
          makeArray (sqlParam ("0", FIRM_ID), sqlParam ("1", SECCODE), sqlParam ("2", CLIENT_CODE), sqlParam ("3", LIMIT_KIND), 
          sqlParam ("4", OPEN_BALANCE),   sqlParam ("5", OPEN_LIMIT), sqlParam ("6", TRDACCID), sqlParam ("7", WA_POSITION_PRICE)));
       end;
       UseProgress(count=count+1);
    end;*/
    vx_param   := LoadDataSetMeta(c_file_code, v_cnt_record);
    commit;
    if v_add_record = 0
       and v_precount = 0
    then
      o_mess := o_mess || 'Обработано строк: ' || v_cnt_record || ' НЕТ ЗАПИСЕЙ для загрузки';
      v_ret  := -1;
    else
      o_mess := o_mess || 'Создано записей: ' || v_add_record || '  отбраковано строк: ' || v_ret || chr(10);
    end if;
    it_log.log(p_msg => o_mess || vx_param.getStringVal);
    return v_ret;
  exception
    when others then
      rollback;
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg => 'Обработано строк :' || v_cnt_record, p_msg_clob => p_DataLimit);
      raise;
  end;
  
  -- Возвращаем  время когда операция зачисления могла попасть в расчет лимитов 
  function GetDT306Limit_dy_nptxop(p_nptxop_id dnptxop_dbt.t_id%type) return date as
    v_tost date;
  begin
    with acc_prov as
     (select distinct prov.t_accountid_receiver
        from doproper_dbt op
       inner join doprdocs_dbt priv
          on op.t_id_operation = priv.t_id_operation
       inner join dacctrn_dbt prov
          on priv.t_acctrnid = prov.t_acctrnid
         and prov.t_chapter = 1
         and prov.t_State = 1
      where op.t_dockind = 4607
         and op.t_kind_operation = 2037
         and op.t_documentid = lpad(p_nptxop_id, 34, '0')
      )
    select NVL(max(t_time), to_date('01/01/0001', 'dd/mm/yyyy'))
      into v_tost
      from (select lcs.t_time
              from ddl_limitcashstock_dbt lcs
             where lcs.t_internalaccount in (select t_accountid_receiver from acc_prov)
            union
            select lfm.t_time
              from ddl_limitfuturmark_dbt lfm
             where lfm.t_internalaccount in (select t_accountid_receiver from acc_prov));
    return v_tost;
  exception
    when no_data_found then
      return null;
  end;


end rshb_limit_util;
/

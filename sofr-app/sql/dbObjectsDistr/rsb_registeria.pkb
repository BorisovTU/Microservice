CREATE OR REPLACE PACKAGE BODY rsb_dlregisteria IS


   PROCEDURE GenCalcIds (SessionID OUT NUMBER, CalcID OUT NUMBER)
   AS
   BEGIN
      SessionID := rsb_dlutils.GETSESSIONID;
      CalcID := DDL_REGIA_LOG_SEQ.NEXTVAL;
      V_SESSIONID := SessionID;
      V_CALCID := CalcID;
   END;

   FUNCTION GenLinkId return NUMBER
   AS
   BEGIN
      return DDL_REGIA_LOG_SEQ.NEXTVAL;
   END;

   FUNCTION to_chardatesql(p_Date date) return varchar2 as
   BEGIN
     RETURN ' to_date('''||to_char(p_Date,'ddmmyyyy')||''',''ddmmyyyy'')';
   END;

   PROCEDURE SetCalcIds (SessionID IN NUMBER, CalcID IN NUMBER, LinkID IN NUMBER DEFAULT NULL)
   AS
   BEGIN
      V_SESSIONID := SessionID;
      V_CALCID := CalcID;
      V_LINKID := LinkID;
   END;

  procedure table_add_partition as
  begin
    execute immediate 'alter table DDL_REGIABUF_DBT add partition p'|| to_char(sysdate, 'yyyymmdd')||'#'|| V_SESSIONID || ' values (' ||V_SESSIONID || ','||V_CALCID||')';
  end;

  procedure table_clear_partition as
  begin
    for tab in (select p.partition_name
                  from user_tab_partitions p
                 where p.table_name = 'DDL_REGIABUF_DBT'
                   and (p.partition_name like '%#'|| V_SESSIONID 
                       or substr(p.partition_name, 2, 8) < to_char(sysdate - 3, 'yyyymmdd')))
    loop
      begin
        execute immediate 'alter table DDL_REGIABUF_DBT drop partition ' || tab.partition_name;
      exception
        when others then
          null;
      end;
    end loop;
  end;

   
   
   PROCEDURE DropBufData(p_start integer default 0)
   AS
   pragma autonomous_transaction ;
   BEGIN
      IF V_SESSIONID is not null and V_CALCID is not null THEN
         table_clear_partition ;
         if p_start != 0 then
          table_add_partition ;
         end if;
      END IF;
      DELETE ITT_FILE f WHERE (f.sessionid = V_SESSIONID or f.create_sysdate < sysdate-3) and f.file_code = it_file.get_constant_str('C_FILE_CODE_REP_NREG');
      commit;
   END;

   PROCEDURE SetIdsDropBufData(p_start integer , SessionID IN NUMBER, CalcID IN NUMBER)
   AS
   BEGIN
      SetCalcIds(SessionID,CalcID);
      DropBufData(p_start);
   END;

   PROCEDURE PushLogLine (LogMessage           IN VARCHAR2,
                          ProgressMessage      IN VARCHAR2 DEFAULT NULL,
                          ActionID             IN NUMBER DEFAULT NULL,
                          LinkID               IN NUMBER DEFAULT NULL
                        )
   AS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN

      IF V_SESSIONID is not null and V_CALCID is not null THEN

        INSERT INTO DDL_REGIA_LOG_DBT (
                                       T_SESSIONID,
                                       T_CALCID,
                                       T_LOGMESSAGE,
                                       T_PROGRESSMESSAGE,
                                       T_ACTIONID,
                                       T_TIMESTAMP,
                                       T_LINKID
                                      )
             VALUES (
                       V_SESSIONID,
                       V_CALCID,
                       CASE WHEN LogMessage is not null then substr(LogMessage,1,500) ELSE null END,
                       CASE WHEN ProgressMessage is not null then substr(ProgressMessage,1,500) ELSE null END,
                       ActionID,
                       SYSTIMESTAMP,
                       LinkID);
      END IF;

      COMMIT;
   END;

  function GetLastProgressMessage return VARCHAR2
  IS
    p_retText VARCHAR2(500);
  BEGIN
      IF V_SESSIONID is not null and V_CALCID is not null THEN
            SELECT T_PROGRESSMESSAGE INTO p_retText
              FROM DDL_REGIA_LOG_DBT
             WHERE T_SESSIONID = V_SESSIONID AND T_CALCID = V_CALCID and T_PROGRESSMESSAGE is not null
          ORDER BY T_TIMESTAMP DESC
           FETCH NEXT 1 ROWS ONLY;
      END IF;

      RETURN p_retText;
  EXCEPTION
    WHEN OTHERS THEN RETURN NULL;
  END;

  function GetLastParallProcId(SessionID IN NUMBER, CalcID IN NUMBER) return NUMBER
  IS
    p_retProcId NUMBER(20) := -1;
  BEGIN
        SELECT T_LINKID
          INTO p_retProcId
          FROM DDL_REGIA_LOG_DBT lg1
         WHERE     lg1.T_SESSIONID = SessionID
               AND lg1.T_CALCID = CalcID
               AND lg1.T_ACTIONID = ACTIONID_PARALLELSTARTED
               AND NOT EXISTS
                          (SELECT 1
                             FROM DDL_REGIA_LOG_DBT lg2
                            WHERE     lg2.T_SESSIONID = SessionID
                                  AND lg2.T_CALCID = CalcID
                                  AND lg2.T_ACTIONID = ACTIONID_PARALLELFINISHED
                                  AND lg2.T_LINKID = lg1.T_LINKID)
      ORDER BY T_TIMESTAMP DESC
         FETCH NEXT 1 ROWS ONLY;

      RETURN p_retProcId;
  EXCEPTION
    WHEN OTHERS THEN RETURN p_retProcId;
  END;

  function GetParallProcProgress(SessionID IN NUMBER, CalcID IN NUMBER, LinkID IN NUMBER) return NUMBER
  IS
      p_Progress NUMBER(10) := 0;
  BEGIN
      SELECT COUNT(*)
        INTO p_Progress
        FROM DDL_REGIA_LOG_DBT lg1
       WHERE     lg1.T_SESSIONID = SessionID
             AND lg1.T_CALCID = CalcID
             AND lg1.T_ACTIONID = ACTIONID_CHUNKFINISHED
             AND lg1.T_LINKID = LinkID;

      RETURN p_Progress;
  EXCEPTION
    WHEN OTHERS THEN RETURN p_Progress;
  END;


  -- отбор счетов по договорам
  PROCEDURE AddAccounts_Contr(BeginDate IN DATE, EndDate IN DATE) IS
  BEGIN
    PushLogLine('Отбор счетов по договорам - begin','Отбор счетов по договорам');
    INSERT /*+ parallel(8)  */ 
    INTO DDL_REGIABUF_DBT
      (T_SESSIONID,
       T_CALCID,
       t_part,
       t_clientcontrid,
       t_acctype,
       t_acccur,
       t_accrest,
       t_account,
       t_accountname,
       t_restprev)
      (select /*+ parallel(8)  */
        V_SESSIONID,
        V_CALCID,
        3,
        acc.t_clientcontrid,
        (case
          when acc.t_catid = CATID_FIN then
           ACC_TYPE_FIN
          when acc.t_catid = CATID_SEC then
           ACC_TYPE_SEC
          when acc.t_catid = CATID_FO then
           ACC_TYPE_FO
        end) t_acctype,
        nvl(fininstr.t_ccy, chr(1)) t_ccy,
        nvl(rsb_account.restall(acc.t_account,
                                acc.t_chapter,
                                acc.t_currency,
                                EndDate),
            0) t_rest,
        acc.t_account,
        nvl((select t_name
              from dmccateg_dbt
             where t_id = acc.t_catid
               and t_leveltype = 1),
            chr(1)) t_nameaccount,
         nvl(rsb_account.restall(acc.t_account,
                                acc.t_chapter,
                                acc.t_currency,
                                EndDate - 1),
            0) t_restprev
         from (select /*+ leading(reg,accdoc,mctemp) */ distinct accdoc.t_catid,
                               accdoc.t_currency,
                               accdoc.t_chapter,
                               accdoc.t_account,
                               reg.t_clientcontrid
                 from dmcaccdoc_dbt accdoc
                 join DDL_REGIABUF_DBT reg
                   on reg.t_part = 3
                  and reg.t_account is NULL
                 join DMCTEMPL_DBT mctemp on mctemp.t_catid = accdoc.t_catid
                where accdoc.t_clientcontrid = reg.t_clientcontrid
                  and accdoc.t_catid in (CATID_FIN, CATID_SEC, CATID_FO)
                  and (    mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7) 
                        OR mctemp.t_catid = CATID_SEC and mctemp.t_value1 = 1 
                        OR mctemp.t_catid = CATID_FO  and mctemp.t_value1 = 3 )
                  and reg.T_SESSIONID = V_SESSIONID
                  and reg.T_CALCID = V_CALCID
                  and (accdoc.t_disablingdate =
                      to_date('01.01.0001', 'dd.mm.yyyy') or
                      accdoc.t_disablingdate >= EndDate)) acc
     
         left join dfininstr_dbt fininstr
           on fininstr.t_fiid = acc.t_currency);

    PushLogLine('Отбор счетов по договорам - end');

  END AddAccounts_Contr;

  -- отбор счетов по договорам
  PROCEDURE AddAccounts_Contr_Parall(start_id in NUMBER, end_id NUMBER, BeginDate IN DATE, EndDate IN DATE) IS
  BEGIN
    PushLogLine('Отбор счетов по договорам parallel ('||TO_CHAR(RSB_DLUTILS.GETSESSIONID)||') (start '||TO_CHAR(start_id)||' end '||TO_CHAR(end_id)||') - begin', NULL, ACTIONID_CHUNKSTARTED, V_LINKID);
    INSERT
    INTO DDL_REGIABUF_DBT
      (T_SESSIONID,
       T_CALCID,
       t_part,
       t_clientcontrid,
       t_acctype,
       t_acccur,
       t_accrest,
       t_account,
       t_accountname)
      (select
        V_SESSIONID,
        V_CALCID,
        3,
        acc.t_clientcontrid,
        (case
          when acc.t_catid = CATID_FIN then
           ACC_TYPE_FIN
          when acc.t_catid = CATID_SEC then
           ACC_TYPE_SEC
          when acc.t_catid = CATID_FO then
           ACC_TYPE_FO
        end) t_acctype,
        nvl(fininstr.t_ccy, chr(1)) t_ccy,
        (select nvl(rsb_account.restall(acc.t_account,
                                acc.t_chapter,
                                acc.t_currency,
                                EndDate),0) from dual) t_rest,
        acc.t_account,
        account.t_nameaccount
         from (select /*+ leading(reg) index(reg DDL_REGIABUF_DBT_IDX3) use_nl(accdoc) */ distinct accdoc.t_catid,
                               accdoc.t_currency,
                               accdoc.t_chapter,
                               accdoc.t_account,
                               reg.t_clientcontrid
                 from dmcaccdoc_dbt accdoc
                 join DDL_REGIABUF_DBT reg
                   on accdoc.t_clientcontrid = reg.t_clientcontrid
                  and reg.t_sessionid = V_SESSIONID
                  and reg.t_calcid = V_CALCID
                  and reg.t_part = 3
                  and reg.t_clientcontrid between start_id and end_id
                  and reg.t_account is null
                 -- and reg.T_AUTOINC between start_id and end_id
                where accdoc.t_catid in (CATID_FIN, CATID_SEC, CATID_FO) 
                  and accdoc.t_iscommon=chr(88)
                  and (accdoc.t_disablingdate =
                      to_date('01.01.0001', 'dd.mm.yyyy') or
                      accdoc.t_disablingdate >= EndDate)) acc
             join daccount_dbt account
                on account.t_chapter = acc.t_chapter
               and account.t_account=acc.t_account
               and account.t_code_currency=acc.t_currency
         left join dfininstr_dbt fininstr
           on fininstr.t_fiid = acc.t_currency
           where  account.t_close_date =
                      to_date('01.01.0001', 'dd.mm.yyyy') or
                      account.t_close_date >= EndDate);

    PushLogLine('Отбор счетов по договорам parallel ('||TO_CHAR(RSB_DLUTILS.GETSESSIONID)||') (start '||TO_CHAR(start_id)||' end '||TO_CHAR(end_id)||')  - end',
                NULL, ACTIONID_CHUNKFINISHED, V_LINKID);

  END AddAccounts_Contr_Parall;

  PROCEDURE AddAccounts_Contr_Parall_Start(BeginDate IN DATE, EndDate IN DATE) IS
  BEGIN
     PushLogLine('Отбор счетов по договорам в многопотоке - begin', 'Отбор счетов по договорам в многопотоке', ACTIONID_PARALLELSTARTED);
     --RSB_DLUTILS.RunFunctionInParallel(
     it_parallel_exec.run_task_chunks_by_sql(p_parallel_level => 8,
                   p_sql_stmt      => 
                                      'begin rsb_dlregisteria.SetCalcIds('||TO_CHAR(V_SESSIONID)||', '||TO_CHAR(V_CALCID)||'); '||
                                       'rsb_dlregisteria.AddAccounts_Contr_Parall(:start_id, :end_id, '||to_chardatesql(BeginDate)||','||to_chardatesql(EndDate)||'); end;',
                   p_chunk_sql      => 
                                       ' select min(t_clientcontrid) start_id '||
                                            ' ,max(t_clientcontrid) end_id '||
                                        ' from (select t_clientcontrid '||
                                                      ' ,NTILE('||TO_CHAR(CHUNK_COUNT)||') over(order by t_clientcontrid) NT'||
                                                ' from (select distinct t_clientcontrid from DDL_REGIABUF_DBT reg '||
                                                ' where reg.t_part = 3 '||
                                                   ' and reg.t_account is null '||
                                                   ' and reg.t_sessionid = '||TO_CHAR(V_SESSIONID)||' and reg.t_calcid = '||TO_CHAR(V_CALCID)||')) group by nt',
                   p_comment        => 'Реест ВУ Отбор счетов по договорам'
                          );
                                       /*'SELECT CASE WHEN t_CurLevel = 1 THEN t_minId ELSE t_minId + t_plusMin + 1 END
                                                  AS start_id,
                                               CASE WHEN t_CurLevel = '||TO_CHAR(CHUNK_COUNT)||' THEN t_maxId ELSE t_minId + t_plusMax END
                                                  AS end_id
                                          FROM (    SELECT q2.*,
                                                           TRUNC (q2.t_count / '||TO_CHAR(CHUNK_COUNT)||' * (LEVEL - 1)) AS t_plusMin,
                                                           TRUNC (q2.t_count / '||TO_CHAR(CHUNK_COUNT)||' * (LEVEL)) AS t_plusMax,
                                                           LEVEL AS t_CurLevel
                                                      FROM (SELECT q1.*, t_maxId - t_minId AS t_count
                                                              FROM (SELECT MIN (T_AUTOINC) t_minId,
                                                                           MAX (T_AUTOINC) t_maxId
                                                                      FROM DDL_REGIABUF_DBT reg
                                                                     WHERE reg.t_part = 3 AND reg.t_account IS NULL AND reg.t_sessionid = '||TO_CHAR(V_SESSIONID)||' and reg.t_calcid = '||TO_CHAR(V_CALCID)||') q1) q2
                                                CONNECT BY LEVEL <= '||TO_CHAR(CHUNK_COUNT)||') q3
                                        '*/

      PushLogLine('Отбор счетов по договорам в многопотоке - end', NULL, ACTIONID_PARALLELFINISHED);
  END AddAccounts_Contr_Parall_Start;

  -- отбор счетов по сделкам
  PROCEDURE AddAccounts_AllDeals(BeginDate IN DATE, EndDate IN DATE) IS
  BEGIN
    PushLogLine('Отбор счетов по сделкам - begin','Отбор счетов по сделкам');
    INSERT /*+ parallel(8)  */
    INTO DDL_REGIABUF_DBT
      (T_SESSIONID,
       T_CALCID,
       t_part,
       t_clientcontrid,
       t_acctype,
       t_acccur,
       t_accrest,
       t_account,
       t_accountname)
      (select /*+ parallel(8)  */
        V_SESSIONID,
        V_CALCID,
        3,
        acc.t_clientcontrid,
        (case
          when acc.t_catid = CATID_FIN then
           ACC_TYPE_FIN
          when acc.t_catid = CATID_SEC then
           ACC_TYPE_SEC
          when acc.t_catid = CATID_FO then
           ACC_TYPE_FO
        end) t_acctype,
        nvl(fininstr.t_ccy, chr(1)) t_ccy,
        nvl(rsb_account.restall(acc.t_account,
                                acc.t_chapter,
                                acc.t_currency,
                                EndDate),
            0) t_rest,
        acc.t_account,
        nvl((select t_name
              from dmccateg_dbt
             where t_id = acc.t_catid
               and t_leveltype = 1),
            chr(1)) t_nameaccount
         from (select /*+  leading(reg) use_nl(accdoc) */ distinct  accdoc.t_catid,
                               accdoc.t_currency,
                               accdoc.t_chapter,
                               accdoc.t_account,
                               reg.t_clientcontrid
                 from dmcaccdoc_dbt accdoc
                 join DDL_REGIABUF_DBT reg
                   on ACCDOC.T_DOCKIND = reg.t_dockind
                  and ACCDOC.T_DOCID = reg.t_dealid
                  and accdoc.t_clientcontrid = reg.t_clientcontrid
                where accdoc.t_catid in (CATID_FIN, CATID_SEC, CATID_FO)
                  and reg.T_SESSIONID = V_SESSIONID
                  and reg.T_CALCID = V_CALCID
                  and reg.t_part in (1,2)
                  and (accdoc.t_disablingdate =
                      to_date('01.01.0001', 'dd.mm.yyyy') or
                      accdoc.t_disablingdate >= EndDate)) acc

         left join dfininstr_dbt fininstr
           on fininstr.t_fiid = acc.t_currency);

    PushLogLine('Отбор счетов по сделкам - end');

  END AddAccounts_AllDeals;


  -- отбор счетов ЦБ по клиентским сделкам
  PROCEDURE AddSCAccountsP(BeginDate IN DATE, EndDate IN DATE) IS
  BEGIN
    PushLogLine('Отбор счетов ЦБ по клиентским сделкам - begin','Отбор счетов ЦБ по клиентским сделкам');
    INSERT /*+ parallel(8)  */
    INTO  DDL_REGIABUF_DBT
      (T_SESSIONID,
       T_CALCID,
       t_part,
       t_dealid,
       t_dockind,
       t_dealsubkind,
       t_dealpart,
       t_clientcontrid,
       t_account,
       t_accountname,
       t_acctype,
       t_accrest,
       t_credit,
       t_debet,
       t_outstandclaims,
       t_outstandobl,
       t_restprev)
      (select /*+ parallel(8)  */
        V_SESSIONID,
        V_CALCID,
        1 t_part,
        rest.t_dealid,
        rest.t_dockind,
        1 as t_dealsubkind,
        rest.t_dealpart,
        rest.t_clientcontrid,
        accdoc.t_account,
        account.t_nameaccount,
        ACC_TYPE_SEC,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                accdoc.t_currency,
                                EndDate),
            0) t_rest,
        rest.t_turnc,
        rest.t_turnd,
        rest.t_planturnc,
        rest.t_planturnd,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                accdoc.t_currency,
                                EndDate - 1),
            0) t_restprev
         from (select /*+ leading(rest) use_hash(rest,accdoc2) index(accdoc2 DMCACCDOC_DBT_IDX2) */ rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_pfi,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc,
                      max(accdoc2.t_id) accdoc_id
               from (select rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_pfi,
                      rest.t_clientcontrid,
                      nvl(sum(case
                                when t_kind = 0 /*DLRQ_KIND_REQUEST*/
                                     and t_factdate !=
                                     to_date('01.01.0001', 'dd.mm.yyyy') then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_turnc,
                      nvl(sum(case
                                when t_kind = 1 /*DLRQ_KIND_COMMIT*/
                                     and t_factdate !=
                                     to_date('01.01.0001', 'dd.mm.yyyy') then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_turnd,
                      nvl(sum(case
                                when t_kind = 1 /*DLRQ_KIND_COMMIT*/
                                     and
                                     t_factdate = to_date('01.01.0001', 'dd.mm.yyyy') then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_planturnd,
                      nvl(sum(case
                                when t_kind = 0 /*DLRQ_KIND_REQUEST*/
                                     and
                                     t_factdate = to_date('01.01.0001', 'dd.mm.yyyy') then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_planturnc
                 from (select /*+ leading(reg,dl_tick)*/
                              (case 
                                when reg.t_invert_debet_credit = 1 then 
                                (case when dlrq.t_kind = 0 then 1 else 0 end)
                                else dlrq.t_kind                          
                              end) t_kind,
                              dlrq.t_factdate,
                              dlrq.t_plandate,
                              dlrq.t_amount,
                              reg.t_dealid,
                              reg.t_dockind,
                              reg.t_pfi,
                              reg.t_clientcontrid,
                              dlrq.t_dealpart
                         from ddl_tick_dbt dl_tick
                         join DDL_REGIABUF_DBT reg
                           on reg.t_part = 1
                          and reg.t_account is NULL
                          and reg.t_dealsubkind = 1
                         join ddl_leg_dbt dl_leg
                           on dl_leg.t_dealid = dl_tick.t_dealid
                          and dl_leg.t_legid = 0
                          and decode(dl_leg.t_legkind, 0, 1, 2) = reg.t_dealpart
                         join ddlrq_dbt dlrq
                           on dlrq.t_dockind = dl_tick.t_bofficekind
                          and dlrq.t_docid = dl_tick.t_dealid
                          and dlrq.t_dealpart =
                              decode(dl_leg.t_legkind, 0, 1, 2)
                          and dlrq.t_state != 7 /*отменено GAA:496245*/
                          and dlrq.t_subkind = 1 /*DLRQ_SUBKIND_AVOIRISS*/
                          and (dlrq.t_kind = 0 /*DLRQ_KIND_REQUEST*/
                              or dlrq.t_kind = 1 /*DLRQ_KIND_COMMIT*/
                              )
                          and case
                                when dlrq.t_factdate =
                                     to_date('01.01.0001', 'dd.mm.yyyy') then
                                 dlrq.t_plandate
                                else
                                 dlrq.t_factdate
                              end >= reg.t_dealtime
                        where dl_tick.t_bofficekind in (101, 117, 127)
                          and dl_tick.t_dealid = reg.t_dealid
                          and ((dl_tick.t_dealtype = 32743 and dl_tick.t_dealstatus = 20 and dlrq.t_id > 0) or dl_tick.t_dealtype != 32743)
                          and reg.T_SESSIONID = V_SESSIONID
                          and reg.T_CALCID = V_CALCID) rest
                        group by t_dealid, t_dockind, t_dealpart, t_pfi, t_clientcontrid) rest
                join  dmcaccdoc_dbt accdoc2
                on ( accdoc2.t_clientcontrid = rest.t_clientcontrid
                  and accdoc2.t_iscommon = chr(88)
                  and accdoc2.t_currency = rest.t_pfi
                  and accdoc2.t_catid = CATID_SEC)
                join  DMCTEMPL_DBT mctemp
                  on ( mctemp.t_catid = CATID_SEC and mctemp.t_value1 = 1)
                group by rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_pfi,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc ) rest
           join dmcaccdoc_dbt accdoc
           on accdoc.t_id =  rest.accdoc_id
             /* (select  max(t_id)
                 from dmcaccdoc_dbt accdoc2, DMCTEMPL_DBT mctemp
                where accdoc2.t_clientcontrid = rest.t_clientcontrid
                  and accdoc2.t_iscommon = chr(88)
                  and accdoc2.t_currency = rest.t_pfi
                  and accdoc2.t_catid = CATID_SEC
                  and mctemp.t_catid = CATID_SEC
                  and (mctemp.t_catid = CATID_SEC and mctemp.t_value1 = 1)
              )*/
    left join daccount_dbt account
           on account.t_chapter = accdoc.t_chapter
          and account.t_account=accdoc.t_account
          and account.t_code_currency=accdoc.t_currency);

    PushLogLine('Отбор счетов ЦБ по клиентским сделкам - end');

  END AddSCAccountsP;

  -- отбор счетов ДС по клиентским сделкам
  PROCEDURE AddSCAccountsM(BeginDate IN DATE, EndDate IN DATE) IS
  BEGIN
    PushLogLine('Отбор счетов ДС по клиентским сделкам - begin','Отбор счетов ДС по клиентским сделкам');
    INSERT /*+ parallel(8)  */
    INTO DDL_REGIABUF_DBT
      (T_SESSIONID,
       T_CALCID,
       t_part,
       t_dealid,
       t_dockind,
       t_dealsubkind,
       t_dealpart,
       t_clientcontrid,
       t_account,
       t_accountname,
       t_acctype,
       t_acccur,
       t_accrest,
       t_credit,
       t_debet,
       t_outstandclaims,
       t_outstandobl,
       t_restprev)
      (select /*+ parallel(8)  */
        V_SESSIONID,
        V_CALCID,
        1 t_part,
        rest.t_dealid,
        rest.t_dockind,
        1 as t_dealsubkind,
        rest.t_dealpart,
        rest.t_clientcontrid,
        accdoc.t_account,
        account.t_nameaccount,
        ACC_TYPE_FIN,
        (select t_ccy from dfininstr_dbt fin where fin.t_fiid = rest.t_fiid) t_ccy,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                rest.t_fiid,
                                EndDate),
            0) t_rest,
        rest.t_turnc,
        rest.t_turnd,
        rest.t_planturnc,
        rest.t_planturnd,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                rest.t_fiid,
                                EndDate - 1),
            0) t_restprev
         from ( select /*+ leading(rest) use_hash(rest,accdoc2) index(accdoc2 DMCACCDOC_DBT_IDX2) */rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc,
                      max(accdoc2.t_id) accdoc_id
                 from (select rest.t_dealid,
                        rest.t_dockind,
                        rest.t_dealpart,
                        rest.t_fiid,
                        rest.t_clientcontrid,
                        nvl(sum(case
                                  when t_kind = 0 /*DLRQ_KIND_REQUEST*/
                                       and t_factdate !=
                                       to_date('01.01.0001', 'dd.mm.yyyy') then
                                   t_amount
                                  else
                                   0
                                end),
                            0) t_turnc,
                        nvl(sum(case
                                  when t_kind = 1 /*DLRQ_KIND_COMMIT*/
                                       and t_factdate !=
                                       to_date('01.01.0001', 'dd.mm.yyyy') then
                                   t_amount
                                  else
                                   0
                                end),
                            0) t_turnd,
                        nvl(sum(case
                                  when t_kind = 1 /*DLRQ_KIND_COMMIT*/
                                       and
                                       t_factdate = to_date('01.01.0001', 'dd.mm.yyyy') then
                                   t_amount
                                  else
                                   0
                                end),
                            0) t_planturnd,
                        nvl(sum(case
                                  when t_kind = 0 /*DLRQ_KIND_REQUEST*/
                                       and
                                       t_factdate = to_date('01.01.0001', 'dd.mm.yyyy') then
                                   t_amount
                                  else
                                   0
                                end),
                            0) t_planturnc
                   from (select /*+ leading(reg,dl_tick) */ dl_tick.t_dealid,
                                dl_tick.t_bofficekind as t_dockind,
                                dlrq.t_dealpart,
                                dlrq.t_fiid,
                                 (case 
                                  when reg.t_invert_debet_credit = 1 then 
                                  (case when dlrq.t_kind = 0 then 1 else 0 end)
                                  else dlrq.t_kind                          
                                end) t_kind,
                                dlrq.t_factdate,
                                dlrq.t_plandate,
                                dlrq.t_amount,
                                reg.t_clientcontrid
                           from ddl_tick_dbt dl_tick
                           join DDL_REGIABUF_DBT reg
                             on reg.t_part = 1
                            and reg.t_account is NULL
                           join ddl_leg_dbt dl_leg
                             on dl_leg.t_dealid = dl_tick.t_dealid
                            and dl_leg.t_legid = 0
                           and decode(dl_leg.t_legkind, 0, 1, 2) = reg.t_dealpart
                           join ddlrq_dbt dlrq
                             on dlrq.t_dockind = dl_tick.t_bofficekind
                            and dlrq.t_docid = dl_tick.t_dealid
                            and dlrq.t_dealpart =
                                decode(dl_leg.t_legkind, 0, 1, 2)
                            and dlrq.t_state != 7 /*отменено GAA:496245*/
                            and not (dlrq.t_type = 6 and reg.t_invert_debet_credit = 1)
                            and dlrq.t_subkind = 0 /*DLRQ_SUBKIND_CURRENCY*/
                            and (dlrq.t_kind = 0 /*DLRQ_KIND_REQUEST*/
                                or dlrq.t_kind = 1 /*DLRQ_KIND_COMMIT*/
                                )
                            and case
                                  when dlrq.t_factdate =
                                       to_date('01.01.0001', 'dd.mm.yyyy') then
                                   dlrq.t_plandate
                                  else
                                   dlrq.t_factdate
                                end >= reg.t_dealtime
                          where dl_tick.t_bofficekind in (101, 117, 127)
                            and dl_tick.t_dealid = reg.t_dealid
                            and ((dl_tick.t_dealtype = 32743 and dl_tick.t_dealstatus = 20 and dlrq.t_id > 0) or dl_tick.t_dealtype != 32743)
                            and reg.T_SESSIONID = V_SESSIONID
                            and reg.T_CALCID = V_CALCID) rest

                      group by t_dealid, t_dockind, t_dealpart, t_fiid, t_clientcontrid ) rest
                 join dmcaccdoc_dbt accdoc2
                  on ( accdoc2.t_clientcontrid = rest.t_clientcontrid
                    and accdoc2.t_iscommon = chr(88)
                    and accdoc2.t_currency = rest.t_fiid
                    and accdoc2.t_catid = CATID_FIN)
                 join  DMCTEMPL_DBT mctemp
                    on ( mctemp.t_catid = CATID_FIN
                    and (    mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7)))
               group by rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc
               ) rest
         join dmcaccdoc_dbt accdoc
           on accdoc.t_id = rest.accdoc_id
              /*(select  max(t_id)
                 from dmcaccdoc_dbt accdoc2, DMCTEMPL_DBT mctemp
                where accdoc2.t_clientcontrid = rest.t_clientcontrid
                  and accdoc2.t_iscommon = chr(88)
                  and accdoc2.t_currency = rest.t_fiid
                  and accdoc2.t_catid = CATID_FIN
                  and mctemp.t_catid = CATID_FIN
                  and (    mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7))
              )*/
    left join daccount_dbt account
           on account.t_chapter = accdoc.t_chapter
          and account.t_account=accdoc.t_account
          and account.t_code_currency=accdoc.t_currency);

    PushLogLine('Отбор счетов отбор счетов ДС по клиентским сделкам - end');

  END AddSCAccountsM;

  -- отбор счетов по клиентским сделкам ФИСС и КО
  PROCEDURE AddDVNAccounts(BeginDate IN DATE, EndDate IN DATE) IS

  BEGIN
    PushLogLine('Отбор счетов по клиентским сделкам валютного рынка - begin','Отбор счетов по клиентским сделкам валютного рынка');
    INSERT /*+ parallel(8)  */
    INTO DDL_REGIABUF_DBT
      (T_SESSIONID,
       T_CALCID,
       t_part,
       t_dealid,
       t_dockind,
       t_dealpart,
       t_dealsubkind,
       t_clientcontrid, 
       t_account,
       t_accountname,
       t_acctype,
       t_acccur,
       t_accrest,
       t_credit,
       t_debet,
       t_outstandclaims,
       t_outstandobl, 
       t_restprev)
      (select /*+ parallel(8)  */
        V_SESSIONID,
        V_CALCID,
        1 t_part,
        rest.t_dealid,
        rest.t_dockind,
        rest.t_dealpart,
        2 t_dealsubkind,
        rest.t_clientcontrid,
        accdoc.t_account,
        account.t_nameaccount,
        (case
          when accdoc.t_catid = CATID_FIN then
           ACC_TYPE_FIN
          when accdoc.t_catid = CATID_SEC then
           ACC_TYPE_SEC
        end) t_acctype,
        (select t_ccy from dfininstr_dbt fin where fin.t_fiid = rest.t_fiid) t_ccy,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                accdoc.t_currency,
                                EndDate),
            0) t_rest,
        rest.t_turnc,
        rest.t_turnd,
        rest.t_planturnc,
        rest.t_planturnd,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                accdoc.t_currency,
                                EndDate - 1),
            0) t_restprev
         from (select /* leading(rest) use_hash(rest,accdoc2) index(accdoc2 DMCACCDOC_DBT_IDX2) */ rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc,
                      max(accdoc2.t_id) accdoc_id
                from (select rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      nvl(sum(case
                                when t_kind = 0 /*pmpaym_KIND_REQUEST*/
                                     and t_isfact = 1 then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_turnc,
                      nvl(sum(case
                                when t_kind = 1 /*pmpaym_KIND_COMMIT*/
                                     and t_isfact = 1 then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_turnd,
                      nvl(sum(case
                                when t_kind = 1 /*pmpaym_KIND_COMMIT*/
                                     and t_isfact != 1 then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_planturnd,
                      nvl(sum(case
                                when t_kind = 0 /*pmpaym_KIND_REQUEST*/
                                     and t_isfact != 1 then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_planturnc
                 from (select /*+ leading(reg,dvndeal) */ pmpaym.t_valuedate,
                              pmpaym.t_amount,
                             (case
                                   when ((t_purpose in (1, 3) and reg.t_isBuy = 1) or (t_purpose in (2, 4) and reg.t_isBuy != 1)) then
                                    0
                                   else
                                    1
                                 end) t_kind,
                              (case
                                when pmpaym.t_paymstatus = 150 or pmpaym.t_isfactpaym = chr(88) then
                                 1
                                else
                                 0
                              end) t_isfact,
                              reg.t_dealid,
                              dvndeal.t_dockind,
                              pmpaym.t_fiid,
                              reg.t_clientcontrid,
                              reg.t_dealpart
                         from ddvndeal_dbt dvndeal
                         join DDL_REGIABUF_DBT reg
                           on reg.t_part = 1
                          and reg.t_account is NULL
                          and reg.t_dealsubkind = 2
                         join ddvnfi_dbt dvnfi
                           on dvnfi.t_dealid = dvndeal.t_id
                          and decode(dvnfi.t_type, 0, 1, 2) = reg.t_dealpart
                         join dpmpaym_dbt pmpaym
                           on pmpaym.t_dockind = dvndeal.t_dockind
                          and pmpaym.t_documentid = dvndeal.t_id
                          and ((pmpaym.t_payer = reg.t_clientid or pmpaym.t_receiver = reg.t_clientid) or (reg.t_clientid <= 0 or reg.t_clientid is null))
                          and ((dvnfi.t_type = 0 and
                              pmpaym.t_purpose in (1, 2, 40, 72)) or
                              (dvnfi.t_type = 2 and
                              pmpaym.t_purpose in (3, 4))) 
                          and case
                                when pmpaym.t_valuedate =
                                     to_date('01.01.0001', 'dd.mm.yyyy') then
                                 pmpaym.t_valuedate
                                else
                                 pmpaym.t_valuedate
                              end >= reg.t_dealtime
                        where dvndeal.t_id = reg.t_dealid
                          and reg.T_SESSIONID = V_SESSIONID
                          and reg.T_CALCID = V_CALCID) rest
                group by t_dealid, t_dockind, t_dealpart, t_fiid, t_clientcontrid) rest
                join  dmcaccdoc_dbt accdoc2
                 on ( accdoc2.t_clientcontrid = rest.t_clientcontrid
                  and accdoc2.t_iscommon = chr(88)
                  and accdoc2.t_currency = rest.t_fiid
                  and accdoc2.t_catid in (CATID_FIN, CATID_SEC))
                join  DMCTEMPL_DBT mctemp on ( mctemp.t_catid = accdoc2.t_catid
                  and (  mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7) 
                        OR mctemp.t_catid = CATID_SEC and mctemp.t_value1 = 1))
                group by rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc   
                ) rest

         join dmcaccdoc_dbt accdoc
           on accdoc.t_id = rest.accdoc_id
              /*(select  max(t_id)
                 from dmcaccdoc_dbt accdoc2, DMCTEMPL_DBT mctemp
                where accdoc2.t_clientcontrid = rest.t_clientcontrid
                  and accdoc2.t_iscommon = chr(88)
                  and accdoc2.t_currency = rest.t_fiid
                  and accdoc2.t_catid in (CATID_FIN, CATID_SEC)
                  and mctemp.t_catid = accdoc2.t_catid
                  and (    mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7) 
                        OR mctemp.t_catid = CATID_SEC and mctemp.t_value1 = 1))*/
    left join daccount_dbt account
           on account.t_chapter = accdoc.t_chapter
          and account.t_account=accdoc.t_account
          and account.t_code_currency=accdoc.t_currency);

    PushLogLine('Отбор счетов по клиентским сделкам валютного рынка - end');

  END AddDVNAccounts;

    -- отбор счетов по клиентским срочным операциям ФИСС и КО
  PROCEDURE AddDVAccounts(BeginDate IN DATE, EndDate IN DATE) IS
  BEGIN
    PushLogLine('Отбор счетов по клиентским сделкам срочного рынка - begin','Отбор счетов по клиентским сделкам срочного рынка');
    INSERT /*+ parallel(8)  */
    INTO DDL_REGIABUF_DBT
      (T_SESSIONID,
       T_CALCID,
       t_part,
       t_dealid,
       t_dockind,
       t_dealsubkind,
       t_dealpart,
       t_clientcontrid,
       t_account,
       t_accountname,
       t_acctype,
       t_acccur,
       t_accrest,
       t_credit,
       t_debet,
       t_restprev)
      (select /*+ parallel(8)  */
        V_SESSIONID,
        V_CALCID,
        1 t_part,
        t_dealid,
        192,
        3 t_dealsubkind,
        0,
        turn.t_clientcontrid,
        turn.t_account,
        turn.t_nameaccount,
        (case
          when turn.t_catid = CATID_SEC then
           ACC_TYPE_SEC
          when turn.t_catid = CATID_FO then
           ACC_TYPE_FO
          else
           ACC_TYPE_FIN
        end) t_acctype,
        (select t_ccy from dfininstr_dbt fin where fin.t_fiid = turn.t_currency) t_ccy,
        nvl(rsb_account.restall(turn.t_account,
                                turn.t_chapter,
                                turn.t_currency,
                                EndDate),
            0) t_rest,
          (case when turn.t_catid = CATID_FIN then nvl((SELECT SUM(nvl(dvdlturn.t_margin, 0))
                                  FROM ddvdlturn_dbt dvdlturn
                                  WHERE dvdlturn.t_dealid = turn.t_dealid
                                  AND dvdlturn.t_margin >= 0
                                  AND dvdlturn.t_date <= EndDate
                                  AND dvdlturn.t_setmargin = chr(88)), 0)
          else 0 end) +
         (case when turn.t_catid = CATID_FO and turn.t_isbuy = 1 then turn.t_amount else 0 end) as t_turnc,
         (case when turn.t_catid = CATID_FIN then nvl((SELECT SUM(nvl(dlc.t_Sum, 0))
                                  FROM ddvdlcom_dbt dlc
                                 WHERE dlc.t_dealid = turn.t_dealid), 0) 
                                 +
                                 (nvl((SELECT SUM(nvl(abs(dvdlturn.t_margin), 0))
                                  FROM ddvdlturn_dbt dvdlturn
                                  WHERE dvdlturn.t_dealid = turn.t_dealid
                                  AND dvdlturn.t_margin < 0
                                  AND dvdlturn.t_date <= EndDate
                                  AND dvdlturn.t_setmargin = chr(88)), 0))
          
         else 0 end) + 
         (case when turn.t_catid = CATID_FO and turn.t_isbuy = 0 then turn.t_amount else 0 end) as turnd,
         nvl(rsb_account.restall(turn.t_account,
                                turn.t_chapter,
                                turn.t_currency,
                                EndDate - 1),
            0) t_restprev
         from (
           select /*+ leading(reg) use_nl(dvdeal) */
           accdoc.t_chapter,
           accdoc.t_account,
           accdoc.t_currency,
           accdoc.t_catid,
           account.t_nameaccount,
           dvdeal.t_id t_dealid,
           dvdeal.t_amount,
           reg.t_clientcontrid,
           reg.t_isbuy
         from ddvdeal_dbt dvdeal
         join ddvfipos_dbt fipos on dvdeal.t_fiid = fipos.t_fiid
          and fipos.t_clientcontr=dvdeal.t_clientcontr
          and fipos.t_department = dvdeal.t_department
          and fipos.t_broker = dvdeal.t_broker
         join DDL_REGIABUF_DBT reg
           on reg.t_part = 1
          and reg.t_account is NULL
          and reg.t_dealsubkind = 3
        join dmcaccdoc_dbt accdoc
           on accdoc.t_id in
              (select t_id
                 from dmcaccdoc_dbt accdoc2, DMCTEMPL_DBT mctemp 
                where accdoc2.t_clientcontrid = dvdeal.t_clientcontr
                  and accdoc2.t_dockind=193
                  and accdoc2.t_docid=fipos.t_id
                  and accdoc2.t_catid in (CATID_FIN, CATID_SEC, CATID_FO)
                  and mctemp.t_catid = accdoc2.t_catid
                  and (    mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7) 
                        OR mctemp.t_catid = CATID_SEC and mctemp.t_value1 = 1 
                        OR mctemp.t_catid = CATID_FO  and mctemp.t_value1 = 3 )
                  and accdoc2.t_activatedate <= EndDate)
        join daccount_dbt account
            on account.t_chapter = accdoc.t_chapter
           and account.t_account=accdoc.t_account
           and account.t_code_currency=accdoc.t_currency
           and (account.t_close_date = to_date('01.01.0001','DD.MM.YYYY') or account.t_close_date <=  EndDate)
        where dvdeal.t_id = reg.t_dealid
          and reg.T_SESSIONID =V_SESSIONID
          and reg.T_CALCID = V_CALCID) turn);

    PushLogLine('Отбор счетов по клиентским сделкам срочного рынка - end');

  END AddDVAccounts;

  PROCEDURE CreateData_Contr(BeginDate      IN DATE,
                             EndDate        IN DATE,
                             StockMarket    IN CHAR,
                             FuturesMarket  IN CHAR,
                             CurrencyMarket IN CHAR,
                             Sector_Brokers IN CHAR,
                             Sector_Dilers  IN CHAR,
                             Sector_Clients IN CHAR,
                             Block_Deals    IN CHAR,
                             Block_Clients  IN CHAR,
                             Block_InAcc    IN CHAR,
                             SelectedClients IN NUMBER,
                             SelectedContrs   IN NUMBER) IS
    TYPE registeria_t IS TABLE OF DDL_REGIABUF_DBT%ROWTYPE;
    g_registeria_ins registeria_t := registeria_t();
    registeria       DDL_REGIABUF_DBT%rowtype;
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    PushLogLine('Отбор договоров обслуживания - begin','Отбор договоров обслуживания');
    IF Block_Clients = CHR(88) THEN
    insert /*+ parallel(8) */ into DDL_REGIABUF_DBT (
                     T_SESSIONID,
                     T_CALCID,
                     t_part,
                     t_clientname,
                     t_clientagr,
                     t_clientagrdate,
                     t_clientagrend,
                     t_exchangecode,
                     t_proxyname,
                     t_proxynum,
                     t_clientform,
                     t_clientcountry,
                     t_clientresident,
                     t_clientcvalinv,
                     t_risklevel,
                     t_cashright,
                     t_clientid,
                     t_clientcontrid
                    )
                      (select
                      V_SESSIONID,
                      V_CALCID,
                      3 as t_part,
                      t_clientshortname as t_clientname,
                      'ДБО №' || t_sfcontrnumber || (case when RSI_NPTO.CheckContrIIS(t_clientcontrid) = 1 then ' / ИИС' else '' end) || ' / ' || t_servkindname || (case when t_servkind != 15 then ' / ' || t_servsubkindname else '' end) || ' / №' || t_subsfcontrnumber as t_clientagr,
                      'ДБО ' || to_char(t_sfcontrbegdate, 'DD.MM.YYYY') || ' / ' || t_servkindname || (case when t_servkind != 15 then ' / ' || t_servsubkindname else '' end) || ' / ' || to_char(t_sfcontrmpbegdate, 'DD.MM.YYYY') as t_clientagrdate,
                      decode(t_sfcontrmpenddate, to_date('01.01.0001','DD.MM.YYYY'), null,
                      ('ДБО ' || (case when t_sfcontrenddate = to_date('01.01.0001','DD.MM.YYYY') then '00.00.0000' else to_char(t_sfcontrenddate, 'DD.MM.YYYY') end ) || ' / ' || t_servkindname || (case when t_servkind != 15 then ' / ' || t_servsubkindname else '' end) || ' / ' || to_char(t_sfcontrmpenddate, 'DD.MM.YYYY'))) as t_clientagrend,
                      t_mpcode as t_exchangecode,
                      t_proxyname as t_proxyname,
                      t_proxynum as t_proxynum,
                      t_clientform as t_clientform,
                       t_clientcountry as t_clientcountry,
                      (case when t_clientform = 'ФЛ' then t_clientresident else '' end) as t_clientresident,
                      t_clientcvalinv as t_clientcvalinv,
                      t_risklevel as t_risklevel,
                      t_cashright as t_cashright,
                      t_clientid as t_clientid,
                      t_clientcontrid as t_clientcontrid
                      from (select /*+ parallel(8) */ sfcontr.t_partyid t_clientid,
                                   sfcontrmp.t_id t_clientcontrid,
                                   sfcontr.t_number t_sfcontrnumber,
                                   sfcontrmp.t_number t_subsfcontrnumber,
                                   sfcontrmp.t_servkind,
                                   sfcontr.t_datebegin t_sfcontrbegdate,
                                   sfcontr.t_dateclose t_sfcontrenddate,
                                   sfcontrmp.t_datebegin t_sfcontrmpbegdate,
                                   sfcontrmp.t_dateclose t_sfcontrmpenddate,
                                   sfcontr.t_name t_sfcontrname,
                                   skind.t_name t_servkindname,
                                   sksub.t_name t_servsubkindname,
                                   (nvl(client.t_shortname, chr(1)) || 
                                     CASE WHEN dlobjcode.t_code IS NOT NULL THEN '/' || dlobjcode.t_code else '' end) t_clientshortname,
                                   RSI_RSBPARTY.PT_GetPartyCode(client.t_PartyID,
                                                                 1) t_clientptcode,
                                   dlcontrmp.t_mpcode,
                                   case
                                     when client.t_legalform is null then
                                      chr(1)
                                     when client.t_legalform = 1 then
                                      'ЮЛ'
                                     when nvl(clientp.t_isemployer, chr(0)) =
                                          chr(88) then
                                      'ИП'
                                     else
                                      'ФЛ'
                                   end t_clientform,
                                   case when client.t_legalform is null then
                                      chr(1)
                                     when client.t_legalform = 1 then
                                      nvl(clientc.t_name, chr(1))                             
                                     when EXISTS (SELECT 1 FROM DPERSN_DBT persn where persn.t_personid = client.t_partyid  and persn.t_isstateless = chr(88))
                                         THEN 'Лицо без гражданства'
                                     when nvl(clientc.t_name, '') != '' THEN nvl(clientc.t_name, '')
                                     when exists (select 1 from DPERSNCIT_DBT where t_personid = client.t_partyid) 
                                         then (select t_name from (select ctry.t_name from DPERSNCIT_DBT pit, dcountry_dbt ctry where pit.t_personid = client.t_partyid 
                                                                                         and ctry.t_parentcountryid = 0 AND
                                                                                         ctry.t_codelat3 = pit.T_COUNTRYCODELAT3 ORDER BY pit.T_COUNTRYCODELAT3) WHERE ROWNUM = 1)
                                     else nvl(clientc.t_name, '')
                                   end                                    
                                     
                                    t_clientcountry,
                                   case
                                     when client.t_notresident is null then
                                      chr(1)
                                     when client.t_notresident = chr(88) then
                                      'нерезидент'
                                     else
                                      'резидент'
                                   end t_clientresident,
                                  case when rsb_common.GetRegFlagValue(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\РУБИЛЬНИК.BOSS-5689') <> CHR(88) 
                                    then
                                   (select 
                                      case when max(kval.attrid) = 2 then 'квалинвестор'
                                           --when kval.attrid = 1 then 'не квалинвестор' DEF-81827
                                           else 'не квалинвестор' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 207
                                       and  objattr.t_groupid = 140
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(dlcontr.t_dlcontrid, 34, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) kval) 
                                    else 
                                      (select case when max(cnt)>0 then 'квалинвестор' else 'не квалинвестор' end
                                         from
                                      (select count(*) as cnt from dscqinv_dbt d
                                      where d.t_partyid = client.t_partyid and d.t_state = 1))
                                    end t_clientcvalinv,
                                   case when sfcontr.t_servkind = 1 then
                                      (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 659
                                       and  objattr.t_groupid = 6
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(sfcontr.t_id, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) pravo)
                                   else
                                       (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                       (select NVL(objatcor.t_attrid, -1) attrid  
                                          from dobjattr_dbt  objattr,
                                               dobjatcor_dbt objatcor,
                                               DDLCONTRMP_DBT contrmp_pr,
                                               DSFCONTR_DBT sfcontr_pr
                                         where contrmp_pr.T_DLCONTRID = dlcontr.t_dlcontrid
                                           and sfcontr_pr.t_id = contrmp_pr.t_sfcontrid
                                           and sfcontr_pr.t_servkind = 1
                                           and objattr.t_objecttype = 659
                                           and objattr.t_groupid = 6
                                           and objatcor.t_objecttype =
                                               objattr.t_objecttype
                                           and objatcor.t_groupid =
                                               objattr.t_groupid
                                           and objatcor.t_attrid =
                                               objattr.t_attrid
                                           and objatcor.t_object =
                                               lpad(sfcontr_pr.t_id, 10, '0')
                                           and objatcor.t_validfromdate <= edt
                                           and objatcor.t_validtodate > edt ORDER BY objatcor.t_attrid) pravo WHERE ROWNUM = 1)
                                   end
                                    t_cashright,
                                   (select trim(nvl(max(decode(t_attrid, 1, 'КСУР', decode(t_attrid, 2, 'КПУР', decode(t_attrid, 3, 'КОУР', decode(t_attrid, 4, 'КНУР'))))),
                                                          'отсутствует'))
                                      from dobjatcor_dbt objatcor
                                     where objatcor.t_objecttype = 3
                                       and /*Субъект экономики*/
                                           objatcor.t_groupid = 95
                                       and objatcor.t_object =
                                           lpad(client.t_partyid, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) t_risklevel,
                                   nvl((select listagg(clientproxy.t_name, '/') within group(order by clientproxy.t_name, ptproxy.t_proxyid)
                                         from dptproxy_dbt ptproxy
                                         join dparty_dbt clientproxy
                                           on clientproxy.t_partyid =
                                              ptproxy.t_proxyid
                                        where ptproxy.t_partyid =
                                              sfcontr.t_partyid
                                          and ptproxy.t_docdate <= edt
                                          and ptproxy.t_validitydate >= edt),
                                       chr(1)) t_proxyname,
                                   nvl((select listagg(ptproxy.t_docnumber ||
                                                      ' с ' ||
                                                      to_char(ptproxy.t_docdate,
                                                              'dd.mm.yyyy') ||
                                                      ' по ' ||
                                                      to_char(ptproxy.t_validitydate,
                                                              'dd.mm.yyyy'),
                                                      '/') within group(order by ptproxy.t_proxyid, ptproxy.t_proxyid)
                                         from dptproxy_dbt ptproxy
                                         join dparty_dbt clientproxy
                                           on clientproxy.t_partyid =
                                              ptproxy.t_proxyid
                                        where ptproxy.t_partyid =
                                              sfcontr.t_partyid
                                          and ptproxy.t_docdate <= edt
                                          and ptproxy.t_validitydate >= edt),
                                       chr(1)) t_proxynum
                              from ddlcontr_dbt dlcontr
                              join (select BeginDate bdt, EndDate edt
                                     from dual)
                                on 1 = 1
                              join dsfcontr_dbt sfcontr
                                on sfcontr.t_id = dlcontr.t_sfcontrid
                              join ddlcontrmp_dbt dlcontrmp
                                on dlcontrmp.t_dlcontrid =
                                   dlcontr.t_dlcontrid
                              join dsfcontr_dbt sfcontrmp
                                on sfcontrmp.t_id = dlcontrmp.t_sfcontrid
                              left join dparty_dbt client
                                on client.t_partyid = sfcontr.t_partyid
                              left join dpersn_dbt clientp
                                on clientp.t_personid = sfcontr.t_partyid
                              left join dcountry_dbt clientc
                                on (clientc.t_parentcountryid = 0 AND
                                   clientc.t_codelat3 = client.t_nrcountry) /* MAA: iS - 517711 */
                              left join dservkind_dbt skind
                                on skind.t_servisekind = sfcontrmp.t_servkind
                              left join dservksub_dbt sksub
                                on sksub.t_servicekind = sfcontrmp.t_servkind
                               and sksub.t_servicekindsub = sfcontrmp.t_servkindsub
                              left join (select dlobjcode.* 
                                           from ddlobjcode_dbt dlobjcode 
                                          where dlobjcode.t_objecttype = 207 
                                            and dlobjcode.t_codekind = 1  
                                         --Если есть ЕКК с нулевой датой - отбираем его, если нет - последний закрытый код  
                                            and dlobjcode.t_bankclosedate = case when exists (select 1 from ddlobjcode_dbt dlobjcodesub1 where dlobjcodesub1.t_objectid = dlobjcode.t_objectid and dlobjcodesub1.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub1.t_codekind = dlobjcode.t_codekind and dlobjcodesub1.t_bankclosedate = to_date('01010001','ddmmyyyy')  ) 
                                           then to_date('01010001','ddmmyyyy') 
                                           else (select max(dlobjcodesub2.t_bankclosedate) from ddlobjcode_dbt dlobjcodesub2 where dlobjcodesub2.t_objectid = dlobjcode.t_objectid and dlobjcodesub2.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub2.t_codekind = dlobjcode.t_codekind) END) DLOBJCODE 
                                     on dlcontr.t_dlcontrid = DLOBJCODE.T_OBJECTID 
                             where ((StockMarket = chr(88) and
                                   sfcontrmp.t_servkind = 1) or
                                   (FuturesMarket = chr(88) and
                                   sfcontrmp.t_servkind = 15) or
                                   (CurrencyMarket = chr(88) and
                                   sfcontrmp.t_servkind = 21))
                               and sfcontrmp.t_datebegin <= edt
                               and (sfcontrmp.t_dateclose >= bdt or
                                   sfcontrmp.t_dateclose =
                                   to_date('01.01.0001', 'dd.mm.yyyy'))
                               and (     SelectedClients = 1 
                                     and EXISTS(SELECT * FROM D_RIA_PANELCLIENTS_DBT ria_cl WHERE ria_cl.t_clientid = sfcontr.t_partyid and ria_cl.t_sessionid = V_SESSIONID and ria_cl.T_CALCID = V_CALCID)
                                      or SelectedClients = 0) 
                               and (     SelectedContrs = 1 
                                     and EXISTS(SELECT * FROM D_RIA_PANELCONTRS_DBT ria_c WHERE ria_c.t_clientid = sfcontr.t_partyid and ria_c.t_dlcontrid = dlcontrmp.t_dlcontrid and ria_c.t_sessionid = V_SESSIONID and ria_c.T_CALCID = V_CALCID)
                                      or SelectedContrs = 0)
                               ) dt);
    else
        insert  into DDL_REGIABUF_DBT (
                     T_SESSIONID,
                     T_CALCID,
                     t_part,
                     t_clientid,
                     t_clientcontrid
                    )
                      (select
                      V_SESSIONID,
                      V_CALCID,
                      3 as t_part,
                     
                      t_clientid as t_clientid,
                      t_clientcontrid as t_clientcontrid
                      from (select sfcontr.t_partyid t_clientid,
                                   sfcontrmp.t_id t_clientcontrid
                                   
                              from ddlcontr_dbt dlcontr
                              join (select BeginDate bdt, EndDate edt
                                     from dual)
                                on 1 = 1
                              join dsfcontr_dbt sfcontr
                                on sfcontr.t_id = dlcontr.t_sfcontrid
                              join ddlcontrmp_dbt dlcontrmp
                                on dlcontrmp.t_dlcontrid =
                                   dlcontr.t_dlcontrid
                              join dsfcontr_dbt sfcontrmp
                                on sfcontrmp.t_id = dlcontrmp.t_sfcontrid
                             where ((StockMarket = chr(88) and
                                   sfcontrmp.t_servkind = 1) or
                                   (FuturesMarket = chr(88) and
                                   sfcontrmp.t_servkind = 15) or
                                   (CurrencyMarket = chr(88) and
                                   sfcontrmp.t_servkind = 21))
                               and sfcontrmp.t_datebegin <= edt
                               and (sfcontrmp.t_dateclose >= bdt or
                                   sfcontrmp.t_dateclose =
                                   to_date('01.01.0001', 'dd.mm.yyyy'))
                               and (     SelectedClients = 1 
                                     and EXISTS(SELECT * FROM D_RIA_PANELCLIENTS_DBT ria_cl WHERE ria_cl.t_clientid = sfcontr.t_partyid and ria_cl.t_sessionid = V_SESSIONID and ria_cl.T_CALCID = V_CALCID)
                                      or SelectedClients = 0) 
                               and (     SelectedContrs = 1 
                                     and EXISTS(SELECT * FROM D_RIA_PANELCONTRS_DBT ria_c WHERE ria_c.t_clientid = sfcontr.t_partyid and ria_c.t_dlcontrid = dlcontrmp.t_dlcontrid and ria_c.t_sessionid = V_SESSIONID and ria_c.T_CALCID = V_CALCID)
                                      or SelectedContrs = 0)
                               ) dt);
    END IF;
    PushLogLine('Отбор договоров обслуживания - end','Отбор счетов договоров обслуживания');

    COMMIT;
    
    IF Block_InAcc = CHR(88) THEN
      AddAccounts_Contr_Parall_Start(BeginDate, EndDate);
    END IF;
    --AddAccounts_Contr(BeginDate, EndDate);

  END CreateData_Contr;
  
     -- отбор счетов по операциям зачисления/списания ДС ФИСС и КО
    PROCEDURE AddInOutAccountsDV(BeginDate IN DATE, EndDate IN DATE) IS

    BEGIN
    PushLogLine('Отбор счетов по операциям зачисления/списания ДС  валютного рынка - begin','Отбор счетов по операциям зачисления/списания ДС  валютного рынка');
        INSERT 
    INTO DDL_REGIABUF_DBT /*+ parallel(8)  */
      (T_SESSIONID,
       T_CALCID,
       t_part,
       t_dealid,
       t_dockind,
       t_dealpart,
       t_dealsubkind,
       t_clientcontrid, 
       t_account,
       t_accountname,
       t_acctype,
       t_acccur,
       t_accrest,
       t_credit,
       t_debet,
       t_outstandclaims,
       t_outstandobl, 
       t_restprev)
      (select /*+ parallel(8)  */
        V_SESSIONID,
        V_CALCID,
        1 t_part,
        rest.t_dealid,
        rest.t_dockind,
        rest.t_dealpart,
        rest.t_dealsubkind,
        rest.t_clientcontrid,
        accdoc.t_account,
        account.t_nameaccount,
        (case
          when accdoc.t_catid = CATID_FIN then
           ACC_TYPE_FIN
          when accdoc.t_catid = CATID_SEC then
           ACC_TYPE_SEC
        end) t_acctype,
        (select t_ccy from dfininstr_dbt fin where fin.t_fiid = rest.t_fiid) t_ccy,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                accdoc.t_currency,
                                EndDate),
            0) t_rest,
        rest.t_turnc,
        rest.t_turnd,
        rest.t_planturnc,
        rest.t_planturnd,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                accdoc.t_currency,
                                EndDate - 1),
            0) t_restprev
         from (select /*+ leading(rest) use_hash(rest,accdoc2) index(accdoc2 DMCACCDOC_DBT_IDX2) */ rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc,
                      rest.t_dealsubkind,
                      max(accdoc2.t_id) accdoc_id
         
                from (select rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      nvl(sum(case
                                when t_kind = 0 /*pmpaym_KIND_REQUEST*/
                                     and t_isfact = 1 then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_turnc,
                      nvl(sum(case
                                when t_kind = 1 /*pmpaym_KIND_COMMIT*/
                                     and t_isfact = 1 then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_turnd,
                      nvl(sum(case
                                when t_kind = 1 /*pmpaym_KIND_COMMIT*/
                                     and t_isfact != 1 then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_planturnd,
                      nvl(sum(case
                                when t_kind = 0 /*pmpaym_KIND_REQUEST*/
                                     and t_isfact != 1 then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_planturnc,
                      rest.t_dealsubkind
                 from (select /*+ leading(reg,nptxop)*/ pmpaym.t_valuedate,
                              pmpaym.t_amount,
                             (case
                                   when t_purpose = 1 and reg.t_isBuy = 1 then
                                    0
                                   else
                                    1
                                 end) t_kind,
                              (case
                                when pmpaym.t_paymstatus = 150 or pmpaym.t_isfactpaym = chr(88) then
                                 1
                                else
                                 0
                              end) t_isfact,
                              reg.t_dealid,
                              nptxop.t_dockind,
                              pmpaym.t_fiid,
                              reg.t_clientcontrid,
                              1 t_dealpart,
                              reg.t_dealsubkind
                         from DNPTXOP_DBT nptxop
                         join DDL_REGIABUF_DBT reg
                           on reg.t_part = 1
                          and reg.t_account is NULL
                          and reg.t_dealsubkind in (2,3)
                         join dpmpaym_dbt pmpaym
                           on pmpaym.t_dockind = nptxop.t_dockind
                          and pmpaym.t_documentid = nptxop.t_id
                          and (pmpaym.t_payer = reg.t_clientid or pmpaym.t_receiver = reg.t_clientid)
                          and  pmpaym.t_purpose = 1
                          and case
                                when pmpaym.t_valuedate =
                                     to_date('01.01.0001', 'dd.mm.yyyy') then
                                 pmpaym.t_valuedate
                                else
                                 pmpaym.t_valuedate
                              end >= nvl(reg.t_dealtime, to_date('01.01.0001', 'dd.mm.yyyy'))
                        where reg.t_dockind = 4607
                          and nptxop.t_dockind = reg.t_dockind
                          and nptxop.t_id = reg.t_dealid
                          and reg.T_SESSIONID = V_SESSIONID
                          and reg.T_CALCID = V_CALCID) rest
                group by t_dealid, t_dockind, t_dealpart, t_fiid, t_clientcontrid, t_dealsubkind) rest
              join dmcaccdoc_dbt accdoc2
                on (accdoc2.t_clientcontrid = rest.t_clientcontrid
                  and accdoc2.t_iscommon = chr(88)
                  and accdoc2.t_currency = rest.t_fiid
                  and accdoc2.t_catid in (CATID_FIN, CATID_SEC))
              join DMCTEMPL_DBT mctemp 
                 on ( mctemp.t_catid = accdoc2.t_catid
                  and (  mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7) 
                        OR mctemp.t_catid = CATID_SEC and mctemp.t_value1 = 1))
              group by rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc,
                      rest.t_dealsubkind) rest
         join dmcaccdoc_dbt accdoc
           on accdoc.t_id = rest.accdoc_id
              /*(select  max(t_id)
                 from dmcaccdoc_dbt accdoc2, DMCTEMPL_DBT mctemp
                where accdoc2.t_clientcontrid = rest.t_clientcontrid
                  and accdoc2.t_iscommon = chr(88)
                  and accdoc2.t_currency = rest.t_fiid
                  and accdoc2.t_catid in (CATID_FIN, CATID_SEC)
                  and mctemp.t_catid = accdoc2.t_catid
                  and (    mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7) 
                        OR mctemp.t_catid = CATID_SEC and mctemp.t_value1 = 1))*/
    left join daccount_dbt account
           on account.t_chapter = accdoc.t_chapter
          and account.t_account=accdoc.t_account
          and account.t_code_currency=accdoc.t_currency);

    PushLogLine('Отбор счетов по операциям зачисления/списания ДС валютного рынка - end');

    END AddInOutAccountsDV;
  
    PROCEDURE AddInOutAccountsSec(BeginDate IN DATE, EndDate IN DATE) IS
    BEGIN
    PushLogLine('Отбор счетов ДС по операциям зачисления/списания ДС - begin','Отбор счетов ДС по операциям зачисления/списания ДС');
    INSERT /*+ parallel(8)  */
    INTO DDL_REGIABUF_DBT
      (T_SESSIONID,
       T_CALCID,
       t_part,
       t_dealid,
       t_dockind,
       t_dealsubkind,
       t_dealpart,
       t_clientcontrid,
       t_account,
       t_accountname,
       t_acctype,
       t_acccur,
       t_accrest,
       t_credit,
       t_debet,
       t_outstandclaims,
       t_outstandobl,
       t_restprev)
      (select /*+ parallel(8)  */
        V_SESSIONID,
        V_CALCID,
        1 t_part,
        rest.t_dealid,
        rest.t_dockind,
        rest.t_dealsubkind,
        rest.t_dealpart,
        rest.t_clientcontrid,
        accdoc.t_account,
        account.t_nameaccount,
        ACC_TYPE_FIN,
        (select t_ccy from dfininstr_dbt fin where fin.t_fiid = rest.t_fiid) t_ccy,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                rest.t_fiid,
                                EndDate),
            0) t_rest,
        rest.t_turnc,
        rest.t_turnd,
        rest.t_planturnc,
        rest.t_planturnd,
        nvl(rsb_account.restall(accdoc.t_account,
                                accdoc.t_chapter,
                                rest.t_fiid,
                                EndDate - 1),
            0) t_restprev
         from (select /*+ leading(rest) use_hash(rest,accdoc2) index(accdoc2 DMCACCDOC_DBT_IDX2) */ rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc,
                      rest.t_dealsubkind,
                      max(accdoc2.t_id) accdoc_id
              from (select rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      nvl(sum(case
                                when t_kind = 0 /*DLRQ_KIND_REQUEST*/
                                     and t_factdate !=
                                     to_date('01.01.0001', 'dd.mm.yyyy') then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_turnc,
                      nvl(sum(case
                                when t_kind = 1 /*DLRQ_KIND_COMMIT*/
                                     and t_factdate !=
                                     to_date('01.01.0001', 'dd.mm.yyyy') then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_turnd,
                      nvl(sum(case
                                when t_kind = 1 /*DLRQ_KIND_COMMIT*/
                                     and
                                     t_factdate = to_date('01.01.0001', 'dd.mm.yyyy') then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_planturnd,
                      nvl(sum(case
                                when t_kind = 0 /*DLRQ_KIND_REQUEST*/
                                     and
                                     t_factdate = to_date('01.01.0001', 'dd.mm.yyyy') then
                                 t_amount
                                else
                                 0
                              end),
                          0) t_planturnc,
                      rest.t_dealsubkind
                 from (select /*+ leading(reg,nptxop)*/ nptxop.t_id t_dealid,
                              nptxop.t_dockind,
                              dlrq.t_dealpart,
                              dlrq.t_fiid,
                               (case 
                                when nptxop.t_subkind_operation = 10 then 
                                (case when dlrq.t_kind = 0 then 1 else 0 end)
                                else dlrq.t_kind                          
                              end) t_kind,
                              dlrq.t_factdate,
                              dlrq.t_plandate,
                              dlrq.t_amount,
                              reg.t_clientcontrid,
                              reg.t_dealsubkind
                         from DNPTXOP_DBT nptxop
                         join DDL_REGIABUF_DBT reg
                           on reg.t_part = 1
                          and reg.t_account is NULL
                          and reg.t_dealsubkind = 1
                         join ddlrq_dbt dlrq
                           on dlrq.t_dockind = nptxop.t_dockind
                          and dlrq.t_docid = nptxop.t_id
                          and dlrq.t_fiid = nptxop.t_currency
                          and dlrq.t_dealpart = 1
                          and dlrq.t_state != 7 /*отменено GAA:496245*/
                          and dlrq.t_type = 2
                          and dlrq.t_num = 0
                          and dlrq.t_subkind = 0 /*DLRQ_SUBKIND_CURRENCY*/
                          and (dlrq.t_kind = 0 /*DLRQ_KIND_REQUEST*/
                              or dlrq.t_kind = 1 /*DLRQ_KIND_COMMIT*/
                              )
                          and case
                                when dlrq.t_factdate =
                                     to_date('01.01.0001', 'dd.mm.yyyy') then
                                 dlrq.t_plandate
                                else
                                 dlrq.t_factdate
                              end >= NVL(reg.t_dealtime, to_date('01.01.0001', 'dd.mm.yyyy'))
                        where reg.t_dockind = 4607
                          and nptxop.t_dockind = reg.t_dockind
                          and nptxop.t_id = reg.t_dealid
                          and reg.T_SESSIONID = V_SESSIONID
                          and reg.T_CALCID = V_CALCID) rest

                       group by t_dealid, t_dockind, t_dealpart, t_fiid, t_clientcontrid, t_dealsubkind) rest
              join dmcaccdoc_dbt accdoc2
                on ( accdoc2.t_clientcontrid = rest.t_clientcontrid
                  and accdoc2.t_iscommon = chr(88)
                  and accdoc2.t_currency = rest.t_fiid
                  and accdoc2.t_catid = CATID_FIN)
              join DMCTEMPL_DBT mctemp 
                 on(    mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7))
              group by rest.t_dealid,
                      rest.t_dockind,
                      rest.t_dealpart,
                      rest.t_fiid,
                      rest.t_clientcontrid,
                      rest.t_turnc,
                      rest.t_turnd,
                      rest.t_planturnd,
                      rest.t_planturnc,
                      rest.t_dealsubkind ) rest
         join dmcaccdoc_dbt accdoc
           on accdoc.t_id = rest.accdoc_id
           /*(select  max(t_id)
                 from dmcaccdoc_dbt accdoc2, DMCTEMPL_DBT mctemp
                where accdoc2.t_clientcontrid = rest.t_clientcontrid
                  and accdoc2.t_iscommon = chr(88)
                  and accdoc2.t_currency = rest.t_fiid
                  and accdoc2.t_catid = CATID_FIN
                  and mctemp.t_catid = CATID_FIN
                  and (    mctemp.t_catid = CATID_FIN and mctemp.t_value1 in (1,3,6,7))
              )*/
    left join daccount_dbt account
           on account.t_chapter = accdoc.t_chapter
          and account.t_account=accdoc.t_account
          and account.t_code_currency=accdoc.t_currency);

    PushLogLine('Отбор счетов отбор счетов ДС по операциям зачисления/списания ДС - end');

    END AddInOutAccountsSec;
  
    PROCEDURE CreateData_OprInOut(BeginDate      IN DATE,
                                EndDate        IN DATE,
                                StockMarket    IN CHAR,
                                FuturesMarket  IN CHAR,
                                CurrencyMarket IN CHAR,
                                Sector_Brokers IN CHAR,
                                Sector_Dilers  IN CHAR,
                                Sector_Clients IN CHAR,
                                Block_Deals    IN CHAR,
                                Block_Clients  IN CHAR,
                                Block_InAcc    IN CHAR,
                                SelectedClients IN NUMBER,
                                SelectedContrs   IN NUMBER) IS
    TYPE registeria_t IS TABLE OF DDL_REGIABUF_DBT%ROWTYPE;
    g_registeria_ins registeria_t := registeria_t();
    registeria       DDL_REGIABUF_DBT%rowtype;
    
  BEGIN
    PushLogLine('Отбор операций Зачисления/Списания ДС - begin','Отбор операций Зачисления/Списания ДС');
    FOR one_rec IN (select dt.clientid,
                           dt.t_clientid,
                           dt.clientcontrid,
                           dt.pfi,
                           dt.t_dealid,
                           dt.t_dockind as t_dockind,
                           dt.t_dealdate,
                           1 t_dealpart,
                           dt.t_dealcode,
                           dt.t_dealdates,
                           dt.t_marketname,
                           dt.t_markettype,
                           dt.t_depsetcode,
                           dt.t_invert_debet_credit,
                           dt.t_sfcontrnumber,
                           dt.t_sfcontrbegdate,
                           decode(dt.t_sfcontrenddate, to_date('01.01.0001','DD.MM.YYYY'), null, dt.t_sfcontrenddate) t_sfcontrenddate,
                           dt.t_clientname,
                           dt.t_clientshortname,
                           dt.t_clientptcode,
                           dt.t_mpcode,
                           dt.t_clientform,
                           dt.t_clientcountry as t_clientcountry,
                           (case when dt.t_clientform = 'ФЛ' then dt.t_clientresident else '' end) as t_clientresident,
                           dt.t_clientcvalinv,
                           dt.t_side2name,
                           dt.t_contragentname,                                                                            
                           (select spgr.t_XLD 
                              from dspground_dbt spgr, dspgrdoc_dbt doc
                             where doc.t_SourceDocKind = dt.t_dockind
                               and doc.t_SourceDocID = dt.t_dealid
                               and spgr.t_SPgroundID = doc.t_SPgroundID
                               and spgr.t_Kind = 251 /*поручение клиента на операцию*/
                               and rownum = 1) t_client_assign,
                           'К' t_dealtype,
                            decode(dt.t_isbuy,
                                     1,
                                     'Покупка',
                                     'Продажа')
                              t_side,
                           dt.t_opername t_side2,
                           dt.t_sector,
                           case when dt.clientcontrid > 0 then
                           decode(dt.t_role, 0,
                           (select lower(objattr.t_name)
                              from dobjattr_dbt  objattr,
                                   dobjatcor_dbt objatcor
                             where objattr.t_objecttype = 659
                               and objattr.t_groupid = 1
                               and objatcor.t_objecttype =
                                   objattr.t_objecttype
                               and objatcor.t_groupid = objattr.t_groupid
                               and objatcor.t_attrid = objattr.t_attrid
                               and objatcor.t_object =
                                   lpad(dt.clientcontrid, 10, '0')
                               and objatcor.t_validfromdate <= edt
                               and objatcor.t_validtodate > edt
                               and rownum = 1),
                           (select lower(objattr.t_name)
                              from dobjattr_dbt objattr
                             where objattr.t_objecttype = 659
                               and objattr.t_groupid = 1
                               and objattr.t_attrid = dt.t_role
                               and rownum = 1))
                           else
                             null
                           end t_status_purcb,
                           dt.t_execmethod,
                           dt.t_formpayment,
                           case
                             when dt.t_dlcontrid is null then
                              chr(1)
                             else
                              case when dt.servkind = 1 then
                                      (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 659
                                       and  objattr.t_groupid = 6
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(dt.clientcontrid, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) pravo)
                                   else
                                       (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                       (select NVL(objatcor.t_attrid, -1) attrid  
                                          from dobjattr_dbt  objattr,
                                               dobjatcor_dbt objatcor,
                                               DDLCONTRMP_DBT contrmp_pr,
                                               DSFCONTR_DBT sfcontr_pr
                                         where contrmp_pr.T_DLCONTRID = dt.t_dlcontrid
                                           and sfcontr_pr.t_id = contrmp_pr.t_sfcontrid
                                           and sfcontr_pr.t_servkind = 1
                                           and objattr.t_objecttype = 659
                                           and objattr.t_groupid = 6
                                           and objatcor.t_objecttype =
                                               objattr.t_objecttype
                                           and objatcor.t_groupid =
                                               objattr.t_groupid
                                           and objatcor.t_attrid =
                                               objattr.t_attrid
                                           and objatcor.t_object =
                                               lpad(sfcontr_pr.t_id, 10, '0')
                                           and objatcor.t_validfromdate <= edt
                                           and objatcor.t_validtodate > edt ORDER BY objatcor.t_attrid) pravo WHERE ROWNUM = 1)
                                   end
                           end t_cashright,
                               (select trim(nvl(max(decode(t_attrid, 1, 'КСУР', decode(t_attrid, 2, 'КПУР', decode(t_attrid, 3, 'КОУР', decode(t_attrid, 4, 'КНУР'))))),
                                                          'отсутствует'))
                                      from dobjatcor_dbt objatcor
                                     where objatcor.t_objecttype = 3
                                       and /*Субъект экономики*/
                                           objatcor.t_groupid = 95
                                       and objatcor.t_object =
                                           lpad(clientid, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt)
                            t_risklevel,
                              nvl((select listagg(clientproxy.t_name, '/') within group(order by clientproxy.t_name, ptproxy.t_proxyid)
                                    from dptproxy_dbt ptproxy
                                    join dparty_dbt clientproxy
                                      on clientproxy.t_partyid =
                                         ptproxy.t_proxyid
                                   where ptproxy.t_partyid = dt.t_clientid
                                     and ptproxy.t_docdate <= edt
                                     and ptproxy.t_validitydate >= edt),
                                  chr(1))
                            t_proxyname,
                              nvl((select listagg(ptproxy.t_docnumber || ' с ' ||
                                                 to_char(ptproxy.t_docdate,
                                                         'dd.mm.yyyy') ||
                                                 ' по ' ||
                                                 to_char(ptproxy.t_validitydate,
                                                         'dd.mm.yyyy'),
                                                 '/') within group(order by ptproxy.t_proxyid, ptproxy.t_proxyid)
                                    from dptproxy_dbt ptproxy
                                    join dparty_dbt clientproxy
                                      on clientproxy.t_partyid =
                                         ptproxy.t_proxyid
                                   where ptproxy.t_partyid = dt.t_clientid
                                     and ptproxy.t_docdate <= edt
                                     and ptproxy.t_validitydate >= edt),
                                  chr(1))
                           t_proxynum,
                           case 
                                when EXISTS (SELECT dlrq.* 
                                               FROM ddlrq_dbt dlrq 
                                              WHERE dlrq.t_dockind = dt.t_dockind
                                                and dlrq.t_docid = dt.t_dealid 
                                                and dlrq.T_FIID = dt.pfi 
                                                and dlrq.t_type = 2 
                                                and dlrq.t_num = 0) 
                                then 1 
                                else 2 
                           end t_dealsubkind
                      from (select bdt,
                                   edt,
                                   dl_tick.t_kind_operation,
                                   client.t_partyid clientid,
                                   dl_tick.t_client t_clientid,
                                   sfcontrmp.t_id clientcontrid,
                                   sfcontrmp.t_servkind servkind,
                                   dl_tick.t_currency pfi,
                                   dlcontrmp.t_role,
                                   dlcontrmp.t_mpcode,
                                   oprkoper.t_name t_opername,
                                   dlmarket.t_code t_depsetcode,
                                   case 
                                        when dl_tick.t_subkind_operation = 10 then 0
                                        else 1
                                   end t_invert_debet_credit, 
                                   dl_tick.t_id t_dealid,
                                   dl_tick.t_operdate t_dealdate,
                                   dl_tick.t_dockind t_dockind,
                                   case when dl_tick.t_MarketSector > 0 then chr(88)
                                   else chr(0) end t_sector,
                                   (case when dl_tick.t_subkind_operation = 10 then 1 else 0 end)  t_isbuy,
                                   dl_tick.t_code t_dealcode,
                                   to_char(dl_tick.t_operdate, 'dd.mm.yyyy') t_dealdates,
                                   case when exists (select 1 from dpartyown_dbt where t_partyid = market.t_partyid and t_partykind=3) 
                                         then market.t_name
                                         else 'Внебиржевой рынок'
                                    end t_marketname,
                                   dlmarket.t_code t_markettype,
                                   nvl(sfcontrmp.t_number, chr(1)) t_sfcontrnumber,
                                   nvl(sfcontr.t_name, chr(1)) t_sfcontrname,
                                   --decode(sfcontr.t_datebegin, to_date('01.01.0001', 'dd.mm.yyyy'), null, sfcontr.t_datebegin) t_sfcontrbegdate,
                                   decode(to_char(sfcontr.t_datebegin, 'dd.mm.yyyy'), '01.01.0001', null, to_char(sfcontr.t_datebegin, 'dd.mm.yyyy')) t_sfcontrbegdate,
                                   --decode(sfcontr.t_dateclose, to_date('01.01.0001', 'dd.mm.yyyy'), null, sfcontr.t_dateclose) t_sfcontrenddate,
                                   decode(to_char(sfcontr.t_dateclose, 'dd.mm.yyyy'), '01.01.0001', null, to_char(sfcontr.t_dateclose, 'dd.mm.yyyy')) t_sfcontrenddate,
                                   nvl(client.t_name, chr(1)) t_clientname,
                                   (nvl(client.t_shortname, chr(1)) || 
                                   CASE WHEN dlobjcode.t_code IS NOT NULL THEN '/' || dlobjcode.t_code else '' end) t_clientshortname,
                                   RSI_RSBPARTY.PT_GetPartyCode(client.t_PartyID,
                                                                1) t_clientptcode,
                                   case
                                     when client.t_legalform is null then
                                      chr(1)
                                     when client.t_legalform = 1 then
                                      'ЮЛ'
                                     when nvl(clientp.t_isemployer, chr(0)) =
                                          chr(88) then
                                      'ИП'
                                     else
                                      'ФЛ'
                                   end t_clientform,
                                   case when client.t_legalform is null then
                                      chr(1)
                                     when client.t_legalform = 1 then
                                      nvl(clientc.t_name, chr(1))                             
                                     else case when EXISTS (SELECT 1 FROM DPERSN_DBT persn where persn.t_personid = client.t_partyid  and persn.t_isstateless = chr(88))
                                               THEN 'Лицо без гражданства'
                                               when nvl(clientc.t_name, '') != '' THEN nvl(clientc.t_name, '')
                                               when exists (select 1 from DPERSNCIT_DBT where t_personid = dl_tick.t_client) 
                                               then (select t_name from (select ctry.t_name from DPERSNCIT_DBT pit, dcountry_dbt ctry where pit.t_personid = dl_tick.t_client
                                                                                         and ctry.t_parentcountryid = 0 AND
                                                                                         ctry.t_codelat3 = pit.T_COUNTRYCODELAT3 ORDER BY pit.T_COUNTRYCODELAT3) WHERE ROWNUM = 1)
                                               else nvl(clientc.t_name, '')
                                               end                                    
                                      end 
                                    t_clientcountry,
                                   case
                                     when client.t_notresident is null then
                                      chr(1)
                                     when client.t_notresident = chr(88) then
                                      'нерезидент'
                                     else
                                      'резидент'
                                   end t_clientresident,
                                   case when rsb_common.GetRegFlagValue(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\РУБИЛЬНИК.BOSS-5689') <> CHR(88) 
                                    then
                                   (select 
                                      case when max(kval.attrid) = 2 then 'квалинвестор'
                                           --when kval.attrid = 1 then 'не квалинвестор' DEF-81827
                                           else 'не квалинвестор' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 207
                                       and  objattr.t_groupid = 140
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(dlcontr.t_dlcontrid, 34, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) kval) 
                                    else 
                                      (select case when max(cnt)>0 then 'квалинвестор' else 'не квалинвестор' end
                                         from
                                      (select count(*) as cnt from dscqinv_dbt d
                                      where d.t_partyid = client.t_partyid  and d.t_state = 1))
                                    end t_clientcvalinv,
                                   nvl(contragent.t_name, chr(1)) t_side2name,
                                   nvl(contragent.t_name, chr(1)) t_contragentname,
                                   case 
                                        when dl_tick.t_subkind_operation = 10 then 'поставка'
                                        else 'оплата'
                                   end t_execmethod,
                                   case
                                     when market.t_partyid is not null then 'платежное поручение'
                                     else
                                      'платежное поручение в формате SWIFT'
                                   end t_formpayment,
                                   dlcontr.t_dlcontrid t_dlcontrid,
                                   dl_tick.t_client
                              from DNPTXOP_DBT dl_tick
                              join doprkoper_dbt oprkoper
                                on oprkoper.t_kind_operation =
                                   dl_tick.t_kind_operation
                              join (select BeginDate bdt, EndDate edt
                                      from dual)
                                 on 1 = 1
                              left join dparty_dbt market
                                on market.t_partyid = dl_tick.t_marketplace2
                              left join dparty_dbt client
                                on client.t_partyid = dl_tick.t_client 
                              left join dpersn_dbt clientp
                                on clientp.t_personid = client.t_partyid
                              left join dcountry_dbt clientc
                                on (clientc.t_parentcountryid = 0 AND
                                   clientc.t_codelat3 = client.t_nrcountry)
                             left join ddlcontrmp_dbt dlcontrmp
                                on (dlcontrmp.t_id = dl_tick.t_contract and dl_tick.t_subkind_operation = 30 or dlcontrmp.t_sfcontrid = dl_tick.t_contract and dl_tick.t_subkind_operation != 30)
                              left join dsfcontr_dbt sfcontrmp
                                on sfcontrmp.t_id = dlcontrmp.t_sfcontrid
                              left join ddlcontr_dbt dlcontr
                                on dlcontr.t_dlcontrid =
                                   dlcontrmp.t_dlcontrid
                              left join dsfcontr_dbt sfcontr
                                on sfcontr.t_id = dlcontr.t_sfcontrid
                              left join dparty_dbt contragent
                                on contragent.t_partyid = 1
                              left join dfininstr_dbt fininstr
                                on fininstr.t_fiid = dl_tick.t_currency
                             left join ddlmarket_dbt dlmarket
                                 on dlmarket.t_id = dl_tick.t_place2
                              left join ddlobjcode_dbt dlobjcode 
                                on dlcontr.t_dlcontrid = dlobjcode.t_objectid 
                               and dlobjcode.t_objecttype = 207 
                               and dlobjcode.t_codekind = 1
                               --Если есть ЕКК с нулевой датой - отбираем его, если нет - последний закрытый код  
                               and dlobjcode.t_bankclosedate = case when exists (select 1 from ddlobjcode_dbt dlobjcodesub1 where dlobjcodesub1.t_objectid = dlobjcode.t_objectid and dlobjcodesub1.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub1.t_codekind = dlobjcode.t_codekind and dlobjcodesub1.t_bankclosedate = to_date('01010001','ddmmyyyy')  ) 
                                   then to_date('01010001','ddmmyyyy') 
                                   else (select max(dlobjcodesub2.t_bankclosedate) from ddlobjcode_dbt dlobjcodesub2 where dlobjcodesub2.t_objectid = dlobjcode.t_objectid and dlobjcodesub2.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub2.t_codekind = dlobjcode.t_codekind) END
                             where (sfcontr.t_id is NULL or
                                   (chr(88) = chr(88) and
                                   sfcontrmp.t_servkind = 1) or
                                   (chr(88) = chr(88) and
                                   sfcontrmp.t_servkind = 15) or
                                   (chr(88) = chr(88) and
                                   sfcontrmp.t_servkind = 21))
                               and  (1 = (CASE WHEN Sector_Brokers = CHR(88) and (client.t_partyid is not null and not exists
                                      (select 1
                                             from ddp_dep_dbt dp_dep
                                            where dp_dep.t_partyid =
                                                  client.t_partyid)) THEN 1 ELSE 0 END))
                              and (     SelectedClients = 1 
                                     and EXISTS(SELECT * FROM D_RIA_PANELCLIENTS_DBT ria_cl WHERE ria_cl.t_clientid = client.t_partyid and ria_cl.t_sessionid = 1 and ria_cl.T_CALCID = 1)
                                             
                                      or SelectedClients = 0) 
                               and (     SelectedContrs = 1 
                                     and EXISTS(SELECT * FROM D_RIA_PANELCONTRS_DBT ria_c WHERE ria_c.t_clientid = client.t_partyid and ria_c.t_dlcontrid = dlcontrmp.t_dlcontrid and ria_c.t_sessionid = 1 and ria_c.T_CALCID = 1)
                                             
                                      or SelectedContrs = 0)
                               and dl_tick.t_status > 0 and dl_tick.t_dockind = 4607 and dl_tick.t_calcndfl = CHR(0) and dl_tick.t_operdate >= bdt and dl_tick.t_operdate <= edt
                                                           ) dt) LOOP

      registeria.t_part := 1;
      

      registeria.t_dealsubkind := one_rec.t_dealsubkind;

      registeria.t_dealid      := one_rec.t_dealid;
      registeria.t_dockind     := one_rec.t_dockind;
      registeria.t_dealpart    := one_rec.t_dealpart;
      
      IF Block_Deals = CHR(88) THEN
          registeria.t_dealcode    := one_rec.t_dealcode;

          registeria.t_dealdate    := one_rec.t_dealdate;
          registeria.t_marketname  := one_rec.t_marketname;
          if (one_rec.t_sector = chr(88)) then
            registeria.t_markettype := CONCAT(CONCAT(one_rec.t_depsetcode,
                                                     '/'),
                                              one_rec.t_markettype);
          else
            registeria.t_markettype := NULL;
          end if;

            registeria.t_dealtype     := 'клиентская';
            registeria.t_status_purcb := one_rec.t_status_purcb;
            registeria.t_side1        := one_rec.t_clientname;
            registeria.t_client_assign := one_rec.t_client_assign;


          registeria.t_side2           := one_rec.t_side2name;
          registeria.t_confirmdoc      := 'документы получены';
          registeria.t_execmethod      := one_rec.t_execmethod;
          registeria.t_formpayment     := one_rec.t_formpayment;
          registeria.t_dealdirection   := one_rec.t_side;
          registeria.t_dealkind        := one_rec.t_side2;
      END IF;
      
      if (Block_Clients = CHR(88)) then
        registeria.t_clientname     := one_rec.t_clientshortname;
        registeria.t_clientagr      := one_rec.t_sfcontrnumber;
        registeria.t_clientagrdate  := one_rec.t_sfcontrbegdate;
        registeria.t_clientagrend   := one_rec.t_sfcontrenddate;
        registeria.t_exchangecode   := one_rec.t_mpcode;
        registeria.t_proxyname      := one_rec.t_proxyname;
        registeria.t_proxynum       := one_rec.t_proxynum;
        registeria.t_clientform     := one_rec.t_clientform;
        registeria.t_clientcountry  := one_rec.t_clientcountry;
        registeria.t_clientresident := one_rec.t_clientresident;
        registeria.t_clientcvalinv  := one_rec.t_clientcvalinv;
        registeria.t_risklevel      := one_rec.t_risklevel;
        registeria.t_cashright      := one_rec.t_cashright;
      else
        registeria.t_clientname     := NULL;
        registeria.t_clientagr      := NULL;
        registeria.t_clientagrdate  := NULL;
        registeria.t_clientagrend   := NULL;
        registeria.t_exchangecode   := NULL;
        registeria.t_proxyname      := NULL;
        registeria.t_proxynum       := NULL;
        registeria.t_clientform     := NULL;
        registeria.t_clientcountry  := NULL;
        registeria.t_clientresident := NULL;
        registeria.t_clientcvalinv  := NULL;
        registeria.t_risklevel      := NULL;
        registeria.t_cashright      := NULL;
      end if;

      --служебные поля
      registeria.t_clientid      := one_rec.clientid;
      registeria.t_clientcontrid := one_rec.clientcontrid;
      registeria.t_pfi           := one_rec.pfi;

      registeria.t_sessionid := V_SESSIONID;
      registeria.t_calcid := V_CALCID;

      g_registeria_ins.extend;
      g_registeria_ins(g_registeria_ins.LAST) := registeria;

    END LOOP;

    IF g_registeria_ins IS NOT EMPTY THEN
      FORALL i IN g_registeria_ins.FIRST .. g_registeria_ins.LAST
        INSERT INTO DDL_REGIABUF_DBT VALUES g_registeria_ins (i);
      g_registeria_ins.delete;
    END IF;

    PushLogLine('операций Зачисления/Списания ДС - end');

    IF Block_InAcc = CHR(88) THEN
      if StockMarket = CHR(88) then
        AddInOutAccountsSec(BeginDate, EndDate);
      end if;
      if FuturesMarket = CHR(88) or CurrencyMarket = CHR(88) then
        AddInOutAccountsDV(BeginDate, EndDate);
      end if;
    END IF;

  END CreateData_OprInOut;

  
  PROCEDURE CorrectData_SC_Deals(BeginDate IN DATE, EndDate IN DATE) IS
  BEGIN
    AddSCAccountsP(BeginDate, EndDate);
    AddSCAccountsM(BeginDate, EndDate);
  END CorrectData_SC_Deals;
 
  PROCEDURE CreateData_SC_Deals( BeginDate      IN DATE,
                                EndDate        IN DATE,
                                StockMarket    IN CHAR,
                                FuturesMarket  IN CHAR,
                                CurrencyMarket IN CHAR,
                                Sector_Brokers IN CHAR,
                                Sector_Dilers  IN CHAR,
                                Sector_Clients IN CHAR,
                                Block_Deals    IN CHAR,
                                Block_Clients  IN CHAR,
                                Block_InAcc    IN CHAR,
                                SelectedClients IN NUMBER,
                                SelectedContrs   IN NUMBER) as
   v_calc_id number;
   v_code varchar2(30000);
  begin
  PushLogLine('Отбор сделок с ЦБ - begin','Отбор сделок с ЦБ');
   v_calc_id := it_parallel_exec.init_calc;
   insert into itt_parallel_exec
    (calc_id
    ,num01
    ,num02
    ,num03
    ,num04
    ,num05
    ,str01
    ,num06
    ,num07
    ,dat01
    ,num08
    ,dat02
    ,num09
    ,str02
    ,num10
    ,num11
    ,num12
    ,num13
    ,num14
    ,num15
    ,num16
    ,str03
    ,str04)
    select v_calc_id
          ,dl_tick.t_DealType -- 1 NUMBER(5)
          ,dl_tick.t_clientid -- 2 NUMBER(10)
          ,dl_tick.t_brokerid -- 3  NUMBER(10)
          ,dl_tick.t_brokercontrid -- 4 NUMBER(10)
          ,dl_tick.t_pfi -- 5  NUMBER(10)
          ,dl_tick.t_ispartyclient -- 1 CHAR(1)
          ,dl_tick.t_partyid --  6 NUMBER(10)
          ,dl_tick.t_dealid --  7 NUMBER(10)
          ,dl_tick.t_dealdate -- 1 date
          ,dl_tick.t_bofficekind -- 8  NUMBER(5)
          ,dl_tick.t_dealtime -- 2 date
          ,dl_tick.t_genagrid --  9 NUMBER(10)
          ,dl_tick.T_USERFIELD1 -- 2 varchar2(120)
          ,dl_tick.t_traderid --  10 NUMBER(10)
          ,dl_tick.t_depsetid --  11 NUMBER(10)
          ,dl_tick.t_marketschemeid -- 12 NUMBER(10)
          ,dl_tick.t_dealstatus --  13 NUMBER(5)
          ,dl_tick.t_marketid  --  14 NUMBER(10)
          ,dl_tick.t_clientcontrid --  15 NUMBER(10)
          ,dl_tick.t_partycontrid
          ,dl_tick.t_dealcodets -- 3 varchar2(30)
          ,dl_tick.t_dealcode  -- 4 varchar2(30)
      from table(rsb_brkrep_u.SelectDealExecDate(BeginDate, EndDate)) dl_tick; 
    commit;
   v_code := 'begin rsb_dlregisteria.SetCalcIds('||TO_CHAR(V_SESSIONID)||', '||TO_CHAR(V_CALCID)||'); 
   rsb_dlregisteria.CreateData_SC_Deals_parallel(Calc_id => '||v_calc_id||',
                                          Row_ID_start    => :start_id,
                                          Row_ID_end      => :end_id,
                                          BeginDate       => '||it_parallel_exec.Date_to_sql(BeginDate)||',        
                                          EndDate         => '||it_parallel_exec.Date_to_sql(EndDate)||',     
                                          StockMarket     => '||it_parallel_exec.Str_to_sql(StockMarket)||',         
                                          FuturesMarket   => '||it_parallel_exec.Str_to_sql(FuturesMarket)||',       
                                          CurrencyMarket  => '||it_parallel_exec.Str_to_sql(CurrencyMarket)||',      
                                          Sector_Brokers  => '||it_parallel_exec.Str_to_sql(Sector_Brokers)||',      
                                          Sector_Dilers   => '||it_parallel_exec.Str_to_sql(Sector_Dilers)||',       
                                          Sector_Clients  => '||it_parallel_exec.Str_to_sql(Sector_Clients)||',      
                                          Block_Deals     => '||it_parallel_exec.Str_to_sql(Block_Deals)||',         
                                          Block_Clients   => '||it_parallel_exec.Str_to_sql(Block_Clients)||',       
                                          Block_InAcc     => '||it_parallel_exec.Str_to_sql(Block_InAcc)||',         
                                          SelectedClients => '||SelectedClients||',                                  
                                          SelectedContrs  => '||SelectedContrs||') ;                                 
    exception when others then
         it_error.put_error_in_stack;
         it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
         raise;
    end;';
  it_log.log(p_msg => 'START parallel',p_msg_clob => v_code) ;
  it_parallel_exec.run_task_chunks_by_calc(p_parallel_level => 8,
                                           p_id             => v_calc_id,
                                           p_sql_stmt       => v_code);

   it_parallel_exec.clear_calc(v_calc_id);
   
   PushLogLine('Отбор сделок с ЦБ - end');

   IF Block_InAcc = CHR(88) THEN
      CorrectData_SC_Deals(BeginDate, EndDate);
   END IF;
  end;
   
    
  PROCEDURE CreateData_SC_Deals_parallel(Calc_id number,
                                Row_ID_start number,
                                Row_ID_end   number,
                                BeginDate      IN DATE,
                                EndDate        IN DATE,
                                StockMarket    IN CHAR,
                                FuturesMarket  IN CHAR,
                                CurrencyMarket IN CHAR,
                                Sector_Brokers IN CHAR,
                                Sector_Dilers  IN CHAR,
                                Sector_Clients IN CHAR,
                                Block_Deals    IN CHAR,
                                Block_Clients  IN CHAR,
                                Block_InAcc    IN CHAR,
                                SelectedClients IN NUMBER,
                                SelectedContrs   IN NUMBER) IS
    TYPE registeria_t IS TABLE OF DDL_REGIABUF_DBT%ROWTYPE;
    g_registeria_ins registeria_t := registeria_t();
    registeria       DDL_REGIABUF_DBT%rowtype;
    
    v_plandatem DATE;
    v_plandatemcnt INTEGER := 0;
    v_plandatemstr VARCHAR2(128);
    
    v_factdatem DATE;
    v_factdatemcnt INTEGER := 0;
    v_factdatemstr VARCHAR2(128);
    
  BEGIN
    it_log.log(Calc_id||' Start:'||Row_ID_start||' End:'||Row_ID_end) ;
    FOR one_rec IN (select dt.clientid,
                           dt.deal_clientid,
                           dt.clientcontrid,
                           dt.pfi,
                           dt.t_dealid,
                           dt.t_bofficekind as t_dockind,
                           dt.t_dealdate,
                           dt.t_dealpart,
                           dt.t_isbank,
                           dt.t_ispartyclient,
                           dt.t_dealcode,
                           dt.t_dealcodets,
                           dt.t_dealdates,
                           dt.t_dealtime,
                           dt.t_planexecdate,
                           dt.t_marketname,
                           dt.t_depsetcode,
                           dt.t_invert_debet_credit,
                           (select t.t_ShortName
                              from dparty_dbt t
                             where t.t_Partyid = dt.brokerid) t_brokername,
                           (select t.t_Number
                              from dsfcontr_dbt t
                             where t.t_id = dt.brokercontrid) t_brokercontrnumber,
                           dt.t_sfcontrnumber,
                           dt.t_sfcontrbegdate,
                           decode(dt.t_sfcontrenddate, to_date('01.01.0001','DD.MM.YYYY'), null, dt.t_sfcontrenddate) t_sfcontrenddate,
                           dt.t_clientname,
                           dt.t_clientshortname,
                           dt.t_clientptcode,
                           dt.t_mpcode,
                           dt.t_clientform,
                           dt.t_clientcountry as t_clientcountry,
                           (case when dt.t_clientform = 'ФЛ' then dt.t_clientresident else '' end) as t_clientresident,
                           dt.t_clientcvalinv,
                           dt.t_side2name,
                           dt.t_genagr,
                           dt.t_contragentname,
                           (case when dt.t_isBank = 0 and dt.t_invert_debet_credit = 0 then
                           nvl((SELECT SUM(nvl(dlc.t_Sum, 0))
                                  FROM ddlcomis_dbt dlc
                                 WHERE dlc.t_DocKind = dt.t_bofficekind
                                   AND dlc.t_DocID = dt.t_dealid
                                   AND dlc.t_IsBack = decode(dt.t_dealpart,1,chr(0),chr(88))
                                   AND dlc.t_ReceiverID in (RsbSessionData.OurBank, 0)), 0) end)
                            t_brokercomiss,
                           ( case when RSB_SECUR.IsAvrWrtIn(RSB_SECUR.get_OperationGroup(rsb_secur.get_OperSysTypes(dt.t_DealType,
                                                                                                            dt.t_BofficeKind))) = 1  or
                           RSB_SECUR.IsAvrWrtOut(RSB_SECUR.get_OperationGroup(rsb_secur.get_OperSysTypes(dt.t_DealType,
                                                                                                            dt.t_BofficeKind))) = 1  then
                               (select spgr.t_XLD 
                              from dspground_dbt spgr, dspgrdoc_dbt doc
                             where doc.t_SourceDocKind = dt.t_bofficekind
                               and doc.t_SourceDocID = dt.t_dealid
                               and spgr.t_SPgroundID = doc.t_SPgroundID
                               and spgr.t_Kind = 299 /*поручение депо*/
                               and rownum = 1)                                                                             
                           when t_marketname != 'Внебиржевой рынок' or (rsb_secur.IsOTC(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(dt.t_DealType,
                                                                                                            dt.t_BofficeKind))) = 1) then
                              ( SELECT r.t_codets
                                  FROM ddl_req_dbt  r
                                       JOIN dspgrdoc_dbt rd
                                           ON r.t_id = rd.t_SOURCEDOCID AND rd.t_sourcedockind = 350
                                       JOIN dspgrdoc_dbt spgrdoc ON rd.t_SPgroundID = spgrdoc.t_SPGroundID
                                 WHERE spgrdoc.t_SourceDocKind = dt.t_bofficekind AND spgrdoc.t_SourceDocID = dt.t_dealid and rownum = 1)
                           else
                             (select spgr.t_XLD 
                              from dspground_dbt spgr, dspgrdoc_dbt doc
                             where doc.t_SourceDocKind = dt.t_bofficekind
                               and doc.t_SourceDocID = dt.t_dealid
                               and spgr.t_SPgroundID = doc.t_SPgroundID
                               and spgr.t_Kind = 251 /*поручение клиента на операцию*/
                               and rownum = 1) end) t_client_assign,
                           case
                             when dt.t_isbank = 1 then
                              'С'
                             else
                              'К'
                           end t_dealtype,
                           case when dt.t_bofficekind != 127 then
                           (case
                             when dt.t_dealpart = 1 then
                              decode(dt.t_isbuy,
                                     1,
                                     'Покупка',
                                     'Продажа')
                             when dt.t_dealpart = 2 then
                              decode(dt.t_isbuy,
                                     1,
                                     'Продажа',
                                     'Покупка')
                             else
                              chr(1)
                           end) end t_side,
                           dt.t_sector,
                           case
                             when (select trim(lower(nvl(max(t_name), chr(1))))
                                     from dobjattr_dbt  objattr,
                                          dobjatcor_dbt objatcor
                                    where objattr.t_objecttype =
                                          dt.t_bofficekind
                                      and objattr.t_groupid = 103
                                      and /*103*/
                                          objatcor.t_objecttype =
                                          objattr.t_objecttype
                                      and objatcor.t_groupid =
                                          objattr.t_groupid
                                      and objatcor.t_attrid = objattr.t_attrid
                                      and objatcor.t_object =
                                          lpad(dt.t_dealid, 34, '0')
                                      and objatcor.t_validfromdate <=
                                          dt.t_dealdate
                                      and objatcor.t_validtodate >
                                          dt.t_dealdate) = 'да' then
                              'СпецРЕПО'
                             else
                              case
                                when dt.t_hastwopart = 1 then
                                 dt.t_opername || ' ' || dt.t_dealpart || ' ч'
                                else
                                 dt.t_opername
                              end
                           end t_side2,
                           dt.t_firootname,
                           dt.t_avrname,
                           dt.t_avrcode,
                           dt.t_issue,
                           dt.t_tranche,
                           dt.t_series,
                           dt.t_finame,
                           dt.t_isin,
                           dt.t_lsin,
                           dt.t_issuername,
                           dt.t_cost,
                           dt.t_currencyname,
                           (case when t_isrepo = 1 or t_isloan = 1 then dt.t_incomerate end) t_incomerate,
                           (case when t_isrepo = 1 or t_isloan = 1 then t_returnincome end) t_returnincome,
                           (case when dt.t_isbasket = 1 then 1 else dt.t_principal end ) t_principal,
                           dt.t_totalcost,
                           dt.t_dvalue, /*GAA:496249*/
                           case when dt.t_bofficekind != 127 then dt.t_curpfi end t_currencyname2,
                           (select max(dlrq.t_plandate)
                              from ddlrq_dbt dlrq
                             where dlrq.t_dockind = dt.t_bofficekind
                               and dlrq.t_docid = dt.t_dealid
                               and dlrq.t_dealpart = dt.t_dealpart
                               and dlrq.t_subkind = 1 /*DLRQ_SUBKIND_AVOIRISS*/
                               and dlrq.t_type = 8 /*DLRQ_TYPE_DELIVERY*/
                            ) t_plandatesp,
                           (select  max(dlrq.t_factdate)
                              from ddlrq_dbt dlrq
                             where dlrq.t_dockind = dt.t_bofficekind
                               and dlrq.t_docid = dt.t_dealid
                               and dlrq.t_dealpart = dt.t_dealpart
                               and dlrq.t_subkind = 1 /*DLRQ_SUBKIND_AVOIRISS*/
                               and dlrq.t_type = 8 /*DLRQ_TYPE_DELIVERY*/
                            ) t_factdatesp,
                           dt.t_curpfi t_curpfi, -- валюта цены
                           case when dt.t_relativeprice=chr(88) then
                           (select t_ccy
                              from dfininstr_dbt
                             where t_fiid = dt.t_facevaluefi) end t_facevaluefi, -- валюта номинала
                           dt.t_conftp,
                           (select 'да'
                              from dobjattr_dbt  objattr,
                                   dobjatcor_dbt objatcor
                             where objattr.t_objecttype = 12
                               and objattr.t_groupid = 28
                               and objatcor.t_objecttype =
                                   objattr.t_objecttype
                               and objatcor.t_groupid = objattr.t_groupid
                               and objatcor.t_attrid = objattr.t_attrid
                               and objatcor.t_object = lpad(dt.pfi, 10, '0')
                               and objatcor.t_validfromdate <= edt
                               and objatcor.t_validtodate > edt
                               and objatcor.t_attrid = 2
                               and rownum = 1) t_qualification,
                           (select trim(max(nvl(objattr.t_name,
                                                objattr.t_nameobject)))
                              from dobjattr_dbt  objattr,
                                   dobjatcor_dbt objatcor
                             where objattr.t_objecttype = 101
                               and objattr.t_groupid = 106
                               and objatcor.t_objecttype =
                                   objattr.t_objecttype
                               and objatcor.t_groupid = objattr.t_groupid
                               and objatcor.t_attrid = objattr.t_attrid
                               and objatcor.t_object =
                                   lpad(dt.t_dealid, 34, '0')
                               and objatcor.t_validfromdate <= edt
                               and objatcor.t_validtodate > edt
                               and rownum = 1) t_markettype,
                           (select decode(objattr.t_name,
                                          'Да',
                                          'безадресная',
                                          'адресная')
                              from dobjattr_dbt  objattr,
                                   dobjatcor_dbt objatcor
                             where objattr.t_objecttype = 101
                               and objattr.t_groupid = 52
                               and objatcor.t_objecttype =
                                   objattr.t_objecttype
                               and objatcor.t_groupid = objattr.t_groupid
                               and objatcor.t_attrid = objattr.t_attrid
                               and objatcor.t_object =
                                   lpad(dt.t_dealid, 34, '0')
                               and objatcor.t_validfromdate <= edt
                               and objatcor.t_validtodate > edt
                               and rownum = 1) t_dealtypeAdr,
                           case when dt.clientcontrid > 0 then
                           decode(dt.t_role, 0,
                           (select lower(objattr.t_name)
                              from dobjattr_dbt  objattr,
                                   dobjatcor_dbt objatcor
                             where objattr.t_objecttype = 659
                               and objattr.t_groupid = 1
                               and objatcor.t_objecttype =
                                   objattr.t_objecttype
                               and objatcor.t_groupid = objattr.t_groupid
                               and objatcor.t_attrid = objattr.t_attrid
                               and objatcor.t_object =
                                   lpad(dt.clientcontrid, 10, '0')
                               and objatcor.t_validfromdate <= edt
                               and objatcor.t_validtodate > edt
                               and rownum = 1),
                           (select lower(objattr.t_name)
                              from dobjattr_dbt objattr
                             where objattr.t_objecttype = 659
                               and objattr.t_groupid = 1
                               and objattr.t_attrid = dt.t_role
                               and rownum = 1))
                           else
                             null
                           end t_status_purcb,
                           dt.t_execmethod,
                           dt.t_formpayment,
                           case
                             when dt.t_isbank = 1 or dt.t_dlcontrid is null then
                              chr(1)
                             else
                              case when dt.servkind = 1 then
                                      (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 659
                                       and  objattr.t_groupid = 6
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(dt.clientcontrid, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) pravo)
                                   else
                                       (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                       (select NVL(objatcor.t_attrid, -1) attrid  
                                          from dobjattr_dbt  objattr,
                                               dobjatcor_dbt objatcor,
                                               DDLCONTRMP_DBT contrmp_pr,
                                               DSFCONTR_DBT sfcontr_pr
                                         where contrmp_pr.T_DLCONTRID = dt.t_dlcontrid
                                           and sfcontr_pr.t_id = contrmp_pr.t_sfcontrid
                                           and sfcontr_pr.t_servkind = 1
                                           and objattr.t_objecttype = 659
                                           and objattr.t_groupid = 6
                                           and objatcor.t_objecttype =
                                               objattr.t_objecttype
                                           and objatcor.t_groupid =
                                               objattr.t_groupid
                                           and objatcor.t_attrid =
                                               objattr.t_attrid
                                           and objatcor.t_object =
                                               lpad(sfcontr_pr.t_id, 10, '0')
                                           and objatcor.t_validfromdate <= edt
                                           and objatcor.t_validtodate > edt ORDER BY objatcor.t_attrid) pravo WHERE ROWNUM = 1)
                                   end
                           end t_cashright,
                           case
                             when dt.t_isbank = 1 then
                              chr(1)
                             else
                               (select trim(nvl(max(decode(t_attrid, 1, 'КСУР', decode(t_attrid, 2, 'КПУР', decode(t_attrid, 3, 'КОУР', decode(t_attrid, 4, 'КНУР'))))),
                                                          'отсутствует'))
                                      from dobjatcor_dbt objatcor
                                     where objatcor.t_objecttype = 3
                                       and /*Субъект экономики*/
                                           objatcor.t_groupid = 95
                                       and objatcor.t_object =
                                           lpad(clientid, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt)
                           end t_risklevel,
                           case
                             when dt.t_isbank = 1 then
                              chr(1)
                             else
                              nvl((select listagg(clientproxy.t_name, '/') within group(order by clientproxy.t_name, ptproxy.t_proxyid)
                                    from dptproxy_dbt ptproxy
                                    join dparty_dbt clientproxy
                                      on clientproxy.t_partyid =
                                         ptproxy.t_proxyid
                                   where ptproxy.t_partyid = dt.t_clientid
                                     and ptproxy.t_docdate <= edt
                                     and ptproxy.t_validitydate >= edt),
                                  chr(1))
                           end t_proxyname,
                           case
                             when dt.t_isbank = 1 then
                              chr(1)
                             else
                              nvl((select listagg(ptproxy.t_docnumber || ' с ' ||
                                                 to_char(ptproxy.t_docdate,
                                                         'dd.mm.yyyy') ||
                                                 ' по ' ||
                                                 to_char(ptproxy.t_validitydate,
                                                         'dd.mm.yyyy'),
                                                 '/') within group(order by ptproxy.t_proxyid, ptproxy.t_proxyid)
                                    from dptproxy_dbt ptproxy
                                    join dparty_dbt clientproxy
                                      on clientproxy.t_partyid =
                                         ptproxy.t_proxyid
                                   where ptproxy.t_partyid = dt.t_clientid
                                     and ptproxy.t_docdate <= edt
                                     and ptproxy.t_validitydate >= edt),
                                  chr(1))
                           end t_proxynum,
                           dt.t_pricefiid,
                           dt.t_princprecision,
                           dt.t_isBasket, /*GAA:495699*/
                           dt.REJECT_DEAL, /*GAA:503363 */
                           nvl(
                           (Select rsb_struct.getString(note.t_text) 
                                   from dnotetext_dbt note 
                                   where note.t_notekind=408 
                                      and note.t_documentid in (lpad(dt.t_dealid,34,'0')) 
                                      and note.t_objecttype = (CASE WHEN dt.t_bofficekind = 117 THEN 117 ELSE 101 END)
                           ),'') t_cond_susp
                      from (select /*+ index(dl_leg DDL_LEG_DBT_IDX0) */ bdt,
                                   edt,
                                   dl_tick.t_DealType,
                                   client.t_partyid clientid,
                                   dl_tick.t_clientid deal_clientid,
                                   sfcontrmp.t_id clientcontrid,
                                   sfcontrmp.t_servkind servkind,
                                   dl_tick.t_brokerid brokerid,
                                   dl_tick.t_brokercontrid brokercontrid,
                                   dl_tick.t_pfi pfi,
                                   dl_tick.t_ispartyclient,
                                   dl_leg.t_relativeprice,
                                   dlcontrmp.t_role,
                                   dlcontrmp.t_mpcode,
                                   fininstr.t_facevaluefi t_facevaluefi,
                                   fininstr.t_name t_avrname,
                                   fininstr.t_fi_code t_avrcode,
                                   oprkoper.t_name t_opername,
                                   dlmarket.t_code t_depsetcode,
                                   (case 
                                   when dl_tick.t_ispartyclient = chr(0) then 0
                                   when  client.t_partyid = dl_tick.t_partyid then 1
                                   else 0
                                  end) t_invert_debet_credit,
                                   case
                                     when client.t_partyid is null or exists
                                      (select 1
                                             from ddp_dep_dbt dp_dep
                                            where dp_dep.t_partyid =
                                                  client.t_partyid) then
                                      1
                                     else
                                      0
                                   end t_isbank,
                                   dl_tick.t_dealid t_dealid,
                                   dl_tick.t_dealdate t_dealdate,
                                   dl_tick.t_bofficekind t_bofficekind,
                                   rsb_brkrep_u.GetPlanExecDate(dl_tick.t_bofficekind,
                                                                dl_tick.t_dealid,
                                                                decode(dl_leg.t_legkind,
                                                                       0,
                                                                       1,
                                                                       2)) t_planexecdate,
                                   RSB_SECUR.IsRepo(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(dl_tick.t_DealType,
                                                                                                            dl_tick.t_BofficeKind))) t_isrepo,
                                   RSB_SECUR.IsLoan(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(dl_tick.t_DealType,
                                                                                                            dl_tick.t_BofficeKind))) t_isloan,
                                   case when not regexp_like(oprkoper.t_systypes, '[v]') and not (dl_tick.t_bofficekind = 127 and market.t_shortname is null) and
                                         (rsb_secur.IsOTC(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(dl_tick.t_DealType,
                                                                                                            dl_tick.t_BofficeKind))) = 0) 
                                                                                                            then chr(88)
                                   else chr(0) end t_sector,
                                   /*RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(dl_tick.t_DealType, dl_tick.t_BofficeKind))) t_isbuy, */
                                    (case when dl_tick.t_ispartyclient='X' and client.t_partyid != dl_tick.t_clientid then RSB_SECUR.IsSale(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(dl_tick.t_DealType,
                                                                                                              dl_tick.t_BofficeKind))) else RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(dl_tick.t_DealType,
                                                                                                              dl_tick.t_BofficeKind))) end)  t_isbuy,
                                    /*RSB_SECUR.IsBuy(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(dl_tick.t_DealType,
                                                                                                              dl_tick.t_BofficeKind))) t_isbuy,*/
                                   decode(dl_leg.t_legkind, 0, 1, 2) t_dealpart,
                                   dl_tick.t_dealcode,
                                   dl_tick.t_dealcodets,
                                   to_char(dl_tick.t_dealdate, 'dd.mm.yyyy') t_dealdates,
                                   dl_tick.t_dealtime,
                                   case when exists (select 1 from dpartyown_dbt where t_partyid = market.t_partyid and t_partykind=3) 
                                         then market.t_name
                                         else case when exists (select 1 from dpartyown_dbt where t_partyid = agent.t_partyid and t_partykind=3) 
                                         then agent.t_name 
                                         else 'Внебиржевой рынок' end
                                    end t_marketname,
                                   nvl(sfcontrmp.t_number, chr(1)) t_sfcontrnumber,
                                   nvl(sfcontr.t_name, chr(1)) t_sfcontrname,
                                   --decode(sfcontr.t_datebegin, to_date('01.01.0001', 'dd.mm.yyyy'), null, sfcontr.t_datebegin) t_sfcontrbegdate,
                                   decode(to_char(sfcontr.t_datebegin, 'dd.mm.yyyy'), '01.01.0001', null, to_char(sfcontr.t_datebegin, 'dd.mm.yyyy')) t_sfcontrbegdate,
                                   --decode(sfcontr.t_dateclose, to_date('01.01.0001', 'dd.mm.yyyy'), null, sfcontr.t_dateclose) t_sfcontrenddate,
                                   decode(to_char(sfcontr.t_dateclose, 'dd.mm.yyyy'), '01.01.0001', null, to_char(sfcontr.t_dateclose, 'dd.mm.yyyy')) t_sfcontrenddate,
                                   nvl(client.t_name, chr(1)) t_clientname,
                                   (nvl(client.t_shortname, chr(1)) || 
                                   CASE WHEN dlobjcode.t_code IS NOT NULL THEN '/' || dlobjcode.t_code else '' end) t_clientshortname,
                                   RSI_RSBPARTY.PT_GetPartyCode(client.t_PartyID,
                                                                1) t_clientptcode,
                                   case
                                     when client.t_legalform is null then
                                      chr(1)
                                     when client.t_legalform = 1 then
                                      'ЮЛ'
                                     when nvl(clientp.t_isemployer, chr(0)) =
                                          chr(88) then
                                      'ИП'
                                     else
                                      'ФЛ'
                                   end t_clientform,
                                   case when client.t_legalform is null then
                                      chr(1)
                                     when client.t_legalform = 1 then
                                      nvl(clientc.t_name, chr(1))                             
                                     else case when EXISTS (SELECT 1 FROM DPERSN_DBT persn where persn.t_personid = client.t_partyid  and persn.t_isstateless = chr(88))
                                               THEN 'Лицо без гражданства'
                                               when nvl(clientc.t_name, '') != '' THEN nvl(clientc.t_name, '')
                                               when exists (select 1 from DPERSNCIT_DBT where t_personid = dl_tick.t_clientid) 
                                               then (select t_name from (select ctry.t_name from DPERSNCIT_DBT pit, dcountry_dbt ctry where pit.t_personid = dl_tick.t_clientid 
                                                                                         and ctry.t_parentcountryid = 0 AND
                                                                                         ctry.t_codelat3 = pit.T_COUNTRYCODELAT3 ORDER BY pit.T_COUNTRYCODELAT3) WHERE ROWNUM = 1)
                                               else nvl(clientc.t_name, '')
                                               end                                    
                                      end 
                                    t_clientcountry,
                                   case
                                     when client.t_notresident is null then
                                      chr(1)
                                     when client.t_notresident = chr(88) then
                                      'нерезидент'
                                     else
                                      'резидент'
                                   end t_clientresident,
                                   case when rsb_common.GetRegFlagValue(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\РУБИЛЬНИК.BOSS-5689') <> CHR(88) 
                                    then
                                   (select 
                                      case when max(kval.attrid) = 2 then 'квалинвестор'
                                           --when kval.attrid = 1 then 'не квалинвестор' DEF-81827
                                           else 'не квалинвестор' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 207
                                       and  objattr.t_groupid = 140
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(dlcontr.t_dlcontrid, 34, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) kval) 
                                    else 
                                      (select case when max(cnt)>0 then 'квалинвестор' else 'не квалинвестор' end
                                         from
                                      (select count(*) as cnt from dscqinv_dbt d
                                      where d.t_partyid = client.t_partyid  and d.t_state = 1))
                                    end t_clientcvalinv,
                                   case
                                     when regexp_like(oprkoper.t_systypes,
                                                      '[v]') or (dl_tick.t_bofficekind = 127 and market.t_shortname is null) then
                                      nvl(contragent.t_name, chr(1))
                                     else
                                      nvl(contragent.t_name,
                                          'НКО НКЦ (АО)')
                                   end t_side2name,
                                   case
                                     when (regexp_like(oprkoper.t_systypes,
                                                       '[v]')) and
                                          (client.t_partyid is null) then
                                      (SELECT genagr.t_code || ' от ' ||
                                              to_char(genagr.t_start,
                                                      'DD.MM.YYYY')
                                         FROM ddl_genagr_dbt genagr
                                        where genagr.t_genagrid =
                                              dl_tick.t_genagrid
                                          and rownum = 1)
                                     else
                                      ''
                                   end t_genagr,
                                   nvl(contragent.t_name, chr(1)) t_contragentname,
                                   --(case when avrkindsroot.t_name = chr(1) then (select t_name from dfikinds_dbt where t_fi_kind = fininstr.t_fi_kind) else avrkindsroot.t_name end) t_firootname,
                                   nvl(avrkindsroot.t_name, chr(1)) t_firootname,
                                   nvl(avrkinds.t_name, chr(1)) t_finame,
                                   nvl(avoiriss.t_isin, chr(1)) t_isin,
                                   nvl(avoiriss.t_lsin, chr(1)) t_lsin,
                                   nvl(avoiriss.t_issue, chr(1)) t_issue,
                                   nvl(avoiriss.t_tranche, chr(1)) t_tranche,
                                   nvl(avoiriss.t_series, chr(1)) t_series,
                                   nvl(issuer.t_shortname, chr(1)) t_issuername,
                                   nvl(currency.t_ccy, chr(1)) t_currencyname,
                                   nvl(round(dl_leg.t_cost /
                                             dl_leg.t_principal,
                                             6),
                                       0) t_cost,
                                   nvl(dl_leg.t_totalcost, 0) t_totalcost,
                                   nvl(dl_leg.t_principal, 0) t_principal,
                                   (case when (avrkindsroot.t_fi_kind = 2 and avrkindsroot.t_avoirkind = 16) then fininstr.t_sumprecision end) t_princprecision,
                                   nvl(dl_leg.t_incomerate, 0) t_incomerate,
                                   nvl(dl_leg.t_returnincome, 0) t_returnincome,
                                   nvl(round(dl_leg.t_cost, 6), 0) t_dvalue, /*GAA:496249*/
                                   lower(nvl(conftp.t_contens, chr(1))) t_conftp,
                                   (select (case
                                             when t_netting != chr(0) then
                                              'неттинг'
                                             when t_cliring != chr(0) then
                                              'клиринг'
                                             when t_type = 8 then
                                              'поставка'
                                             else
                                              'оплата'
                                           end) t_type
                                      from ddlrq_dbt
                                     where t_kind =
                                       (case 
                                        when dl_tick.t_ispartyclient = chr(88)  and client.t_partyid != dl_tick.t_partyid  then 1 /*обязательство*/
                                        when dl_tick.t_ispartyclient = chr(88)  and client.t_partyid = dl_tick.t_partyid  then  0 /*требование*/ 
                                        else  1
                                       end )
                                       and t_dockind = dl_tick.t_bofficekind
                                       and t_docid = dl_tick.t_dealid
                                       and t_dealpart =
                                           decode(dl_leg.t_legkind, 0, 1, 2)
                                       and t_type in
                                           (2 /*DLRQ_TYPE_PAYMENT*/,
                                            8 /*DLRQ_TYPE_DELIVERY*/)
                                       and rownum = 1) t_execmethod,
                                   case
                                     when not regexp_like(oprkoper.t_systypes,
                                                      '[v]') and (rsb_secur.IsOTC(rsb_secur.get_OperationGroup(rsb_secur.get_OperSysTypes(dl_tick.t_DealType,
                                                                                                            dl_tick.t_BofficeKind))) = 0) and not (dl_tick.t_bofficekind = 127 and market.t_shortname is null) then
                                      'платежное поручение'
                                     else
                                      'платежное поручение в формате SWIFT'
                                   end t_formpayment,
                                   nvl(paycurrency.t_ccy, chr(1)) t_curpfi,
                                   paycurrency.t_fiid t_pricefiid,
                                   dlcontr.t_dlcontrid t_dlcontrid,
                                   dl_tick.t_clientid,
                                   case
                                     when regexp_like(oprkoper.t_systypes,
                                                      '[K]') then
                                      1
                                     else
                                      0
                                   end t_isBasket, /*GAA:495699*/
                                   dl_tick.T_USERFIELD1, /*GAA: 495919*/
                                   case
                                     when dl_leg.T_REJECTDATE !=
                                          to_date('01.01.0001', 'dd.mm.yyyy') then
                                      1
                                     else
                                      0
                                   end REJECT_DEAL, /*GAA:503363 Отказ от исполнения сделки */
                                   (case
                                     when ((select count(1)
                                              from ddl_leg_dbt
                                             where t_dealid = dl_tick.t_dealid
                                               and t_legid = 0) > 1) then
                                      1
                                     else
                                      0
                                   end) t_hastwopart
                              from ( select num01  t_DealType , -- dl_tick.t_DealType -- 1 NUMBER(5)
                                            num02  t_clientid ,-- dl_tick.t_clientid -- 2 NUMBER(10)
                                            num03  t_brokerid    ,--dl_tick.t_brokerid -- 3  NUMBER(10)
                                            num04  t_brokercontrid             ,--dl_tick.t_brokercontrid -- 4 NUMBER(10)
                                            num05  t_pfi          ,--dl_tick.t_pfi -- 5  NUMBER(10)
                                            str01  t_ispartyclient             ,--dl_tick.t_ispartyclient -- 1 CHAR(1)
                                            num06  t_partyid            ,--dl_tick.t_partyid --  6 NUMBER(10)
                                            num07  t_dealid            ,--dl_tick.t_dealid --  7 NUMBER(10)
                                            dat01  t_dealdate              ,--dl_tick.t_dealdate -- 1 date
                                            num08  t_bofficekind             ,--dl_tick.t_bofficekind -- 8  NUMBER(5)
                                            dat02  t_dealtime             ,--dl_tick.t_dealtime -- 2 date
                                            num09  t_genagrid             ,--dl_tick.t_genagrid --  9 NUMBER(10)
                                            str02  T_USERFIELD1             ,--dl_tick.T_USERFIELD1 -- 2 varchar2(120)
                                            num10  t_traderid            ,--dl_tick.t_traderid --  10 NUMBER(10)
                                            num11  t_depsetid             ,--dl_tick.t_depsetid --  11 NUMBER(10)
                                            num12  t_marketschemeid            ,--dl_tick.t_marketschemeid -- 12 NUMBER(10)
                                            num13  t_dealstatus  ,            --dl_tick.t_dealstatus --  13 NUMBER(5)
                                            num14  t_marketid,
                                            num15 t_clientcontrid,
                                            num16 t_partycontrid,
                                            str03 t_dealcodets,
                                            str04 t_dealcode
                                     from itt_parallel_exec p
                                     where p.calc_id = Calc_id 
                                         and p.row_id between Row_ID_start  and Row_ID_end   ) dl_tick
                              join doprkoper_dbt oprkoper
                                on oprkoper.t_kind_operation =
                                   dl_tick.t_dealtype
                               and
                                  -- Покупка/Продажа
                                   regexp_like(oprkoper.t_systypes,
                                               '[B|S|Z|G]') /*GAA:496240  +G*/
                              join (select BeginDate bdt, EndDate edt
                                      from dual)
                                 on 1 = 1
                              join ddl_leg_dbt dl_leg 
                                on dl_leg.t_dealid = dl_tick.t_dealid
                               and dl_leg.t_legid = 0
                               and dl_leg.t_legkind in (0, 2)
                              left join dparty_dbt market
                                on market.t_partyid = dl_tick.t_marketid
                              left join dparty_dbt agent
                                on agent.t_partyid = dl_tick.t_traderid
                              left join dparty_dbt client
                                on (client.t_partyid = dl_tick.t_clientid or (dl_tick.t_ispartyclient = chr(88)  and client.t_partyid = dl_tick.t_partyid))
                              left join dpersn_dbt clientp
                                on clientp.t_personid = client.t_partyid
                              left join dcountry_dbt clientc
                                on (clientc.t_parentcountryid = 0 AND
                                   clientc.t_codelat3 = client.t_nrcountry) /* MAA: iS - 517711 */
                             left join ddlcontrmp_dbt dlcontrmp
                                on ((dl_tick.t_ispartyclient = chr(0) and dlcontrmp.t_sfcontrid = dl_tick.t_clientcontrid)
                                or (dl_tick.t_ispartyclient = chr(88) and dlcontrmp.t_sfcontrid = 
                                 (case 
                                   when  client.t_partyid = dl_tick.t_partyid then dl_tick.t_partycontrid 
                                   else dl_tick.t_clientcontrid
                                  end)))
                              left join dsfcontr_dbt sfcontrmp
                                on sfcontrmp.t_id = dlcontrmp.t_sfcontrid
                              left join ddlcontr_dbt dlcontr
                                on dlcontr.t_dlcontrid =
                                   dlcontrmp.t_dlcontrid
                              left join dsfcontr_dbt sfcontr
                                on sfcontr.t_id = dlcontr.t_sfcontrid
                              left join dparty_dbt contragent
                                on ((dl_tick.t_ispartyclient = chr(0) and contragent.t_partyid = dl_tick.t_partyid) 
                                or (dl_tick.t_ispartyclient = chr(88) and contragent.t_partyid = (case when client.t_partyid != dl_tick.t_partyid then dl_tick.t_partyid else (case when dl_tick.t_clientid > 0 then dl_tick.t_clientid else 1 end ) end)))
                              left join dfininstr_dbt fininstr
                                on fininstr.t_fiid = dl_tick.t_pfi
                              left join davoiriss_dbt avoiriss
                                on avoiriss.t_fiid = dl_tick.t_pfi
                              left join davrkinds_dbt avrkinds
                                on avrkinds.t_fi_kind = fininstr.t_fi_kind
                               and avrkinds.t_avoirkind =
                                   fininstr.t_avoirkind
                              left join davrkinds_dbt avrkindsroot
                                on avrkindsroot.t_fi_kind =
                                   fininstr.t_fi_kind
                               and avrkindsroot.t_avoirkind =
                                   rsb_fiinstr.fi_avrkindsgetroot(fininstr.t_fi_kind,
                                                                  fininstr.t_avoirkind)
                              left join dparty_dbt issuer
                                on issuer.t_partyid = fininstr.t_issuer
                              --left join ddldepset_dbt depset
                             --   on depset.t_depsetid = dl_tick.t_depsetid
                             left join ddlmarket_dbt dlmarket
                                 on dlmarket.t_id = dl_tick.t_marketschemeid
                              left join dfininstr_dbt currency
                                on currency.t_fiid = dl_leg.t_cfi
                              left join dtypeac_dbt conftp
                                on conftp.t_inumtype = 155
                               and conftp.t_type_account =
                                   chr(dl_leg.t_formula)
                              left join dfininstr_dbt paycurrency
                                on paycurrency.t_fiid = dl_leg.t_payfiid
                              left join ddlobjcode_dbt dlobjcode 
                                on dlcontr.t_dlcontrid = DLOBJCODE.T_OBJECTID 
                               and dlobjcode.t_objecttype = 207 
                               and dlobjcode.t_codekind = 1  
                               --Если есть ЕКК с нулевой датой - отбираем его, если нет - последний закрытый код  
                               and dlobjcode.t_bankclosedate = case when exists (select 1 from ddlobjcode_dbt dlobjcodesub1 where dlobjcodesub1.t_objectid = dlobjcode.t_objectid and dlobjcodesub1.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub1.t_codekind = dlobjcode.t_codekind and dlobjcodesub1.t_bankclosedate = to_date('01010001','ddmmyyyy')  ) 
                                   then to_date('01010001','ddmmyyyy') 
                                   else (select max(dlobjcodesub2.t_bankclosedate) from ddlobjcode_dbt dlobjcodesub2 where dlobjcodesub2.t_objectid = dlobjcode.t_objectid and dlobjcodesub2.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub2.t_codekind = dlobjcode.t_codekind) END
                             where dl_tick.t_bofficekind in (101, 117, 127)
                               and (sfcontr.t_id is NULL or
                                   (StockMarket = chr(88) and
                                   sfcontrmp.t_servkind = 1) or
                                   (FuturesMarket = chr(88) and
                                   sfcontrmp.t_servkind = 15) or
                                   (CurrencyMarket = chr(88) and
                                   sfcontrmp.t_servkind = 21))
                               and (1 = (CASE WHEN Sector_Dilers = CHR(88) and (client.t_partyid is null or exists
                                      (select 1
                                             from ddp_dep_dbt dp_dep
                                            where dp_dep.t_partyid =
                                                  client.t_partyid)) THEN 1 ELSE 0 END)
                               or 1 = (CASE WHEN Sector_Brokers = CHR(88) and (client.t_partyid is not null and not exists
                                      (select 1
                                             from ddp_dep_dbt dp_dep
                                            where dp_dep.t_partyid =
                                                  client.t_partyid)) THEN 1 ELSE 0 END))
                              and (     SelectedClients = 1 
                                     and (EXISTS(SELECT * FROM D_RIA_PANELCLIENTS_DBT ria_cl WHERE ria_cl.t_clientid = client.t_partyid and ria_cl.t_sessionid = V_SESSIONID and ria_cl.T_CALCID = V_CALCID)
                                              OR (1 = (CASE WHEN Sector_Dilers = CHR(88) and (client.t_partyid is null or exists
                                                      (select 1
                                                             from ddp_dep_dbt dp_dep
                                                            where dp_dep.t_partyid =
                                                                  client.t_partyid)) THEN 1 ELSE 0 END))
                                          )
                                      or SelectedClients = 0) 
                               and (     SelectedContrs = 1 
                                     and (EXISTS(SELECT * FROM D_RIA_PANELCONTRS_DBT ria_c WHERE ria_c.t_clientid = client.t_partyid and ria_c.t_dlcontrid = dlcontrmp.t_dlcontrid and ria_c.t_sessionid = V_SESSIONID and ria_c.T_CALCID = V_CALCID)
                                              OR (1 = (CASE WHEN Sector_Dilers = CHR(88) and (client.t_partyid is null or exists
                                                      (select 1
                                                             from ddp_dep_dbt dp_dep
                                                            where dp_dep.t_partyid =
                                                                  client.t_partyid)) THEN 1 ELSE 0 END))
                                         )
                                      or SelectedContrs = 0)
                               and dl_tick.t_dealstatus > 0
                                and (     dl_tick.t_bofficekind != 127
                                    or  (    dl_tick.t_bofficekind = 127 
                                         and dl_tick.t_dealtype != 32011 
                                         and NOT EXISTS (
                                        (select objatcor.t_attrid attrid
                                              from dobjattr_dbt  objattr,
                                                   dobjatcor_dbt objatcor
                                             where objattr.t_objecttype = 101
                                               and  objattr.t_groupid = 210
                                               and objatcor.t_objecttype =
                                                   objattr.t_objecttype
                                               and objatcor.t_groupid =
                                                   objattr.t_groupid
                                               and objatcor.t_attrid =
                                                   objattr.t_attrid
                                               and objatcor.t_object =
                                                   lpad(dl_tick.t_dealid, 34, '0')
                                               and objatcor.t_validfromdate <= edt
                                               and objatcor.t_validtodate > edt)
                                         )
                                        )
                                    )
                                    
                                  and  ((      dl_tick.t_dealtype = 32743 
                                            and dl_tick.t_dealstatus = 20 
                                            and exists ( select rq.t_id
                                                               from ddlrq_dbt rq 
                                                             where rq.t_dockind = dl_tick.t_bofficekind
                                                                and rq.t_docid = dl_tick.t_dealid
                                                                and rq.t_dealpart = decode(dl_leg.t_legkind, 0, 1, 2)
                                                               and rq.t_state != 7)) 
                                              or dl_tick.t_dealtype != 32743)
                                  /* from (SELECT \*+ index(dl_tick DDL_TICK_DBT_IDX4)*\
                                             T_DEALID,
                                              rsb_brkrep_u.
                                               GetPlanExecDate (dl_tick.t_bofficekind, dl_tick.t_dealid, 1)
                                                 AS t_firstPartDate,
                                              rsb_brkrep_u.
                                               GetPlanExecDate (dl_tick.t_bofficekind, dl_tick.t_dealid, 2)
                                                 AS t_SecondPartDate,
                                                 dl_tick.t_dealdate
                                         FROM ddl_tick_dbt dl_tick
                                        WHERE dl_tick.t_dealstatus > 0
                                              AND dl_tick.t_bofficekind IN (101, 117, 127)
                                              AND (DL_TICK.T_DEALDATE <= edt)) q1
                                WHERE
                                q1.T_DEALDATE BETWEEN bdt
                                                           AND edt
                                    OR (q1.T_DEALDATE <= bdt
                                         AND ((q1.T_FIRSTPARTDATE >= bdt)
                                         OR (q1.T_SECONDPARTDATE >= bdt)))*/
                                                           ) dt) LOOP
      if (one_rec.t_isbank = 0) then
        registeria.t_part := 1;
      else
        registeria.t_part := 2;
      end if;
      registeria.t_dealsubkind := 1;
      registeria.t_dealid      := one_rec.t_dealid;
      registeria.t_dockind     := one_rec.t_dockind;
      
      IF Block_Deals = CHR(88) THEN
          registeria.t_dealcode    := one_rec.t_dealcode;
          registeria.t_dealcodets  := one_rec.t_dealcodets;
          registeria.t_dealdate    := one_rec.t_dealdate;
          registeria.t_marketname  := one_rec.t_marketname;
          if (one_rec.t_sector = chr(88)) then
            registeria.t_markettype := CONCAT(CONCAT(one_rec.t_depsetcode,
                                                     '/'),
                                              one_rec.t_markettype);
          else
            registeria.t_markettype := NULL;
          end if;

          registeria.t_extbroker    := one_rec.t_brokername;
          registeria.t_extbrokerdoc := one_rec.t_brokercontrnumber;

          if (one_rec.t_isbank = 0) then
            registeria.t_dealtype     := 'клиентская';
            registeria.t_status_purcb := one_rec.t_status_purcb;
            registeria.t_side1        := one_rec.t_clientname;
            registeria.t_client_assign := one_rec.t_client_assign;
          else
            registeria.t_dealtype     := 'собственная';
            registeria.t_status_purcb := 'хозяйствующий субъект';
            registeria.t_side1        := 'АО "РОССЕЛЬХОЗБАНК"';
            registeria.t_client_assign := NULL;
          end if;

          registeria.t_side2           := one_rec.t_side2name;
          registeria.t_genagr          := one_rec.t_genagr;
          registeria.t_confirmdoc      := 'документы получены';
          registeria.t_dueprocess      := one_rec.t_conftp;
          registeria.t_execmethod      := one_rec.t_execmethod;
          registeria.t_formpayment     := one_rec.t_formpayment;
          registeria.t_unqualifiedsec  := one_rec.t_qualification;
          registeria.t_dealtypeAdr     := one_rec.t_dealtypeAdr;
          registeria.t_dealdirection   := one_rec.t_side;
          registeria.t_dealkind        := one_rec.t_side2;
          registeria.t_seckind         := one_rec.t_firootname;
          if (one_rec.t_firootname != one_rec.t_finame) then
             registeria.t_secsubkind := one_rec.t_finame;
          else
            registeria.t_secsubkind := NULL;
          end if;
          registeria.t_seccode         := one_rec.t_avrname || '/' ||
                                          one_rec.t_avrcode;
          if (not (one_rec.t_lsin = chr(1) and one_rec.t_isin = chr(1))) then
             registeria.t_isin            := '№' || one_rec.t_lsin || '/ ISIN ' ||
                                          one_rec.t_isin;
          else 
            registeria.t_isin            := NULL;
          end if;
          if (not (one_rec.t_issue = chr(1) and one_rec.t_tranche = chr(1) and one_rec.t_series = chr(1))) then
             registeria.t_secseries       := 'выпуск №' || one_rec.t_issue || '/ транш №' ||
                                          one_rec.t_tranche || '/ серия №' ||
                                          one_rec.t_series;
          else
           registeria.t_secseries       := NULL;
          end if;
          registeria.t_issuername := one_rec.t_issuername;
          registeria.t_cost            := one_rec.t_cost;
          registeria.t_principal      := one_rec.t_principal;
          registeria.t_dealvalue    := one_rec.t_dvalue;
          registeria.t_totalcost     := one_rec.t_totalcost;
          

          select max(dlrq.t_plandate) t_plandate, 
                   count(1) as t_cnt, 
                   listagg(namealg.t_sznamealg || ' ' || to_char(dlrq.t_plandate, 'DD.MM.YYYY'), ', ') within group (order by dlrq.t_plandate, namealg.t_sznamealg) t_plandatestr
            into v_plandatem, v_plandatemcnt, v_plandatemstr
           from ddlrq_dbt dlrq
            join dnamealg_dbt namealg on namealg.t_itypealg = 7513 and dlrq.t_type = namealg.t_inumberalg 
        where dlrq.t_dockind = one_rec.t_dockind
            and dlrq.t_docid = one_rec.t_dealid
            and dlrq.t_dealpart = one_rec.t_dealpart
            and dlrq.t_subkind = 0 /*DLRQ_SUBKIND_CURRENCY*/
            and dlrq.t_type in (0 /*DLRQ_TYPE_AVANCE*/,
                                       1 /*DLRQ_TYPE_DEPOSIT*/,
                                       2 /*DLRQ_TYPE_PAYMENT*/);
          
        if (v_plandatemcnt > 1) then
          registeria.t_plandatem := v_plandatemstr;
        else
          registeria.t_plandatem := TO_CHAR(v_plandatem, 'DD.MM.YYYY');
        end if;
        
         select max(dlrq.t_factdate) t_factdate, 
                   count(1) as t_cnt, 
                   listagg(namealg.t_sznamealg || ' ' || to_char(dlrq.t_factdate, 'DD.MM.YYYY'), ', ') within group (order by dlrq.t_factdate, namealg.t_sznamealg) t_factdatestr
            into v_factdatem, v_factdatemcnt, v_factdatemstr
           from ddlrq_dbt dlrq
            join dnamealg_dbt namealg on namealg.t_itypealg = 7513 and dlrq.t_type = namealg.t_inumberalg 
        where dlrq.t_dockind = one_rec.t_dockind
            and dlrq.t_factdate != to_date('01.01.0001','DD.MM.YYYY')
            and dlrq.t_docid = one_rec.t_dealid
            and dlrq.t_dealpart = one_rec.t_dealpart
            and dlrq.t_subkind = 0 /*DLRQ_SUBKIND_CURRENCY*/
            and dlrq.t_type in (0 /*DLRQ_TYPE_AVANCE*/,
                                       1 /*DLRQ_TYPE_DEPOSIT*/,
                                       2 /*DLRQ_TYPE_PAYMENT*/);
                                       
         if ((v_plandatemcnt > v_factdatemcnt) or (v_factdatemcnt > 1)) then
          registeria.t_factdatem := v_factdatemstr;
        else
          registeria.t_factdatem := TO_CHAR(v_factdatem, 'DD.MM.YYYY');
        end if;
                      
          if (one_rec.t_pricefiid != 0) then
           registeria.t_costnatcur      := round(rsi_rsb_fiinstr.convsum(round(one_rec.t_cost, 6),
                                                                         one_rec.t_pricefiid,
                                                                         0,
                                                                         nvl(v_factdatem, v_plandatem)), 6);
          registeria.t_dealvaluerur   := round(rsi_rsb_fiinstr.convsum(round(one_rec.t_dvalue, 6),
                                                                         one_rec.t_pricefiid,
                                                                         0,
                                                                         nvl(v_factdatem, v_plandatem)), 6);
          registeria.t_totalcostrur    := round(rsi_rsb_fiinstr.convsum(round(one_rec.t_totalcost, 6),
                                                                         one_rec.t_pricefiid,
                                                                         0,
                                                                         nvl(v_factdatem, v_plandatem)), 6);
          else
             registeria.t_costnatcur    := NULL;
             registeria.t_dealvaluerur := NULL;
             registeria.t_totalcostrur  := NULL;
          end if;
          
          if (one_rec.t_isbasket = 1) then
           registeria.t_pricecurrency   := 'RUB';
           registeria.t_cost                := NULL;
           registeria.t_costnatcur       := NULL;
          else
            registeria.t_pricecurrency  := one_rec.t_curpfi;
          end if;
          
          registeria.t_nominalcurrency := one_rec.t_facevaluefi;
          registeria.t_setcurrency     := one_rec.t_currencyname2;
          registeria.t_interestrate    := one_rec.t_incomerate;
          registeria.t_interestamount  := one_rec.t_returnincome;
          registeria.t_brokercomiss    := one_rec.t_brokercomiss;
          registeria.t_plandatesp      := one_rec.t_plandatesp;
          registeria.t_factdatesp      := one_rec.t_factdatesp;
          registeria.t_cond_susp      := substr(one_rec.t_cond_susp,1,100);
      END IF;
      
      if (one_rec.t_isbank = 0 and Block_Clients = CHR(88)) then
        registeria.t_clientname     := one_rec.t_clientshortname;
        registeria.t_clientagr      := one_rec.t_sfcontrnumber;
        --registeria.t_clientagrdate  := to_char(one_rec.t_sfcontrbegdate, 'DD.MM.YYYY');
        --registeria.t_clientagrend   := to_char(one_rec.t_sfcontrenddate, 'DD.MM.YYYY');
        registeria.t_clientagrdate  := one_rec.t_sfcontrbegdate;
        registeria.t_clientagrend   := one_rec.t_sfcontrenddate;
        registeria.t_exchangecode   := one_rec.t_mpcode;
        registeria.t_proxyname      := one_rec.t_proxyname;
        registeria.t_proxynum       := one_rec.t_proxynum;
        registeria.t_clientform     := one_rec.t_clientform;
        registeria.t_clientcountry  := one_rec.t_clientcountry;
        registeria.t_clientresident := one_rec.t_clientresident;
        registeria.t_clientcvalinv  := one_rec.t_clientcvalinv;
        registeria.t_risklevel      := one_rec.t_risklevel;
        registeria.t_cashright      := one_rec.t_cashright;
      else
        registeria.t_clientname     := NULL;
        registeria.t_clientagr      := NULL;
        registeria.t_clientagrdate  := NULL;
        registeria.t_clientagrend   := NULL;
        registeria.t_exchangecode   := NULL;
        registeria.t_proxyname      := NULL;
        registeria.t_proxynum       := NULL;
        registeria.t_clientform     := NULL;
        registeria.t_clientcountry  := NULL;
        registeria.t_clientresident := NULL;
        registeria.t_clientcvalinv  := NULL;
        registeria.t_risklevel      := NULL;
        registeria.t_cashright      := NULL;
      end if;
      
      -- Biq-9103
      registeria.t_margincall := NULL; 
      if( RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_SECDEAL, LPAD(one_rec.t_DealID, 34, '0'), 116, one_rec.t_DealDate) = 1) THEN 
        registeria.t_margincall := 'X'; 
      end if;

      --служебные поля
      registeria.t_clientid      := one_rec.clientid;
      registeria.t_clientcontrid := one_rec.clientcontrid;
      registeria.t_pfi           := one_rec.pfi;
      registeria.t_dealpart      := one_rec.t_dealpart;
      registeria.t_isbasket      := one_rec.t_isbasket;
      registeria.t_invert_debet_credit := one_rec.t_invert_debet_credit;
      registeria.t_princprecision := one_rec.t_princprecision;
      registeria.t_dealtime    := one_rec.t_dealtime;

      registeria.t_sessionid := V_SESSIONID;
      registeria.t_calcid := V_CALCID;

      g_registeria_ins.extend;
      g_registeria_ins(g_registeria_ins.LAST) := registeria;
      
      if (one_rec.t_ispartyclient = chr(88) and one_rec.deal_clientid <= 0) then
      
        registeria.t_part := 2;
        registeria.t_side2 := one_rec.t_clientname; 
        registeria.t_invert_debet_credit := 0;
        registeria.t_clientname     := NULL;
        registeria.t_clientagr      := NULL;
        registeria.t_clientagrdate  := NULL;
        registeria.t_clientagrend   := NULL;
        registeria.t_exchangecode   := NULL;
        registeria.t_proxyname      := NULL;
        registeria.t_proxynum       := NULL;
        registeria.t_clientform     := NULL;
        registeria.t_clientcountry  := NULL;
        registeria.t_clientresident := NULL;
        registeria.t_clientcvalinv  := NULL;
        registeria.t_risklevel      := NULL;
        registeria.t_cashright      := NULL;
        registeria.t_brokercomiss := NULL;
        
        if Block_Deals = CHR(88) THEN
            if (registeria.t_dealdirection = 'Покупка') then
               registeria.t_dealdirection := 'Продажа';
            else
               registeria.t_dealdirection := 'Покупка';
            end if;
            
            registeria.t_dealtype     := 'собственная';
            registeria.t_status_purcb := 'хозяйствующий субъект';
            registeria.t_side1        := 'АО "РОССЕЛЬХОЗБАНК"';
            
            registeria.t_execmethod := NULL;
        END IF;
        BEGIN
           SELECT (CASE
                      WHEN t_netting != CHR (0) THEN 'неттинг'
                      WHEN t_cliring != CHR (0) THEN 'клиринг'
                      WHEN t_type = 8 THEN 'поставка'
                      ELSE 'оплата'
                   END) t_type
             INTO registeria.t_execmethod
             FROM ddlrq_dbt
            WHERE     t_kind = 1
                  AND t_dockind = registeria.t_dockind
                  AND t_docid = registeria.t_dealid
                  AND t_dealpart = registeria.t_dealpart
                  AND t_type IN (2  /*DLRQ_TYPE_PAYMENT*/, 8  /*DLRQ_TYPE_DELIVERY*/)
          AND ROWNUM = 1;
        EXCEPTION
           WHEN OTHERS
           THEN
              NULL;
        END;
        
        g_registeria_ins.extend;
        g_registeria_ins(g_registeria_ins.LAST) := registeria;
        IF g_registeria_ins.count >= 100000  THEN
        FORALL i IN g_registeria_ins.FIRST .. g_registeria_ins.LAST
           INSERT INTO DDL_REGIABUF_DBT VALUES g_registeria_ins (i);
           g_registeria_ins.delete;
        END IF;
      
      end if;

    END LOOP;

    IF g_registeria_ins IS NOT EMPTY THEN
      FORALL i IN g_registeria_ins.FIRST .. g_registeria_ins.LAST
        INSERT INTO DDL_REGIABUF_DBT VALUES g_registeria_ins (i);
      g_registeria_ins.delete;
    END IF;
    
  exception when others then
         it_error.put_error_in_stack;
         it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
         raise;
  END CreateData_SC_Deals_parallel;

  PROCEDURE CreateData_DV_NDeals(BeginDate      IN DATE,
                                 EndDate        IN DATE,
                                 StockMarket    IN CHAR,
                                 FuturesMarket  IN CHAR,
                                 CurrencyMarket IN CHAR,
                                 Sector_Brokers IN CHAR,
                                 Sector_Dilers  IN CHAR,
                                 Sector_Clients IN CHAR,
                                 Block_Deals    IN CHAR,
                                 Block_Clients  IN CHAR,
                                 Block_InAcc    IN CHAR,
                                 SelectedClients IN NUMBER,
                                 SelectedContrs   IN NUMBER) as 
    v_calc_id number;
   v_code varchar2(30000);
  begin
   PushLogLine('Отбор сделок валютного рынка - begin','Отбор сделок валютного рынка');
   v_calc_id := it_parallel_exec.init_calc;
   insert into itt_parallel_exec
    (calc_id
    ,num01
    )
    SELECT /*+ index(dvndeal DDVNDEAL_DBT_IDX2) index(ddvnfi_dbt DDVNFI_DBT_IDX1) */
        v_calc_id
        ,dvndeal.t_id
         FROM    ddvndeal_dbt dvndeal
          INNER JOIN ddvnfi_dbt dvnfi
           ON  dvnfi.t_type in (0,2) and dvnfi.t_dealid = dvndeal.t_id
         WHERE dvndeal.t_state > 0
         AND (dvndeal.T_DATE BETWEEN BeginDate  AND EndDate
         OR (dvndeal.T_DATE <= BeginDate AND dvnfi.t_ExecDate >=BeginDate)); 
    commit;
   v_code := 'begin rsb_dlregisteria.SetCalcIds('||TO_CHAR(V_SESSIONID)||', '||TO_CHAR(V_CALCID)||'); 
   rsb_dlregisteria.CreateData_DV_NDeals_parallel(Calc_id => '||v_calc_id||',
                                          Row_ID_start    => :start_id,
                                          Row_ID_end      => :end_id,
                                          BeginDate       => '||it_parallel_exec.Date_to_sql(BeginDate)||',        
                                          EndDate         => '||it_parallel_exec.Date_to_sql(EndDate)||',     
                                          StockMarket     => '||it_parallel_exec.Str_to_sql(StockMarket)||',         
                                          FuturesMarket   => '||it_parallel_exec.Str_to_sql(FuturesMarket)||',       
                                          CurrencyMarket  => '||it_parallel_exec.Str_to_sql(CurrencyMarket)||',      
                                          Sector_Brokers  => '||it_parallel_exec.Str_to_sql(Sector_Brokers)||',      
                                          Sector_Dilers   => '||it_parallel_exec.Str_to_sql(Sector_Dilers)||',       
                                          Sector_Clients  => '||it_parallel_exec.Str_to_sql(Sector_Clients)||',      
                                          Block_Deals     => '||it_parallel_exec.Str_to_sql(Block_Deals)||',         
                                          Block_Clients   => '||it_parallel_exec.Str_to_sql(Block_Clients)||',       
                                          Block_InAcc     => '||it_parallel_exec.Str_to_sql(Block_InAcc)||',         
                                          SelectedClients => '||SelectedClients||',                                  
                                          SelectedContrs  => '||SelectedContrs||') ;                                 
    exception when others then
         it_error.put_error_in_stack;
         it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
         raise;
    end;';
  it_log.log(p_msg => 'START parallel',p_msg_clob => v_code) ;
  it_parallel_exec.run_task_chunks_by_calc(p_parallel_level => 8,
                                           p_id             => v_calc_id,
                                           p_sql_stmt       => v_code);

   it_parallel_exec.clear_calc(v_calc_id);
   
   PushLogLine('Отбор сделок валютного рынка - end');
    
    IF Block_InAcc = CHR(88) THEN
      AddDVNAccounts(BeginDate, EndDate);
    END IF;
  end;
  
  PROCEDURE CreateData_DV_NDeals_parallel(Calc_id number,
                                 Row_ID_start number,
                                 Row_ID_end   number,
                                 BeginDate      IN DATE,
                                 EndDate        IN DATE,
                                 StockMarket    IN CHAR,
                                 FuturesMarket  IN CHAR,
                                 CurrencyMarket IN CHAR,
                                 Sector_Brokers IN CHAR,
                                 Sector_Dilers  IN CHAR,
                                 Sector_Clients IN CHAR,
                                 Block_Deals    IN CHAR,
                                 Block_Clients  IN CHAR,
                                 Block_InAcc    IN CHAR,
                                 SelectedClients IN NUMBER,
                                 SelectedContrs   IN NUMBER) IS
    TYPE registeria_t IS TABLE OF DDL_REGIABUF_DBT%ROWTYPE;
    g_registeria_ins registeria_t := registeria_t();
    registeria       DDL_REGIABUF_DBT%rowtype;
  BEGIN
    FOR one_rec IN (select dt.clientid,
                           dt.clientcontrid,
                           dt.pfi,
                           dt.t_dealid,
                           dt.t_dockind,
                           dt.t_date,
                           dt.t_dealpart,
                           dt.t_isbank,
                           dt.t_code,
                           dt.t_extcode,
                           dt.t_dates,
                           dt.t_time,
                           dt.t_planexecdate,
                           dt.t_marketname,
                           dt.t_optiontype,
                           dt.t_operkind,
                           dt.t_kindpfi,
                           (select t.t_ShortName
                              from dparty_dbt t
                             where t.t_Partyid = dt.brokerid) t_brokername,
                           (select t.t_Number
                              from dsfcontr_dbt t
                             where t.t_id = dt.brokercontrid) t_brokercontrnumber,
                           dt.t_sfcontrnumber,
                           dt.t_sfcontrbegdate,
                           decode(dt.t_sfcontrenddate, to_date('01.01.0001','DD.MM.YYYY'), null, dt.t_sfcontrenddate) t_sfcontrenddate,
                           dt.t_clientname,
                           dt.t_clientshortname,
                           dt.t_clientptcode,
                           dt.t_mpcode,
                           dt.t_clientform,
                           dt.t_clientcountry as t_clientcountry,
                           (case when dt.t_clientform = 'ФЛ' then dt.t_clientresident else '' end) as t_clientresident,
                           dt.t_clientcvalinv,
                           dt.t_side2name,
                           dt.t_genagr,
                           dt.t_contragentname,
                           (case when dt.t_isBank = 0 then
                           nvl((SELECT SUM(nvl(dlc.t_Sum, 0))
                                  FROM ddlcomis_dbt dlc
                                 WHERE dlc.t_DocKind = dt.t_dockind
                                   AND dlc.t_DocID = dt.t_dealid
                                   AND dlc.t_IsBack = decode(dt.t_dealpart,1,chr(0),chr(88))
                                   AND dlc.t_ReceiverID in (RsbSessionData.OurBank, 0)), 0) end)
                            t_brokercomiss,
                           ( case when t_marketname != 'Внебиржевой рынок' then
                              ( SELECT r.t_codets
                                  FROM ddl_req_dbt  r
                                       JOIN dspgrdoc_dbt rd
                                           ON r.t_id = rd.t_SOURCEDOCID AND rd.t_sourcedockind = 350
                                       JOIN dspgrdoc_dbt spgrdoc ON rd.t_SPgroundID = spgrdoc.t_SPGroundID
                                 WHERE spgrdoc.t_SourceDocKind = dt.t_dockind AND spgrdoc.t_SourceDocID = dt.t_dealid and rownum = 1)
                           else
                             (select spgr.t_XLD 
                              from dspground_dbt spgr, dspgrdoc_dbt doc
                             where doc.t_SourceDocKind = dt.t_dockind
                               and doc.t_SourceDocID = dt.t_dealid
                               and spgr.t_SPgroundID = doc.t_SPgroundID
                               and spgr.t_Kind = 251 /*поручение клиента на операцию*/
                               and rownum = 1) end) t_client_assign,
                           case
                             when dt.t_isbank = 1 then
                              'С'
                             else
                              'К'
                           end t_kind,
                           case when dt.t_isprcswap = 0 then 
                           decode(dt.t_isbuy,
                                  1,
                                  'Покупка',
                                  'Продажа') end t_side,
                           dt.t_isbuy,
                           case
                             when dt.t_hastwopart = 1 then
                              dt.t_opername || ' ' || dt.t_dealpart || ' ч'
                             else
                              dt.t_opername
                           end t_side2,
                           dt.t_firootname,
                           dt.t_avrname,
                           dt.t_avrcode,
                           dt.t_issue,
                           dt.t_tranche,
                           dt.t_series,
                           dt.t_finame,
                           dt.t_isin,
                           dt.t_lsin,
                           dt.t_issuername,
                           dt.t_currencyname,
                           (case when t_optiontype is not null then dt.t_bonus end) t_optionpremium,
                           (case when t_optiontype is not null then (select t_ccy from dfininstr_dbt where t_fiid = dt.t_bonusfiid)  end) t_quotecur,
                           (select t_ccy from dfininstr_dbt setfiid
                             join dpmpaym_dbt pmpaym on pmpaym.t_fiid = setfiid.t_fiid
                             where pmpaym.t_dockind = dt.t_dockind
                               and pmpaym.t_documentid = dt.t_dealid
                               and ((dt.t_dealpart = 1 and pmpaym.t_purpose = 2) or (dt.t_dealpart = 2 and pmpaym.t_purpose = 4)) -- КА
                            )
                             t_setcurrency,
                           (select max(pmpaym.t_valuedate)
                              from dpmpaym_dbt pmpaym
                             where pmpaym.t_dockind = dt.t_dockind
                               and pmpaym.t_documentid = dt.t_dealid
                               and ((dt.t_dealpart = 1 and pmpaym.t_purpose = 1) or (dt.t_dealpart = 2 and pmpaym.t_purpose = 3)) -- КА
                            ) t_plandatesp,
                           case when dt.t_state = 2 then
                           (select max(pmpaym.t_valuedate)
                              from dpmpaym_dbt pmpaym
                             where pmpaym.t_dockind = dt.t_dockind
                               and pmpaym.t_documentid = dt.t_dealid
                              and ((dt.t_dealpart = 1 and pmpaym.t_purpose = 1) or (dt.t_dealpart = 2 and pmpaym.t_purpose = 3)) -- КА
                               and (pmpaym.t_isfactpaym = chr(88) or pmpaym.t_paymstatus=150)
                            ) end t_factdatesp,
                           (select max(pmpaym.t_valuedate)
                              from dpmpaym_dbt pmpaym
                             where pmpaym.t_dockind = dt.t_dockind
                               and pmpaym.t_documentid = dt.t_dealid
                               and ((dt.t_dealpart = 1 and pmpaym.t_purpose = 2) or (dt.t_dealpart = 2 and pmpaym.t_purpose = 4)) -- КА
                            ) t_plandatem,
                            case when dt.t_state = 2 then
                           (select max(pmpaym.t_valuedate)
                              from dpmpaym_dbt pmpaym
                             where pmpaym.t_dockind = dt.t_dockind
                               and pmpaym.t_documentid = dt.t_dealid
                               and ((dt.t_dealpart = 1 and pmpaym.t_purpose = 2) or (dt.t_dealpart = 2 and pmpaym.t_purpose = 4)) -- КА
                               and (pmpaym.t_isfactpaym = chr(88) or pmpaym.t_paymstatus=150)
                            ) end t_factdatem,
                            (select t_ccy from dfininstr_dbt where t_fiid = dt.t_pricefiid) t_pricecurrency, -- валюта цены
                           (case when not (dt.t_sector = chr(88) and dt.t_ispfi = chr(88)) then dt.t_conftp end) t_conftp,
                           (select 'да'
                              from dobjattr_dbt  objattr,
                                   dobjatcor_dbt objatcor
                             where objattr.t_objecttype = 12
                               and objattr.t_groupid = 28
                               and objatcor.t_objecttype =
                                   objattr.t_objecttype
                               and objatcor.t_groupid = objattr.t_groupid
                               and objatcor.t_attrid = objattr.t_attrid
                               and objatcor.t_object = lpad(dt.pfi, 10, '0')
                               and objatcor.t_validfromdate <= edt
                               and objatcor.t_validtodate > edt
                               and objatcor.t_attrid = 2
                               and rownum = 1) t_qualification,
                           case when dt.t_clientcontr > 0 then
                           decode(dt.t_role, 0,
                           (select lower(objattr.t_name)
                              from dobjattr_dbt  objattr,
                                   dobjatcor_dbt objatcor
                             where objattr.t_objecttype = 659
                               and objattr.t_groupid = 1
                               and objatcor.t_objecttype =
                                   objattr.t_objecttype
                               and objatcor.t_groupid = objattr.t_groupid
                               and objatcor.t_attrid = objattr.t_attrid
                               and objatcor.t_object =
                                   lpad(dt.t_clientcontr, 10, '0')
                               and objatcor.t_validfromdate <= edt
                               and objatcor.t_validtodate > edt
                               and rownum = 1),
                           (select lower(objattr.t_name)
                              from dobjattr_dbt objattr
                             where objattr.t_objecttype = 659
                               and objattr.t_groupid = 1
                               and objattr.t_attrid = dt.t_role
                               and rownum = 1))
                           else
                             null
                           end t_status_purcb,
                           dt.t_execmethod,
                           dt.t_formpayment,
                           dt.t_ispfi,
                           case
                             when dt.t_isbank = 1 or dt.t_dlcontrid is null then
                              chr(1)
                             else
                              case when dt.servkind = 1 then
                                      (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 659
                                       and  objattr.t_groupid = 6
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(dt.clientcontrid, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) pravo)
                                   else
                                       (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                       (select NVL(objatcor.t_attrid, -1) attrid  
                                          from dobjattr_dbt  objattr,
                                               dobjatcor_dbt objatcor,
                                               DDLCONTRMP_DBT contrmp_pr,
                                               DSFCONTR_DBT sfcontr_pr
                                         where contrmp_pr.T_DLCONTRID = dt.t_dlcontrid
                                           and sfcontr_pr.t_id = contrmp_pr.t_sfcontrid
                                           and sfcontr_pr.t_servkind = 1
                                           and objattr.t_objecttype = 659
                                           and objattr.t_groupid = 6
                                           and objatcor.t_objecttype =
                                               objattr.t_objecttype
                                           and objatcor.t_groupid =
                                               objattr.t_groupid
                                           and objatcor.t_attrid =
                                               objattr.t_attrid
                                           and objatcor.t_object =
                                               lpad(sfcontr_pr.t_id, 10, '0')
                                           and objatcor.t_validfromdate <= edt
                                           and objatcor.t_validtodate > edt ORDER BY objatcor.t_attrid) pravo WHERE ROWNUM = 1)
                                   end
                           end t_cashright,
                           case
                             when dt.t_isbank = 1 then
                              chr(1)
                             else
                               (select trim(nvl(max(decode(t_attrid, 1, 'КСУР', decode(t_attrid, 2, 'КПУР', decode(t_attrid, 3, 'КОУР', decode(t_attrid, 4, 'КНУР'))))),
                                                          'отсутствует'))
                                      from dobjatcor_dbt objatcor
                                     where objatcor.t_objecttype = 3
                                       and /*Субъект экономики*/
                                           objatcor.t_groupid = 95
                                       and objatcor.t_object =
                                           lpad(clientid, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt)
                           end t_risklevel,
                           case
                             when dt.t_isbank = 1 then
                              chr(1)
                             else
                              nvl((select listagg(clientproxy.t_name, '/') within group(order by clientproxy.t_name, ptproxy.t_proxyid)
                                    from dptproxy_dbt ptproxy
                                    join dparty_dbt clientproxy
                                      on clientproxy.t_partyid =
                                         ptproxy.t_proxyid
                                   where ptproxy.t_partyid = dt.t_client
                                     and ptproxy.t_docdate <= edt
                                     and ptproxy.t_validitydate >= edt),
                                  chr(1))
                           end t_proxyname,
                           case
                             when dt.t_isbank = 1 then
                              chr(1)
                             else
                              nvl((select listagg(ptproxy.t_docnumber || ' с ' ||
                                                 to_char(ptproxy.t_docdate,
                                                         'dd.mm.yyyy') ||
                                                 ' по ' ||
                                                 to_char(ptproxy.t_validitydate,
                                                         'dd.mm.yyyy'),
                                                 '/') within group(order by ptproxy.t_proxyid, ptproxy.t_proxyid)
                                    from dptproxy_dbt ptproxy
                                    join dparty_dbt clientproxy
                                      on clientproxy.t_partyid =
                                         ptproxy.t_proxyid
                                   where ptproxy.t_partyid = dt.t_client
                                     and ptproxy.t_docdate <= edt
                                     and ptproxy.t_validitydate >= edt),
                                  chr(1))
                           end t_proxynum,
                           case when dt.t_ispfi = chr(88) then
                           (select t_sznamealg
                                      from dnamealg_dbt
                                     where t_itypealg = 3421
                                       and t_inumberalg = t_bakind)
                           end t_bakindname,
                           dt.t_isBasket, /*GAA:495699*/
                           dt.t_markettype,
                           (case when dt.t_isbasket = 0 then nvl(round(dt.t_cost/dt.t_amount, 6), 0)  end ) t_cost,
                           case when dt.t_isprcswap = 0 then (nvl(dt.t_cost, 0)) else dt.t_contramount end t_totalcost,
                           (case when dt.t_isbasket = 1 then 1 else dt.t_amount end ) t_principal,
                           dt.t_princprecision,
                           dt.t_isprcswap,
                           dt.t_pricefiid,
                           dt.t_ExecDate,
                           case when dt.t_isprcswap = 0 then nvl(round(dt.t_cost, 6), 0) else dt.t_contramount end t_dvalue,
                            nvl((Select rsb_struct.getString(note.t_text) 
                                   from dnotetext_dbt note 
                                   where note.t_notekind=408 
                                      and note.t_documentid in (lpad(dt.t_dealid,34,'0')) 
                                      and note.t_objecttype = (CASE WHEN dt.t_dockind = 199 THEN 145 ELSE 148 END)
                           ),'') t_cond_susp
                      from (select bdt,
                                    edt,
                                    dvnfi.t_ExecDate,
                                    dlmarket.t_code t_markettype,
                                    dvndeal.t_client clientid,
                                    dvndeal.t_sector,
                                    dvndeal.t_state,
                                    dvndeal.t_bonus,
                                    dvndeal.t_bonusfiid,
                                    dvndeal.t_kind t_operkind,
                                    dvndeal.t_clientcontr clientcontrid,
                                    sfcontr.t_servkind servkind,
                                    dvndeal.t_agent brokerid,
                                    dvndeal.t_agentcontr brokercontrid,
                                    direction.t_sznamealg t_kindpfi,
                                    dvnfi.t_fiid pfi,
                                    dvnfi.t_type,
                                    dvnfi.t_cost,
                                    (case when dvndeal.t_kind = 22720 then 1 else 0 end) t_isprcswap,
                                    dvndeal.t_clientcontr,
                                    dvndeal.t_ispfi,
                                    dlcontrmp.t_role,
                                    fininstr.t_name t_avrname,
                                    fininstr.t_fi_code t_avrcode,
                                    fininstr.t_fi_kind t_fi_kind,
                                    rsb_secur.get_BA_Kind(fininstr.t_fi_kind,
                                                             fininstr.t_avoirkind,
                                                             avrkinds.t_root)
                                    t_bakind,
                                    oprkoper.t_name t_opername,
                                    optiontype.t_sznamealg t_optiontype,
                                    case
                                      when dvndeal.t_client <= 0 or exists
                                       (select 1
                                              from ddp_dep_dbt dp_dep
                                             where dp_dep.t_partyid =
                                                   dvndeal.t_client) then
                                       1
                                      else
                                       0
                                    end t_isbank,
                                    dvndeal.t_id t_dealid,
                                    dvndeal.t_date t_date,
                                    dvndeal.t_dockind t_dockind,
                                    rsb_brkrep_u.GetPlanExecDate(dvndeal.t_dockind,
                                                                 dvndeal.t_id,
                                                                 decode(dvnfi.t_type,
                                                                        0,
                                                                        1,
                                                                        2)) t_planexecdate,
                                    case
                                      when dvndeal.t_type = 1 or
                                           (dvndeal.t_type = 5 and
                                           decode(dvnfi.t_type, 0, 1, 2) = 1) or
                                           (dvndeal.t_type = 6 and
                                           decode(dvnfi.t_type, 0, 1, 2) = 2) then
                                       1
                                      else
                                       0
                                    end t_isbuy,
                                    decode(dvnfi.t_type, 0, 1, 2) t_dealpart,
                                    dvndeal.t_code,
                                    dvndeal.t_extcode,
                                    to_char(dvndeal.t_date, 'dd.mm.yyyy') t_dates,
                                    dvndeal.t_time,
                                    case when exists (select 1 from dpartyown_dbt where t_partyid = market.t_partyid and t_partykind=3) 
                                         then market.t_name
                                         else case when exists (select 1 from dpartyown_dbt where t_partyid = agent.t_partyid and t_partykind=3) 
                                         then agent.t_name 
                                         else 'Внебиржевой рынок' end
                                    end t_marketname,
                                    nvl(sfcontrmp.t_number, chr(1)) t_sfcontrnumber,
                                    nvl(sfcontr.t_name, chr(1)) t_sfcontrname,
                                    --decode(sfcontr.t_datebegin, to_date('01.01.0001', 'dd.mm.yyyy'), null, sfcontr.t_datebegin) t_sfcontrbegdate,
                                    decode(to_char(sfcontr.t_datebegin, 'dd.mm.yyyy'), '01.01.0001', null, to_char(sfcontr.t_datebegin, 'dd.mm.yyyy')) t_sfcontrbegdate,
                                    --decode(sfcontr.t_dateclose, to_date('01.01.0001', 'dd.mm.yyyy'), null, sfcontr.t_dateclose) t_sfcontrenddate,
                                    decode(to_char(sfcontr.t_dateclose, 'dd.mm.yyyy'), '01.01.0001', null, to_char(sfcontr.t_dateclose, 'dd.mm.yyyy')) t_sfcontrenddate,
                                    nvl(client.t_name, chr(1)) t_clientname,
                                    (nvl(client.t_shortname, chr(1)) || 
                                     CASE WHEN dlobjcode.t_code IS NOT NULL THEN '/' || dlobjcode.t_code else '' end) t_clientshortname,
                                    RSI_RSBPARTY.PT_GetPartyCode(client.t_PartyID,
                                                                 1) t_clientptcode,
                                    dlcontrmp.t_mpcode,
                                    case
                                      when client.t_legalform is null then
                                       chr(1)
                                      when client.t_legalform = 1 then
                                       'ЮЛ'
                                      when nvl(clientp.t_isemployer, chr(0)) =
                                           chr(88) then
                                       'ИП'
                                      else
                                       'ФЛ'
                                    end t_clientform,
                                    case when client.t_legalform is null then
                                      chr(1)
                                     when client.t_legalform = 1 then
                                      nvl(clientc.t_name, chr(1))                             
                                     else case when EXISTS (SELECT 1 FROM DPERSN_DBT persn where persn.t_personid = client.t_partyid  and persn.t_isstateless = chr(88))
                                               THEN 'Лицо без гражданства'
                                               when nvl(clientc.t_name, '') != '' THEN nvl(clientc.t_name, '')
                                               when exists (select 1 from DPERSNCIT_DBT where t_personid = dvndeal.t_client) 
                                               then (select t_name from (select ctry.t_name from DPERSNCIT_DBT pit, dcountry_dbt ctry where pit.t_personid = dvndeal.t_client
                                                                                         and ctry.t_parentcountryid = 0 AND
                                                                                         ctry.t_codelat3 = pit.T_COUNTRYCODELAT3 ORDER BY pit.T_COUNTRYCODELAT3) WHERE ROWNUM = 1)
                                               else nvl(clientc.t_name, '')
                                               end                                    
                                      end 
                                    t_clientcountry,
                                    case
                                      when client.t_notresident is null then
                                       chr(1)
                                      when client.t_notresident = chr(88) then
                                       'нерезидент'
                                      else
                                       'резидент'
                                    end t_clientresident,
                                    case when rsb_common.GetRegFlagValue(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\РУБИЛЬНИК.BOSS-5689') <> CHR(88) 
                                    then
                                   (select 
                                      case when max(kval.attrid) = 2 then 'квалинвестор'
                                           --when kval.attrid = 1 then 'не квалинвестор' DEF-81827
                                           else 'не квалинвестор' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 207
                                       and  objattr.t_groupid = 140
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(dlcontr.t_dlcontrid, 34, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) kval) 
                                    else 
                                      (select case when max(cnt)>0 then 'квалинвестор' else 'не квалинвестор' end
                                         from
                                      (select count(*) as cnt from dscqinv_dbt d
                                      where d.t_partyid = client.t_partyid  and d.t_state = 1))
                                    end t_clientcvalinv,
                                    case
                                      when dvndeal.t_sector = chr(0) then
                                       nvl(contragent.t_name, chr(1))
                                      else
                                       nvl(contragent.t_name,
                                           'НКО НКЦ (АО)')
                                    end t_side2name,
                                    case
                                      when (dvndeal.t_sector = chr(0)) and
                                           (dvndeal.t_client = -1) then
                                       (SELECT genagr.t_code || ' от ' ||
                                               to_char(genagr.t_start,
                                                       'DD.MM.YYYY')
                                          FROM ddl_genagr_dbt genagr
                                         where genagr.t_genagrid =
                                               dvndeal.t_genagrid
                                           and rownum = 1)
                                      else
                                       ''
                                    end t_genagr,
                                    case
                                      when dvndeal.t_sector = chr(0) then
                                       chr(1)
                                      else
                                       nvl(contragent.t_name, chr(1))
                                    end t_contragentname,
                                    /*nvl(rsb_brkrep_u.GetBrokerComissSum(dvndeal.t_dockind,
                                                                        dvndeal.t_id,
                                                                        decode(dvnfi.t_type,
                                                                               0,
                                                                               1,
                                                                               2)),
                                        0) t_brokercomiss,*/
                                    --(case when avrkindsroot.t_name = chr(1) then (select t_name from dfikinds_dbt where t_fi_kind = fininstr.t_fi_kind) else avrkindsroot.t_name end) t_firootname,
                                    nvl(avrkindsroot.t_name, (select t_name from dfikinds_dbt where t_fi_kind = fininstr.t_fi_kind)) t_firootname,
                                    nvl(avrkinds.t_name, chr(1)) t_finame,
                                    nvl(avoiriss.t_isin, chr(1)) t_isin,
                                    nvl(avoiriss.t_lsin, chr(1)) t_lsin,
                                    nvl(avoiriss.t_issue, chr(1)) t_issue,
                                    nvl(avoiriss.t_tranche, chr(1)) t_tranche,
                                    nvl(avoiriss.t_series, chr(1)) t_series,
                                    nvl(issuer.t_shortname, chr(1)) t_issuername,
                                    nvl(fininstr.t_ccy, chr(1)) t_currencyname,
                                    (case when (avrkindsroot.t_fi_kind = 2 and avrkindsroot.t_avoirkind = 16) then fininstr.t_sumprecision end) t_princprecision,
                                    (case when dvndeal.t_kind != 22720 /*процентный своп*/ then dvnfi.t_price when dvnfi.t_course in (0, 1) then
                                    nvl((dvnfi_contr.t_amount),0)/nvl((dvnfi.t_amount), 1)
                                    else dvnfi.t_course end)   t_price,
                                    (case when dvndeal.t_kind != 22720 /*процентный своп*/ then dvnfi.t_price when dvnfi_contr.t_course in (0, 1) then
                                    nvl((dvnfi.t_amount),0)/nvl((dvnfi_contr.t_amount), 1)
                                    else dvnfi_contr.t_course end)   t_contrprice,
                                    (case when dvndeal.t_kind != 22720 /*процентный своп*/ then dvnfi.t_pricefiid else dvnfi_contr.t_fiid end) t_pricefiid,
                                    dvnfi.t_amount t_amount,
                                    dvnfi.t_nkd t_nkd,
                                    dvnfi_contr.t_amount t_contramount,
                                    dvnfi_contr.t_fiid t_contrfiid,
                                    lower(nvl(exectypename.t_sznamealg, chr(1))) t_conftp,
                                    decode(dvnfi.t_exectype, 1,
                                    case
                                      when exists
                                       (select 1
                                              from dpmpaym_dbt
                                             where t_dockind = dvndeal.t_dockind
                                               and t_documentid = dvndeal.t_id
                                               and t_netting = 'X'
                                               and t_paymstatus = 150
                                               and ((decode(dvnfi.t_type, 0, 1, 2) = 1 and t_purpose in (1, 2)) or (decode(dvnfi.t_type, 0, 1, 2) = 2 and t_purpose in (3, 4)))
                                               and rownum = 1) then
                                       'Поставка неттинг'
                                      when exectypename.t_inumberalg = 1 then
                                       'Поставка'
                                    end) t_execmethod,
                                    case when dvndeal.t_sector=chr(88) then
                                      'платежное поручение'
                                     else
                                      'платежное поручение в формате SWIFT'
                                   end t_formpayment,
                                    nvl(fininstr.t_ccy, chr(1)) t_curpfi,
                                    dlcontr.t_dlcontrid t_dlcontrid,
                                    dvndeal.t_client,
                                    0 t_isBasket,
                                    (case
                                      when ((select count(1)
                                               from ddvnfi_dbt
                                              where t_dealid = dvndeal.t_id) > 1) then
                                       1
                                      else
                                       0
                                    end) t_hastwopart
                               from ddvndeal_dbt dvndeal
                               join ddvnfi_dbt dvnfi
                                 on dvnfi.t_dealid = dvndeal.t_id
                               left join ddvnfi_dbt dvnfi_contr
                                 on dvndeal.t_kind = 22720 --процентный своп
                                 and dvnfi_contr.t_dealid = dvndeal.t_id
                                 and dvnfi_contr.t_type != dvnfi.t_type
                               join doprkoper_dbt oprkoper
                                 on oprkoper.t_kind_operation = dvndeal.t_kind
                               join (select BeginDate bdt, EndDate edt
                                      from dual)
                                 on 1 = 1
                               left join dparty_dbt market
                                 on market.t_partyid = dvndeal.t_marketid
                               left join ddlmarket_dbt dlmarket
                                 on dlmarket.t_id = dvndeal.t_marketschemeid
                                 and dvndeal.t_sector = chr(88)
                               left join ddlcontrmp_dbt dlcontrmp
                                 on dlcontrmp.t_sfcontrid =
                                    dvndeal.t_clientcontr
                               left join dsfcontr_dbt sfcontrmp
                                 on sfcontrmp.t_id = dvndeal.t_clientcontr
                               left join ddlcontr_dbt dlcontr
                                 on dlcontr.t_dlcontrid =
                                    dlcontrmp.t_dlcontrid
                               left join dsfcontr_dbt sfcontr
                                 on sfcontr.t_id = dlcontr.t_sfcontrid
                               left join dparty_dbt client
                                 on client.t_partyid = dvndeal.t_client
                               left join dpersn_dbt clientp
                                 on clientp.t_personid = dvndeal.t_client
                               left join dcountry_dbt clientc
                                 on (clientc.t_parentcountryid = 0 AND
                                    clientc.t_codelat3 = client.t_nrcountry) /* MAA: iS - 517711 */
                               left join dparty_dbt contragent
                                 on contragent.t_partyid =
                                    dvndeal.t_contractor
                               left join dfininstr_dbt fininstr
                                 on fininstr.t_fiid = dvnfi.t_fiid
                               left join davoiriss_dbt avoiriss
                                 on avoiriss.t_fiid = dvnfi.t_fiid
                               left join davrkinds_dbt avrkinds
                                 on avrkinds.t_fi_kind = fininstr.t_fi_kind
                                and avrkinds.t_avoirkind =
                                    fininstr.t_avoirkind
                               left join davrkinds_dbt avrkindsroot
                                 on avrkindsroot.t_fi_kind =
                                    fininstr.t_fi_kind
                                and avrkindsroot.t_avoirkind =
                                    rsb_fiinstr.fi_avrkindsgetroot(fininstr.t_fi_kind,
                                                                   fininstr.t_avoirkind)
                               left join dparty_dbt issuer
                                 on issuer.t_partyid = fininstr.t_issuer
                               left join dnamealg_dbt exectypename
                                 on exectypename.t_itypealg = 3422
                                and exectypename.t_inumberalg =
                                    dvnfi.t_exectype
                               left join dnamealg_dbt optiontype
                                 on optiontype.t_itypealg = 3423
                                and optiontype.t_inumberalg =
                                    dvndeal.t_optiontype
                              left join dnamealg_dbt direction
                                 on dvndeal.t_kind = 22720 --процентный своп
                                 and direction.t_itypealg = 7004
                                and direction.t_inumberalg =
                                    dvndeal.t_type
                              left join dparty_dbt agent
                                 on contragent.t_partyid =
                                    dvndeal.t_agent
                              left join ddlobjcode_dbt dlobjcode 
                                     on dlcontr.t_dlcontrid = DLOBJCODE.T_OBJECTID 
                                    and dlobjcode.t_objecttype = 207 
                                    and dlobjcode.t_codekind = 1  
                                    --Если есть ЕКК с нулевой датой - отбираем его, если нет - последний закрытый код  
                                    and dlobjcode.t_bankclosedate = case when exists (select 1 from ddlobjcode_dbt dlobjcodesub1 where dlobjcodesub1.t_objectid = dlobjcode.t_objectid and dlobjcodesub1.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub1.t_codekind = dlobjcode.t_codekind and dlobjcodesub1.t_bankclosedate = to_date('01010001','ddmmyyyy')  ) 
                                      then to_date('01010001','ddmmyyyy') 
                                      else (select max(dlobjcodesub2.t_bankclosedate) from ddlobjcode_dbt dlobjcodesub2 where dlobjcodesub2.t_objectid = dlobjcode.t_objectid and dlobjcodesub2.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub2.t_codekind = dlobjcode.t_codekind) END
                              where --dvndeal.t_dockind in (199, 4813)
                              dvndeal.t_state > 0
                              and dvndeal.t_id
                              in (select num01 
                                     from itt_parallel_exec p
                                     where p.calc_id = Calc_id 
                                         and p.row_id between Row_ID_start  and Row_ID_end 
                                 /*SELECT \*+ PRECOMPUTE_SUBQUERY index(dvndeal DDVNDEAL_DBT_IDX2) index(ddvnfi_dbt DDVNFI_DBT_IDX1) *\
                                   dvndeal.t_id
                                   FROM    ddvndeal_dbt dvndeal
                                    INNER JOIN ddvnfi_dbt dvnfi
                                              ON  dvnfi.t_type in (0,2) and dvnfi.t_dealid = dvndeal.t_id
                                    WHERE dvndeal.t_state > 0
                                       AND (dvndeal.T_DATE BETWEEN bdt  AND edt
                                        OR (dvndeal.T_DATE <= bdt AND dvnfi.t_ExecDate >=bdt))*/)
                           and(sfcontrmp.t_id is NULL or
                              (StockMarket = chr(88) and
                              sfcontrmp.t_servkind = 1) or
                              (FuturesMarket = chr(88) and
                              sfcontrmp.t_servkind = 15) or
                              (CurrencyMarket = chr(88) and
                              sfcontrmp.t_servkind = 21))
                           and (1 = (case
                                      when Sector_Dilers = CHR(88) and (dvndeal.t_client <= 0 or exists
                                       (select 1
                                              from ddp_dep_dbt dp_dep
                                             where dp_dep.t_partyid =
                                                   dvndeal.t_client)) then 1 else 0 end)
                           or 1 = (case
                                      when Sector_Brokers = CHR(88) and dvndeal.t_client > 0 and not exists
                                       (select 1
                                              from ddp_dep_dbt dp_dep
                                             where dp_dep.t_partyid =
                                                   dvndeal.t_client) then 1 else 0 end))
                           and (     SelectedClients = 1 
                                     and (EXISTS(SELECT * FROM D_RIA_PANELCLIENTS_DBT ria_cl WHERE ria_cl.t_clientid = dvndeal.t_client and ria_cl.t_sessionid = V_SESSIONID and ria_cl.T_CALCID = V_CALCID)
                                              OR (1 = (case when Sector_Dilers = CHR(88) and (dvndeal.t_client <= 0 or exists
                                                       (select 1
                                                              from ddp_dep_dbt dp_dep
                                                             where dp_dep.t_partyid =
                                                                   dvndeal.t_client)) then 1 else 0 end))
                                         )
                                      or SelectedClients = 0) 
                               and (     SelectedContrs = 1 
                                     and (EXISTS(SELECT * FROM D_RIA_PANELCONTRS_DBT ria_c WHERE ria_c.t_clientid = dvndeal.t_client and ria_c.t_dlcontrid = dlcontrmp.t_dlcontrid and ria_c.t_sessionid = V_SESSIONID and ria_c.T_CALCID = V_CALCID)
                                              OR (1 = (case when Sector_Dilers = CHR(88) and (dvndeal.t_client <= 0 or exists
                                                       (select 1
                                                              from ddp_dep_dbt dp_dep
                                                             where dp_dep.t_partyid =
                                                                   dvndeal.t_client)) then 1 else 0 end))
                                         )
                                      or SelectedContrs = 0)   
                              ) dt
                              where ((StockMarket = chr(88) and dt.t_bakind in (20, 30, 33, 36, 80) and dt.t_operkind !=12690 )
                                 or (CurrencyMarket = chr(88) and dt.t_bakind not in (20, 30, 33, 36, 80))) or dt.t_dealid = registeria.t_dealid)
                            LOOP
      if (one_rec.t_isbank = 0) then
        registeria.t_part := 1;
      else
        registeria.t_part := 2;
      end if;
      registeria.t_dealsubkind := 2;
      registeria.t_dealid      := one_rec.t_dealid;
      registeria.t_dockind     := one_rec.t_dockind;
      
      if Block_Deals = CHR(88) THEN
          registeria.t_dealcode    := one_rec.t_code;
          registeria.t_dealcodets  := one_rec.t_extcode;
          registeria.t_dealdate    := one_rec.t_date;
          registeria.t_marketname  := one_rec.t_marketname;
          registeria.t_bakindname  := one_rec.t_bakindname;
          registeria.t_optiontype  := one_rec.t_optiontype;
          registeria.t_markettype  := one_rec.t_markettype;

          registeria.t_extbroker    := one_rec.t_brokername;
          registeria.t_extbrokerdoc := one_rec.t_brokercontrnumber;

          registeria.t_bakindname      := one_rec.t_bakindname;
          registeria.t_optiontype        := one_rec.t_optiontype;
          registeria.t_optionpremium := one_rec.t_optionpremium;
          registeria.t_quotecur          := one_rec.t_quotecur;

          if (one_rec.t_isbank = 0) then
            registeria.t_dealtype     := 'клиентская';
            registeria.t_status_purcb := one_rec.t_status_purcb;
            registeria.t_side1        := one_rec.t_clientname;
            registeria.t_client_assign := one_rec.t_client_assign;
          else
            registeria.t_dealtype     := 'собственная';
            registeria.t_status_purcb := 'хозяйствующий субъект';
            registeria.t_side1        := 'АО "РОССЕЛЬХОЗБАНК"';
            registeria.t_client_assign := NULL;
          end if;
          
          if (one_rec.t_IsPFI = 'X') then
            if (one_rec.t_kindpfi is not null ) then
               registeria.t_kindpfi := one_rec.t_kindpfi;
            else
               registeria.t_kindpfi := one_rec.t_side;
            end if;
          else
            registeria.t_kindpfi := NULL;
          end if;

          registeria.t_side2          := one_rec.t_side2name;
          registeria.t_genagr         := one_rec.t_genagr;
          registeria.t_confirmdoc     := 'документы получены';
          registeria.t_dueprocess     := one_rec.t_conftp;
          registeria.t_execmethod     := one_rec.t_execmethod;
          registeria.t_formpayment    := one_rec.t_formpayment;
          registeria.t_unqualifiedsec := one_rec.t_qualification;
          --registeria.t_dealtypeAdr     := one_rec.t_dealtypeAdr;
          registeria.t_dealdirection   := one_rec.t_side;
          registeria.t_dealkind        := one_rec.t_side2;
          registeria.t_seckind         := one_rec.t_firootname;
          if (one_rec.t_firootname != one_rec.t_finame) then
             registeria.t_secsubkind := one_rec.t_finame;
          else
            registeria.t_secsubkind := NULL;
          end if;
          registeria.t_seccode         := one_rec.t_avrname || '/' ||
                                          one_rec.t_avrcode;
          if (not (one_rec.t_lsin = chr(1) and one_rec.t_isin = chr(1))) then
             registeria.t_isin            := '№' || one_rec.t_lsin || '/ ISIN ' ||
                                          one_rec.t_isin;
          else 
            registeria.t_isin            := NULL;
          end if;
          if (not (one_rec.t_issue = chr(1) and one_rec.t_tranche = chr(1) and one_rec.t_series = chr(1))) then
             registeria.t_secseries       := 'выпуск №' || one_rec.t_issue || '/ транш №' ||
                                          one_rec.t_tranche || '/ серия №' ||
                                          one_rec.t_series;
          else
           registeria.t_secseries       := NULL;
          end if;
          registeria.t_issuername      := one_rec.t_issuername;
          registeria.t_principal       := one_rec.t_principal;
          registeria.t_cost            := one_rec.t_cost;
          registeria.t_dealvalue       := one_rec.t_dvalue;
          registeria.t_totalcost       := one_rec.t_totalcost;
          
          if (one_rec.t_pricefiid != 0) then
          registeria.t_dealvaluerur   := round(rsi_rsb_fiinstr.convsum(round(one_rec.t_dvalue, 6),
                                                                         one_rec.t_pricefiid,
                                                                         0,
                                                                         COALESCE(one_rec.t_factdatem, one_rec.t_plandatem, one_rec.t_execdate)), 6);
          registeria.t_totalcostrur    := round(rsi_rsb_fiinstr.convsum(round(one_rec.t_totalcost, 6),
                                                                         one_rec.t_pricefiid,
                                                                         0,
                                                                         COALESCE(one_rec.t_factdatem, one_rec.t_plandatem, one_rec.t_execdate)), 6);
          registeria.t_costnatcur      := round(rsi_rsb_fiinstr.convsum(round(one_rec.t_cost, 6),
                                                                         one_rec.t_pricefiid,
                                                                         0,
                                                                         COALESCE(one_rec.t_factdatem, one_rec.t_plandatem, one_rec.t_execdate)), 6);                                                                  
         else 
          registeria.t_costnatcur := NULL;
          registeria.t_dealvaluerur  := NULL;  
          registeria.t_totalcostrur  := NULL;                                       
         end if;
         
          if (one_rec.t_isbasket = 1) then
           registeria.t_pricecurrency   := 'RUB';
           registeria.t_cost                := NULL;
           registeria.t_costnatcur       := NULL;
          else
           registeria.t_pricecurrency   := one_rec.t_pricecurrency;
          end if;

          registeria.t_setcurrency     := one_rec.t_setcurrency;
          --registeria.t_interestrate    := one_rec.t_incomerate;
          --registeria.t_interestamount  := one_rec.t_returnincome;
          registeria.t_brokercomiss := one_rec.t_brokercomiss;
          registeria.t_plandatesp   := one_rec.t_plandatesp;
          registeria.t_factdatesp   := one_rec.t_factdatesp;
          registeria.t_plandatem    := TO_CHAR(one_rec.t_plandatem, 'DD.MM.YYYY');
          registeria.t_factdatem    := TO_CHAR(one_rec.t_factdatem, 'DD.MM.YYYY');
          registeria.t_cond_susp   := substr(one_rec.t_cond_susp,1,100);
      END IF;
      
      if (one_rec.t_isbank = 0 and Block_Clients = CHR(88)) then
        registeria.t_clientname     := one_rec.t_clientshortname;
        registeria.t_clientagr      := one_rec.t_sfcontrnumber;
        --registeria.t_clientagrdate  := to_char(one_rec.t_sfcontrbegdate, 'DD.MM.YYYY');
        --registeria.t_clientagrend   := to_char(one_rec.t_sfcontrenddate, 'DD.MM.YYYY');
        registeria.t_clientagrdate  := one_rec.t_sfcontrbegdate;
        registeria.t_clientagrend   := one_rec.t_sfcontrenddate;
        registeria.t_exchangecode   := one_rec.t_mpcode;
        registeria.t_proxyname      := one_rec.t_proxyname;
        registeria.t_proxynum       := one_rec.t_proxynum;
        registeria.t_clientform     := one_rec.t_clientform;
        registeria.t_clientcountry  := one_rec.t_clientcountry;
        registeria.t_clientresident := one_rec.t_clientresident;
        registeria.t_clientcvalinv  := one_rec.t_clientcvalinv;
        registeria.t_risklevel      := one_rec.t_risklevel;
        registeria.t_cashright      := one_rec.t_cashright;
      else
        registeria.t_clientname     := NULL;
        registeria.t_clientagr      := NULL;
        registeria.t_clientagrdate  := NULL;
        registeria.t_clientagrend   := NULL;
        registeria.t_exchangecode   := NULL;
        registeria.t_proxyname      := NULL;
        registeria.t_proxynum       := NULL;
        registeria.t_clientform     := NULL;
        registeria.t_clientcountry  := NULL;
        registeria.t_clientresident := NULL;
        registeria.t_clientcvalinv  := NULL;
        registeria.t_risklevel      := NULL;
        registeria.t_cashright      := NULL;
      end if;

      -- Biq-9103
      registeria.t_margincall := NULL; 
      if( RSB_SECUR.GetMainObjAttr(RSB_SECUR.GetDvnObjType(one_rec.t_dockind), LPAD(one_rec.t_dealid, 34, '0'), 116, one_rec.t_date) = 1) THEN
        registeria.t_margincall := 'X'; 
      end if;

      --служебные поля
      registeria.t_clientid      := one_rec.clientid;
      registeria.t_clientcontrid := one_rec.clientcontrid;
      registeria.t_pfi           := one_rec.pfi;
      registeria.t_dealpart      := one_rec.t_dealpart;
      registeria.t_isbasket      := one_rec.t_isbasket;
      registeria.t_isbuy         := one_rec.t_isbuy;
      registeria.t_princprecision := one_rec.t_princprecision;
      registeria.t_dealtime       := one_rec.t_time;
      registeria.t_sessionid := V_SESSIONID;
      registeria.t_calcid := V_CALCID;

      g_registeria_ins.extend;
      g_registeria_ins(g_registeria_ins.LAST) := registeria;
      IF g_registeria_ins.count >= 100000  THEN
        FORALL i IN g_registeria_ins.FIRST .. g_registeria_ins.LAST
           INSERT INTO DDL_REGIABUF_DBT VALUES g_registeria_ins (i);
           g_registeria_ins.delete;
       END IF;

    END LOOP;

    IF g_registeria_ins IS NOT EMPTY THEN
      FORALL i IN g_registeria_ins.FIRST .. g_registeria_ins.LAST
        INSERT INTO DDL_REGIABUF_DBT VALUES g_registeria_ins (i);
      g_registeria_ins.delete;
    END IF;
    
  END CreateData_DV_NDeals_parallel;

  PROCEDURE CreateData_DV_Deals(BeginDate      IN DATE,
                                 EndDate        IN DATE,
                                 StockMarket    IN CHAR,
                                 FuturesMarket  IN CHAR,
                                 CurrencyMarket IN CHAR,
                                 Sector_Brokers IN CHAR,
                                 Sector_Dilers  IN CHAR,
                                 Sector_Clients IN CHAR,
                                 Block_Deals    IN CHAR,
                                 Block_Clients  IN CHAR,
                                 Block_InAcc    IN CHAR,
                                 SelectedClients IN NUMBER,
                                 SelectedContrs   IN NUMBER) IS
    TYPE registeria_t IS TABLE OF DDL_REGIABUF_DBT%ROWTYPE;
    g_registeria_ins registeria_t := registeria_t();
    registeria       DDL_REGIABUF_DBT%rowtype;
  BEGIN
    PushLogLine('Отбор сделок срочного рынка - begin','Отбор сделок срочного рынка');
    FOR one_rec IN (select dt.clientid,
                           dt.clientcontrid,
                           dt.pfi,
                           dt.t_dealid,
                           dt.t_dockind,
                           dt.t_date,
                           dt.t_isbank,
                           dt.t_code,
                           dt.t_extcode,
                           dt.t_dates,
                           dt.t_time,
                           dt.t_planexecdate,
                           dt.t_marketname,
                           dt.t_optiontype,
                           dt.t_futuresprice,
                           dt.t_optionbonus,
                           dt.t_positioncost,
                           dt.t_futuresamount,
                           dt.t_quoteccy,
                           (select t.t_ShortName
                              from dparty_dbt t
                             where t.t_Partyid = dt.brokerid) t_brokername,
                           (select t.t_Number
                              from dsfcontr_dbt t
                             where t.t_id = dt.brokercontrid) t_brokercontrnumber,
                           dt.t_sfcontrnumber,
                           dt.t_sfcontrbegdate t_sfcontrbegdate,
                           decode(dt.t_sfcontrenddate, to_date('01.01.0001','DD.MM.YYYY'), null, dt.t_sfcontrenddate) t_sfcontrenddate,
                           dt.t_clientname,
                           dt.t_clientshortname,
                           dt.t_clientptcode,
                           dt.t_mpcode,
                           dt.t_clientform,
                           dt.t_clientcountry as t_clientcountry,
                           (case when dt.t_clientform = 'ФЛ' then dt.t_clientresident else '' end) as t_clientresident,
                           dt.t_clientcvalinv,
                           dt.t_side2name,
                           dt.t_genagr,
                           dt.t_contragentname,
                           (case when dt.t_isBank = 0 then    
                           nvl((SELECT SUM(nvl(dlc.t_Sum, 0))
                                  FROM ddvdlcom_dbt dlc, dsfcomiss_dbt sfcom
                                 WHERE dlc.t_dealid = dt.t_dealid
                                 AND sfcom.t_ComissID = dlc.t_ComissID
                                 --AND dlc.t_isbankexpenses=chr(0)
                                 AND sfcom.t_ReceiverID != dt.t_marketid), 0) 
                           --rsb_brkrep_rshb.DV_GetMarketComissSum(dt.t_DealID, dt.t_marketid, edt)
                            end)
                            t_brokercomiss,
                            (select substr(rsb_struct.getString(note_assignt.t_Text),1,50)
                               from dnotetext_dbt note_assignt 
                              where note_assignt.t_objecttype = 140
                                and note_assignt.t_notekind = 101
                                and note_assignt.t_documentid = lpad(dt.t_dealid, 34, '0')
                             ) t_client_assign,
                           case
                             when dt.t_isbank = 1 then
                              'С'
                             else
                              'К'
                           end t_kind,
                            (case  when not (dt.t_kindoper = 12630 or dt.t_kindoper = 12640 or dt.t_kindoper=2645) then
                              decode(dt.t_isbuy,
                                  1,
                                  'Покупка',
                                  'Продажа') end) t_side,
                           dt.t_isbuy,
                           dt.t_opername t_side2,
                           (case 
                              when dt.t_kindoper = 12630 or dt.t_kindoper = 12640 or dt.t_kindoper=2645
                                  then 'Исполнение' || (case when dt.t_position = 1 then ' короткие позиции' when dt.t_position = 2 then ' длинные позиции' end) 
                               else decode(dt.t_isbuy,
                                  1,
                                  'Покупка',
                                  'Продажа')
                            end) t_kindpfi,
                           dt.t_firootname,
                           dt.t_avrname,
                           dt.t_avrcode,
                           dt.t_issue,
                           dt.t_tranche,
                           dt.t_series,
                           dt.t_finame,
                           dt.t_isin,
                           dt.t_lsin,
                           dt.t_issuername,
                           dt.t_currencyname,
                           (select 'да'
                              from dobjattr_dbt  objattr,
                                   dobjatcor_dbt objatcor
                             where objattr.t_objecttype = 12
                               and objattr.t_groupid = 28
                               and objatcor.t_objecttype =
                                   objattr.t_objecttype
                               and objatcor.t_groupid = objattr.t_groupid
                               and objatcor.t_attrid = objattr.t_attrid
                               and objatcor.t_object = lpad(dt.pfi, 10, '0')
                               and objatcor.t_validfromdate <= edt
                               and objatcor.t_validtodate > edt
                               and objatcor.t_attrid = 2
                               and rownum = 1) t_qualification,
                           case when dt.t_clientcontr > 0 then
                           decode(dt.t_role, 0,
                           (select lower(objattr.t_name)
                              from dobjattr_dbt  objattr,
                                   dobjatcor_dbt objatcor
                             where objattr.t_objecttype = 659
                               and objattr.t_groupid = 1
                               and objatcor.t_objecttype =
                                   objattr.t_objecttype
                               and objatcor.t_groupid = objattr.t_groupid
                               and objatcor.t_attrid = objattr.t_attrid
                               and objatcor.t_object =
                                   lpad(dt.t_clientcontr, 10, '0')
                               and objatcor.t_validfromdate <= edt
                               and objatcor.t_validtodate > edt
                               and rownum = 1),
                           (select lower(objattr.t_name)
                              from dobjattr_dbt objattr
                             where objattr.t_objecttype = 659
                               and objattr.t_groupid = 1
                               and objattr.t_attrid = dt.t_role
                               and rownum = 1))
                           else
                             null
                           end t_status_purcb,
                           dt.t_formpayment,
                           dt.t_ispfi,
                           case
                             when dt.t_isbank = 1 or dt.t_dlcontrid is null then
                              chr(1)
                             else
                              case when dt.servkind = 1 then
                                      (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 659
                                       and  objattr.t_groupid = 6
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(dt.clientcontrid, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) pravo)
                                   else
                                       (select 
                                      case when pravo.attrid = 2 then 'право не предоставлено'
                                           when pravo.attrid = 1 then 'право предоставлено' 
                                           else '' end
                                     from 
                                       (select NVL(objatcor.t_attrid, -1) attrid  
                                          from dobjattr_dbt  objattr,
                                               dobjatcor_dbt objatcor,
                                               DDLCONTRMP_DBT contrmp_pr,
                                               DSFCONTR_DBT sfcontr_pr
                                         where contrmp_pr.T_DLCONTRID = dt.t_dlcontrid
                                           and sfcontr_pr.t_id = contrmp_pr.t_sfcontrid
                                           and sfcontr_pr.t_servkind = 1
                                           and objattr.t_objecttype = 659
                                           and objattr.t_groupid = 6
                                           and objatcor.t_objecttype =
                                               objattr.t_objecttype
                                           and objatcor.t_groupid =
                                               objattr.t_groupid
                                           and objatcor.t_attrid =
                                               objattr.t_attrid
                                           and objatcor.t_object =
                                               lpad(sfcontr_pr.t_id, 10, '0')
                                           and objatcor.t_validfromdate <= edt
                                           and objatcor.t_validtodate > edt ORDER BY objatcor.t_attrid) pravo WHERE ROWNUM = 1)
                                   end
                           end t_cashright,
                           (select (case when lower(rsb_struct.getString(t_Text)) like 'безадресная заявка%' then 'безадресная' when rsb_struct.getString(t_Text) like 'адресная заявка%' then 'адресная' end)
                            from dnotetext_dbt notetext 
                          where t_objecttype=140
                            and t_notekind=102
                            and t_documentid=lpad(dt.t_dealid, 34, '0')
                             )  t_dealtypeAdr,
                           case
                             when dt.t_isbank = 1 then
                              chr(1)
                             else
                               (select trim(nvl(max(decode(t_attrid, 1, 'КСУР', decode(t_attrid, 2, 'КПУР', decode(t_attrid, 3, 'КОУР', decode(t_attrid, 4, 'КНУР'))))),
                                                          'отсутствует'))
                                      from dobjatcor_dbt objatcor
                                     where objatcor.t_objecttype = 3
                                       and /*Субъект экономики*/
                                           objatcor.t_groupid = 95
                                       and objatcor.t_object =
                                           lpad(clientid, 10, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt)
                           end t_risklevel,
                           case
                             when dt.t_isbank = 1 then
                              chr(1)
                             else
                              nvl((select listagg(clientproxy.t_name, '/') within group(order by clientproxy.t_name, ptproxy.t_proxyid)
                                    from dptproxy_dbt ptproxy
                                    join dparty_dbt clientproxy
                                      on clientproxy.t_partyid =
                                         ptproxy.t_proxyid
                                   where ptproxy.t_partyid = dt.t_client
                                     and ptproxy.t_docdate <= edt
                                     and ptproxy.t_validitydate >= edt),
                                  chr(1))
                           end t_proxyname,
                           case
                             when dt.t_isbank = 1 then
                              chr(1)
                             else
                              nvl((select listagg(ptproxy.t_docnumber || ' с ' ||
                                                 to_char(ptproxy.t_docdate,
                                                         'dd.mm.yyyy') ||
                                                 ' по ' ||
                                                 to_char(ptproxy.t_validitydate,
                                                         'dd.mm.yyyy'),
                                                 '/') within group(order by ptproxy.t_proxyid, ptproxy.t_proxyid)
                                    from dptproxy_dbt ptproxy
                                    join dparty_dbt clientproxy
                                      on clientproxy.t_partyid =
                                         ptproxy.t_proxyid
                                   where ptproxy.t_partyid = dt.t_client
                                     and ptproxy.t_docdate <= edt
                                     and ptproxy.t_validitydate >= edt),
                                  chr(1))
                           end t_proxynum,
                           dt.t_isBasket, /*GAA:495699*/
                           dt.t_bakind,
                           case when dt.t_ispfi = chr(88) then
                           (select t_sznamealg
                                      from dnamealg_dbt
                                     where t_itypealg = 3421
                                       and t_inumberalg = t_bakind)
                           end t_bakindname,
                            dt.t_markettype,
                            dt.t_futuresamount t_principal,
                            round(dt.t_positioncost/dt.t_futuresamount,6) t_cost,
                            dt.t_positioncost t_dealvalue,
                            dt.t_positioncost t_totalcost,
                            dt.t_parentfi t_pricefiid,
                             nvl((Select rsb_struct.getString(note.t_text) 
                                   from dnotetext_dbt note 
                                   where note.t_notekind=408 
                                      and note.t_documentid in (lpad(dt.t_dealid,34,'0')) 
                                      and note.t_objecttype = 140
                           ),'') t_cond_susp
                      from (select bdt,
                                    edt,
                                    ptoffice.t_officename t_markettype,
                                    dvdeal.t_client clientid,
                                    dvdeal.t_position,
                                    dvdeal.t_clientcontr clientcontrid,
                                    sfcontr.t_servkind servkind,
                                    dvdeal.t_broker brokerid,
                                    dvdeal.t_brokercontr brokercontrid,
                                    dvdeal.t_fiid pfi,
                                    dvdeal.t_kind t_kindoper,
                                    dvdeal.t_clientcontr,
                                    pfi.t_parentfi,
                                    market.t_partyid t_marketid,
                                    dlcontrmp.t_mpcode,
                                    'X' t_ispfi,
                                    fininstr.t_facevaluefi t_facevaluefi,
                                    fininstr.t_name t_avrname,
                                    fininstr.t_fi_code t_avrcode,
                                    fininstr.t_fi_kind t_fi_kind,
                                    dlcontrmp.t_role,
                                    dvdeal.t_price t_futuresprice, --цена фьючерского контракта
                                    (case when pfi.t_avoirkind = 2 then dvdeal.t_bonus end) t_optionbonus, --премия по опциону
                                    dvdeal.t_positioncost, --сумма по операции
                                    dvdeal.t_amount t_futuresamount, --количество контракто
                                    nvl(quote.t_ccy, chr(1)) t_quoteccy, -- валюта котировки
                                    rsb_secur.get_BA_Kind(fininstr.t_fi_kind,
                                                          fininstr.t_avoirkind,
                                                          avrkinds.t_root)
                                    t_bakind, --вид базового актива
                                    oprkoper.t_name t_opername,
                                    optiontype.t_sznamealg t_optiontype,
                                    case
                                      when dvdeal.t_client <= 0 or exists
                                       (select 1
                                              from ddp_dep_dbt dp_dep
                                             where dp_dep.t_partyid =
                                                   dvdeal.t_client) then
                                       1
                                      else
                                       0
                                    end t_isbank,
                                    dvdeal.t_id t_dealid,
                                    dvdeal.t_date t_date,
                                    192 t_dockind,
                                    rsb_brkrep_u.GetPlanExecDate(192,
                                                                 dvdeal.t_id,
                                                                 1) t_planexecdate,
                                    case
                                      when dvdeal.t_type = 'B' then
                                       1
                                      else
                                       0
                                    end t_isbuy,
                                    dvdeal.t_code,
                                    dvdeal.t_extcode,
                                    to_char(dvdeal.t_date, 'dd.mm.yyyy') t_dates,
                                    dvdeal.t_time,
                                    nvl(market.t_name, chr(1)) t_marketname,
                                    nvl(sfcontrmp.t_number, chr(1)) t_sfcontrnumber,
                                    nvl(sfcontr.t_name, chr(1)) t_sfcontrname,
                                    --decode(sfcontr.t_datebegin, to_date('01.01.0001', 'dd.mm.yyyy'), null, sfcontr.t_datebegin) t_sfcontrbegdate,
                                    decode(to_char(sfcontr.t_datebegin, 'dd.mm.yyyy'), '01.01.0001', null, to_char(sfcontr.t_datebegin, 'dd.mm.yyyy')) t_sfcontrbegdate,
                                    --decode(sfcontr.t_dateclose, to_date('01.01.0001', 'dd.mm.yyyy'), null, sfcontr.t_dateclose) t_sfcontrenddate,
                                    decode(to_char(sfcontr.t_dateclose, 'dd.mm.yyyy'), '01.01.0001', null, to_char(sfcontr.t_dateclose, 'dd.mm.yyyy')) t_sfcontrenddate,
                                    nvl(client.t_name, chr(1)) t_clientname,
                                    (nvl(client.t_shortname, chr(1)) || 
                                     CASE WHEN dlobjcode.t_code IS NOT NULL THEN '/' || dlobjcode.t_code else '' end) t_clientshortname,
                                    RSI_RSBPARTY.PT_GetPartyCode(client.t_PartyID,
                                                                 1) t_clientptcode,
                                    case
                                      when client.t_legalform is null then
                                       chr(1)
                                      when client.t_legalform = 1 then
                                       'ЮЛ'
                                      when nvl(clientp.t_isemployer, chr(0)) =
                                           chr(88) then
                                       'ИП'
                                      else
                                       'ФЛ'
                                    end t_clientform,
                                    case when client.t_legalform is null then
                                      chr(1)
                                     when client.t_legalform = 1 then
                                      nvl(clientc.t_name, chr(1))                             
                                     else case when EXISTS (SELECT 1 FROM DPERSN_DBT persn where persn.t_personid = client.t_partyid  and persn.t_isstateless = chr(88))
                                               THEN 'Лицо без гражданства'
                                               when nvl(clientc.t_name, '') != '' THEN nvl(clientc.t_name, '')
                                               when exists (select 1 from DPERSNCIT_DBT where t_personid = dvdeal.t_client) 
                                               then (select t_name from (select ctry.t_name from DPERSNCIT_DBT pit, dcountry_dbt ctry where pit.t_personid = dvdeal.t_client 
                                                                                         and ctry.t_parentcountryid = 0 AND
                                                                                         ctry.t_codelat3 = pit.T_COUNTRYCODELAT3 ORDER BY pit.T_COUNTRYCODELAT3) WHERE ROWNUM = 1)
                                               else nvl(clientc.t_name, '')
                                               end                                    
                                      end 
                                    t_clientcountry,
                                    case
                                      when client.t_notresident is null then
                                       chr(1)
                                      when client.t_notresident = chr(88) then
                                       'нерезидент'
                                      else
                                       'резидент'
                                    end t_clientresident,
                                    case when rsb_common.GetRegFlagValue(p_KeyPath => 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\РУБИЛЬНИК.BOSS-5689') <> CHR(88) 
                                    then
                                   (select 
                                      case when max(kval.attrid) = 2 then 'квалинвестор'
                                           --when kval.attrid = 1 then 'не квалинвестор' DEF-81827
                                           else 'не квалинвестор' end
                                     from 
                                   (select NVL(objatcor.t_attrid, -1) attrid  
                                      from dobjattr_dbt  objattr,
                                           dobjatcor_dbt objatcor
                                     where objattr.t_objecttype = 207
                                       and  objattr.t_groupid = 140
                                       and objatcor.t_objecttype =
                                           objattr.t_objecttype
                                       and objatcor.t_groupid =
                                           objattr.t_groupid
                                       and objatcor.t_attrid =
                                           objattr.t_attrid
                                       and objatcor.t_object =
                                           lpad(dlcontr.t_dlcontrid, 34, '0')
                                       and objatcor.t_validfromdate <= edt
                                       and objatcor.t_validtodate > edt) kval) 
                                    else 
                                      (select case when max(cnt)>0 then 'квалинвестор' else 'не квалинвестор' end
                                         from
                                      (select count(*) as cnt from dscqinv_dbt d
                                      where d.t_partyid = client.t_partyid  and d.t_state = 1))
                                    end t_clientcvalinv,
                                    'НКО НКЦ (АО)'  t_side2name,
                                    (SELECT genagr.t_code || ' от ' ||
                                               to_char(genagr.t_start,
                                                       'DD.MM.YYYY')
                                          FROM ddl_genagr_dbt genagr
                                         where genagr.t_genagrid =
                                               dvdeal.t_genagrid
                                           and rownum = 1) t_genagr,
                                    case
                                      when dvdeal.t_sector = chr(0) then
                                       chr(1)
                                      else
                                       nvl(issuer.t_name, chr(1))
                                    end t_contragentname,
                                    nvl(avrkindsroot.t_name, (select t_name from dfikinds_dbt where t_fi_kind = fininstr.t_fi_kind)) t_firootname,
                                    nvl(avrkinds.t_name, chr(1)) t_finame,
                                    nvl(avoiriss.t_isin, chr(1)) t_isin,
                                    nvl(avoiriss.t_lsin, chr(1)) t_lsin,
                                    nvl(avoiriss.t_issue, chr(1)) t_issue,
                                    nvl(avoiriss.t_tranche, chr(1)) t_tranche,
                                    nvl(avoiriss.t_series, chr(1)) t_series,
                                    (case when not (avrkindsroot.t_fi_kind = 4 and avrkindsroot.t_avoirkind = 1) then nvl(issuer.t_shortname, chr(1))  end) t_issuername,
                                    nvl(curfi.t_ccy, chr(1)) t_currencyname,
                                    'платежное поручение' t_formpayment,
                                    nvl(curfi.t_ccy, chr(1)) t_curpfi,
                                    dlcontr.t_dlcontrid t_dlcontrid,
                                    dvdeal.t_client,
                                    0 t_isBasket,
                                    (case
                                      when ((select count(1)
                                               from ddvnfi_dbt
                                              where t_dealid = dvdeal.t_id) > 1) then
                                       1
                                      else
                                       0
                                    end) t_hastwopart
                               from ddvdeal_dbt dvdeal
                               join dfideriv_dbt fideriv
                                 on fideriv.t_fiid = dvdeal.t_fiid
                               join doprkoper_dbt oprkoper
                                 on oprkoper.t_kind_operation = dvdeal.t_kind
                               join (select BeginDate bdt, EndDate edt
                                      from dual)
                                 on 1 = 1
                               left join ddlmarket_dbt dlmarket
                                 on dlmarket.t_id = dvdeal.t_marketschemeid
                               left join dptoffice_dbt ptoffice
                                 on ptoffice.t_partyid = dlmarket.t_centr
                                 and ptoffice.t_officeid = dlmarket.t_centroffice
                               left join ddlcontrmp_dbt dlcontrmp
                                 on dlcontrmp.t_sfcontrid =
                                    dvdeal.t_clientcontr
                               left join dsfcontr_dbt sfcontrmp
                                 on sfcontrmp.t_id = dvdeal.t_clientcontr
                               left join ddlcontr_dbt dlcontr
                                 on dlcontr.t_dlcontrid =
                                    dlcontrmp.t_dlcontrid
                               left join dsfcontr_dbt sfcontr
                                 on sfcontr.t_id = dlcontr.t_sfcontrid
                               left join dparty_dbt client
                                 on client.t_partyid = dvdeal.t_client
                               left join dpersn_dbt clientp
                                 on clientp.t_personid = dvdeal.t_client
                               left join dcountry_dbt clientc
                                 on (clientc.t_parentcountryid = 0 AND
                                    clientc.t_codelat3 = client.t_nrcountry) /* MAA: iS - 517711 */
                               left join dfininstr_dbt pfi
                                 on pfi.t_fiid = dvdeal.t_fiid
                               left join dparty_dbt market
                                 on market.t_partyid = pfi.t_issuer
                               left join dfininstr_dbt fininstr
                                 on fininstr.t_fiid = pfi.t_facevaluefi
                               left join dfininstr_dbt curfi
                                 on curfi.t_fiid = pfi.t_parentfi
                               left join dfininstr_dbt quote
                                 on quote.t_fiid = fideriv.t_tickfiid
                               left join davoiriss_dbt avoiriss
                                 on avoiriss.t_fiid = fininstr.t_fiid
                               left join davrkinds_dbt avrkinds
                                 on avrkinds.t_fi_kind = fininstr.t_fi_kind
                                and avrkinds.t_avoirkind =
                                    fininstr.t_avoirkind
                               left join davrkinds_dbt avrkindsroot
                                 on avrkindsroot.t_fi_kind =
                                    fininstr.t_fi_kind
                                and avrkindsroot.t_avoirkind =
                                    rsb_fiinstr.fi_avrkindsgetroot(fininstr.t_fi_kind,
                                                                   fininstr.t_avoirkind)
                               left join dparty_dbt issuer
                                 on issuer.t_partyid = fininstr.t_issuer
                               left join dnamealg_dbt optiontype
                                 on optiontype.t_itypealg = 3423
                                and optiontype.t_inumberalg =
                                    fideriv.t_optiontype
                               left join ddlobjcode_dbt dlobjcode 
                                     on dlcontr.t_dlcontrid = DLOBJCODE.T_OBJECTID 
                                    and dlobjcode.t_objecttype = 207 
                                    and dlobjcode.t_codekind = 1  
                               --Если есть ЕКК с нулевой датой - отбираем его, если нет - последний закрытый код  
                               and dlobjcode.t_bankclosedate = case when exists (select 1 from ddlobjcode_dbt dlobjcodesub1 where dlobjcodesub1.t_objectid = dlobjcode.t_objectid and dlobjcodesub1.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub1.t_codekind = dlobjcode.t_codekind and dlobjcodesub1.t_bankclosedate = to_date('01010001','ddmmyyyy')  ) 
                                   then to_date('01010001','ddmmyyyy') 
                                   else (select max(dlobjcodesub2.t_bankclosedate) from ddlobjcode_dbt dlobjcodesub2 where dlobjcodesub2.t_objectid = dlobjcode.t_objectid and dlobjcodesub2.t_objecttype = dlobjcode.t_objecttype and dlobjcodesub2.t_codekind = dlobjcode.t_codekind) END                              
                              where dvdeal.t_state > 0 and dvdeal.T_DATE between BeginDate and EndDate
                           and (sfcontrmp.t_id is NULL or
                              (StockMarket = chr(88) and
                              sfcontrmp.t_servkind = 1) or
                              (FuturesMarket = chr(88) and
                              sfcontrmp.t_servkind = 15) or
                              (CurrencyMarket = chr(88) and
                              sfcontrmp.t_servkind = 21))
                            AND (1 = (case
                                      when Sector_Dilers = CHR(88) and (dvdeal.t_client <= 0 or exists
                                       (select 1
                                              from ddp_dep_dbt dp_dep
                                             where dp_dep.t_partyid =
                                                   dvdeal.t_client)) then 1 ELSE 0 END)
                            OR 1 = (case
                                      when Sector_Brokers = CHR(88) and dvdeal.t_client > 0 and not exists
                                       (select 1
                                              from ddp_dep_dbt dp_dep
                                             where dp_dep.t_partyid =
                                                   dvdeal.t_client) then 1 ELSE 0 END))
                            AND (     SelectedClients = 1 
                                     and (EXISTS(SELECT * FROM D_RIA_PANELCLIENTS_DBT ria_cl WHERE ria_cl.t_clientid = dvdeal.t_client and ria_cl.t_sessionid = V_SESSIONID and ria_cl.T_CALCID = V_CALCID)
                                              OR (1 = (case when Sector_Dilers = CHR(88) and (dvdeal.t_client <= 0 or exists
                                                           (select 1
                                                                  from ddp_dep_dbt dp_dep
                                                                 where dp_dep.t_partyid =
                                                                       dvdeal.t_client)) then 1 ELSE 0 END))
                                         )
                                      or SelectedClients = 0) 
                            AND (     SelectedContrs = 1 
                                     and (EXISTS(SELECT * FROM D_RIA_PANELCONTRS_DBT ria_c WHERE ria_c.t_clientid = dvdeal.t_client and ria_c.t_dlcontrid = dlcontrmp.t_dlcontrid and ria_c.t_sessionid = V_SESSIONID and ria_c.T_CALCID = V_CALCID)
                                              OR (1 = (case when Sector_Dilers = CHR(88) and (dvdeal.t_client <= 0 or exists
                                                           (select 1
                                                                  from ddp_dep_dbt dp_dep
                                                                 where dp_dep.t_partyid =
                                                                       dvdeal.t_client)) then 1 ELSE 0 END))
                                         )
                                      or SelectedContrs = 0) 
                              ) dt
  ) LOOP
      if (one_rec.t_isbank = 0) then
        registeria.t_part := 1;
      else
        registeria.t_part := 2;
      end if;
      registeria.t_dealsubkind := 3;
      registeria.t_dealpart := 0;
      registeria.t_dealid      := one_rec.t_dealid;
      registeria.t_dockind     := one_rec.t_dockind;
      
      IF Block_Deals = CHR(88) THEN
          registeria.t_dealcode    := one_rec.t_code;
          registeria.t_dealcodets  := one_rec.t_extcode;
          registeria.t_dealdate    := one_rec.t_date;
          registeria.t_marketname  := one_rec.t_marketname;
          registeria.t_markettype  := one_rec.t_markettype;
          registeria.t_dealtypeAdr     := one_rec.t_dealtypeAdr;

          registeria.t_extbroker    := one_rec.t_brokername;
          registeria.t_extbrokerdoc := one_rec.t_brokercontrnumber;

          registeria.t_futuresprice    := one_rec.t_futuresprice;
          registeria.t_optionpremium   := one_rec.t_optionbonus;
          registeria.t_optiondealbonus := one_rec.t_positioncost;
          registeria.t_futuresamount   := one_rec.t_futuresamount;
          registeria.t_quotecur        := one_rec.t_quoteccy;
          registeria.t_bakindname      := one_rec.t_bakindname;
          registeria.t_optiontype      := one_rec.t_optiontype;
          registeria.t_kindpfi         := one_rec.t_kindpfi;

          if (one_rec.t_isbank = 0) then
            registeria.t_dealtype     := 'клиентская';
            registeria.t_status_purcb := one_rec.t_status_purcb;
            registeria.t_side1        := one_rec.t_clientname;
            registeria.t_client_assign := one_rec.t_client_assign;
          else
            registeria.t_dealtype     := 'собственная';
            registeria.t_status_purcb := 'хозяйствующий субъект';
            registeria.t_side1        := 'АО "РОССЕЛЬХОЗБАНК"';
            registeria.t_client_assign := NULL;
          end if;

          registeria.t_side2          := one_rec.t_side2name;
          registeria.t_genagr         := one_rec.t_genagr;
          registeria.t_confirmdoc     := 'документы получены';
          --registeria.t_dueprocess     := one_rec.t_conftp;
          registeria.t_formpayment    := one_rec.t_formpayment;
          registeria.t_unqualifiedsec := one_rec.t_qualification;
          --registeria.t_dealtypeAdr     := one_rec.t_dealtypeAdr;
          registeria.t_dealdirection   := one_rec.t_side;
          registeria.t_dealkind        := one_rec.t_side2;
          registeria.t_seckind         := one_rec.t_firootname;
          if (one_rec.t_firootname != one_rec.t_finame) then
             registeria.t_secsubkind := one_rec.t_finame;
          else
            registeria.t_secsubkind := NULL;
          end if;
          registeria.t_seccode         := one_rec.t_avrname || '/' ||
                                          one_rec.t_avrcode;
          if (not (one_rec.t_lsin = chr(1) and one_rec.t_isin = chr(1))) then
             registeria.t_isin            := '№' || one_rec.t_lsin || '/ ISIN ' ||
                                          one_rec.t_isin;
          else 
            registeria.t_isin            := NULL;
          end if;
          if (not (one_rec.t_issue = chr(1) and one_rec.t_tranche = chr(1) and one_rec.t_series = chr(1))) then
             registeria.t_secseries       := 'выпуск №' || one_rec.t_issue || '/ транш №' ||
                                          one_rec.t_tranche || '/ серия №' ||
                                          one_rec.t_series;
          else
           registeria.t_secseries       := NULL;
          end if;
          registeria.t_issuername      := one_rec.t_issuername;
          registeria.t_brokercomiss   := one_rec.t_brokercomiss;
          registeria.t_principal       := one_rec.t_principal;
          registeria.t_cost            := one_rec.t_cost;
          registeria.t_dealvalue       := one_rec.t_dealvalue;
          registeria.t_totalcost       := one_rec.t_totalcost;
          
          if (one_rec.t_pricefiid != 0) then
            registeria.t_dealvaluerur   := round(rsi_rsb_fiinstr.convsum(round(one_rec.t_dealvalue, 6),
                                                                         one_rec.t_pricefiid,
                                                                         0,
                                                                         one_rec.t_planexecdate), 6);
            registeria.t_totalcostrur    := round(rsi_rsb_fiinstr.convsum(round(one_rec.t_totalcost, 6),
                                                                         one_rec.t_pricefiid,
                                                                         0,
                                                                         one_rec.t_planexecdate), 6);
            registeria.t_costnatcur      := round(rsi_rsb_fiinstr.convsum(round(one_rec.t_cost, 6),
                                                                         one_rec.t_pricefiid,
                                                                         0,
                                                                         one_rec.t_planexecdate), 6);                                                                  
          else 
            registeria.t_costnatcur := NULL;
            registeria.t_dealvaluerur  := NULL;  
            registeria.t_totalcostrur  := NULL;                                       
          end if;
         
          registeria.t_pricecurrency   := one_rec.t_currencyname;
          registeria.t_setcurrency     := one_rec.t_currencyname;
          registeria.t_cond_susp     := substr(one_rec.t_cond_susp,1,100);
      END IF;
      
      if (one_rec.t_isbank = 0 and Block_Clients = CHR(88)) then
        registeria.t_clientname     := one_rec.t_clientshortname;
        registeria.t_clientagr      := one_rec.t_sfcontrnumber;
        registeria.t_clientagrdate  := one_rec.t_sfcontrbegdate;
        registeria.t_clientagrend   := one_rec.t_sfcontrenddate;
        registeria.t_exchangecode   := one_rec.t_mpcode;
        registeria.t_proxyname      := one_rec.t_proxyname;
        registeria.t_proxynum       := one_rec.t_proxynum;
        registeria.t_clientform     := one_rec.t_clientform;
        registeria.t_clientcountry  := one_rec.t_clientcountry;
        registeria.t_clientresident := one_rec.t_clientresident;
        registeria.t_clientcvalinv  := one_rec.t_clientcvalinv;
        registeria.t_risklevel      := one_rec.t_risklevel;
        registeria.t_cashright      := one_rec.t_cashright;
      else
        registeria.t_clientname     := NULL;
        registeria.t_clientagr      := NULL;
        registeria.t_clientagrdate  := NULL;
        registeria.t_clientagrend   := NULL;
        registeria.t_exchangecode   := NULL;
        registeria.t_proxyname      := NULL;
        registeria.t_proxynum       := NULL;
        registeria.t_clientform     := NULL;
        registeria.t_clientcountry  := NULL;
        registeria.t_clientresident := NULL;
        registeria.t_clientcvalinv  := NULL;
        registeria.t_risklevel      := NULL;
        registeria.t_cashright      := NULL;
      end if;

      -- Biq-9103
      registeria.t_margincall := NULL; 
      if (RSB_SECUR.GetMainObjAttr(RSB_SECUR.OBJTYPE_OPER_DV, LPAD(one_rec.t_dealid, 34, '0'), 116, one_rec.t_date) = 1) THEN
        registeria.t_margincall := 'X'; 
      end if;
      
      

      --служебные поля
      registeria.t_clientid      := one_rec.clientid;
      registeria.t_clientcontrid := one_rec.clientcontrid;
      registeria.t_pfi           := one_rec.pfi;
      --registeria.t_dealpart      := one_rec.t_dealpart;
      registeria.t_isbasket      := one_rec.t_isbasket;
      registeria.t_isbuy         := one_rec.t_isbuy;
      registeria.t_dealtime    := one_rec.t_time;

      registeria.t_sessionid := V_SESSIONID;
      registeria.t_calcid := V_CALCID;

      g_registeria_ins.extend;
      g_registeria_ins(g_registeria_ins.LAST) := registeria;
      IF g_registeria_ins.count >= 100000  THEN
        FORALL i IN g_registeria_ins.FIRST .. g_registeria_ins.LAST
           INSERT INTO DDL_REGIABUF_DBT VALUES g_registeria_ins (i);
           g_registeria_ins.delete;
       END IF;

    END LOOP;

    IF g_registeria_ins IS NOT EMPTY THEN
      FORALL i IN g_registeria_ins.FIRST .. g_registeria_ins.LAST
        INSERT INTO DDL_REGIABUF_DBT VALUES g_registeria_ins (i);
      g_registeria_ins.delete;
    END IF;

    PushLogLine('Отбор сделок срочного рынка - end');

    IF Block_InAcc = CHR(88) THEN
      AddDVAccounts(BeginDate, EndDate);
    END IF;

  END CreateData_DV_Deals;

    function MakeCSVPart3(p_report_date IN DATE
                  ,p_count_part  IN INTEGER
                  ,p_part        IN INTEGER) return number is

    v_id_file number;
    v_rep       clob;
    v_header    clob;
    v_file_name varchar2(2000);
  begin
    it_log.log('START MakeCSV p_report_part ' || 3 ||
               ' Part ' || p_part);
    v_file_name := 'nregisteria_' || 3 || '_' || p_part || '_' || to_char(p_report_date, 'YYYYMMDD') || '_' || to_char(sysdate, 'hh24miss') ||
                   '.csv';
    dbms_lob.createtemporary(lob_loc => v_rep, cache => true);
    for cur in (select t.*
                  from (SELECT /*+ use_nl(acc_b) index(acc_b DDL_REGIABUF_DBT_idx3)*/ rownum npp,
                               REG.T_FUTURESPRICE,
                               REG.T_ISBUY,
                               REG.T_OPTIONPREMIUM,
                               REG.T_KINDPFI,
                               REG.T_OPTIONTYPE,
                               REG.T_AUTOINC,
                               REG.T_DEALPART,
                               REG.T_CLIENTID,
                               REG.T_PFI,
                               REG.T_CLIENTCONTRID,
                               REG.T_PART,
                               REG.T_DEALID,
                               REG.T_CURRCLAIMOBL,
                               REG.T_DEALCODE,
                               REG.T_CASHRIGHT,
                               REG.T_RISKLEVEL,
                               REG.T_CLIENTCVALINV,
                               REG.T_CLIENTRESIDENT,
                               REG.T_CLIENTCOUNTRY,
                               REG.T_CLIENTFORM,
                               REG.T_PROXYNUM,
                               REG.T_PROXYNAME,
                               REG.T_EXCHANGECODE,
                               REG.T_CLIENTAGREND,
                               REG.T_CLIENTAGRDATE,
                               REG.T_CLIENTAGR,
                               REG.T_CLIENTNAME,
                               REG.T_FACTDATEM,
                               REG.T_PLANDATEM,
                               REG.T_FACTDATESP,
                               REG.T_PLANDATESP,
                               REG.T_BROKERCOMISS,
                               REG.T_INTERESTAMOUNT,
                               REG.T_INTERESTRATE,
                               REG.T_SETCURRENCY,
                               REG.T_NOMINALCURRENCY,
                               REG.T_PRICECURRENCY,
                               REG.T_DEALVALUERUR,
                               REG.T_DEALVALUE,
                               REG.T_TOTALCOSTRUR,
                               REG.T_TOTALCOST,
                               REG.T_COSTNATCUR,
                               REG.T_COST,
                               REG.T_PRINCIPAL,
                               REG.T_ISSUERNAME,
                               REG.T_SECSERIES,
                               REG.T_ISIN,
                               REG.T_SECCODE,
                               REG.T_SECSUBKIND,
                               REG.T_SECKIND,
                               REG.T_DEALKIND,
                               REG.T_DEALDIRECTION,
                               REG.T_DEALTYPEADR,
                               REG.T_UNQUALIFIEDSEC,
                               REG.T_GENAGR,
                               REG.T_DEALCODETS,
                               REG.T_DEALDATE,
                               CASE WHEN REG.T_DEALDATE IS NOT NULL THEN REG.T_DEALTIME ELSE NULL END T_DEALTIME,
                               REG.T_MARKETNAME,
                               REG.T_MARKETTYPE,
                               REG.T_EXTBROKER,
                               REG.T_EXTBROKERDOC,
                               REG.T_DEALTYPE,
                               REG.T_STATUS_PURCB,
                               REG.T_SIDE1,
                               REG.T_SIDE2,
                               REG.T_CLIENT_ASSIGN,
                               REG.T_CONFIRMDOC,
                               REG.T_DUEPROCESS,
                               REG.T_EXECMETHOD,
                               REG.T_FORMPAYMENT,
                               REG.T_ISBASKET,
                               REG.T_DEALSUBKIND,
                               REG.T_OPTIONTERMS,
                               REG.T_BAKINDNAME,
                               REG.T_OPTIONDEALBONUS,
                               REG.T_QUOTECUR,
                               REG.T_FUTURESAMOUNT,
                               ACC_B.T_ACCOUNT,
                               ACC_B.T_ACCOUNTNAME,
                               ACC_B.T_ACCTYPE,
                               ACC_B.T_ACCREST,
                               ACC_B.T_CREDIT,
                               ACC_B.T_DEBET,
                               ACC_B.t_outstandobl,
                               ACC_B.t_outstandclaims,
                               ACC_B.T_ACCCUR,
                               NVL(REG.T_MARGINCALL, CHR(0)) AS T_MARGINCALL, -- Biq-9103
                               NVL(REG.T_COND_SUSP, '') AS T_COND_SUSP,
                               ACC_B.T_RESTPREV
                          FROM DDL_REGIABUF_DBT reg
                     LEFT JOIN DDL_REGIABUF_DBT acc_b
                            ON reg.t_part = acc_b.t_part
                           AND acc_b.t_acctype is not null
                           and acc_b.T_SESSIONID = V_SESSIONID
                           and acc_b.T_CALCID = V_CALCID
                           AND reg.t_clientcontrid = acc_b.t_clientcontrid and acc_b.t_dealid is null
                         WHERE REG.T_ACCTYPE IS NULL
                           and reg.T_SESSIONID = V_SESSIONID
                           and reg.T_CALCID = V_CALCID
                           AND REG.T_PART = 3
                           and reg.t_chunk = p_part
                         ORDER BY reg.t_clientcontrid,
                                  t_acctype

                        ) t
                   /*where npp between ((p_part - 1) * p_count_part + 1) and
                       (p_part * p_count_part)*/
                       ) loop
                      dbms_lob.append(v_rep
                     ,it_rsl_string.GetCell(cur.t_dealcode) || it_rsl_string.GetCell(cur.t_dealcodets) || it_rsl_string.GetCell(to_char(cur.t_dealdate, 'dd.mm.yyyy')) ||
                      it_rsl_string.GetCell(to_char(cur.t_dealtime,'hh24:mi:ss')) || it_rsl_string.GetCell(cur.t_marketname) || it_rsl_string.GetCell(cur.t_markettype) ||
                      it_rsl_string.GetCell(cur.t_extbroker) || it_rsl_string.GetCell(cur.t_extbrokerdoc) || it_rsl_string.GetCell(cur.t_dealtype) ||
                      it_rsl_string.GetCell(cur.t_status_purcb) || it_rsl_string.GetCell(cur.t_side1) || it_rsl_string.GetCell(cur.t_side2) ||
                      it_rsl_string.GetCell(cur.t_genagr) || it_rsl_string.GetCell(cur.t_client_assign) || it_rsl_string.GetCell(cur.t_confirmdoc) ||
                      it_rsl_string.GetCell(cur.t_dueprocess) || it_rsl_string.GetCell(cur.t_execmethod) ||
                      it_rsl_string.GetCell(cur.t_formpayment) || it_rsl_string.GetCell(cur.t_unqualifiedsec) || it_rsl_string.GetCell(cur.t_dealtypeAdr) ||
                      it_rsl_string.GetCell(cur.t_dealdirection) || it_rsl_string.GetCell(cur.t_dealkind) ||
                      it_rsl_string.GetCell(cur.t_kindpfi) || it_rsl_string.GetCell(cur.t_optiontype) ||
                      it_rsl_string.GetCell(cur.t_optionterms) || it_rsl_string.GetCell(cur.t_bakindname) || it_rsl_string.GetCell(cur.t_futuresprice) ||
                      it_rsl_string.GetCell(cur.t_optionpremium) || it_rsl_string.GetCell(cur.t_optiondealbonus) || it_rsl_string.GetCell(cur.t_quotecur) ||
                      it_rsl_string.GetCell(cur.t_futuresamount) || it_rsl_string.GetCell(cur.t_seckind) ||
                      it_rsl_string.GetCell(cur.t_secsubkind) || it_rsl_string.GetCell(cur.t_seccode) ||
                      it_rsl_string.GetCell(cur.t_isin) || it_rsl_string.GetCell(cur.t_secseries) || it_rsl_string.GetCell(cur.t_issuername) ||
                      it_rsl_string.GetCell(cur.t_principal) || it_rsl_string.GetCell(cur.t_cost) ||
                      it_rsl_string.GetCell(cur.t_costnatcur) || it_rsl_string.GetCell(cur.t_dealvalue) ||
                      it_rsl_string.GetCell(cur.t_dealvaluerur) || it_rsl_string.GetCell(cur.t_totalcost) || it_rsl_string.GetCell(cur.t_totalcostrur) ||
                      it_rsl_string.GetCell(cur.t_pricecurrency)|| it_rsl_string.GetCell(cur.t_nominalcurrency) || it_rsl_string.GetCell(cur.t_setcurrency) ||
                      it_rsl_string.GetCell(cur.t_interestrate) || it_rsl_string.GetCell(cur.t_interestamount) || it_rsl_string.GetCell(cur.t_brokercomiss) ||
                      it_rsl_string.GetCell(to_char(cur.t_plandatesp, 'dd.mm.yyyy')) || it_rsl_string.GetCell(to_char(cur.t_factdatesp, 'dd.mm.yyyy')) || it_rsl_string.GetCell(cur.t_plandatem) ||
                      it_rsl_string.GetCell(cur.t_factdatem) || it_rsl_string.GetCell(cur.t_cond_susp) || it_rsl_string.GetCell(cur.t_clientname) || it_rsl_string.GetCell(cur.t_clientagr) ||
                      it_rsl_string.GetCell(cur.t_clientagrdate) || it_rsl_string.GetCell(cur.t_clientagrend) ||
                      it_rsl_string.GetCell(cur.t_exchangecode) || it_rsl_string.GetCell(cur.t_proxyname) || it_rsl_string.GetCell(cur.t_proxynum) ||
                      it_rsl_string.GetCell(cur.t_clientform) || it_rsl_string.GetCell(cur.t_clientcountry) ||
                      it_rsl_string.GetCell(cur.t_clientresident) || it_rsl_string.GetCell(cur.t_clientcvalinv) ||
                      it_rsl_string.GetCell(cur.t_risklevel) || it_rsl_string.GetCell(cur.t_cashright) || it_rsl_string.GetCell(cur.t_account) || it_rsl_string.GetCell(cur.t_accountname) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_restprev else '' end) || 
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_credit else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_debet else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_accrest else '' end) || 
                      it_rsl_string.GetCell('') ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_restprev else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_credit else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_debet else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_accrest else '' end) || it_rsl_string.GetCell('') ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_acccur else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_restprev else '' end) ||   
                      it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_credit else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_debet else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_accrest else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_outstandobl else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_outstandobl else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_outstandclaims else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_outstandclaims else '' end) ||
                      it_rsl_string.GetCell('')
                      || it_rsl_string.GetCell((case when cur.t_margincall = 'X' then 'Принудительное закрытие позиций' else '' end), true) -- Biq-9103
                   );
    end loop;
    if dbms_lob.getlength(v_rep) != 0 then
      v_id_file := it_file.insert_file(p_file_clob   => v_rep,
                                       p_file_name   => v_file_name,
                                       p_from_system => it_file.C_SOFR_DB,
                                       p_from_module => $$plsql_unit,
                                       p_to_system   => it_file.C_SOFR_RSBANK,
                                       p_to_module   => null,
                                       p_create_user => user,
                                       p_file_code => it_file.C_FILE_CODE_REP_NREG,
                                       p_part_no => 3000000+p_part,
                                       p_sessionid => V_SESSIONID);
      it_log.log('v_id_file=' || v_id_file);
    else
      it_log.log(' NO DATA');
    end if;
    it_log.log('END MakeCSV');
    return v_id_file;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  function MakeCSVPart1(p_report_date IN DATE
                  ,p_count_part  IN INTEGER
                  ,p_part        IN INTEGER ) return number is

    v_id_file number;
    v_rep       clob;
    v_header    clob;
    v_file_name varchar2(2000);
  begin
    it_log.log('START MakeCSV p_report_part ' || 1 ||
               ' Part ' || p_part);
    v_file_name := 'nregisteria_' || 1 || '_' || p_part || '_' || to_char(p_report_date, 'YYYYMMDD') || '_' || to_char(sysdate, 'hh24miss') ||
                   '.csv';
    dbms_lob.createtemporary(lob_loc => v_rep, cache => true);
    for cur in (select t.*
                  from (SELECT /*+ use_nl(acc_a) index(acc_a DDL_REGIABUF_DBT_idx1) */ rownum npp,
                               REG.T_FUTURESPRICE,
                               REG.T_ISBUY,
                               REG.T_OPTIONPREMIUM,
                               REG.T_KINDPFI,
                               REG.T_OPTIONTYPE,
                               REG.T_AUTOINC,
                               REG.T_DEALPART,
                               REG.T_CLIENTID,
                               REG.T_PFI,
                               REG.T_CLIENTCONTRID,
                               REG.T_PART,
                               REG.T_DEALID,
                               REG.T_CURRCLAIMOBL,
                               REG.T_DEALCODE,
                               REG.T_CASHRIGHT,
                               REG.T_RISKLEVEL,
                               REG.T_CLIENTCVALINV,
                               REG.T_CLIENTRESIDENT,
                               REG.T_CLIENTCOUNTRY,
                               REG.T_CLIENTFORM,
                               REG.T_PROXYNUM,
                               REG.T_PROXYNAME,
                               REG.T_EXCHANGECODE,
                               REG.T_CLIENTAGREND,
                               REG.T_CLIENTAGRDATE,
                               REG.T_CLIENTAGR,
                               REG.T_CLIENTNAME,
                               REG.T_FACTDATEM,
                               REG.T_PLANDATEM,
                               REG.T_FACTDATESP,
                               REG.T_PLANDATESP,
                               REG.T_BROKERCOMISS,
                               REG.T_INTERESTAMOUNT,
                               REG.T_INTERESTRATE,
                               REG.T_SETCURRENCY,
                               REG.T_NOMINALCURRENCY,
                               REG.T_PRICECURRENCY,
                               REG.T_DEALVALUERUR,
                               REG.T_DEALVALUE,
                               REG.T_TOTALCOSTRUR,
                               REG.T_TOTALCOST,
                               REG.T_COSTNATCUR,
                               REG.T_COST,
                               REG.T_PRINCIPAL,
                               REG.T_ISSUERNAME,
                               REG.T_SECSERIES,
                               REG.T_ISIN,
                               REG.T_SECCODE,
                               REG.T_SECSUBKIND,
                               REG.T_SECKIND,
                               REG.T_DEALKIND,
                               REG.T_DEALDIRECTION,
                               REG.T_DEALTYPEADR,
                               REG.T_UNQUALIFIEDSEC,
                               REG.T_GENAGR,
                               REG.T_DEALCODETS,
                               REG.T_DEALDATE,
                               CASE WHEN REG.T_DEALDATE IS NOT NULL THEN REG.T_DEALTIME ELSE NULL END T_DEALTIME,
                               REG.T_MARKETNAME,
                               REG.T_MARKETTYPE,
                               REG.T_EXTBROKER,
                               REG.T_EXTBROKERDOC,
                               REG.T_DEALTYPE,
                               REG.T_STATUS_PURCB,
                               REG.T_SIDE1,
                               REG.T_SIDE2,
                               REG.T_CLIENT_ASSIGN,
                               REG.T_CONFIRMDOC,
                               REG.T_DUEPROCESS,
                               REG.T_EXECMETHOD,
                               REG.T_FORMPAYMENT,
                               REG.T_ISBASKET,
                               REG.T_DEALSUBKIND,
                               REG.T_OPTIONTERMS,
                               REG.T_BAKINDNAME,
                               REG.T_OPTIONDEALBONUS,
                               REG.T_QUOTECUR,
                               REG.T_FUTURESAMOUNT,
                               REG.T_PRINCPRECISION,
                               ACC_A.T_ACCOUNT,
                               ACC_A.T_ACCOUNTNAME, 
                               ACC_A.T_ACCTYPE,
                               ACC_A.T_ACCREST,
                               ACC_A.T_CREDIT,
                               ACC_A.T_DEBET,
                               ACC_A.t_outstandobl,
                               ACC_A.t_outstandclaims,
                               ACC_A.T_ACCCUR,
                               NVL(REG.T_MARGINCALL, CHR(0)) AS T_MARGINCALL, -- Biq-9103
                               NVL(REG.T_COND_SUSP, '') AS T_COND_SUSP,
                               ACC_A.T_RESTPREV
                          FROM DDL_REGIABUF_DBT reg
                     LEFT JOIN DDL_REGIABUF_DBT acc_a
                            ON reg.t_part = acc_a.t_part
                           AND acc_a.t_acctype is not null
                           and acc_a.T_SESSIONID = V_SESSIONID
                           and acc_a.T_CALCID = V_CALCID
                           AND (reg.t_dealid = acc_a.t_dealid) and (reg.t_dealsubkind = acc_a.t_dealsubkind) and (reg.t_dealpart = acc_a.t_dealpart) and (reg.t_clientcontrid = acc_a.t_clientcontrid)
                         WHERE REG.T_ACCTYPE IS NULL
                           and reg.T_SESSIONID = V_SESSIONID
                           and reg.T_CALCID = V_CALCID
                           AND REG.T_PART = 1
                           and reg.t_chunk = p_part
                         ORDER BY reg.t_dealdate,
                                  reg.t_dealtime,
                                  reg.t_dealid,
                                  reg.t_dealpart,
                                  reg.t_clientcontrid,
                                  t_acctype

                        ) t
                   /*where npp between ((p_part - 1) * p_count_part + 1) and
                       (p_part * p_count_part)*/
                       ) loop
                      dbms_lob.append(v_rep
                     ,it_rsl_string.GetCell(cur.t_dealcode) || it_rsl_string.GetCell(cur.t_dealcodets) || it_rsl_string.GetCell(to_char(cur.t_dealdate, 'dd.mm.yyyy')) ||
                      it_rsl_string.GetCell(to_char(cur.t_dealtime,'hh24:mi:ss')) || it_rsl_string.GetCell(cur.t_marketname) || it_rsl_string.GetCell(cur.t_markettype) ||
                      it_rsl_string.GetCell(cur.t_extbroker) || it_rsl_string.GetCell(cur.t_extbrokerdoc) || it_rsl_string.GetCell(cur.t_dealtype) ||
                      it_rsl_string.GetCell(cur.t_status_purcb) || it_rsl_string.GetCell(cur.t_side1) || it_rsl_string.GetCell(cur.t_side2) ||
                      it_rsl_string.GetCell(cur.t_genagr) || it_rsl_string.GetCell(cur.t_client_assign) || it_rsl_string.GetCell(cur.t_confirmdoc) ||
                      it_rsl_string.GetCell(cur.t_dueprocess) || it_rsl_string.GetCell(cur.t_execmethod) ||
                      it_rsl_string.GetCell(cur.t_formpayment) || it_rsl_string.GetCell(cur.t_unqualifiedsec) || it_rsl_string.GetCell(cur.t_dealtypeAdr) ||
                      it_rsl_string.GetCell(cur.t_dealdirection) || it_rsl_string.GetCell(cur.t_dealkind) ||
                      it_rsl_string.GetCell(cur.t_kindpfi) || it_rsl_string.GetCell(cur.t_optiontype) ||
                      it_rsl_string.GetCell(cur.t_optionterms) || it_rsl_string.GetCell(cur.t_bakindname) || it_rsl_string.GetCell(cur.t_futuresprice) ||
                      it_rsl_string.GetCell(cur.t_optionpremium) || it_rsl_string.GetCell(cur.t_optiondealbonus) || it_rsl_string.GetCell(cur.t_quotecur) ||
                      it_rsl_string.GetCell(cur.t_futuresamount) || it_rsl_string.GetCell(cur.t_seckind) ||
                      it_rsl_string.GetCell(cur.t_secsubkind) || it_rsl_string.GetCell(cur.t_seccode) ||
                      it_rsl_string.GetCell(cur.t_isin) || it_rsl_string.GetCell(cur.t_secseries) || it_rsl_string.GetCell(cur.t_issuername) ||
                      it_rsl_string.GetCell((case when cur.t_princprecision is not null then trunc(cur.t_principal) || ',' || rpad(replace(mod(cur.t_principal, 1), '.'), cur.t_princprecision, '0') else replace((case when substr(to_char(cur.t_principal), 1, 1) =  '.' then '0' || to_char(cur.t_principal) else to_char(cur.t_principal) end), '.', ',') end)) || it_rsl_string.GetCell(cur.t_cost) ||
                      it_rsl_string.GetCell(cur.t_costnatcur) || it_rsl_string.GetCell(cur.t_dealvalue) ||
                      it_rsl_string.GetCell(cur.t_dealvaluerur) || it_rsl_string.GetCell(cur.t_totalcost) || it_rsl_string.GetCell(cur.t_totalcostrur) ||
                      it_rsl_string.GetCell(cur.t_pricecurrency)|| it_rsl_string.GetCell(cur.t_nominalcurrency) || it_rsl_string.GetCell(cur.t_setcurrency) ||
                      it_rsl_string.GetCell(cur.t_interestrate) || it_rsl_string.GetCell(cur.t_interestamount) || it_rsl_string.GetCell(cur.t_brokercomiss) ||
                      it_rsl_string.GetCell(case when cur.t_plandatesp = to_date('01.01.0001','DD.MM.YYYY') then '' else to_char(cur.t_plandatesp, 'dd.mm.yyyy') end) || it_rsl_string.GetCell(case when cur.t_factdatesp = to_date('01.01.0001','DD.MM.YYYY') then '' else to_char(cur.t_factdatesp, 'dd.mm.yyyy') end) || it_rsl_string.GetCell(cur.t_plandatem) ||
                      it_rsl_string.GetCell(cur.t_factdatem) || it_rsl_string.GetCell(cur.t_cond_susp) || it_rsl_string.GetCell(cur.t_clientname) || it_rsl_string.GetCell(cur.t_clientagr) ||
                      it_rsl_string.GetCell(cur.t_clientagrdate) || it_rsl_string.GetCell(cur.t_clientagrend) ||
                      it_rsl_string.GetCell(cur.t_exchangecode) || it_rsl_string.GetCell(cur.t_proxyname) || it_rsl_string.GetCell(cur.t_proxynum) ||
                      it_rsl_string.GetCell(cur.t_clientform) || it_rsl_string.GetCell(cur.t_clientcountry) ||
                      it_rsl_string.GetCell(cur.t_clientresident) || it_rsl_string.GetCell(cur.t_clientcvalinv) ||
                      it_rsl_string.GetCell(cur.t_risklevel) || it_rsl_string.GetCell(cur.t_cashright) || it_rsl_string.GetCell(cur.t_account) || it_rsl_string.GetCell(cur.t_accountname) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_restprev else '' end) || 
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_credit else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_debet else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_accrest else '' end) || 
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_accrest + nvl(cur.t_outstandclaims, 0) - nvl(cur.t_outstandobl, 0) else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_restprev else '' end) || 
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_credit else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_debet else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_account is not null and cur.t_acctype = 2 then cur.t_accrest else '' end) || it_rsl_string.GetCell(case when cur.t_account is not null and cur.t_acctype = 2 then cur.t_accrest + nvl(cur.t_outstandclaims, 0) - nvl(cur.t_outstandobl, 0) else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_acccur else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_restprev else '' end) || 
                      it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_credit else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_debet else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_accrest else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_outstandobl else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_outstandobl else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_outstandclaims else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_outstandclaims else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_part != 3 and cur.t_acctype = 2 then cur.t_acccur else '' end)
                      || it_rsl_string.GetCell((case when cur.t_margincall = 'X' then 'Принудительное закрытие позиций' else '' end), true) -- Biq-9103
                  );
    end loop;
    if dbms_lob.getlength(v_rep) != 0 then
      v_id_file := it_file.insert_file(p_file_clob   => v_rep,
                                       p_file_name   => v_file_name,
                                       p_from_system => it_file.C_SOFR_DB,
                                       p_from_module => $$plsql_unit,
                                       p_to_system   => it_file.C_SOFR_RSBANK,
                                       p_to_module   => null,
                                       p_create_user => user,
                                       p_file_code => it_file.C_FILE_CODE_REP_NREG,
                                       p_part_no => 1000000+p_part,
                                       p_sessionid => V_SESSIONID);
      it_log.log('v_id_file=' || v_id_file);
    else
      it_log.log(' NO DATA');
    end if;
    it_log.log('END MakeCSV');
    return v_id_file;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;
  
    function MakeCSVPart2(p_report_date IN DATE
                  ,p_count_part  IN INTEGER
                  ,p_part        IN INTEGER) return number is

    v_id_file number;
    v_rep       clob;
    v_header    clob;
    v_file_name varchar2(2000);
  begin
    it_log.log('START MakeCSV p_report_part ' || 2 ||
               ' Part ' || p_part);
    v_file_name := 'nregisteria_' || 2 || '_' || p_part || '_' || to_char(p_report_date, 'YYYYMMDD') || '_' || to_char(sysdate, 'hh24miss') ||
                   '.csv';
    dbms_lob.createtemporary(lob_loc => v_rep, cache => true);
    for cur in (select t.*
                  from (SELECT rownum npp,
                               REG.T_FUTURESPRICE,
                               REG.T_ISBUY,
                               REG.T_OPTIONPREMIUM,
                               REG.T_KINDPFI,
                               REG.T_OPTIONTYPE,
                               REG.T_AUTOINC,
                               REG.T_DEALPART,
                               REG.T_CLIENTID,
                               REG.T_PFI,
                               REG.T_CLIENTCONTRID,
                               REG.T_PART,
                               REG.T_DEALID,
                               REG.T_CURRCLAIMOBL,
                               REG.T_DEALCODE,
                               REG.T_CASHRIGHT,
                               REG.T_RISKLEVEL,
                               REG.T_CLIENTCVALINV,
                               REG.T_CLIENTRESIDENT,
                               REG.T_CLIENTCOUNTRY,
                               REG.T_CLIENTFORM,
                               REG.T_PROXYNUM,
                               REG.T_PROXYNAME,
                               REG.T_EXCHANGECODE,
                               REG.T_CLIENTAGREND,
                               REG.T_CLIENTAGRDATE,
                               REG.T_CLIENTAGR,
                               REG.T_CLIENTNAME,
                               REG.T_FACTDATEM,
                               REG.T_PLANDATEM,
                               REG.T_FACTDATESP,
                               REG.T_PLANDATESP,
                               REG.T_BROKERCOMISS,
                               REG.T_INTERESTAMOUNT,
                               REG.T_INTERESTRATE,
                               REG.T_SETCURRENCY,
                               REG.T_NOMINALCURRENCY,
                               REG.T_PRICECURRENCY,
                               REG.T_DEALVALUERUR,
                               REG.T_DEALVALUE,
                               REG.T_TOTALCOSTRUR,
                               REG.T_TOTALCOST,
                               REG.T_COSTNATCUR,
                               REG.T_COST,
                               REG.T_PRINCIPAL,
                               REG.T_ISSUERNAME,
                               REG.T_SECSERIES,
                               REG.T_ISIN,
                               REG.T_SECCODE,
                               REG.T_SECSUBKIND,
                               REG.T_SECKIND,
                               REG.T_DEALKIND,
                               REG.T_DEALDIRECTION,
                               REG.T_DEALTYPEADR,
                               REG.T_UNQUALIFIEDSEC,
                               REG.T_GENAGR,
                               REG.T_DEALCODETS,
                               REG.T_DEALDATE,
                               CASE WHEN REG.T_DEALDATE IS NOT NULL THEN REG.T_DEALTIME ELSE NULL END T_DEALTIME,
                               REG.T_MARKETNAME,
                               REG.T_MARKETTYPE,
                               REG.T_EXTBROKER,
                               REG.T_EXTBROKERDOC,
                               REG.T_DEALTYPE,
                               REG.T_STATUS_PURCB,
                               REG.T_SIDE1,
                               REG.T_SIDE2,
                               REG.T_CLIENT_ASSIGN,
                               REG.T_CONFIRMDOC,
                               REG.T_DUEPROCESS,
                               REG.T_EXECMETHOD,
                               REG.T_FORMPAYMENT,
                               REG.T_ISBASKET,
                               REG.T_DEALSUBKIND,
                               REG.T_OPTIONTERMS,
                               REG.T_BAKINDNAME,
                               REG.T_OPTIONDEALBONUS,
                               REG.T_QUOTECUR,
                               REG.T_FUTURESAMOUNT,
                               REG.T_ACCOUNT,
                               REG.T_ACCOUNTNAME, 
                               REG.T_ACCTYPE,
                               REG.T_ACCREST,
                               REG.T_CREDIT,
                               REG.T_DEBET,
                               REG.t_outstandobl,
                               REG.t_outstandclaims,
                               REG.T_ACCCUR,
                               REG.T_PRINCPRECISION,
                               NVL(REG.T_MARGINCALL, CHR(0)) AS T_MARGINCALL, -- Biq-9103
                               NVL(REG.T_COND_SUSP, '') AS T_COND_SUSP,
                               REG.T_RESTPREV
                          FROM DDL_REGIABUF_DBT reg
                         WHERE REG.T_ACCTYPE IS NULL
                           and reg.T_SESSIONID = V_SESSIONID
                           and reg.T_CALCID = V_CALCID
                           AND REG.T_PART = 2
                           and reg.t_chunk = p_part
                         ORDER BY reg.t_dealdate,
                                  reg.t_dealtime,
                                  reg.t_dealid,
                                  reg.t_dealpart,
                                  reg.t_clientcontrid,
                                  t_acctype

                        ) t
                   /*where npp between ((p_part - 1) * p_count_part + 1) and
                       (p_part * p_count_part)*/
                       ) loop
                      dbms_lob.append(v_rep
                     ,it_rsl_string.GetCell(cur.t_dealcode) || it_rsl_string.GetCell(cur.t_dealcodets) || it_rsl_string.GetCell(to_char(cur.t_dealdate, 'dd.mm.yyyy')) ||
                      it_rsl_string.GetCell(to_char(cur.t_dealtime,'hh24:mi:ss')) || it_rsl_string.GetCell(cur.t_marketname) || it_rsl_string.GetCell(cur.t_markettype) ||
                      it_rsl_string.GetCell(cur.t_extbroker) || it_rsl_string.GetCell(cur.t_extbrokerdoc) || it_rsl_string.GetCell(cur.t_dealtype) ||
                      it_rsl_string.GetCell(cur.t_status_purcb) || it_rsl_string.GetCell(cur.t_side1) || it_rsl_string.GetCell(cur.t_side2) ||
                      it_rsl_string.GetCell(cur.t_genagr) || it_rsl_string.GetCell(cur.t_client_assign) || it_rsl_string.GetCell(cur.t_confirmdoc) ||
                      it_rsl_string.GetCell(cur.t_dueprocess) || it_rsl_string.GetCell(cur.t_execmethod) ||
                      it_rsl_string.GetCell(cur.t_formpayment) || it_rsl_string.GetCell(cur.t_unqualifiedsec) || it_rsl_string.GetCell(cur.t_dealtypeAdr) ||
                      it_rsl_string.GetCell(cur.t_dealdirection) || it_rsl_string.GetCell(cur.t_dealkind) ||
                      it_rsl_string.GetCell(cur.t_kindpfi) || it_rsl_string.GetCell(cur.t_optiontype) ||
                      it_rsl_string.GetCell(cur.t_optionterms) || it_rsl_string.GetCell(cur.t_bakindname) || it_rsl_string.GetCell(cur.t_futuresprice) ||
                      it_rsl_string.GetCell(cur.t_optionpremium) || it_rsl_string.GetCell(cur.t_optiondealbonus) || it_rsl_string.GetCell(cur.t_quotecur) ||
                      it_rsl_string.GetCell(cur.t_futuresamount) || it_rsl_string.GetCell(cur.t_seckind) ||
                      it_rsl_string.GetCell(cur.t_secsubkind) || it_rsl_string.GetCell(cur.t_seccode) ||
                      it_rsl_string.GetCell(cur.t_isin) || it_rsl_string.GetCell(cur.t_secseries) || it_rsl_string.GetCell(cur.t_issuername) ||
                      it_rsl_string.GetCell((case when cur.t_princprecision is not null then trunc(cur.t_principal) || ',' || rpad(replace(mod(cur.t_principal, 1), '.'), cur.t_princprecision, '0') else replace((case when substr(to_char(cur.t_principal), 1, 1) =  '.' then '0' || to_char(cur.t_principal) else to_char(cur.t_principal) end), '.', ',') end)) || it_rsl_string.GetCell(cur.t_cost) ||
                      it_rsl_string.GetCell(cur.t_costnatcur) || it_rsl_string.GetCell(cur.t_dealvalue) ||
                      it_rsl_string.GetCell(cur.t_dealvaluerur) || it_rsl_string.GetCell(cur.t_totalcost) || it_rsl_string.GetCell(cur.t_totalcostrur) ||
                      it_rsl_string.GetCell(cur.t_pricecurrency)|| it_rsl_string.GetCell(cur.t_nominalcurrency) || it_rsl_string.GetCell(cur.t_setcurrency) ||
                      it_rsl_string.GetCell(cur.t_interestrate) || it_rsl_string.GetCell(cur.t_interestamount) || it_rsl_string.GetCell(cur.t_brokercomiss) ||
                      it_rsl_string.GetCell(case when cur.t_plandatesp = to_date('01.01.0001','DD.MM.YYYY') then '' else to_char(cur.t_plandatesp, 'dd.mm.yyyy') end) || it_rsl_string.GetCell(case when cur.t_factdatesp = to_date('01.01.0001','DD.MM.YYYY') then '' else to_char(cur.t_factdatesp, 'dd.mm.yyyy') end) || it_rsl_string.GetCell(cur.t_plandatem) ||
                      it_rsl_string.GetCell(cur.t_factdatem) || it_rsl_string.GetCell(cur.t_cond_susp) || it_rsl_string.GetCell(cur.t_clientname) || it_rsl_string.GetCell(cur.t_clientagr) ||
                      it_rsl_string.GetCell(cur.t_clientagrdate) || it_rsl_string.GetCell(cur.t_clientagrend) ||
                      it_rsl_string.GetCell(cur.t_exchangecode) || it_rsl_string.GetCell(cur.t_proxyname) || it_rsl_string.GetCell(cur.t_proxynum) ||
                      it_rsl_string.GetCell(cur.t_clientform) || it_rsl_string.GetCell(cur.t_clientcountry) ||
                      it_rsl_string.GetCell(cur.t_clientresident) || it_rsl_string.GetCell(cur.t_clientcvalinv) ||
                      it_rsl_string.GetCell(cur.t_risklevel) || it_rsl_string.GetCell(cur.t_cashright) || it_rsl_string.GetCell(cur.t_account) || it_rsl_string.GetCell(cur.t_accountname) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_restprev else '' end) ||  
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_credit else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_debet else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_accrest else '' end) || 
                      it_rsl_string.GetCell(case when cur.t_part != 3 and cur.t_acctype = 1 then cur.t_accrest + nvl(cur.t_outstandclaims, 0) - nvl(cur.t_outstandobl, 0) else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_restprev else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_credit else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_debet else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_accrest else '' end) || it_rsl_string.GetCell(case when cur.t_part != 3 and cur.t_acctype = 2 then cur.t_accrest + nvl(cur.t_outstandclaims, 0) - nvl(cur.t_outstandobl, 0) else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_acccur else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_restprev else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_credit else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_debet else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 3 then cur.t_accrest else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_outstandobl else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_outstandobl else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_acctype = 2 then cur.t_outstandclaims else '' end) || it_rsl_string.GetCell(case when cur.t_acctype = 1 then cur.t_outstandclaims else '' end) ||
                      it_rsl_string.GetCell(case when cur.t_part != 3 and cur.t_acctype = 2 then cur.t_acccur else '' end) 
                      || it_rsl_string.GetCell((case when cur.t_margincall = 'X' then 'Принудительное закрытие позиций' else '' end), true) -- Biq-9103
                  );
    end loop;
    if dbms_lob.getlength(v_rep) != 0 then
      v_id_file := it_file.insert_file(p_file_clob   => v_rep,
                                       p_file_name   => v_file_name,
                                       p_from_system => it_file.C_SOFR_DB,
                                       p_from_module => $$plsql_unit,
                                       p_to_system   => it_file.C_SOFR_RSBANK,
                                       p_to_module   => null,
                                       p_create_user => user,
                                       p_file_code => it_file.C_FILE_CODE_REP_NREG,
                                       p_part_no => 2000000+p_part,
                                       p_sessionid => V_SESSIONID);
      it_log.log('v_id_file=' || v_id_file);
    else
      it_log.log(' NO DATA');
    end if;
    it_log.log('END MakeCSV');
    return v_id_file;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;
  
    
  function MakeCSV(p_report_date IN DATE
                  ,p_report_part IN INTEGER
                  ,p_count_part  IN INTEGER
                  ,p_part        IN INTEGER) return number is
  begin
    if (p_report_part = 1) then
      return MakeCSVPart1(p_report_date, p_count_part, p_part);
    elsif (p_report_part = 2) then
      return MakeCSVPart2(p_report_date, p_count_part, p_part);
    else
      return MakeCSVPart3(p_report_date, p_count_part, p_part);
    end if;
  end;
  
  procedure MakeAllCSV(p_report_date IN DATE
                     ,p_count_part  IN INTEGER)  is
  /*v_part integer;
  begin
    for report_part in 1..3
      loop
        v_part := 1;
        loop
          if (report_part = 1) then
            exit when  nvl(MakeCSVPart1(p_report_date, p_count_part, v_part),0)= 0;
          elsif (report_part = 2) then
            exit when  nvl(MakeCSVPart2(p_report_date, p_count_part, v_part),0)= 0;
          else
            exit when  nvl(MakeCSVPart3(p_report_date, p_count_part, v_part),0)= 0;
          end if;
          v_part:= v_part+1;
        end loop;
      end loop;
  end;*/
   begin
     PushLogLine('Подготовка данных для формирования CSV - begin', 'Подготовка данных для формирования CSV');
     merge /*+ use_hash(z,r) */ into DDL_REGIABUF_DBT r
        using (select report_part
             ,t_autoinc
             ,min(npp) npp
             ,min(zchunk) zchunk
         from (select 1 report_part
                     ,t_autoinc
                     ,rownum npp
                     ,TRUNC(rownum / p_count_part) + 1 zchunk
                 from (select /*+ use_hash(acc_a) index(acc_a DDL_REGIABUF_DBT_IDX1)*/ reg.t_autoinc
                         from DDL_REGIABUF_DBT reg
                         left join DDL_REGIABUF_DBT acc_a
                           on reg.t_part = acc_a.t_part
                          and acc_a.t_acctype is not null
                          and acc_a.T_SESSIONID = V_SESSIONID
                          and acc_a.T_CALCID = V_CALCID
                          and (reg.t_dealid = acc_a.t_dealid)
                          and (reg.t_dealsubkind = acc_a.t_dealsubkind)
                          and (reg.t_dealpart = acc_a.t_dealpart)
                          and (reg.t_clientcontrid = acc_a.t_clientcontrid)
                        where REG.T_ACCTYPE is null
                          and reg.T_SESSIONID = V_SESSIONID
                          and reg.T_CALCID = V_CALCID
                          and REG.T_PART = 1
                        order by reg.t_dealdate
                                ,reg.t_dealtime
                                ,reg.t_dealid
                                ,reg.t_dealpart
                                ,reg.t_clientcontrid
                                ,reg.t_acctype)
               union all
               select 2
                     ,t_autoinc
                     ,rownum npp
                     ,TRUNC(rownum / p_count_part) + 1 zchunk
                 from (select reg.t_autoinc
                         from DDL_REGIABUF_DBT reg
                        where REG.T_ACCTYPE is null
                          and reg.T_SESSIONID = V_SESSIONID
                          and reg.T_CALCID = V_CALCID
                          and REG.T_PART = 2
                        order by reg.t_dealdate
                                ,reg.t_dealtime
                                ,reg.t_dealid
                                ,reg.t_dealpart
                                ,reg.t_clientcontrid
                                ,t_acctype)
               union all
               select 3
                     ,t_autoinc
                     ,rownum npp
                     ,TRUNC(rownum / p_count_part) + 1 zchunk
                 from (select /*+ use_hash(acc_b) index(acc_b DDL_REGIABUF_DBT_idx3)*/
                        reg.t_autoinc
                         from DDL_REGIABUF_DBT reg
                         left join DDL_REGIABUF_DBT acc_b
                           on reg.t_part = acc_b.t_part
                          and acc_b.t_acctype is not null
                          and acc_b.T_SESSIONID = V_SESSIONID
                          and acc_b.T_CALCID = V_CALCID
                          and reg.t_clientcontrid = acc_b.t_clientcontrid
                          and acc_b.t_dealid is null
                        where REG.T_ACCTYPE is null
                          and reg.T_SESSIONID = V_SESSIONID
                          and reg.T_CALCID = V_CALCID
                          and REG.T_PART = 3
                        order by reg.t_clientcontrid
                                ,reg.t_acctype))
        group by report_part
                ,t_autoinc) Z
   on (r.t_part = z.report_part and r.t_autoinc = z.t_autoinc and r.T_SESSIONID = V_SESSIONID and r.T_CALCID = V_CALCID)
     when matched then
      update
          set r.t_npp   = z.npp
           ,r.t_chunk = z.zchunk ;
   commit ;
     PushLogLine('Формирование CSV файлов отчета - begin', 'Формирование CSV файлов отчета в многопотоке');
     --RSB_DLUTILS.RunFunctionInParallel(
     it_parallel_exec.run_task_chunks_by_sql(p_parallel_level => 8,
                  p_sql_stmt      => 'declare v_id number ;begin rsb_dlregisteria.SetCalcIds('||TO_CHAR(V_SESSIONID)||', '||TO_CHAR(V_CALCID)||'); '||
                                       'v_id := rsb_dlregisteria.MakeCSV('||to_chardatesql(p_report_date)||',:start_id,'||p_count_part||', :end_id ); end;',
                  p_chunk_sql       =>  'select distinct reg.t_part, reg.t_chunk
      from DDL_REGIABUF_DBT reg
     where reg.T_SESSIONID = '||TO_CHAR(V_SESSIONID)||'
       and reg.T_CALCID = '||TO_CHAR(V_CALCID)||'
       and reg.t_chunk is not null
       order by reg.t_part,reg.t_chunk',
       p_comment => 'Реестр ВУ Формирование CSV ');
      PushLogLine('Формирование CSV файлов отчета - end', 'Формирование CSV файлов отчета в многопотоке');
   end;
  

  PROCEDURE CreateData_Deals(BeginDate      IN DATE,
                              EndDate        IN DATE,
                              StockMarket    IN CHAR,
                              FuturesMarket  IN CHAR,
                              CurrencyMarket IN CHAR,
                              Sector_Brokers IN CHAR,
                              Sector_Dilers  IN CHAR,
                              Sector_Clients IN CHAR,
                              Block_Deals    IN CHAR,
                              Block_Clients  IN CHAR,
                              Block_InAcc    IN CHAR,
                              SelectedClients IN NUMBER,
                              SelectedContrs   IN NUMBER) IS
  BEGIN
    if (StockMarket = chr(88)) then
       CreateData_SC_Deals(BeginDate,
                        EndDate,
                        StockMarket,
                        FuturesMarket,
                        CurrencyMarket,
                        Sector_Brokers,
                        Sector_Dilers,
                        Sector_Clients,
                        Block_Deals,
                        Block_Clients,
                        Block_InAcc,
                        SelectedClients,
                        SelectedContrs);
        commit;
    end if;
    if (FuturesMarket = chr(88)) then
       CreateData_DV_Deals(BeginDate,
                           EndDate,
                           StockMarket,
                           FuturesMarket,
                           CurrencyMarket,
                           Sector_Brokers,
                           Sector_Dilers,
                           Sector_Clients,
                           Block_Deals,
                           Block_Clients,
                           Block_InAcc,
                           SelectedClients,
                           SelectedContrs);
        commit;
    end if;
    if (StockMarket = chr(88) or CurrencyMarket = chr (88)) then
     CreateData_DV_NDeals(BeginDate,
                         EndDate,
                         StockMarket,
                         FuturesMarket,
                         CurrencyMarket,
                         Sector_Brokers,
                         Sector_Dilers,
                         Sector_Clients,
                         Block_Deals,
                         Block_Clients,
                         Block_InAcc,
                         SelectedClients,
                         SelectedContrs);
        commit;
    end if;
    
    if Sector_Brokers = CHR(88) then
     CreateData_OprInOut(BeginDate,
                         EndDate,
                         StockMarket,
                         FuturesMarket,
                         CurrencyMarket,
                         Sector_Brokers,
                         Sector_Dilers,
                         Sector_Clients,
                         Block_Deals,
                         Block_Clients,
                         Block_InAcc,
                         SelectedClients,
                         SelectedContrs);
        commit;
    end if;
    --AddAccounts_AllDeals(BeginDate, EndDate);
  END CreateData_Deals;


END rsb_dlregisteria;
/
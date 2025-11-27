CREATE OR REPLACE package body QB_DWH_EXPORT_SECUR is

  n0                 constant pls_integer := 0;
  n1                 constant pls_integer := 1;
  n_1                constant pls_integer := -1;
  n2                 constant pls_integer := 2;
  n4                 constant pls_integer := 4;
  n5                 constant pls_integer := 5;
  n8                 constant pls_integer := 8;
  n9                 constant pls_integer := 9;
  n11                constant pls_integer := 11;
  n12                constant pls_integer := 12;
  n20                constant pls_integer := 20;
  n24                constant pls_integer := 24;
  n26                constant pls_integer := 26;
  n47                constant pls_integer := 47;
  n62                constant pls_integer := 62;
  n101               constant pls_integer := 101;
  n102               constant pls_integer := 102;
  n109               constant pls_integer := 109;
  n135               constant pls_integer := 135;
  n141               constant pls_integer := 141;
  n164               constant pls_integer := 164;
  n176               constant pls_integer := 176;
  n191               constant pls_integer := 191;
  n450               constant pls_integer := 450;
  n462               constant pls_integer := 462;
  n1001              constant pls_integer := 1001;
  n1492              constant pls_integer := 1492;
  n2020              constant pls_integer := 2020;
  n3503              constant pls_integer := 3503;
  n5000              constant pls_integer := 5000;
  n12401             constant pls_integer := 12401;
  chr0               constant varchar2(1) := chr(0);
  chr88              constant varchar2(1) := chr(88);
  v2                 constant varchar2(1) := '2';
  v50505             constant varchar2(5) := '50505';
  cSECKIND_BILL      constant varchar2(20) := 'SECKIND_BILL';
  cSECKIND_UIT       constant varchar2(20) := 'SECKIND_UIT';
  cSECKIND_ALL       constant varchar2(20) := 'SECKIND_ALL';
  cACC_CHAPTERS      constant varchar2(20) := 'ACC_CHAPTERS';
  cDEALSKIND_SEC     constant varchar2(20) := 'DEALSKIND_SEC';
  cCATEXP_OBILL      constant varchar2(20) := 'CATEXP_OBILL';
  cCATEXP_DBILL      constant varchar2(20) := 'CATEXP_DBILL';
  cDEALSKIND_DBILL   constant varchar2(20) := 'DEALSKIND_DBILL';
  cLOT_STATE         constant varchar2(20) := 'LOT_STATE';
  cEXP_DOCKIND       constant varchar2(20) := 'EXP_DOCKIND';
  cEXP_CAT_TMP0      constant varchar2(20) := 'EXP_CAT_TMP0';
  cEXP_CAT_TMP3      constant varchar2(20) := 'EXP_CAT_TMP3';
  cEXP_CAT_TMP1      constant varchar2(20) := 'EXP_CAT_TMP1';
  cEXP_CAT_TMP2      constant varchar2(20) := 'EXP_CAT_TMP2';
  cEXP_CAT462_TEMPL  constant varchar2(20) := 'EXP_CAT462_TEMPL';
  cDEALSKIND_DBILL_2 constant varchar2(20) := 'DEALSKIND_DBILL_2';
  cOPER_DOCKIND      constant varchar2(20) := 'OPER_DOCKIND';
  cCOM_DOCKIND       constant varchar2(20) := 'COM_DOCKIND';
  cFIID_PORTFOLIO_2  constant varchar2(20) := 'FIID_PORTFOLIO_2';
  cSECKIND_STOCK     constant varchar2(20) := 'SECKIND_STOCK';
  cSECKIND_BOND      constant varchar2(20) := 'SECKIND_BOND';
  cSECKIND_RECEIPT   constant varchar2(20) := 'SECKIND_RECEIPT';
  cRES_SUBKIND       constant varchar2(20) := 'RES_SUBKIND';
  firstDate          constant date := to_date('01011980', 'ddmmyyyy');
  emptDate           constant date := to_date('01010001', 'ddmmyyyy');
  minDate            constant date := to_date('01012019', 'ddmmyyyy');
  maxDate            constant date := to_date('31129999', 'ddmmyyyy');
  cCODE_TYPERISK     constant varchar2(20) := '254i';
  exch_code          constant varchar2(100):= qb_dwh_utils.GetComponentCode('DET_SUBJECT', -- DEF-20402, code review, part2
                                              qb_dwh_utils.System_IBSO,
                                              1,
                                              2) ;
   BIQ_7477_78           constant number := 1; --Рубильник для BIQ_7477_78. 1 = включено, оставляем нужную строку и компилируем. 
  --BIQ_7477_78           constant number := 0; --Рубильник для BIQ_7477_78. 0 = ВЫключено.


  TYPE sec_basket IS RECORD
   (
     fiid      number(10),
     fi_code   varchar2(100),
     bd        date,
     ed        date,
     date1     date,
     date2     date,
     totalcost number(30, 2),
     cnt       number(30),
     nkd       number(30, 2),
     main_code varchar2(100),
     part_code varchar2(100),
     legid0    number(10),
     legid2    number(10),
     dealid    number(10),
     costfiid  number(10),
     sump2     number(30, 2)

   );


--->

  Procedure call_scwrthistex(p_date IN DATE) is
       r_source_data scwrthistex_aat;

   CURSOR c_source_data IS
                             select /*+ PARALLEL(4) */
                                   v.T_FIID,
                                   v.T_SUMID,
                                   v.T_PORTFOLIO,
                                   v.T_CHANGEDATE,
                                   v.T_TIME,
                                   v.T_AMOUNT,
                                   v.T_SUM,
                                   v.T_COST,
                                   v.T_STATE,
                                   v.T_PARENT,
                                   v.T_DEALID,
                                   v.T_INSTANCE,
                                   v.T_DOCKIND,
                                   v.T_DOCID,
--                                   max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                   v.T_CORRINTTOEIR c2eps,
                                   decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                   v.T_RESERVAMOUNT c2rpbu,
                                   v.T_INCOMERESERV c2rpbu_nkd,
                                   v.T_NKDAMOUNT    nkd,
                                   v.T_DISCOUNTINCOME discount,
                                   v.T_INTERESTINCOME interest,
                                   v.T_BEGBONUS begbonus,
                                   v.T_BONUS bonus,
                                   v.T_OVERAMOUNT over,
                                   v.T_BEGDISCOUNT begdiscount
                              from  v_scwrthistex v
                               where exists (select 1 from dwh_histsum_tmp where sumid = v.T_SUMID)
                                 and v.T_CHANGEDATE <= p_date;

   BEGIN
     execute immediate 'truncate table DWH_scwrthistex_TMP';
     EXECUTE IMMEDIATE 'ALTER INDEX DWH_SCWRTHISTEX_TMP_IDX0 UNUSABLE';
     EXECUTE IMMEDIATE 'ALTER INDEX DWH_SCWRTHISTEX_TMP_IDX1 UNUSABLE';
     EXECUTE IMMEDIATE 'ALTER INDEX DWH_SCWRTHISTEX_TMP_IDX2 UNUSABLE';
     EXECUTE IMMEDIATE 'ALTER INDEX DWH_SCWRTHISTEX_TMP_IDX3 UNUSABLE';
     execute immediate 'alter session set skip_unusable_indexes=true';

     OPEN c_source_data;
     LOOP
       FETCH c_source_data BULK COLLECT INTO r_source_data LIMIT 100000;
       FOR l_row IN 1 .. r_source_data.COUNT
             LOOP
              insert into DWH_scwrthistex_TMP
              (       T_FIID,
                      T_SUMID,
                      T_PORTFOLIO ,
                      T_CHANGEDATE,
                      T_TIME      ,
                      T_AMOUNT    ,
                      T_SUM       ,
                      T_COST      ,
                      T_STATE     ,
                      T_PARENT    ,
                      T_DEALID    ,
                      T_INSTANCE  ,
                      T_DOCKIND   ,
                      T_DOCID     ,
--                      maxinstance ,
                      c2eps,
                      c2oku,
                      c2rpbu,
                      c2rpbu_nkd,
                      nkd,
                      discount,
                      interest,
                      begbonus,
                      bonus,
                      over,
                      begdiscount)
               VALUES
               (
                 r_source_data(l_row).T_FIID,
                 r_source_data(l_row).T_SUMID,
                 r_source_data(l_row).T_PORTFOLIO ,
                 r_source_data(l_row).T_CHANGEDATE,
                 r_source_data(l_row).T_TIME      ,
                 r_source_data(l_row).T_AMOUNT    ,
                 r_source_data(l_row).T_SUM       ,
                 r_source_data(l_row).T_COST      ,
                 r_source_data(l_row).T_STATE     ,
                 r_source_data(l_row).T_PARENT    ,
                 r_source_data(l_row).T_DEALID    ,
                 r_source_data(l_row).T_INSTANCE  ,
                 r_source_data(l_row).T_DOCKIND   ,
                 r_source_data(l_row).T_DOCID     ,
--                 r_source_data(l_row).maxinstance ,
                 r_source_data(l_row).c2eps     ,
                 r_source_data(l_row).c2oku,
                 r_source_data(l_row).c2rpbu,
                 r_source_data(l_row).c2rpbu_nkd,
                 r_source_data(l_row).nkd,
                 r_source_data(l_row).discount,
                 r_source_data(l_row).interest,
                 r_source_data(l_row).begbonus,
                 r_source_data(l_row).bonus,
                 r_source_data(l_row).over,
                 r_source_data(l_row).begdiscount);

             END LOOP;
      EXIT WHEN c_source_data%NOTFOUND;
     END LOOP;
   CLOSE c_source_data;
-- commit;

    EXECUTE IMMEDIATE 'ALTER INDEX DWH_SCWRTHISTEX_TMP_IDX0 REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX DWH_SCWRTHISTEX_TMP_IDX1 REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX DWH_SCWRTHISTEX_TMP_IDX2 REBUILD';
    EXECUTE IMMEDIATE 'ALTER INDEX DWH_SCWRTHISTEX_TMP_IDX3 REBUILD';

  end call_scwrthistex;

---<
  procedure StartEvent(in_idEventKind  in number,
                       procid          in number,
                         out_idEvent     out number) is
   nId number(10);
  begin

    select DQB_SEQ_PROCESS.NEXTVAL into nId from dual;
    
      delete from DQB_BP_EVENT_ERROR_DBT where t_idevent = nId; commit;  -- Ошибки произошедьшии при событии
      delete from DQB_BP_EVENT_DBT where t_id = nId;       commit;  -- События
      
    insert into dqb_bp_event_dbt (t_id, t_idevent, t_value)
                          values (nId, in_idEventKind, procid)
     returning T_ID into out_idEvent;
     commit;
  end StartEvent;


  -- Проверка вхождения значения в список констант
  function InConst(cname qb_dwh_const4exp.name%type,
                   val qb_dwh_const4exp_val.value%type) return boolean
  is
    cnt pls_integer;
    rv boolean;
  begin
    select count(1)
      into cnt
      from QB_DWH_CONST4EXP c
     inner join QB_DWH_CONST4EXP_VAl v
        on (c.id = v.id)
    where c.name = cname
      and v.value = val;
    if (cnt > 0) then
      rv := true;
    else
      rv := false;
    end if;
    return rv;
  end;

  ------------------------------------------------------------
  -- Заимствовано из ПКЛ (требуется поддерживать актуализацию)
  ------------------------------------------------------------
  FUNCTION GetAccountByLot (p_sumid IN NUMBER, p_date IN DATE) --05.04.2022 упраздним код функции в пользу дистрибутивной RSB_SPREPFUN.GetLotCostAccountID 
    RETURN VARCHAR2
    IS
        acc_id    NUMBER (22) := 0;
        acc_num   VARCHAR2 (25) := '-1';
     BEGIN
       acc_id := RSB_SPREPFUN.GetLotCostAccountID(p_SumID, p_Date);
       if (acc_id is null) or (acc_id <= 0) then
         acc_num := '-1';
       else  
         select NVL(t_account, '-1') into acc_num from daccount_dbt where t_accountid = acc_id;
       end if;             
    return acc_num;
  END;

  FUNCTION GetPortfolioMSFO(fiid IN NUMBER, portf IN NUMBER, acc IN VARCHAR2, cdate IN DATE, sumid in number)
    RETURN VARCHAR2
  IS
    cnt      PLS_INTEGER;
    vfiid    v_scwrthistex.T_FIID%type;
    vportf   v_scwrthistex.T_PORTFOLIO%type;
    vchgdate v_scwrthistex.T_CHANGEDATE%type;
    vsumid   v_scwrthistex.T_SUMID%type;
    vacc     daccount_dbt.t_account%type;
  BEGIN
    SELECT count(1)
      INTO cnt
      FROM DOBJATCOR_DBT AC
     INNER JOIN DOBJGROUP_DBT GR
        ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
     INNER JOIN dobjattr_dbt at
        ON (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid AND
           ac.t_attrid = at.t_attrid)
     WHERE AC.T_OBJECTTYPE = n12
       AND gr.t_type = chr88
       AND gr.t_groupid = n62
       AND ac.t_object = lpad(to_char(fiid), 10, '0')
       AND cdate BETWEEN ac.t_Validfromdate AND AC.T_VALIDTODATE
       AND ac.t_attrid  = n2;
    IF (cnt > 0) THEN
      RETURN '3';
    ELSIF (portf = 5) THEN
      RETURN '1';
    ELSIF (portf = 2) THEN
      RETURN '2';
    ELSIF (portf = 1) THEN
      RETURN '3';
    ELSIF (portf = 3) THEN
      RETURN '-1';
    ELSIF (portf = 4) THEN
      -- Определим портфель по лоту до перевода на просрочку
      begin
        select T_FIID, T_PORTFOLIO, T_CHANGEDATE, T_SUMID
          into vfiid, vportf, vchgdate, vsumid
          from (select /*+ DYNAMIC_SAMPLING(s 10) */
                       l.*,
                       max(l.T_CHANGEDATE) over(partition by t_sumid) maxchgdate,
                       max(l.T_INSTANCE) over (partition by l.T_SUMID, l.T_CHANGEDATE) maxinstance
                  from DWH_scwrthistex_TMP l
                 where l.t_sumid = sumid
                   and l.T_CHANGEDATE < cdate)
         where t_changedate = maxchgdate
           and t_instance = maxinstance
           and t_portfolio <> n4;
      exception
        when no_data_found then
          if (InConst(cFIID_PORTFOLIO_2, fiid)) then
            return '2';
          else
            return  '-1';
          end if;
      end;
      return  GetPortfolioMSFO(fiid => vfiid, portf => vportf, acc => null, cdate => vchgdate, sumid => vsumid);
    ELSIF (portf = 6) THEN
      RETURN '-1';
    ELSIF (portf = 15) THEN
      IF (acc IS NULL) or (acc = '-1') THEN
        vacc := GetAccountByLot(p_sumid => sumid, p_date => cdate);
      ELSE
        vacc := acc;
      END IF;
      IF (vacc IS NOT NULL) and (vacc <> '-1') THEN
        IF (substr(vacc, 1, 3) = '501') THEN
          RETURN '3';
        ELSIF (substr(vacc, 1, 3) = '502') THEN
          RETURN '2';
        ELSIF (substr(vacc, 1, 3) = '504') THEN
          RETURN '1';
        END IF;
      END IF;
    END IF;
    RETURN '-1';
  END;

  ------------------------------------------------------
  -- очистка данных по ценным бумагам
  ------------------------------------------------------
  procedure clearSecurData(in_Type number default 0) is
  begin
    execute immediate 'truncate table ldr_infa_cb.Det_Finstr';
    execute immediate 'truncate table ldr_infa_cb.Det_Security';
    execute immediate 'truncate table ldr_infa_cb.Det_Bill';
    execute immediate 'truncate table ldr_infa_cb.Det_Stock';
    execute immediate 'truncate table ldr_infa_cb.Det_Bond';
    execute immediate 'truncate table ldr_infa_cb.Det_Uit';
    execute immediate 'truncate table ldr_infa_cb.Det_Receipt';
    execute immediate 'truncate table ldr_infa_cb.Fct_Finstr_Rate';
    execute immediate 'truncate table ldr_infa_cb.Det_Exchange';
    execute immediate 'truncate table ldr_infa_cb.Fct_Secexchange';
    execute immediate 'truncate table ldr_infa_cb.Det_Type_Rate';
    execute immediate 'truncate table ldr_infa_cb.Ass_Rate_Exchange';
    execute immediate 'truncate table ldr_infa_cb.Fct_Security_Quot';
    execute immediate 'truncate table ldr_infa_cb.Fct_Bill_State';
    execute immediate 'truncate table ldr_infa_cb.Fct_Secnominal';
    execute immediate 'truncate table ldr_infa_cb.Ass_Accountsecurity';
    execute immediate 'truncate table ldr_infa_cb.det_Roleaccount_Deal'; -- Чистится в qb_dwh_utils.clearall
    execute immediate 'truncate table ldr_infa_cb.det_sec_portfolio';
    execute immediate 'truncate table ldr_infa_cb.ass_sec_portfolio';
    execute immediate 'truncate table ldr_infa_cb.det_security_attr';
    execute immediate 'truncate table ldr_infa_cb.fct_security_attr';
    execute immediate 'truncate table ldr_infa_cb.fct_security_attr_multi';
    execute immediate 'truncate table ldr_infa_cb.fct_securityamount';
    execute immediate 'truncate table ldr_infa_cb.fct_sec_sell_result';
    execute immediate 'truncate table ldr_infa_cb.fct_secrepayschedule';
    execute immediate 'truncate table ldr_infa_cb.fct_security_check';
    execute immediate 'truncate table ldr_infa_cb.fct_sec_adjustment';
    execute immediate 'truncate table ldr_infa_cb.det_security_type_711';
    execute immediate 'truncate table ldr_infa_cb.det_procbase';
    execute immediate 'truncate table ldr_infa_cb.det_rating_type';
    execute immediate 'truncate table ldr_infa_cb.det_rating';
    execute immediate 'truncate table ldr_infa_cb.fct_sec_rating';
    execute immediate 'truncate table ldr_infa_cb.det_subject';
    execute immediate 'truncate table ldr_infa_cb.det_juridic_person';
    execute immediate 'truncate table ldr_infa_cb.fct_securityrisk';
    execute immediate 'truncate table ldr_infa_cb.det_kindprocrate';
    execute immediate 'truncate table ldr_infa_cb.det_subkindprocrate';
    execute immediate 'truncate table ldr_infa_cb.fct_procrate_security';
    execute immediate 'truncate table ldr_infa_cb.det_sertificate';
    execute immediate 'truncate table ldr_infa_cb.fct_sertificate_state';



    /*if in_type = 0 then
      delete from DQB_BP_EVENT_ERROR_DBT;commit;  -- Ошибки произошедьшии при событии
      delete from DQB_BP_EVENT_ATTR_DBT;commit;   -- Аттрибуты
      delete from DQB_BP_EVENT_DBT;commit;        -- События
    end if;*/
  end;

    ------------------------------------------------------
  -- очистка данных по сделкам
  ------------------------------------------------------
  procedure clearDealsData(in_Type number default 0) is
  begin
    execute immediate 'truncate table ldr_infa_cb.Fct_Deal'; -- Чистится в qb_dwh_utils.clearall
    execute immediate 'truncate table ldr_infa_cb.Fct_SecurityDeal';
    execute immediate 'truncate table ldr_infa_cb.Fct_Secdeal_Finstr';
    execute immediate 'truncate table ldr_infa_cb.Fct_RepayDeal';
    execute immediate 'truncate table ldr_infa_cb.Fct_RepoDeal';
    execute immediate 'truncate table ldr_infa_cb.Fct_RepoDeal_Reverse';
    execute immediate 'truncate table ldr_infa_cb.Ass_Fct_Deal';   -- Чистится в qb_dwh_utils.clearall
    execute immediate 'truncate table ldr_infa_cb.det_deal_cat';   -- Чистится в qb_dwh_utils.clearall
    execute immediate 'truncate table ldr_infa_cb.det_deal_cat_val';  -- Чистится в qb_dwh_utils.clearall
    execute immediate 'truncate table ldr_infa_cb.ass_deal_cat_val';  -- Чистится в qb_dwh_utils.clearall
    execute immediate 'truncate table ldr_infa_cb.det_deal_typeattr'; -- Чистится в qb_dwh_utils.clearall
    execute immediate 'truncate table ldr_infa_cb.fct_deal_indicator';   -- Чистится в qb_dwh_utils.clearall
    execute immediate 'truncate table ldr_infa_cb.fct_overdue_securitydeal';
    execute immediate 'truncate table ldr_infa_cb.ass_accountdeal';   -- Чистится в qb_dwh_utils.clearall
    execute immediate 'truncate table ldr_infa_cb.ass_carrydeal';
    execute immediate 'truncate table ldr_infa_cb.fct_dealrisk';
    execute immediate 'truncate table ldr_infa_cb.fct_repayschedule_dm';
    /*if in_type = 0 then
      delete from DQB_BP_EVENT_ERROR_DBT;commit;  -- Ошибки произошедшие при событии
      delete from DQB_BP_EVENT_ATTR_DBT;commit;   -- Аттрибуты
      delete from DQB_BP_EVENT_DBT;commit;        -- События
    end if;*/
  end;

  ------------------------------------------------------
  -- очистка данных по комиссиям
  ------------------------------------------------------
  procedure clearCommData(in_Type number default 0) is
  begin
    execute immediate 'truncate table ldr_infa_cb.det_commission';
    execute immediate 'truncate table ldr_infa_cb.det_comm_cat';
    execute immediate 'truncate table ldr_infa_cb.det_comm_cat_val';
    execute immediate 'truncate table ldr_infa_cb.ass_comm_cat_val';
    execute immediate 'truncate table ldr_infa_cb.fct_deal_commission';
    execute immediate 'truncate table ldr_infa_cb.fct_sec_commission';
    /*if in_type = 0 then
      delete from DQB_BP_EVENT_ERROR_DBT;commit;  -- Ошибки произошедьшии при событии
      delete from DQB_BP_EVENT_ATTR_DBT;commit;   -- Аттрибуты
      delete from DQB_BP_EVENT_DBT;commit;        -- События
    end if;*/
  end;

  procedure export_SecurKIND(procid in number) is
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    EventID      number := 0;
    begin
    startevent(cEvent_EXPORT_Secur, procid, EventID);
    qb_bp_utils.SetAttrValue(EventID,QB_DWH_EXPORT.cAttrRec_Status,qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDepartment, 1);  
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDT, sysdate);
    qb_dwh_export.InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE); 
     
    execute immediate 'truncate table ldr_infa_cb.DET_SECURITY_KIND';
    execute immediate 'truncate table ldr_infa_cb.DET_SECURITY_KIND_ASS_KIND';
    execute immediate 'truncate table ldr_infa_cb.ASS_SECURITY_KIND'; 
       
    --выгрузка справочников по видам ценных бумаг 
    insert into LDR_INFA_CB.DET_SECURITY_KIND_ASS_KIND (security_kind_ass_kind_code, security_kind_ass_kind_name, dt, rec_status, sysmoment, ext_file)
           values ('9999#SOFRXXX#PARENT','Родительский',qb_dwh_utils.DateToChar(to_date('01-01-1980','dd-mm-yyyy')),0,dwhSysMoment,dwhEXT_FILE);
    insert into LDR_INFA_CB.DET_SECURITY_KIND_ASS_KIND (security_kind_ass_kind_code, security_kind_ass_kind_name, dt, rec_status, sysmoment, ext_file)
           values ('9999#SOFRXXX#ROOT','Корневой',qb_dwh_utils.DateToChar(to_date('01-01-1980','dd-mm-yyyy')),0,dwhSysMoment,dwhEXT_FILE);
    commit;
    
    insert into LDR_INFA_CB.DET_SECURITY_KIND (code, name, name_s, is_emissive, is_individual, security_kind_num, gennefimode, 
                                               dt, rec_status, sysmoment, ext_file)
    select '9999#SOFRXXX#'||to_char(dd.t_fi_kind)||'#'||to_char(dd.t_avoirkind) as CODE,
       dd.t_name as NAME,
       dd.t_shortname as NAME_S,
       decode(dd.t_isemissive,'X',1,0) as IS_EMISSIVE,
       decode(dd.t_isindividual,'X',1,0) as IS_INDIVIDUAL,
       dd.t_num as SECURITY_KIND_NUM,
       --dd.t_numlist as SECURITY_KIND_NUMLIST,
       dd.t_gennefimode as GENNEFIMODE,
       qb_dwh_utils.DateToChar(to_date('01-01-1980','dd-mm-yyyy')) as DT,
       0 as REC_STATUS,
       dwhSysMoment as SYSMOMENT,
       dwhEXT_FILE as EXT_FILE

       from davrkinds_dbt dd;      
     commit;
     
     insert into LDR_INFA_CB.ASS_SECURITY_KIND
      select par.security_kind_ass_kind_code as SECURITY_KIND_ASS_KIND_CODE,
             '9999#SOFRXXX#'||to_char(dd.t_fi_kind)||'#'||to_char(dd.t_avoirkind) as SECURITY_KIND_PAR_CODE,
             '9999#SOFRXXX#'||to_char(dd_par.t_fi_kind)||'#'||to_char(dd_par.t_avoirkind) as SECURITY_KIND_CHILD_CODE,
             qb_dwh_utils.DateToChar(to_date('01-01-1980','dd-mm-yyyy')) as DT,
             0 as REC_STATUS,
             dwhSysMoment as SYSMOMENT,
             dwhEXT_FILE as EXT_FILE

      from davrkinds_dbt dd
      inner join davrkinds_dbt dd_par on dd_par.t_parent = dd.t_avoirkind and dd.t_fi_kind = dd_par.t_fi_kind and nvl(dd_par.t_parent,0)<>0
      left join LDR_INFA_CB.DET_SECURITY_KIND_ASS_KIND par on par.security_kind_ass_kind_code = '9999#SOFRXXX#PARENT'

      union all

      select root.security_kind_ass_kind_code as SECURITY_KIND_ASS_KIND_CODE,
             '9999#SOFRXXX#'||to_char(dd.t_fi_kind)||'#'||to_char(dd.t_avoirkind) as SECURITY_KIND_PAR_CODE,
             '9999#SOFRXXX#'||to_char(dd_root.t_fi_kind)||'#'||to_char(dd_root.t_avoirkind) as SECURITY_KIND_CHILD_CODE,
             qb_dwh_utils.DateToChar(to_date('01-01-1980','dd-mm-yyyy')) as DT,
             0 as REC_STATUS,
             dwhSysMoment as SYSMOMENT,
             dwhEXT_FILE as EXT_FILE

      from davrkinds_dbt dd
      inner join davrkinds_dbt dd_root on dd_root.T_ROOT = dd.t_avoirkind and dd.t_fi_kind = dd_root.t_fi_kind and nvl(dd_root.t_root,0)<>0
      left join LDR_INFA_CB.DET_SECURITY_KIND_ASS_KIND root on root.security_kind_ass_kind_code = '9999#SOFRXXX#ROOT';
     commit;
    
    
  
  end export_SecurKIND;


  procedure export_Secur_9996(fiid         in dfininstr_dbt.t_fiid%type,
                              avoirkind    in dfininstr_dbt.t_avoirkind%type,
                              bill_kind    in pls_integer,
                              in_date      in date,
                              dwhRecStatus in varchar2,
                              dwhDT        in varchar2,
                              dwhSysMoment in varchar2,
                              dwhEXT_FILE  in varchar2) is
   vcode  varchar2(30);
   vname  varchar2(250);
   vnames varchar2(50);
   stype  varchar2(1);
   dateis varchar2(10);
   vnomin varchar2(40);
   codeis varchar2(250);
   ficode varchar2(30);
   date_is date;
   vdt    date;
   quot   varchar2(1);
   prevst varchar2(2);
   vregnum varchar2(50);
   bank_ptid ddp_dep_dbt.t_partyid%type;
   deal_code varchar2(100);
   sum_buy varchar2(26);
   issub davoiriss_dbt.t_subordinated%type;
   nkdbase davoiriss_dbt.t_nkdbase_kind%type;
   procbase varchar2(100);
   SEC_KIND_CODE VARCHAR2(100);
  begin
    select t_partyid
      into bank_ptid
      from ddp_dep_dbt dp
     where dp.t_parentcode = n0;
    if InConst(cSECKIND_BILL, avoirkind) then
      -- Векселя
      for rec in (select bn.t_bcid,
                         to_char(bn.t_bcid) || '#BNR' vcode,
                         substr('Вексель ' || trim(bn.t_issuername) || ' серия: ' || trim(bn.t_bcseries) || ' номер: ' || trim(bn.t_bcnumber), 1, 250) vname,
                         substr('Вексель серия: ' || trim(bn.t_bcseries) || ' номер: ' || trim(bn.t_bcnumber), 1, 50) vnames,
                         qb_dwh_utils.DateToChar(decode(bn.t_issuedate, emptDate, firstDate, bn.t_issuedate)) dateis,
                         qb_dwh_utils.NumberToChar(round(leg.t_principal, 12), 12) vnomin,
                         qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     bn.t_issuer) codeis,
                         pfi.t_iso_number ficode,
                         case when bn.t_issuekind = 0 then
                                '1'
                              when bn.t_issuekind = 1 then
                                '2'
                              else
                                null
                         end typebill,
                         case when leg.t_formula = 1 then
                                '1'
                              when leg.t_formula = 0 then
                                '2'
                              else
                                null
                         end typeprofit,
                         case when bn.t_bctermformula = 10 then
                                '1'
                              when bn.t_bctermformula = 20 and leg.t_maturity = emptDate and leg.t_expiry = emptDate then
                                '2'
                              when bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry = emptDate then
                                '5'
                              when bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry <> emptDate then
                                '6'
                              when bn.t_bctermformula = 20 and leg.t_maturity = emptDate and leg.t_expiry <> emptDate then
                                '7'
                              else
                                null
                         end typerepay,
                         bn.t_bcnumber numberbill,
                         bn.t_bcseries seriesbill,
                         null numberform,
                         case when (bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry = emptDate) or
                                   (bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry <> emptDate) then
                                qb_dwh_utils.DateToChar(leg.t_maturity)
                              else
                                null
                         end lowerdate,
                         case when (bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry <> emptDate) or
                                   (bn.t_bctermformula = 20 and leg.t_maturity = emptDate and leg.t_expiry <> emptDate) then
                                qb_dwh_utils.DateToChar(leg.t_expiry)
                              else
                                null
                         end upperdate,
                         case when leg.t_maturity <> emptDate then
                                qb_dwh_utils.DateToChar(decode(leg.t_maturity, emptDate, firstDate,leg.t_maturity))
                              else
                                null
                         end maturitydate,
                         null maturitydelay,
                         case when leg.t_formula = 1 then
                                qb_dwh_utils.DateToChar(decode(leg.t_expiry, emptDate, firstDate,leg.t_expiry))
                              else
                                null
                         end endpersentdate,
                         case when leg.t_formula = 1 then
                           qb_dwh_utils.NumberToChar(leg.t_price/power(10, leg.t_point), 3)--!!!!!!!!!!!!!! dfiwarnts_dbt.t_oncallrate
                         else
                           null
                         end proc_rate,
                         round(bnin.t_perc, 9) discount,
                         --Case when (bn.t_bcid in (2449, 2454, 2457, 2458, 2459, 2462, 2463, 2512, 2513, 2514, 2515, 2516, 2517)) then
                         --  '-1'
                         --else
                           qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                         qb_dwh_utils.System_IBSO,
                                                         1,
                                                         decode(bn.t_holder, -1, bank_ptid, bn.t_holder))
                         --end 
                         remitent_subject_code,
                         '0' accepted,
                         '0' aval,
                         null typeclause,
                         bn.t_issueplace issue_place,
                         null repayment_place,
                         case when decode(bn.t_registrationdate,emptDate,maxDate, bn.t_registrationdate) < decode(bn.t_issuedate,emptDate,maxDate, bn.t_issuedate) then
                                 decode(bn.t_registrationdate,emptDate,firstDate, bn.t_registrationdate)
                              else
                                decode(bn.t_issuedate,emptDate,firstDate, bn.t_issuedate)
                         end dt,
                         case when bn.t_portfolioid = 30 then
                                '1'
                              when bn.t_portfolioid = 31 then
                                '2'
                              when bn.t_portfolioid = 32 then
                                '3'
                              else
                                '-1'
                          end portfolio,
                          '-1' deal_code,
                          case when leg.t_typepercent = 0 then
                            '1' -- Фиксированная ставка
                          else
                            '2'
                          end type_proc_rate,
                          nvl2(dd.t_avoirkind, '9999#SOFRXXX#'||to_char(dd.t_fi_kind)||'#'||to_char(dd.t_avoirkind),null) as SECURITY_KIND_CODE
                    from dvsbanner_dbt bn
                    left join ddl_leg_dbt leg
                      on (bn.t_bcid = leg.t_dealid and leg.t_legid = n0 and leg.t_legkind = n1)
                    left join dfininstr_dbt pfi
                      on (leg.t_pfi = pfi.t_fiid)
                    left join dfininstr_dbt pfi_d
                      on (bn.t_fiid = pfi_d.t_fiid)
                    left join davrkinds_dbt dd
                      on dd.t_fi_kind = pfi_d.t_fi_kind and dd.t_avoirkind = pfi_d.t_avoirkind                      
                    left join dpartcode_dbt pc
                      on pc.t_partyid = bn.t_issuer and pc.t_codekind = n101
                    left join dpartcode_dbt pc1
                      on pc1.t_partyid = bn.t_holder and pc1.t_codekind = n101
                    left join dvsincome_dbt bnin
                      on (bn.t_bcid = bnin.t_bcid and bnin.t_incometype = n9)
                   where bn.t_fiid = fiid)
       loop
          -- Вставка в DET_FINSTR
          Insert into ldr_infa_cb.det_finstr(finstr_code, finstr_name, finstr_name_s, typefinstr, dt, rec_status,sysmoment, ext_file)
                 values (rec.vcode, rec.vname, rec.vnames, 2, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          -- Вставка в DET_SECURITY
          stype := case when InConst(cSECKIND_STOCK, avoirkind) then           -- акция
                      1
                   when InConst(cSECKIND_BILL, avoirkind)  then                -- вексель
                      3
                   when InConst(cSECKIND_UIT, avoirkind)  then                 -- ПИФ
                      5
                   when InConst(cSECKIND_BOND, avoirkind) then                 -- облигация
                      2
                   when InConst(cSECKIND_RECEIPT, avoirkind) then              -- депозитарная расписка
                      6
                   else
                      null
                   end;
          /* Все ошибки аккумулируются при загрузке
          if (stype is null) then
            Raise_Application_Error(-20001, 'Ошибка при добавлении записи в DET_SECURITY. Не удалось определить тип ценной бумаги!');
          end if;
          */
          Insert into ldr_infa_cb.det_security(typesecurity, code, date_issue, nominal, regnum, finstrsecurity_finstr_code, issuer_code, underwriter_code, finstrcurnom_finstr_code,
                      procbase, dt, SECURITY_KIND_CODE, rec_status, sysmoment, ext_file)
                 values (stype, rec.vcode, rec.dateis, rec.vnomin, null, rec.vcode, rec.codeis, '-1', rec.ficode, '9999#SOFRXXX#1', qb_dwh_utils.DateToChar(rec.dt), rec.SECURITY_KIND_CODE, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          -- Вставка в DET_BILL
          insert into ldr_infa_cb.det_bill(typebill, typeprofit, typerepay, numberbill, seriesbill, numberform, lowerdate, upperdate, maturitydate, maturitydelay,
                      endpersentdate,security_code, proc_rate, discount, repay_subject_code, remitent_subject_code, accepted, aval, typeclause, issue_place, repayment_place, type_proc_rate, dt, rec_status,
                      sysmoment, ext_file)
                 values (rec.typebill, rec.typeprofit, rec.typerepay, rec.numberbill, rec.seriesbill, rec.numberform, rec.lowerdate, rec.upperdate, rec.maturitydate, rec.maturitydelay,
                      rec.endpersentdate, rec.vcode, rec.proc_rate, qb_dwh_utils.NumberToChar(rec.discount, 9), rec.codeis, rec.remitent_subject_code, rec.accepted, rec.aval, rec.typeclause, rec.issue_place, rec.repayment_place, rec.type_proc_rate, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                      
          -- Вставка в FCT_BILL_STATE
          prevst := '-1';
          if    (bill_kind = 1) then -- История статусов по собственным векселям
            for bstate_rec in (select bill_state, chdate,
                                      bcid,
                                      (select '0000#IBSOXXX#' || uf4
                                         from (select t_docid,  catacc.t_account,
                                                                --cacc.t_userfield4 uf4,
                                                                case
                                                                  when (cacc.t_accountid is null) then
                                                                    catacc.t_account
                                                                  when (cacc.t_userfield4 is null) or
                                                                      (cacc.t_userfield4 = chr(0)) or
                                                                      (cacc.t_userfield4 = chr(1)) or
                                                                      (cacc.t_userfield4 like '0x%') then
                                                                    cacc.t_account
                                                                  else
                                                                    cacc.t_userfield4
                                                                end uf4,
                                                                row_number() over (order by t_activatedate desc) as rnk
                                                                                from DMCACCDOC_DBT catacc
                                                                               left join daccount_dbt cacc
                                                                                  on (catacc.t_chapter = cacc.t_chapter and catacc.t_account = cacc.t_account and catacc.t_currency = cacc.t_code_currency)
                                                                               where t_dockind = n164 and catacc.t_docid = bcid
                                                                                 and catacc.t_catnum = n450
                                                                                 and catacc.t_activatedate <= chdate) where rnk = n1
                                                                                 ) acc,
                                     row_number() over (partition by bcid order by chdate desc) as rnk_last_state
                                from (select id,
                                             max(id) over(partition by chdate) lastchgindate,
                                             chdate,
                                             status,
                                             state,
                                             case
                                               when status = 25 then
                                                '10'
                                               when status in (5, 30) then
                                                '4'
                                               when status = 40 then
                                                '5'
                                               when status = 50 then
                                                '3'
                                               when instr(state, 'T') > 0 then
                                                '6'
                                               when instr(state, 'Ф') > 0 then
                                                '7'
                                               else
                                                '1'
                                             end bill_state,
                                             bcid
                                        from (select T_ID           id,
                                                     sh.t_bcid bcid,
                                                     T_CHANGEDATE   chdate,
                                                     T_NEWABCSTATUS status,
                                                     T_NEWBCSTATE   state
                                                from dvsbnrbck_dbt sh
                                               where sh.t_bcid = rec.t_bcid
                                                 and sh.t_bcstatus = chr88
                                                 AND sh.t_abcstatus = chr0
                                              union all
                                              select T_ID,
                                                     sh.t_bcid bcid,
                                                     T_CHANGEDATE,
                                                     (select t_newabcstatus
                                                        from dvsbnrbck_dbt
                                                       where t_id = (select max(t_id)
                                                                       from dvsbnrbck_dbt
                                                                      where t_bcid = sh.t_bcid
                                                                        and t_bcstatus = chr88
                                                                        and t_id < sh.t_id)) T_NEWABCSTATUS,
                                                     T_NEWBCSTATE
                                                from dvsbnrbck_dbt sh
                                               where sh.t_bcid = rec.t_bcid
                                                 and sh.t_bcstatus = chr0
                                                 AND sh.t_abcstatus = chr0
                                               order by id))
                               where id = lastchgindate)
            loop
              if (bstate_rec.acc is not null) then
                Insert into ldr_infa_cb.fct_bill_state(Bill_State,bill_code, Subject_Code, Dt, Rec_Status, Sysmoment, Ext_File)
                       values(bstate_rec.bill_state, rec.vcode, rec.remitent_subject_code, qb_dwh_utils.DateToChar(bstate_rec.chdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                       
                if (bstate_rec.bill_state <> '4') and (bstate_rec.rnk_last_state = 1) then
                  begin
                    select deal_code, sum_buy
                      into deal_code, sum_buy
                      from (select to_char(ord.t_contractid) || '#ORD' deal_code,
                                   to_char(bn.t_bcid) || '#BNR',
                                   ord.t_signdate dt,
                                   qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                                   row_number() over(partition by bn.t_bcid order by ord.t_signdate desc) rnk_last_deal
                              from dvsbanner_dbt bn
                             inner join ddl_leg_dbt leg
                                on (bn.t_bcid = leg.t_dealid)
                             inner join dvsordlnk_dbt lnk
                                on (bn.t_bcid = lnk.t_bcid)
                             inner join ddl_order_dbt ord
                                on (lnk.t_contractid = ord.t_contractid and
                                   lnk.t_dockind = ord.t_dockind)
                             inner join ddp_dep_dbt dp
                                on (bn.t_issuer = dp.t_partyid)
                             where bn.t_bcid = rec.t_bcid
                               and leg.t_legid = n0
                               and leg.t_legkind = n1
                               and ord.t_signdate <= in_date
                               and ord.t_dockind = n109 -- выпуск
                            )
                      where rnk_last_deal = n1;
                    exception
                      when no_data_found then
                        deal_code := '-1';
                        sum_buy   := '0';
                    end;

                    for i in 0 ..  in_date - bstate_rec.chdate
                    loop
                      insert into ldr_infa_cb.fct_securityamount(amount, account_code, security_code, deal_code, sec_portfolio_code, lot_num, dt, rec_status, sysmoment, ext_file)
                             values (case when  bstate_rec.bill_state in ('4') then '0' else '1' end, bstate_rec.acc, rec.vcode, deal_code, rec.portfolio, '-1', qb_dwh_utils.DateToChar(bstate_rec.chdate + i), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                             
                      insert into ldr_infa_cb.fct_sec_sell_result(deal_code, security_code, sec_portfolio_code, purchase_amount, sell_amount, sum_of_disposal, lot_num, dt, rec_status, sysmoment, ext_file)
                            values (deal_code, rec.vcode, rec.portfolio, sum_buy, '0', '0', '-1', qb_dwh_utils.DateToChar(bstate_rec.chdate + i), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                            
                    end loop;
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '4', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '5', qb_dwh_utils.NumberToChar(rec.discount, 2), qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '6', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '7', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '8', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '9', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                end if;
              end if;
            end loop;
            -- корректировка ЭПС
            for receps in (SELECT sec_code,
                                   qb_dwh_utils.NumberToChar(round(sum(t_rest), 2), 2) sumrest,
                                   qb_dwh_utils.DateToChar(t_restdate) restdate,
                                   qb_dwh_utils.DateToChar(lead(t_restdate - 1, 1, in_date) over (partition by sec_code order by t_restdate)) next_date,
                                   t_restdate
                              from (select to_char(cacc.t_docid) || '#BNR' sec_code,
                                           acc.t_accountid,
                                           acc.t_account,
                                           rd.t_rest,
                                           rd.t_restdate,
                                           row_number() over(partition by cacc.t_docid order by rd.t_restdate desc) rnk
                                      from DMCACCDOC_DBT cacc
                                     inner join daccount_dbt acc
                                        on (cacc.t_chapter = acc.t_chapter and
                                           cacc.t_account = acc.t_account and
                                           cacc.t_currency = acc.t_code_currency)
                                     inner join drestdate_dbt rd
                                        on (acc.t_accountid = rd.t_accountid)
                                     where cacc.t_catnum in (select v.value
                                                               from qb_dwh_const4exp c
                                                              inner join qb_dwh_const4exp_val v
                                                                 on (c.id = v.id)
                                                              where c.name = cCATEXP_OBILL)
                                       and cacc.t_dockind = n164
                                       and cacc.t_docid = rec.t_bcid
                                       and rd.t_restdate <= in_date
                                       and rd.t_rest <> n0)
                              group by sec_code, t_restdate)
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(ord.t_contractid) || '#ORD' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               ord.t_signdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by ord.t_signdate desc) rnk_last_deal
                          from dvsbanner_dbt bn
                         inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                         inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                         inner join ddl_order_dbt ord
                            on (lnk.t_contractid = ord.t_contractid and
                               lnk.t_dockind = ord.t_dockind)
                         inner join ddp_dep_dbt dp
                            on (bn.t_issuer = dp.t_partyid)
                         where bn.t_bcid = rec.t_bcid
                           and leg.t_legid = n0
                           and leg.t_legkind = n1
                           and ord.t_signdate <= receps.t_restdate
                           and ord.t_dockind = n109 -- выпуск
                        )
                  where rnk_last_deal = 1;
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, receps.sec_code, '-1', '1', receps.sumrest, receps.restdate, receps.next_date, receps.restdate,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
            end loop;
            -- Корректировка ОКУ
            for lrec in(with dates as (
                          select distinct rd.t_lnkdate lnkdate
                            from ddlreslnk_dbt rd
                           where rd.t_parentid = rec.t_bcid
                             and rd.t_type = n2
                             and rd.t_reservesubkind in (select v.value
                                                           from qb_dwh_const4exp c
                                                          inner join qb_dwh_const4exp_val v
                                                             on (c.id = v.id)
                                                          where c.name = cRES_SUBKIND)
                             and rd.t_lnkdate <= in_date),
                               res as (
                          select lnkdate,
                                 lead(lnkdate - 1, 1, in_date) over (order by lnkdate) nxt_date,
                                 nvl( ( select * from (select r0.t_reserveamount
                                         from ddlreslnk_dbt r0
                                        where r0.t_parentid = rec.t_bcid
                                          and r0.t_type = n2
                                          and r0.t_reservesubkind = n0
                                          order by r0.t_id desc) where rownum = 1
                                          ), 0) res0,
                                 nvl( ( select * from (select r5.t_reserveamount
                                         from ddlreslnk_dbt r5
                                        where r5.t_parentid = rec.t_bcid
                                          and r5.t_type = n2
                                          and r5.t_reservesubkind = n5
                                          order by r5.t_id desc) where rownum = 1
                                          ), 0) res5

                            from dates)
                        select qb_dwh_utils.NumberToChar(res5 - res0, 2) as c2oku,
                               qb_dwh_utils.DateToChar(lnkdate) bd,
                               qb_dwh_utils.DateToChar(nxt_date) ed,
                               lnkdate
                          from res
                         where (res5 - res0) <> n0
                      )
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(ord.t_contractid) || '#ORD' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               ord.t_signdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by ord.t_signdate desc) rnk_last_deal
                          from dvsbanner_dbt bn
                         inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                         inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                         inner join ddl_order_dbt ord
                            on (lnk.t_contractid = ord.t_contractid and
                               lnk.t_dockind = ord.t_dockind)
                         inner join ddp_dep_dbt dp
                            on (bn.t_issuer = dp.t_partyid)
                         where bn.t_bcid = rec.t_bcid
                           and leg.t_legid = n0
                           and leg.t_legkind = n1
                           and ord.t_signdate <= lrec.lnkdate
                           and ord.t_dockind = n109 -- выпуск
                        )
                  where rnk_last_deal = 1;
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, rec.vcode, '-1', '2', lrec.c2oku, lrec.bd, lrec.ed, lrec.bd,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
            end loop;
            -- Корректировка РПБУ
            for lrec in(SELECT qb_dwh_utils.NumberToChar(res0_sum, 2) c2rpbu,
                               qb_dwh_utils.DateToChar(res0_lnkdate) bd,
                               qb_dwh_utils.DateToChar(nxt_date) ed,
                               res0_lnkdate lnkdate
                          from (select bn.t_bcid,
                                       nvl(res0.t_reserveamount, 0) res0_sum,
                                       nvl(res0.t_lnkdate, in_date) res0_lnkdate,
                                       lead(res0.t_lnkdate - 1, 1, in_date) over (order by res0.t_lnkdate) nxt_date
                                  from dvsbanner_dbt bn
                                  left join ddlreslnk_dbt res0
                                    on (bn.t_bcid = res0.t_parentid and res0.t_type = n2 and
                                       res0.t_reservesubkind = n0 and res0.t_lnkdate <= in_date)

                                  where bn.t_bcid = rec.t_bcid
                                  )
                         where res0_sum <> n0)
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(ord.t_contractid) || '#ORD' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               ord.t_signdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by ord.t_signdate desc) rnk_last_deal
                          from dvsbanner_dbt bn
                         inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                         inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                         inner join ddl_order_dbt ord
                            on (lnk.t_contractid = ord.t_contractid and
                               lnk.t_dockind = ord.t_dockind)
                         inner join ddp_dep_dbt dp
                            on (bn.t_issuer = dp.t_partyid)
                         where bn.t_bcid = rec.t_bcid
                           and leg.t_legid = n0
                           and leg.t_legkind = n1
                           and ord.t_signdate <= lrec.lnkdate
                           and ord.t_dockind = n109 -- выпуск
                        )
                  where rnk_last_deal = n1;
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, rec.vcode, '-1', '3', lrec.c2rpbu, lrec.bd, lrec.ed, lrec.bd,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
            end loop;
          elsif (bill_kind = 2) then  -- История статусов по учтеннм векселям
            for bstate_rec in (select bill_state, chdate,
                                      bcid,
                                      (select '0000#IBSOXXX#' || uf4
                                         from (select t_docid,
                                                      catacc.t_account,
                                                      --acc.t_userfield4 uf4,
                                                      case
                                                        when (acc.t_accountid is null) then
                                                          catacc.t_account
                                                        when (acc.t_userfield4 is null) or
                                                            (acc.t_userfield4 = chr(0)) or
                                                            (acc.t_userfield4 = chr(1)) or
                                                            (acc.t_userfield4 like '0x%') then
                                                          acc.t_account
                                                        else
                                                          acc.t_userfield4
                                                      end uf4,
                                                      row_number() over (order by t_activatedate desc) as rnk
                                                                                from DMCACCDOC_DBT catacc
                                                                               left join daccount_dbt acc
                                                                                  on (catacc.t_chapter = acc.t_chapter and catacc.t_account = acc.t_account and catacc.t_currency = acc.t_code_currency)
                                                                               where t_dockind = n164 and catacc.t_docid = bcid
                                                                                 and catacc.t_catnum = n462
                                                                                 and catacc.t_activatedate <= chdate) where rnk = n1
                                                                                 ) acc,
                                      row_number() over (partition by bcid order by chdate desc) as rnk_last_state
                                from (select id,
                                             max(id) over(partition by chdate) lastchgindate,
                                             chdate,
                                             status,
                                             state,
                                             case
                                               when status = 200 then
                                                '4'
                                               when instr(state, 'Г') > 0 then
                                                '11'
                                               when instr(state, 'Д') > 0 then
                                                '11'
                                               when instr(state, 'З') > 0 then
                                                '10'
                                               when instr(state, 'К') > 0 then
                                                '4'
                                               when instr(state, 'Л') > 0 then
                                                '12'
                                               when instr(state, 'Н') > 0 then
                                                '10'
                                               when instr(state, 'О') > 0 then
                                                '11'
                                               when instr(state, 'П') > 0 then
                                                '3'
                                               when instr(state, 'Р') > 0 then
                                                '13'
                                               when instr(state, 'Т') > 0 then
                                                '6'
                                               when instr(state, 'Ф') > 0 then
                                                '7'
                                               else
                                                '1'
                                             end bill_state,
                                             bcid
                                        from (select T_ID           id,
                                                     sh.t_bcid      bcid,
                                                     T_CHANGEDATE   chdate,
                                                     T_NEWABCSTATUS status,
                                                     T_NEWBCSTATE   state
                                                from dvsbnrbck_dbt sh
                                               where sh.t_bcid = rec.t_bcid
                                                 and sh.t_bcstatus = chr0
                                                 AND sh.t_abcstatus = chr88
                                              union all
                                              select T_ID,
                                                     sh.t_bcid      bcid,
                                                     T_CHANGEDATE,
                                                     (select t_newabcstatus
                                                        from dvsbnrbck_dbt
                                                       where t_id = (select max(t_id)
                                                                       from dvsbnrbck_dbt
                                                                      where t_bcid = sh.t_bcid
                                                                        and t_abcstatus = chr88
                                                                        and t_id < sh.t_id)) T_NEWABCSTATUS,
                                                     T_NEWBCSTATE
                                                from dvsbnrbck_dbt sh
                                               where sh.t_bcid = rec.t_bcid
                                                 and sh.t_bcstatus = chr0
                                                 AND sh.t_abcstatus = chr0
                                               order by id))
                               where id = lastchgindate
                              )
            loop
              if (prevst <> bstate_rec.bill_state) then
                Insert into ldr_infa_cb.fct_bill_state(Bill_State,bill_code, Subject_Code, Dt, Rec_Status, Sysmoment, Ext_File)
                       values(bstate_rec.bill_state, rec.vcode, rec.remitent_subject_code, qb_dwh_utils.DateToChar(bstate_rec.chdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                       
                prevst := bstate_rec.bill_state;
                if (bstate_rec.bill_state <> '4') and (bstate_rec.rnk_last_state = 1) then
                  begin
                    select deal_code, sum_buy
                      into deal_code, sum_buy
                      from (select to_char(tick.t_dealid) || '#TCK' deal_code,
                                   to_char(bn.t_bcid) || '#BNR',
                                   tick.t_dealdate dt,
                                   qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                                   row_number() over(partition by bn.t_bcid order by tick.t_dealdate desc nulls last) rnk_last_deal
                              from dvsbanner_dbt bn
                              inner join ddl_leg_dbt leg
                                on (bn.t_bcid = leg.t_dealid)
                              inner join dvsordlnk_dbt lnk
                                on (bn.t_bcid = lnk.t_bcid)
                              inner join ddl_tick_dbt tick
                                on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                              where bn.t_bcid = rec.t_bcid
                                and leg.t_legid = n0 and leg.t_legkind = n1
                                and tick.t_bofficekind = n141 and tick.t_dealtype = n12401 -- покупка векселя
                                and tick.t_dealdate <= in_date
                            )
                      where rnk_last_deal = n1;  -- последняя сделка
                    exception
                      when no_data_found then
                        deal_code := '-1';
                    end;

                  for i in 0 ..  in_date - bstate_rec.chdate
                  loop
                    insert into ldr_infa_cb.fct_securityamount(amount, account_code, security_code, deal_code, sec_portfolio_code, lot_num, dt, rec_status, sysmoment, ext_file)
                           values (case when  bstate_rec.bill_state in ('4') then '0' else '1' end, bstate_rec.acc, rec.vcode, deal_code, rec.portfolio, '-1', qb_dwh_utils.DateToChar(bstate_rec.chdate + i), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                           
                    insert into ldr_infa_cb.fct_sec_sell_result(deal_code, security_code, sec_portfolio_code, purchase_amount, sell_amount, sum_of_disposal, lot_num, dt, rec_status, sysmoment, ext_file)
                          values (deal_code, rec.vcode, rec.portfolio, sum_buy, '0', '0', '-1', qb_dwh_utils.DateToChar(bstate_rec.chdate + i), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          

                  end loop;
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '4', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '5', qb_dwh_utils.NumberToChar(rec.discount, 2), qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '6', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '7', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '8', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '9', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                end if;
              end if;
            end loop;
          -- корректировка ЭПС
          for receps in (SELECT sec_code,
                                   qb_dwh_utils.NumberToChar(round(sum(t_rest), 2), 2) sumrest,
                                   qb_dwh_utils.DateToChar(t_restdate) restdate,
                                   qb_dwh_utils.DateToChar(lead(t_restdate - 1, 1, in_date) over (partition by sec_code order by t_restdate)) next_date,
                                   t_restdate
                              from (select to_char(cacc.t_docid) || '#BNR' sec_code,
                                           acc.t_accountid,
                                           acc.t_account,
                                           rd.t_rest,
                                           rd.t_restdate,
                                           row_number() over(partition by cacc.t_docid order by rd.t_restdate desc) rnk
                                      from DMCACCDOC_DBT cacc
                                     inner join daccount_dbt acc
                                        on (cacc.t_chapter = acc.t_chapter and
                                           cacc.t_account = acc.t_account and
                                           cacc.t_currency = acc.t_code_currency)
                                     inner join drestdate_dbt rd
                                        on (acc.t_accountid = rd.t_accountid)
                                     where cacc.t_catnum in (select v.value
                                                               from qb_dwh_const4exp c
                                                              inner join qb_dwh_const4exp_val v
                                                                 on (c.id = v.id)
                                                              where c.name = cCATEXP_DBILL)
                                       and cacc.t_dockind = n164
                                       and cacc.t_docid = rec.t_bcid
                                       and rd.t_restdate <= in_date
                                       and rd.t_rest <> n0)
                              group by sec_code, t_restdate)
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(tick.t_dealid) || '#TCK' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               tick.t_dealdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by tick.t_dealdate desc nulls last) rnk_last_deal
                          from dvsbanner_dbt bn
                          inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                          inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                          inner join ddl_tick_dbt tick
                            on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                          where bn.t_bcid = rec.t_bcid
                            and leg.t_legid = n0 and leg.t_legkind = n1
                            and tick.t_bofficekind = n141 and tick.t_dealtype = n12401 -- покупка векселя
                            and tick.t_dealdate <= receps.t_restdate
                        )
                  where rnk_last_deal = n1;  -- последняя сделка
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, receps.sec_code, '-1', '1', receps.sumrest, receps.restdate, receps.next_date, receps.restdate,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;
            end loop;
            -- Корректировка ОКУ
            for lrec in(with dates as (
                          select distinct rd.t_lnkdate lnkdate
                            from ddlreslnk_dbt rd
                           where rd.t_parentid = rec.t_bcid
                             and rd.t_type = n2
                             and rd.t_reservesubkind in (select v.value
                                                           from qb_dwh_const4exp c
                                                          inner join qb_dwh_const4exp_val v
                                                             on (c.id = v.id)
                                                          where c.name = cRES_SUBKIND)
                             and rd.t_lnkdate <= in_date),
                               res as (
                          select lnkdate,
                                 lead(lnkdate - 1, 1, in_date) over (order by lnkdate) nxt_date,
                                 nvl( ( select * from (select r0.t_reserveamount
                                         from ddlreslnk_dbt r0
                                        where r0.t_parentid = rec.t_bcid
                                          and r0.t_type = n2
                                          and r0.t_reservesubkind = n0
                                          order by r0.t_id desc) where rownum = 1
                                          ), 0) res0,
                                 nvl( ( select * from (select r5.t_reserveamount
                                         from ddlreslnk_dbt r5
                                        where r5.t_parentid = rec.t_bcid
                                          and r5.t_type = n2
                                          and r5.t_reservesubkind = n5
                                          order by r5.t_id desc) where rownum = 1 
                                          ), 0) res5

                            from dates)
                        select qb_dwh_utils.NumberToChar(res5 - res0, 2) as c2oku,
                               qb_dwh_utils.DateToChar(lnkdate) bd,
                               qb_dwh_utils.DateToChar(nxt_date) ed,
                               lnkdate
                          from res
                         where (res5 - res0) <> n0
                      )
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(tick.t_dealid) || '#TCK' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               tick.t_dealdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by tick.t_dealdate desc nulls last) rnk_last_deal
                          from dvsbanner_dbt bn
                          inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                          inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                          inner join ddl_tick_dbt tick
                            on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                          where bn.t_bcid = rec.t_bcid
                            and leg.t_legid = n0 and leg.t_legkind = n1
                            and tick.t_bofficekind = n141 and tick.t_dealtype = n12401 -- покупка векселя
                            and tick.t_dealdate <= lrec.lnkdate
                        )
                  where rnk_last_deal = n1;  -- последняя сделка
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, rec.vcode, '-1', '2', lrec.c2oku, lrec.bd, lrec.ed, lrec.bd,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
            end loop;
            -- Корректировка РПБУ
            for lrec in(SELECT qb_dwh_utils.NumberToChar(res0_sum, 2) c2rpbu,
                               qb_dwh_utils.DateToChar(res0_lnkdate) bd,
                               qb_dwh_utils.DateToChar(nxt_date) ed,
                               res0_lnkdate lnkdate
                          from (select bn.t_bcid,
                                       nvl(res0.t_reserveamount, 0) res0_sum,
                                       nvl(res0.t_lnkdate, in_date) res0_lnkdate,
                                       lead(res0.t_lnkdate - 1, 1, in_date) over (order by res0.t_lnkdate) nxt_date
                                  from dvsbanner_dbt bn
                                  left join ddlreslnk_dbt res0
                                    on (bn.t_bcid = res0.t_parentid and res0.t_type = 2 and
                                       res0.t_reservesubkind = 0 and res0.t_lnkdate <= in_date)

                                  where bn.t_bcid = rec.t_bcid
                                  )
                         where res0_sum <> n0)
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(tick.t_dealid) || '#TCK' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               tick.t_dealdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by tick.t_dealdate desc nulls last) rnk_last_deal
                          from dvsbanner_dbt bn
                          inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                          inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                          inner join ddl_tick_dbt tick
                            on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                          where bn.t_bcid = rec.t_bcid
                            and leg.t_legid = n0 and leg.t_legkind = n1
                            and tick.t_bofficekind = n141 and tick.t_dealtype = n12401 -- покупка векселя
                            and tick.t_dealdate <= lrec.lnkdate
                        )
                  where rnk_last_deal = n1;  -- последняя сделка
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, rec.vcode, '-1', '3', lrec.c2rpbu, lrec.bd, lrec.ed, lrec.bd,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
            end loop;

          end if;
          -- Вставка в ASS_ACCOUNTCECURITY
          for acrec in (select distinct '0000#IBSOXXX#' || case
                                                              when (acc.t_userfield4 is null) or
                                                                  (acc.t_userfield4 = chr(0)) or
                                                                  (acc.t_userfield4 = chr(1)) or
                                                                  (acc.t_userfield4 like '0x%') then
                                                                acc.t_account
                                                              else
                                                                acc.t_userfield4
                                                            end account_code,
            --> 2020-05-15 AS
                                        /* '0000#SOFR#' || */ cat.t_code ||
                                          case when catacc.t_catnum in (235, 464) and templ.t_value4 = 1  THEN
                                                '_П'
                                               when catacc.t_catnum in (235, 464) and templ.t_value4 = 2  THEN
                                                '_Д'
                                               when catacc.t_catnum = 1492 and templ.t_value1 >= 0 THEN
                                                 (select '#' || t_code from dllvalues_dbt where t_list = n3503 and t_element = templ.t_value1)
                                               else
                                                 null
                                          end roleaccount_deal_code,
                                          case when catacc.t_activatedate < vdt then
                                                 vdt
                                               else
                                                 catacc.t_activatedate
                                          end dt
                          from dmcaccdoc_dbt catacc
                         inner join daccount_dbt acc
                            on (catacc.t_chapter = acc.t_chapter and catacc.t_account = acc.t_account and catacc.t_currency = acc.t_code_currency)
                         inner join dmccateg_dbt cat
                            on (catacc.t_catid = cat.t_id)
                          left join dmctempl_dbt templ
                            on (catacc.t_catid = templ.t_catid and catacc.t_templnum = templ.t_number)
                         where t_dockind = n164 and catacc.t_docid = rec.t_bcid
                           and catacc.t_chapter in (select v.value
                                                      from qb_dwh_const4exp c
                                                     inner join qb_dwh_const4exp_val v
                                                        on (c.id = v.id)
                                                     where c.name = cACC_CHAPTERS)
                           and exists (select 1 from dfininstr_dbt fi where fi.t_fiid = catacc.t_currency and fi.t_fi_kind = n1)
                           and catacc.t_activatedate <
                               decode(catacc.t_disablingdate,
                                      emptDate,
                                      maxDate,
                                      catacc.t_disablingdate)
                           and exists (select 1 from daccount_dbt a where a.t_chapter = catacc.t_chapter and a.t_account = catacc.t_account and a.t_code_currency = catacc.t_currency)
                         order by roleaccount_deal_code, dt)
          loop
            insert into ldr_infa_cb.ass_accountsecurity(account_code,security_code, roleaccount_deal_code, dt,rec_status, sysmoment, ext_file)
                   values (acrec.account_code, rec.vcode, acrec.roleaccount_deal_code, qb_dwh_utils.DateToChar(acrec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end loop;
          if (rec.portfolio is not null) then
            insert into ldr_infa_cb.ass_sec_portfolio(security_code, sec_portfolio_code, dt,rec_status, sysmoment, ext_file)
                   values(rec.vcode, rec.portfolio, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          -- вставка в FCT_SECURITY_ATTR
          insert into ldr_infa_cb.fct_security_attr
             (SELECT distinct rec.vcode,
                     nvl(rec.portfolio, '-1'),
                     TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                     null number_value,
                     null date_value,
                     substr(nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name), 1, 250) string_value,
                     substr(nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name), 1, 550) value,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) ) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                FROM DOBJATCOR_DBT AC
               INNER JOIN DOBJGROUP_DBT GR
                  ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
               inner join dobjattr_dbt at
                  on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                     ac.t_attrid = at.t_attrid)
               WHERE AC.T_OBJECTTYPE = n24
                 and gr.t_type = chr88
                 and gr.t_groupid <> n101
                 and ac.t_object = lpad(to_char(rec.t_bcid), 10, '0')
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) <= in_date
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) >= rec.dt
              UNION ALL
              select distinct rec.vcode,
                     rec.portfolio,
                     code code_securtity_attr,
                     case
                       when type in (0, 1, 2, 3, 4, 25) then
                        noteval
                       else
                        null
                     end number_value,
                     case
                       when type in (9, 10) then
                        noteval
                       else
                        null
                     end date_value,
                     case
                       when type in (7, 12) then
                        substr(noteval, 1, 250)
                       else
                        null
                     end string_value,
                     substr(noteval, 1, 550) value,
                     qb_dwh_utils.DateToChar(t_date) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                from (SELECT to_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                             UPPER(TRIM(NK.T_NAME)) NAME,
                             nk.t_notetype type,
                             case nk.t_notetype
                               when 0 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getInt(nt.t_text), 0)
                               when 1 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getLong(nt.t_text), 0)
                               when 2 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 3 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 4 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 7 then
                                Rsb_Struct.getString(nt.t_text)
                               when 9 then
                                qb_dwh_utils.DateToChar(Rsb_Struct.getDate(nt.t_text))
                               when 10 then
                                qb_dwh_utils.DateTimeToChar(Rsb_Struct.getTime(nt.t_text))
                               when 12 then
                                Rsb_Struct.getChar(nt.t_text)
                               when 25 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getMoney(nt.t_text), 2)
                               else
                                null
                             end noteval,
                             decode(nt.t_date, emptDate, rec.dt, nt.t_date) t_date,
                             nt.t_documentid
                        FROM DNOTETEXT_DBT NT
                       INNER JOIN DNOTEKIND_DBT NK
                          ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND
                             NT.T_NOTEKIND = NK.T_NOTEKIND)
                       WHERE NT.T_OBJECTTYPE  =  n24
                         and nt.t_documentid = lpad(to_char(rec.t_bcid), 10, '0')
                         and decode(nt.t_date, emptDate, rec.dt, nt.t_date) <= in_date
                         and decode(nt.t_date, emptDate, rec.dt, nt.t_date) >= rec.dt)
                     );
commit;                     
          -- вставка в FCT_SECURITY_ATTR_MULTY
          insert into ldr_infa_cb.fct_security_attr_multi
             (SELECT distinct rec.vcode,
                     nvl(rec.portfolio, '-1'),
                     TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                     null number_value,
                     null date_value,
                     substr(nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name), 1, 250) string_value,
                     substr(nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name), 1, 550) value,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate)) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                FROM DOBJATCOR_DBT AC
               INNER JOIN DOBJGROUP_DBT GR
                  ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
               inner join dobjattr_dbt at
                  on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                     ac.t_attrid = at.t_attrid)
               WHERE AC.T_OBJECTTYPE = n24
                 and gr.t_type = chr0
                 and ac.t_object = lpad(to_char(rec.t_bcid), 10, '0')
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) <= in_date
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) >= rec.dt
                     );
commit;                     
          insert into ldr_infa_cb.fct_security_check
              (SELECT distinct rec.vcode,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate)) dt_check,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate)) dt_redefinition,
                     nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) check_result,
                     null check_reason,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate)) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                FROM DOBJATCOR_DBT AC
               INNER JOIN DOBJGROUP_DBT GR
                  ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
               inner join dobjattr_dbt at
                  on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                     ac.t_attrid = at.t_attrid)
               WHERE AC.T_OBJECTTYPE = n24
                 and gr.t_type = chr88
                 and gr.t_groupid = n101
                 and ac.t_object = lpad(to_char(rec.t_bcid), 10, '0')
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) <= in_date
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) >= rec.dt
                 );
commit;                 
       end loop;
    else
      -- Прочие ц/б
      select to_char(fiid) || '#FIN',
             substr(trim(fi.t_name), 1, 250),
             substr(trim(fi.t_definition), 1, 50),
             -- qb_dwh_utils.DateToChar(decode(fi.t_issued, emptDate, firstDate, fi.t_issued)),
             -- 2020-09-02 AS алгоритм определения даты выпуска, когда в открых источниках нет данных
             /* согласован Исаковой Н. */
             case
               when  decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate) <=
                     decode(fi.t_issued, emptDate, maxDate, fi.t_issued)
                and  decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate) <=
                     decode(av.t_begplacementdate, emptDate, maxDate, av.t_begplacementdate) then
                 decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)
               when  decode(fi.t_issued, emptDate, maxDate, fi.t_issued) <=
                     decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate)
                and  decode(fi.t_issued, emptDate, maxDate, fi.t_issued) <=
                     decode(av.t_begplacementdate, emptDate, maxDate, av.t_begplacementdate) then
                 fi.t_issued
               else
                 av.t_begplacementdate
             end date_is,
             qb_dwh_utils.NumberToChar(fi.t_facevalue),
             --case when fi.t_fiid in (220, 1924, 1416, 1536, 2007, 1406, 1722, 1660, 2369, 1596, 1555, 2212, 2148, 5369, 5567, 9556, 7969, 16163, 11860, 11790, 14705) then        -- !!!!!!!! Для эмитентов по этим бумагам  нет выгружаемого кода БИСКВИТ
             --       '-1'
             --     else
                     qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                 qb_dwh_utils.System_IBSO,
                                                 1,
                                                 fi.t_issuer)
             --end
             ,
             pfi.t_iso_number,
             --(select max(tick.t_dealdate) from ddl_tick_dbt tick where tick.t_pfi = fi.t_fiid),
             case
               when  decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate) <=
                     decode(fi.t_issued, emptDate, maxDate, fi.t_issued)
                and  decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate) <=
                     decode(av.t_begplacementdate, emptDate, maxDate, av.t_begplacementdate) then
                 decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)
               when  decode(fi.t_issued, emptDate, maxDate, fi.t_issued) <=
                     decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate)
                and  decode(fi.t_issued, emptDate, maxDate, fi.t_issued) <=
                     decode(av.t_begplacementdate, emptDate, maxDate, av.t_begplacementdate) then
                 fi.t_issued
               else
                 av.t_begplacementdate
             end mindate,
             av.t_lsin,
             av.t_subordinated,
             av.t_nkdbase_kind,
             nvl2(dd.t_avoirkind, '9999#SOFRXXX#'||to_char(dd.t_fi_kind)||'#'||to_char(dd.t_avoirkind),null)
        into vcode, vname, vnames, date_is, vnomin, codeis, ficode, vdt, vregnum, issub, nkdbase, SEC_KIND_CODE
        from dfininstr_dbt fi
       inner join davoiriss_dbt av
          on (fi.t_fiid = av.t_fiid)
        left join dfininstr_dbt pfi
          on (fi.t_facevaluefi = pfi.t_fiid)
        left join davrkinds_dbt dd
          on dd.t_fi_kind = fi.t_fi_kind and dd.t_avoirkind = fi.t_avoirkind            
       where fi.t_fiid = fiid
         and rownum = n1;

         dateis := qb_dwh_utils.DateToChar(date_is);

       procbase := case nkdbase
                         when 0 then
                            '365'
                         when 1 then
                            '30/360'
                         when 2 then
                            '360/0'
                         when 3 then
                            '30/365'
                         when 4 then
                            '366'
                         when 5 then
                            '30/366'
                         when 6 then
                            'Act/по_купонным_периодам'
                         when 7 then
                            'Act/365L'
                         when 8 then
                            'Act/364'
                         when 9 then
                            '30E/360'
                         when 10 then
                            '30/360_ISDA'
                         when 11 then
                            'Act/Act_ICMA'
                         else
                           '-1'
                       end;
      procbase := '9999#SOFRXXX#' || procbase;

      -- Вставка в DET_FINSTR
      Insert into ldr_infa_cb.det_finstr(finstr_code, finstr_name, finstr_name_s,typefinstr, dt, rec_status,sysmoment, ext_file)
             values (vcode, vname, vnames, 2, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      -- Вставка в f
      stype := case when InConst(cSECKIND_STOCK, avoirkind) then           -- акция
                  1
               when InConst(cSECKIND_BILL, avoirkind)  then                -- вексель
                  3
               when InConst(cSECKIND_UIT, avoirkind)  then                 -- ПИФ
                  5
               when InConst(cSECKIND_BOND, avoirkind) then                 -- облигация
                  2
               when InConst(cSECKIND_RECEIPT, avoirkind) then              -- депозитарная расписка
                  6
               else
                  null
               end;
      Insert into ldr_infa_cb.det_security(typesecurity, code, date_issue, nominal, regnum, finstrsecurity_finstr_code, issuer_code, underwriter_code, finstrcurnom_finstr_code,
                  procbase, dt, SECURITY_KIND_CODE, rec_status, sysmoment, ext_file)
             values (stype, vcode, dateis, vnomin, vregnum, vcode, codeis, '-1', ficode, procbase, qb_dwh_utils.DateToChar(vdt), SEC_KIND_CODE, dwhRecStatus, dwhSysMoment, dwhEXT_FILE/*, vregnum*/);
commit;             
      if InConst(cSECKIND_STOCK, avoirkind) then
        -- Вставка в DET_STOCK
        for rec in (select to_char(fi.t_avoirkind) typestock,
                           to_char(fi.t_fiid) || '#FIN' backofficecode,
                           av.t_isin isinoldcode,
                           null isinnewcode,
                           av.t_lsin regnum,
                           case when fi.t_settlement_code = 1 then
                                  '1'
                                when fi.t_settlement_code = 0 then
                                  '2'
                                else
                                  '-1'
                           end issueform,
                           av.t_lsin secissue,
                           qb_dwh_utils.DateToChar(decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)) secissueregdate,
                           to_char(av.t_qty) secissuevolume
                      from dfininstr_dbt fi
                     inner join davoiriss_dbt av
                        on (fi.t_fiid = av.t_fiid)
                     where fi.t_fiid = fiid)
        loop
          insert into ldr_infa_cb.det_stock(typestock, backofficecode, isinoldcode, isinnewcode, regnum, issueform, secissue, secissueregdate, secissuevolume,
                                        security_code, dt, rec_status, sysmoment, ext_file)
                 values (rec.typestock, rec.backofficecode, rec.isinoldcode, rec.isinnewcode, rec.regnum, rec.issueform, rec.secissue, rec.secissueregdate, rec.secissuevolume,
                         vcode, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                         
        end loop;
      elsif InConst(cSECKIND_BOND, avoirkind) then
        -- Вставка в DET_BOND
        for rec in (select case when fi.t_avoirkind = 21 then
                                  '6'
                                when fi.t_avoirkind = 24 then
                                  '3'
                                when fi.t_avoirkind = 25 then
                                  '6'
                                when fi.t_avoirkind = 28 then
                                  '1'
                                when fi.t_avoirkind = 38 then
                                  '6'
                                when fi.t_avoirkind = 40 then
                                  '4'
                                when fi.t_avoirkind = 42 then
                                  '5'
                                when fi.t_avoirkind = 43 then
                                  '8'
                                when fi.t_avoirkind = 50 then
                                  '6'
                                else
                                  '0'
                           end typebond,
                           to_char(fi.t_fiid) || '#FIN' backofficecode,
                           av.t_isin isinoldcode,
                           null isinnewcode,
                           av.t_lsin regnum,
                           case when fi.t_settlement_code = 1 then
                                  '1'
                                when fi.t_settlement_code = 0 then
                                  '2'
                                else
                                  '0'
                           end issueform,
                           av.t_lsin secissue,
                           qb_dwh_utils.DateToChar(decode(av.t_begplacementdate, emptDate, firstDate, av.t_begplacementdate)) secissueregdate,
                           to_char(av.t_qty) secissuevolume,
                           qb_dwh_utils.DateToChar(decode(fi.t_drawingdate, emptDate, firstDate, fi.t_drawingdate)) maturitydate
                      from dfininstr_dbt fi
                     inner join davoiriss_dbt av
                        on (fi.t_fiid = av.t_fiid)
                     where fi.t_fiid = fiid)
        loop
          insert into ldr_infa_cb.det_bond(typebond, backofficecode, isinoldcode, isinnewcode, regnum, issueform, secissue, secissueregdate, secissuevolume, maturitydate,
                                        security_code, dt, rec_status, sysmoment, ext_file)
                 values (rec.typebond, rec.backofficecode, rec.isinoldcode, rec.isinnewcode, rec.regnum, rec.issueform, rec.secissue, rec.secissueregdate, rec.secissuevolume, rec.maturitydate,
                         vcode, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                         
        end loop;
      elsif InConst(cSECKIND_UIT, avoirkind) then
        -- Вставка в DET_UIT
        for rec in (select case when inv.t_type = 1 then
                                  '1'
                                when inv.t_type = 2 then
                                  '2'
                                when inv.t_type = 0 then
                                  '3'
                                when inv.t_type = 3 then
                                  '4'
                                else
                                  '0'
                           end typeuit,
                           to_char(fi.t_fiid) || '#FIN' backofficecode,
                           av.t_lsin regnum,
                           qb_dwh_utils.DateToChar(decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)) regdate,
                           null maturitydate,
                           qb_dwh_utils.DateToChar(decode(inv.t_formperiodstart, emptDate, to_date('01011980','ddmmyyyy'), inv.t_formperiodstart)) begindate,
                           qb_dwh_utils.DateToChar(decode(inv.t_formperiodend, emptDate, to_date('01013001','ddmmyyyy'), inv.t_formperiodend)) enddate
                      from dfininstr_dbt fi
                     inner join davoiriss_dbt av
                        on (fi.t_fiid = av.t_fiid)
                     inner join davrinvst_dbt inv
                        on (fi.t_fiid = inv.t_fiid)
                     where fi.t_fiid = fiid)
        loop
          insert into ldr_infa_cb.det_uit(typeuit, backofficecode, regnum,security_code, regdate, maturitydate, begindate, enddate, dt, rec_status, sysmoment, ext_file)
                 values (rec.typeuit, rec.backofficecode, rec.regnum, vcode, rec.regdate, rec.maturitydate, rec.begindate, rec.enddate, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        end loop;
      elsif InConst(cSECKIND_RECEIPT, avoirkind) then
        -- Вставка в DET_RECEIPT
        for rec in (select case when fi.t_avoirkind = 45 then
                                  1
                                when fi.t_avoirkind = 46 then
                                  2
                                when fi.t_avoirkind = 47 then
                                  3
                                when fi.t_avoirkind = 49 then
                                  4
                           end typereceipt,
                           to_char(fi.t_fiid) || '#FIN' backofficecode,
                           av.t_isin isincode,
                           av.t_lsin regnum,
                           (select case
                                     when t_fi_kind = 2 then
                                       to_number(t_fiid) || '#FIN'
                                     when t_fi_kind = 1 then
                                       t_iso_number
                                     else
                                       to_number(t_fiid) || '-1'
                                   end
                             from dfininstr_dbt where t_fiid = fi.t_parentfi) base_finstr_code,
                           qb_dwh_utils.DateToChar(decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)) secissueregdate,
                           to_char(av.t_qty) secissuevolume,
                           to_char(av.t_numbasefi) basesecurityvolume
                      from dfininstr_dbt fi
                     inner join davoiriss_dbt av
                        on (fi.t_fiid = av.t_fiid)
                     left join davoiriss_dbt base_av
                        on (fi.t_parentfi = base_av.t_fiid)
                     where fi.t_fiid = fiid)
        loop
          insert into ldr_infa_cb.det_receipt(typereceipt, backofficecode, isincode, regnum, base_finstr_code, secissueregdate, secissuevolume, basesecurityvolume,security_code, dt, rec_status, sysmoment, ext_file)
                 values (rec.typereceipt, rec.backofficecode, rec.isincode, rec.regnum, rec.base_finstr_code, rec.secissueregdate, rec.secissuevolume, rec.basesecurityvolume, vcode, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        end loop;
      end if;
      -- Вставка в FCT_SECURITY_QUOT
      select case when (count(1) > 0) then
                    '1'
                  else
                    '0'
             end
        into quot
        from dobjatcor_dbt ac
       where ac.t_objecttype = 12
         and ac.t_groupid = 18
         and in_date between ac.t_validfromdate and ac.t_validtodate
         and ac.t_attrid = 1
         and to_number(ac.t_object) = fiid;
      insert into ldr_infa_cb.fct_security_quot(is_reval,security_code, dt, rec_status,sysmoment,ext_file)
             values(quot, vcode, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      -- Вставка в FCT_SECNOMINAL
      for rec in (select distinct *
                    from (
                  select nh.t_fiid fiid,
                         decode(nh.T_BEGDATE, emptDate, firstDate, nh.T_BEGDATE) chdate,
                         nh.T_FACEVALUE nominal
                    from dv_fi_facevalue_hist nh
                    where nh.t_fiid = fiid
                  union all
                  select wr.t_fiid,
                         decode(wr.t_drawingdate, emptDate, firstDate, wr.t_drawingdate),
                         rsb_fiinstr.FI_GetNominalOnDate(wr.t_fiid, wr.t_drawingdate)
                    from dfiwarnts_dbt wr
                   where t_ispartial = chr88
                     and wr.t_fiid = fiid)
                   order by chdate)
      loop
        insert into ldr_infa_cb.fct_secnominal(nominal,security_code, dt, rec_status, sysmoment,ext_file)
               values (qb_dwh_utils.NumberToChar(round(rec.nominal, 9), 9), vcode, qb_dwh_utils.DateToChar(rec.chdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      end loop;
      -- Вставка в ASS_ACCOUNTCECURITY
      for acrec in (select distinct '0000#IBSOXXX#' || case
                                                          when (acc.t_userfield4 is null) or
                                                              (acc.t_userfield4 = chr(0)) or
                                                              (acc.t_userfield4 = chr(1)) or
                                                              (acc.t_userfield4 like '0x%') then
                                                            acc.t_account
                                                          else
                                                            acc.t_userfield4
                                                        end account_code,
                                    /* '0000#SOFR#' || */ cat.t_code ||
                                          case when catacc.t_catnum in (235, 464) and templ.t_value4 = 1  THEN
                                                '_П'
                                               when catacc.t_catnum in (235, 464) and templ.t_value4 = 2  THEN
                                                '_Д'
                                               when catacc.t_catnum = 1492 and templ.t_value1 >= 0 THEN
                                                 (select '#' || t_code from dllvalues_dbt where t_list = n3503 and t_element = templ.t_value1)
                                               else
                                                 null
                                          end roleaccount_deal_code,
                                    case when catacc.t_activatedate < vdt then
                                           vdt
                                         else
                                           catacc.t_activatedate
                                    end dt
                                    --decode(catacc.t_activatedate, emptDate, vdt, catacc.t_activatedate) dt
                      from dmcaccdoc_dbt catacc
                     inner join daccount_dbt acc
                        on (catacc.t_chapter = acc.t_chapter and catacc.t_account = acc.t_account and catacc.t_currency = acc.t_code_currency)
                     inner join dmccateg_dbt cat
                        on (catacc.t_catid = cat.t_id)
                     left join dmctempl_dbt templ
                       on (catacc.t_catid = templ.t_catid and catacc.t_templnum = templ.t_number)
                     where catacc.t_fiid = fiid
                       and catacc.t_chapter in (select v.value
                                                  from qb_dwh_const4exp c
                                                 inner join qb_dwh_const4exp_val v
                                                    on (c.id = v.id)
                                                 where c.name = cACC_CHAPTERS)
                       and exists (select 1 from dfininstr_dbt fi where fi.t_fiid = catacc.t_currency and fi.t_fi_kind = n1)
                       and catacc.t_activatedate <
                           decode(catacc.t_disablingdate,
                                  emptDate,
                                  maxDate,
                                  catacc.t_disablingdate)
                       and exists (select 1 from daccount_dbt a where a.t_chapter = catacc.t_chapter and a.t_account = catacc.t_account and a.t_code_currency = catacc.t_currency)
                     order by roleaccount_deal_code, dt)
      loop
        insert into ldr_infa_cb.ass_accountsecurity(account_code,security_code, roleaccount_deal_code, dt, rec_status, sysmoment, ext_file)
               values (acrec.account_code, vcode, acrec.roleaccount_deal_code, qb_dwh_utils.DateToChar(acrec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      end loop;

    insert into ldr_infa_cb.fct_security_attr
      SELECT distinct to_char(t_fiid) || '#FIN',
             nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio
                           where security_code = vcode
                             and to_date(dt, 'dd-mm-yyyy') <= decode(t_dateredemption, emptDate, vdt, t_dateredemption))
                   where rnk = n1), '-1') portf,
             'DATE_OFFER',
             null,
             qb_dwh_utils.DateToChar(decode(t_dateredemption, emptDate, vdt, t_dateredemption)),
             null,
             qb_dwh_utils.DateToChar(decode(t_dateredemption, emptDate, vdt, t_dateredemption)),
             qb_dwh_utils.DateToChar(vdt),
             dwhRecStatus,
             dwhSysMoment,
             dwhEXT_FILE
        FROM doffers_dbt
       WHERE t_fiid = fiid;
commit;


      -- вставка в FCT_SECURITY_ATTR
      insert into ldr_infa_cb.fct_security_attr
          (SELECT distinct vcode,
                  nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio
                           where security_code = vcode
                             and to_date(dt, 'dd-mm-yyyy') <= decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate))
                   where rnk = n1), '-1') portf,
                 TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                 null number_value,
                 null date_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) string_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) value,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
           INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           WHERE AC.T_OBJECTTYPE = n12
             and gr.t_type = chr88
             and gr.t_groupid <> n62
             and ac.t_object = lpad(to_char(fiid), 10, '0')
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) <= in_date
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) >= vdt
          UNION ALL --ТЗ, табл.2, п.3 Ипотечное покрытие
          SELECT distinct vcode,
                  nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio
                           where security_code = vcode
                             and to_date(dt, 'dd-mm-yyyy') <= decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate))
                   where rnk = n1), '-1') portf,
                 TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                 null number_value,
                 null date_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) string_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) value,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
           INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           WHERE AC.T_OBJECTTYPE = n12
             and gr.t_type = chr88
             and gr.t_groupid = n101
             and ac.t_object = lpad(to_char(fiid), 10, '0')
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) <= in_date
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) >= vdt
             and BIQ_7477_78 = 1
          UNION ALL
          select distinct vcode,
                 nvl((select sec_portfolio_code
                   from (select sec_portfolio_code,
                                row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                           from ldr_infa_cb.ass_sec_portfolio
                          where security_code = vcode
                            and to_date(dt, 'dd-mm-yyyy') <= t_date )
                  where rnk = n1), '-1') portf,
                 code code_securtity_attr,
                 case
                   when type in (0, 1, 2, 3, 4, 25) then
                    noteval
                   else
                    null
                 end number_value,
                 case
                   when type in (9, 10) then
                    noteval
                   else
                    null
                 end date_value,
                 case
                   when type in (7, 12) then
                    substr(noteval, 1, 250)
                   else
                    null
                 end string_value,
                 substr(noteval, 1, 250) value,
                 qb_dwh_utils.DateToChar(t_date) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from (SELECT to_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                         UPPER(TRIM(NK.T_NAME)) NAME,
                         nk.t_notetype type,
                         case nk.t_notetype
                           when 0 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getInt(nt.t_text), 0)
                           when 1 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getLong(nt.t_text), 0)
                           when 2 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                           when 3 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                           when 4 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                           when 7 then
                            Rsb_Struct.getString(nt.t_text)
                           when 9 then
                            qb_dwh_utils.DateToChar(Rsb_Struct.getDate(nt.t_text))
                           when 10 then
                            qb_dwh_utils.DateTimeToChar(Rsb_Struct.getTime(nt.t_text))
                           when 12 then
                                Rsb_Struct.getChar(nt.t_text)
                           when 25 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getMoney(nt.t_text), 2)
                           else
                            null
                         end noteval,
                         decode(nt.t_date, emptDate, vdt, nt.t_date) t_date,
                         nt.t_documentid
                    FROM DNOTETEXT_DBT NT
                   INNER JOIN DNOTEKIND_DBT NK
                      ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND
                         NT.T_NOTEKIND = NK.T_NOTEKIND)
                   WHERE NT.T_OBJECTTYPE  =  n12
                     and nt.t_documentid = lpad(to_char(fiid), 10, '0')
                     and decode(nt.t_date, emptDate, vdt, nt.t_date) <= in_date
                     and decode(nt.t_date, emptDate, vdt, nt.t_date) >= vdt)
                 );
commit;                 
      -- вставка в FCT_SECURITY_ATTR_MULTI
      insert into ldr_infa_cb.fct_security_attr_multi
          (SELECT distinct vcode,
                 nvl((select sec_portfolio_code
                   from (select sec_portfolio_code,
                                row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                           from ldr_infa_cb.ass_sec_portfolio
                          where security_code = vcode
                            and to_date(dt, 'dd-mm-yyyy') <= decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate))
                  where rnk = n1), '-1') portf,
                 TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                 null number_value,
                 null date_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) string_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) value,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
           INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           WHERE AC.T_OBJECTTYPE = n12
             and gr.t_type = chr0
             and ac.t_object = lpad(to_char(fiid), 10, '0')
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) <= in_date
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) >= vdt
                 );
commit;                 
      insert into ldr_infa_cb.fct_security_check
          (SELECT distinct vcode,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt_check,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt_redefinition,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) check_result,
                 null check_reason,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
           INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           WHERE AC.T_OBJECTTYPE = n12
             and gr.t_type = chr88
             and gr.t_groupid = n62
             and ac.t_object = lpad(to_char(fiid), 10, '0')
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) <= in_date
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) >= vdt
             );
commit;             
       -- вставка в FCT_SECREPAYSCHEDULE
       for rec in ( select t.fiid,
                           t.code,
                           t.typeschedule,
                           t.typerepaysec,
                           t.begindate_tbl,
                           t.begindate_calc,
                           case
                             when t.begindate_tbl > begindate_calc then
                              t.begindate_tbl
                             else
                              t.begindate_calc
                           end begindate,
                           t.enddate,
                           t.proc_rate,
                           t.proc_sum
                      from (select wr.t_fiid fiid,
                                   to_char(wr.t_id) code,
                                   case
                                     when wr.t_ispartial = chr(0) then
                                      '1'
                                     when wr.t_ispartial = chr(88) then
                                      '2'
                                   end typeschedule,
                                   case
                                     when wr.t_ispartial = chr(0) then
                                      '1'
                                     when wr.t_ispartial = chr(88) then
                                      '2'
                                   end typerepaysec,
                                   wr.t_firstdate begindate_tbl,
                                   lag(decode(wr.t_drawingdate,
                                              emptDate,
                                              vdt,
                                              wr.t_drawingdate) + 1,
                                       1, vdt) over(partition by wr.t_fiid, wr.t_ispartial order by wr.t_fiid, wr.t_ispartial, decode(wr.t_drawingdate, emptDate, vdt, wr.t_drawingdate)) begindate_calc,
                                   decode(wr.t_drawingdate, emptDate, vdt, wr.t_drawingdate) enddate,
                                   case
                                     when wr.t_incomerate = 0 then
                                       nvl((select h.t_incomerate
                                              from dflrhist_dbt h
                                             where h.t_fiwarntid = wr.t_id and rownum = 1), 0)
                                     else
                                       wr.t_incomerate
                                   end proc_rate,
                                   wr.t_incomevolume proc_sum
                              from dfiwarnts_dbt wr
                             inner join davoiriss_dbt av
                                on (wr.t_fiid = av.t_fiid)
                             where wr.t_fiid = fiid
                               and wr.t_drawingdate > emptDate
                               and  (round(wr.t_incomerate, 3) <> n0 or round(wr.t_incomevolume, 3) <> n0)) t)
       loop
         if (rec.begindate <= rec.enddate) then
           insert into ldr_infa_cb.fct_secrepayschedule(code, typeschedule, typerepaysec, begindate, enddate, proc_rate, proc_sum,security_code, dt, rec_status, sysmoment, ext_file)
           values (rec.code, rec.typeschedule, rec.typerepaysec, qb_dwh_utils.DateToChar(rec.begindate) , qb_dwh_utils.DateToChar(rec.enddate), qb_dwh_utils.NumberToChar(round(rec.proc_rate, 3), 3), qb_dwh_utils.NumberToChar(round(rec.proc_sum, 3), 3),
                   vcode,  dateis, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
         end if;
       end loop;
       insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
              values(vcode,
                     nvl((select sec_portfolio_code
                            from (select sec_portfolio_code,
                                         row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                                    from ldr_infa_cb.ass_sec_portfolio
                                   where security_code = vcode
                                     and to_date(dt, 'dd-mm-yyyy') <= in_date )
                           where rnk = n1), '-1'),
                     'IS_SUBORDINATED',
                     null,                               -- number_value
                     null,                               -- date_value
                     decode(issub, chr88, 'ДА', 'НЕТ'),  -- string_value
                     decode(issub, chr88, 'ДА', 'НЕТ'),  -- value
                     qb_dwh_utils.DateToChar(vdt),
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE);
commit;                     
    end if; -- Прочие ц/б
    -- Выгрузка в FCT_FINSTR_RATE
    for rec in (select * from (
                select fir.t_iso_number FINSTR_DENUMERATOR_FINSTR_CODE,
                       qb_dwh_utils.NumberToChar(rh.t_rate/power(10, rh.t_point), rh.t_point) FINSTR_RATE,
                       to_char(rh.t_scale) FINSTR_SCALE,
                       rd.t_type TYPE_FINSTR_RATE_TYPE_RATE_COD,
                       qb_dwh_utils.DateToChar(decode(rh.t_sincedate, emptDate, firstDate, rh.t_sincedate)) dt,
                       rh.t_sincedate
                  from dfininstr_dbt fi
                 inner join dratedef_dbt rd
                    on (fi.t_fiid = rd.t_otherfi)
                 inner join dfininstr_dbt fir
                    on (fir.t_fiid = rd.t_fiid)
                 inner join dratehist_dbt rh
                   on (rd.t_rateid = rh.t_rateid)
                 where fi.t_fiid = fiid
               union all
                select fir.t_iso_number FINSTR_NUMERATOR_FINSTR_CODE,
                       qb_dwh_utils.NumberToChar(rd.t_rate/power(10, rd.t_point), rd.t_point) FINSTR_RATE,
                       to_char(rd.t_scale) FINSTR_SCALE,
                       rd.t_type TYPE_FINSTR_RATE_TYPE_RATE_COD,
                       qb_dwh_utils.DateToChar(decode(rd.t_sincedate, emptDate, firstDate, rd.t_sincedate)) dt,
                       rd.t_sincedate
                  from dfininstr_dbt fi
                 inner join dratedef_dbt rd
                    on (fi.t_fiid = rd.t_otherfi)
                 inner join dfininstr_dbt fir
                    on (fir.t_fiid = rd.t_fiid)
                 where fi.t_fiid = fiid
               order by TYPE_FINSTR_RATE_TYPE_RATE_COD, t_sincedate)
               where to_date(dt,'dd-mm-yyyy') <= in_date)
    loop
          insert into ldr_infa_cb.fct_finstr_rate(finstr_numerator_finstr_code, finstr_denumerator_finstr_code, finstr_rate, finstr_scale, type_finstr_rate_type_rate_cod, dt,rec_status,sysmoment, ext_file)
                 values (vcode, rec.finstr_denumerator_finstr_code, rec.finstr_rate, rec.finstr_scale, rec.type_finstr_rate_type_rate_cod, rec.dt, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
    end loop;
    -- Вставка в FCT_SECEXCHANGE
    for rec in (select distinct ob.t_code CODE,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                    qb_dwh_utils.System_IBSO,
                                                     1,
                                                     2) EXCHANGE_CODE,
                       qb_dwh_utils.DateToChar(case when ob.t_bankdate < vdt then
                                                    vdt
                                               else
                                                    ob.t_bankdate
                                               end) dt
                 from dobjcode_dbt ob
                 where ob.t_objecttype = n9 and ob.t_codekind = n11 and ob.t_objectid = fiid /*and ob.t_state = n0*/)
    loop

      insert into ldr_infa_cb.fct_secexchange(code,security_code, exchange_code, dt, rec_status, sysmoment, ext_file)
             values(rec.code, vcode, rec.exchange_code, rec.dt, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;
    end loop;
  end;

  procedure export_Secur(fiid         in dfininstr_dbt.t_fiid%type,
                         avoirkind    in dfininstr_dbt.t_avoirkind%type,
                         bill_kind    in pls_integer,
                         in_date      in date,
                         dwhRecStatus in varchar2,
                         dwhDT        in varchar2,
                         dwhSysMoment in varchar2,
                         dwhEXT_FILE  in varchar2) is
   vcode  varchar2(30);
   vname  varchar2(250);
   vnames varchar2(50);
   stype  varchar2(1);
   dateis varchar2(10);
   vnomin varchar2(40);
   codeis varchar2(250);
   ficode varchar2(30);
   date_is date;
   vdt    date;
   quot   varchar2(1);
   prevst varchar2(2);
   vregnum varchar2(50);
   bank_ptid ddp_dep_dbt.t_partyid%type;
   deal_code varchar2(100);
   sum_buy varchar2(26);
   issub davoiriss_dbt.t_subordinated%type;
   nkdbase davoiriss_dbt.t_nkdbase_kind%type;
   procbase varchar2(100);
  begin
    select t_partyid
      into bank_ptid
      from ddp_dep_dbt dp
     where dp.t_parentcode = n0;
    if InConst(cSECKIND_BILL, avoirkind) then
      -- Векселя
      for rec in (select bn.t_bcid,
                         to_char(bn.t_bcid) || '#BNR' vcode,
                         substr('Вексель ' || trim(bn.t_issuername) || ' серия: ' || trim(bn.t_bcseries) || ' номер: ' || trim(bn.t_bcnumber), 1, 250) vname,
                         substr('Вексель серия: ' || trim(bn.t_bcseries) || ' номер: ' || trim(bn.t_bcnumber), 1, 50) vnames,
                         qb_dwh_utils.DateToChar(decode(bn.t_issuedate, emptDate, firstDate, bn.t_issuedate)) dateis,
                         qb_dwh_utils.NumberToChar(round(leg.t_principal, 12), 12) vnomin,
                         qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     bn.t_issuer) codeis,
                         pfi.t_iso_number ficode,
                         case when bn.t_issuekind = 0 then
                                '1'
                              when bn.t_issuekind = 1 then
                                '2'
                              else
                                null
                         end typebill,
                         case when leg.t_formula = 1 then
                                '1'
                              when leg.t_formula = 0 then
                                '2'
                              else
                                null
                         end typeprofit,
                         case when bn.t_bctermformula = 10 then
                                '1'
                              when bn.t_bctermformula = 20 and leg.t_maturity = emptDate and leg.t_expiry = emptDate then
                                '2'
                              when bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry = emptDate then
                                '5'
                              when bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry <> emptDate then
                                '6'
                              when bn.t_bctermformula = 20 and leg.t_maturity = emptDate and leg.t_expiry <> emptDate then
                                '7'
                              else
                                null
                         end typerepay,
                         bn.t_bcnumber numberbill,
                         bn.t_bcseries seriesbill,
                         null numberform,
                         case when (bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry = emptDate) or
                                   (bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry <> emptDate) then
                                qb_dwh_utils.DateToChar(leg.t_maturity)
                              else
                                null
                         end lowerdate,
                         case when (bn.t_bctermformula = 20 and leg.t_maturity <> emptDate and leg.t_expiry <> emptDate) or
                                   (bn.t_bctermformula = 20 and leg.t_maturity = emptDate and leg.t_expiry <> emptDate) then
                                qb_dwh_utils.DateToChar(leg.t_expiry)
                              else
                                null
                         end upperdate,
                         case when leg.t_maturity <> emptDate then
                                qb_dwh_utils.DateToChar(decode(leg.t_maturity, emptDate, firstDate,leg.t_maturity))
                              else
                                null
                         end maturitydate,
                         null maturitydelay,
                         case when leg.t_formula = 1 then
                                qb_dwh_utils.DateToChar(decode(leg.t_expiry, emptDate, firstDate,leg.t_expiry))
                              else
                                null
                         end endpersentdate,
                         case when leg.t_formula = 1 then
                           qb_dwh_utils.NumberToChar(leg.t_price/power(10, leg.t_point), 3)--!!!!!!!!!!!!!! dfiwarnts_dbt.t_oncallrate
                         else
                           null
                         end proc_rate,
                         round(bnin.t_perc, 9) discount,
                         --Case when (bn.t_bcid in (2449, 2454, 2457, 2458, 2459, 2462, 2463, 2512, 2513, 2514, 2515, 2516, 2517)) then
                         --  '-1'
                         --else
                           qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                         qb_dwh_utils.System_IBSO,
                                                         1,
                                                         decode(bn.t_holder, -1, bank_ptid, bn.t_holder))
                         --end 
                         remitent_subject_code,
                         '0' accepted,
                         '0' aval,
                         null typeclause,
                         bn.t_issueplace issue_place,
                         null repayment_place,
                         case when decode(bn.t_registrationdate,emptDate,maxDate, bn.t_registrationdate) < decode(bn.t_issuedate,emptDate,maxDate, bn.t_issuedate) then
                                 decode(bn.t_registrationdate,emptDate,firstDate, bn.t_registrationdate)
                              else
                                decode(bn.t_issuedate,emptDate,firstDate, bn.t_issuedate)
                         end dt,
                         case when bn.t_portfolioid = 30 then
                                '1'
                              when bn.t_portfolioid = 31 then
                                '2'
                              when bn.t_portfolioid = 32 then
                                '3'
                              else
                                '-1'
                          end portfolio,
                          '-1' deal_code,
                          case when leg.t_typepercent = 0 then
                            '1' -- Фиксированная ставка
                          else
                            '2'
                          end type_proc_rate
                    from dvsbanner_dbt bn
                    left join ddl_leg_dbt leg
                      on (bn.t_bcid = leg.t_dealid and leg.t_legid = n0 and leg.t_legkind = n1)
                    left join dfininstr_dbt pfi
                      on (leg.t_pfi = pfi.t_fiid)
                    left join dpartcode_dbt pc
                      on pc.t_partyid = bn.t_issuer and pc.t_codekind = n101
                    left join dpartcode_dbt pc1
                      on pc1.t_partyid = bn.t_holder and pc1.t_codekind = n101
                    left join dvsincome_dbt bnin
                      on (bn.t_bcid = bnin.t_bcid and bnin.t_incometype = n9)
                   where bn.t_fiid = fiid)
       loop
          -- Вставка в DET_FINSTR
          Insert into ldr_infa_cb.det_finstr(finstr_code, finstr_name, finstr_name_s, typefinstr, dt, rec_status,sysmoment, ext_file)
                 values (rec.vcode, rec.vname, rec.vnames, 2, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          -- Вставка в DET_SECURITY
          stype := case when InConst(cSECKIND_STOCK, avoirkind) then           -- акция
                      1
                   when InConst(cSECKIND_BILL, avoirkind)  then                -- вексель
                      3
                   when InConst(cSECKIND_UIT, avoirkind)  then                 -- ПИФ
                      5
                   when InConst(cSECKIND_BOND, avoirkind) then                 -- облигация
                      2
                   when InConst(cSECKIND_RECEIPT, avoirkind) then              -- депозитарная расписка
                      6
                   else
                      null
                   end;
          /* Все ошибки аккумулируются при загрузке
          if (stype is null) then
            Raise_Application_Error(-20001, 'Ошибка при добавлении записи в DET_SECURITY. Не удалось определить тип ценной бумаги!');
          end if;
          */
          Insert into ldr_infa_cb.det_security(typesecurity, code, date_issue, nominal, regnum, finstrsecurity_finstr_code, issuer_code, underwriter_code, finstrcurnom_finstr_code,
                      procbase, dt, rec_status, sysmoment, ext_file)
                 values (stype, rec.vcode, rec.dateis, rec.vnomin, null, rec.vcode, rec.codeis, '-1', rec.ficode, '9999#SOFRXXX#1', qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          -- Вставка в DET_BILL
          insert into ldr_infa_cb.det_bill(typebill, typeprofit, typerepay, numberbill, seriesbill, numberform, lowerdate, upperdate, maturitydate, maturitydelay,
                      endpersentdate,security_code, proc_rate, discount, repay_subject_code, remitent_subject_code, accepted, aval, typeclause, issue_place, repayment_place, type_proc_rate, dt, rec_status,
                      sysmoment, ext_file)
                 values (rec.typebill, rec.typeprofit, rec.typerepay, rec.numberbill, rec.seriesbill, rec.numberform, rec.lowerdate, rec.upperdate, rec.maturitydate, rec.maturitydelay,
                      rec.endpersentdate, rec.vcode, rec.proc_rate, qb_dwh_utils.NumberToChar(rec.discount, 9), rec.codeis, rec.remitent_subject_code, rec.accepted, rec.aval, rec.typeclause, rec.issue_place, rec.repayment_place, rec.type_proc_rate, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                      
          -- Вставка в FCT_BILL_STATE
          prevst := '-1';
          if    (bill_kind = 1) then -- История статусов по собственным векселям
            for bstate_rec in (select bill_state, chdate,
                                      bcid,
                                      (select '0000#IBSOXXX#' || uf4
                                         from (select t_docid,  catacc.t_account,
                                                                --cacc.t_userfield4 uf4,
                                                                case
                                                                  when (cacc.t_accountid is null) then
                                                                    catacc.t_account
                                                                  when (cacc.t_userfield4 is null) or
                                                                      (cacc.t_userfield4 = chr(0)) or
                                                                      (cacc.t_userfield4 = chr(1)) or
                                                                      (cacc.t_userfield4 like '0x%') then
                                                                    cacc.t_account
                                                                  else
                                                                    cacc.t_userfield4
                                                                end uf4,
                                                                row_number() over (order by t_activatedate desc) as rnk
                                                                                from DMCACCDOC_DBT catacc
                                                                               left join daccount_dbt cacc
                                                                                  on (catacc.t_chapter = cacc.t_chapter and catacc.t_account = cacc.t_account and catacc.t_currency = cacc.t_code_currency)
                                                                               where t_dockind = n164 and catacc.t_docid = bcid
                                                                                 and catacc.t_catnum = n450
                                                                                 and catacc.t_activatedate <= chdate) where rnk = n1
                                                                                 ) acc,
                                     row_number() over (partition by bcid order by chdate desc) as rnk_last_state
                                from (select id,
                                             max(id) over(partition by chdate) lastchgindate,
                                             chdate,
                                             status,
                                             state,
                                             case
                                               when status = 25 then
                                                '10'
                                               when status in (5, 30) then
                                                '4'
                                               when status = 40 then
                                                '5'
                                               when status = 50 then
                                                '3'
                                               when instr(state, 'T') > 0 then
                                                '6'
                                               when instr(state, 'Ф') > 0 then
                                                '7'
                                               else
                                                '1'
                                             end bill_state,
                                             bcid
                                        from (select T_ID           id,
                                                     sh.t_bcid bcid,
                                                     T_CHANGEDATE   chdate,
                                                     T_NEWABCSTATUS status,
                                                     T_NEWBCSTATE   state
                                                from dvsbnrbck_dbt sh
                                               where sh.t_bcid = rec.t_bcid
                                                 and sh.t_bcstatus = chr88
                                                 AND sh.t_abcstatus = chr0
                                              union all
                                              select T_ID,
                                                     sh.t_bcid bcid,
                                                     T_CHANGEDATE,
                                                     (select t_newabcstatus
                                                        from dvsbnrbck_dbt
                                                       where t_id = (select max(t_id)
                                                                       from dvsbnrbck_dbt
                                                                      where t_bcid = sh.t_bcid
                                                                        and t_bcstatus = chr88
                                                                        and t_id < sh.t_id)) T_NEWABCSTATUS,
                                                     T_NEWBCSTATE
                                                from dvsbnrbck_dbt sh
                                               where sh.t_bcid = rec.t_bcid
                                                 and sh.t_bcstatus = chr0
                                                 AND sh.t_abcstatus = chr0
                                               order by id))
                               where id = lastchgindate)
            loop
              if (bstate_rec.acc is not null) then
                Insert into ldr_infa_cb.fct_bill_state(Bill_State,bill_code, Subject_Code, Dt, Rec_Status, Sysmoment, Ext_File)
                       values(bstate_rec.bill_state, rec.vcode, rec.remitent_subject_code, qb_dwh_utils.DateToChar(bstate_rec.chdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                       
                if (bstate_rec.bill_state <> '4') and (bstate_rec.rnk_last_state = 1) then
                  begin
                    select deal_code, sum_buy
                      into deal_code, sum_buy
                      from (select to_char(ord.t_contractid) || '#ORD' deal_code,
                                   to_char(bn.t_bcid) || '#BNR',
                                   ord.t_signdate dt,
                                   qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                                   row_number() over(partition by bn.t_bcid order by ord.t_signdate desc) rnk_last_deal
                              from dvsbanner_dbt bn
                             inner join ddl_leg_dbt leg
                                on (bn.t_bcid = leg.t_dealid)
                             inner join dvsordlnk_dbt lnk
                                on (bn.t_bcid = lnk.t_bcid)
                             inner join ddl_order_dbt ord
                                on (lnk.t_contractid = ord.t_contractid and
                                   lnk.t_dockind = ord.t_dockind)
                             inner join ddp_dep_dbt dp
                                on (bn.t_issuer = dp.t_partyid)
                             where bn.t_bcid = rec.t_bcid
                               and leg.t_legid = n0
                               and leg.t_legkind = n1
                               and ord.t_signdate <= in_date
                               and ord.t_dockind = n109 -- выпуск
                            )
                      where rnk_last_deal = n1;
                    exception
                      when no_data_found then
                        deal_code := '-1';
                        sum_buy   := '0';
                    end;

                    for i in 0 ..  in_date - bstate_rec.chdate
                    loop
                      insert into ldr_infa_cb.fct_securityamount(amount, account_code, security_code, deal_code, sec_portfolio_code, lot_num, dt, rec_status, sysmoment, ext_file)
                             values (case when  bstate_rec.bill_state in ('4') then '0' else '1' end, bstate_rec.acc, rec.vcode, deal_code, rec.portfolio, '-1', qb_dwh_utils.DateToChar(bstate_rec.chdate + i), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                             
                      insert into ldr_infa_cb.fct_sec_sell_result(deal_code, security_code, sec_portfolio_code, purchase_amount, sell_amount, sum_of_disposal, lot_num, dt, rec_status, sysmoment, ext_file)
                            values (deal_code, rec.vcode, rec.portfolio, sum_buy, '0', '0', '-1', qb_dwh_utils.DateToChar(bstate_rec.chdate + i), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                            
                    end loop;
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '4', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '5', qb_dwh_utils.NumberToChar(rec.discount, 2), qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '6', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '7', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '8', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                          values(deal_code, rec.vcode, '-1', '9', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                          
                end if;
              end if;
            end loop;
            -- корректировка ЭПС
            for receps in (SELECT sec_code,
                                   qb_dwh_utils.NumberToChar(round(sum(t_rest), 2), 2) sumrest,
                                   qb_dwh_utils.DateToChar(t_restdate) restdate,
                                   qb_dwh_utils.DateToChar(lead(t_restdate - 1, 1, in_date) over (partition by sec_code order by t_restdate)) next_date,
                                   t_restdate
                              from (select to_char(cacc.t_docid) || '#BNR' sec_code,
                                           acc.t_accountid,
                                           acc.t_account,
                                           rd.t_rest,
                                           rd.t_restdate,
                                           row_number() over(partition by cacc.t_docid order by rd.t_restdate desc) rnk
                                      from DMCACCDOC_DBT cacc
                                     inner join daccount_dbt acc
                                        on (cacc.t_chapter = acc.t_chapter and
                                           cacc.t_account = acc.t_account and
                                           cacc.t_currency = acc.t_code_currency)
                                     inner join drestdate_dbt rd
                                        on (acc.t_accountid = rd.t_accountid)
                                     where cacc.t_catnum in (select v.value
                                                               from qb_dwh_const4exp c
                                                              inner join qb_dwh_const4exp_val v
                                                                 on (c.id = v.id)
                                                              where c.name = cCATEXP_OBILL)
                                       and cacc.t_dockind = n164
                                       and cacc.t_docid = rec.t_bcid
                                       and rd.t_restdate <= in_date
                                       and rd.t_rest <> n0)
                              group by sec_code, t_restdate)
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(ord.t_contractid) || '#ORD' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               ord.t_signdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by ord.t_signdate desc) rnk_last_deal
                          from dvsbanner_dbt bn
                         inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                         inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                         inner join ddl_order_dbt ord
                            on (lnk.t_contractid = ord.t_contractid and
                               lnk.t_dockind = ord.t_dockind)
                         inner join ddp_dep_dbt dp
                            on (bn.t_issuer = dp.t_partyid)
                         where bn.t_bcid = rec.t_bcid
                           and leg.t_legid = n0
                           and leg.t_legkind = n1
                           and ord.t_signdate <= receps.t_restdate
                           and ord.t_dockind = n109 -- выпуск
                        )
                  where rnk_last_deal = 1;
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, receps.sec_code, '-1', '1', receps.sumrest, receps.restdate, receps.next_date, receps.restdate,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;
            end loop;
            -- Корректировка ОКУ
            for lrec in(with dates as (
                          select distinct rd.t_lnkdate lnkdate
                            from ddlreslnk_dbt rd
                           where rd.t_parentid = rec.t_bcid
                             and rd.t_type = n2
                             and rd.t_reservesubkind in (select v.value
                                                           from qb_dwh_const4exp c
                                                          inner join qb_dwh_const4exp_val v
                                                             on (c.id = v.id)
                                                          where c.name = cRES_SUBKIND)
                             and rd.t_lnkdate <= in_date),
                               res as (
                          select lnkdate,
                                 lead(lnkdate - 1, 1, in_date) over (order by lnkdate) nxt_date,
                                 nvl( ( select * from (select r0.t_reserveamount
                                         from ddlreslnk_dbt r0
                                        where r0.t_parentid = rec.t_bcid
                                          and r0.t_type = n2
                                          and r0.t_reservesubkind = n0
                                          order by r0.t_id desc) where rownum = 1
                                          ), 0) res0,
                                 nvl( ( select * from (select r5.t_reserveamount
                                         from ddlreslnk_dbt r5
                                        where r5.t_parentid = rec.t_bcid
                                          and r5.t_type = n2
                                          and r5.t_reservesubkind = n5
                                          order by r5.t_id desc) where rownum = 1
                                          ), 0) res5

                            from dates)
                        select qb_dwh_utils.NumberToChar(res5 - res0, 2) as c2oku,
                               qb_dwh_utils.DateToChar(lnkdate) bd,
                               qb_dwh_utils.DateToChar(nxt_date) ed,
                               lnkdate
                          from res
                         where (res5 - res0) <> n0
                      )
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(ord.t_contractid) || '#ORD' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               ord.t_signdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by ord.t_signdate desc) rnk_last_deal
                          from dvsbanner_dbt bn
                         inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                         inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                         inner join ddl_order_dbt ord
                            on (lnk.t_contractid = ord.t_contractid and
                               lnk.t_dockind = ord.t_dockind)
                         inner join ddp_dep_dbt dp
                            on (bn.t_issuer = dp.t_partyid)
                         where bn.t_bcid = rec.t_bcid
                           and leg.t_legid = n0
                           and leg.t_legkind = n1
                           and ord.t_signdate <= lrec.lnkdate
                           and ord.t_dockind = n109 -- выпуск
                        )
                  where rnk_last_deal = 1;
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, rec.vcode, '-1', '2', lrec.c2oku, lrec.bd, lrec.ed, lrec.bd,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
            end loop;
            -- Корректировка РПБУ
            for lrec in(SELECT qb_dwh_utils.NumberToChar(res0_sum, 2) c2rpbu,
                               qb_dwh_utils.DateToChar(res0_lnkdate) bd,
                               qb_dwh_utils.DateToChar(nxt_date) ed,
                               res0_lnkdate lnkdate
                          from (select bn.t_bcid,
                                       nvl(res0.t_reserveamount, 0) res0_sum,
                                       nvl(res0.t_lnkdate, in_date) res0_lnkdate,
                                       lead(res0.t_lnkdate - 1, 1, in_date) over (order by res0.t_lnkdate) nxt_date
                                  from dvsbanner_dbt bn
                                  left join ddlreslnk_dbt res0
                                    on (bn.t_bcid = res0.t_parentid and res0.t_type = n2 and
                                       res0.t_reservesubkind = n0 and res0.t_lnkdate <= in_date)

                                  where bn.t_bcid = rec.t_bcid
                                  )
                         where res0_sum <> n0)
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(ord.t_contractid) || '#ORD' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               ord.t_signdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by ord.t_signdate desc) rnk_last_deal
                          from dvsbanner_dbt bn
                         inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                         inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                         inner join ddl_order_dbt ord
                            on (lnk.t_contractid = ord.t_contractid and
                               lnk.t_dockind = ord.t_dockind)
                         inner join ddp_dep_dbt dp
                            on (bn.t_issuer = dp.t_partyid)
                         where bn.t_bcid = rec.t_bcid
                           and leg.t_legid = n0
                           and leg.t_legkind = n1
                           and ord.t_signdate <= lrec.lnkdate
                           and ord.t_dockind = n109 -- выпуск
                        )
                  where rnk_last_deal = n1;
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, rec.vcode, '-1', '3', lrec.c2rpbu, lrec.bd, lrec.ed, lrec.bd,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
            end loop;
          elsif (bill_kind = 2) then  -- История статусов по учтеннм векселям
            for bstate_rec in (select bill_state, chdate,
                                      bcid,
                                      (select '0000#IBSOXXX#' || uf4
                                         from (select t_docid,
                                                      catacc.t_account,
                                                      --acc.t_userfield4 uf4,
                                                      case
                                                        when (acc.t_accountid is null) then
                                                          catacc.t_account
                                                        when (acc.t_userfield4 is null) or
                                                            (acc.t_userfield4 = chr(0)) or
                                                            (acc.t_userfield4 = chr(1)) or
                                                            (acc.t_userfield4 like '0x%') then
                                                          acc.t_account
                                                        else
                                                          acc.t_userfield4
                                                      end uf4,
                                                      row_number() over (order by t_activatedate desc) as rnk
                                                                                from DMCACCDOC_DBT catacc
                                                                               left join daccount_dbt acc
                                                                                  on (catacc.t_chapter = acc.t_chapter and catacc.t_account = acc.t_account and catacc.t_currency = acc.t_code_currency)
                                                                               where t_dockind = n164 and catacc.t_docid = bcid
                                                                                 and catacc.t_catnum = n462
                                                                                 and catacc.t_activatedate <= chdate) where rnk = n1
                                                                                 ) acc,
                                      row_number() over (partition by bcid order by chdate desc) as rnk_last_state
                                from (select id,
                                             max(id) over(partition by chdate) lastchgindate,
                                             chdate,
                                             status,
                                             state,
                                             case
                                               when status = 200 then
                                                '4'
                                               when instr(state, 'Г') > 0 then
                                                '11'
                                               when instr(state, 'Д') > 0 then
                                                '11'
                                               when instr(state, 'З') > 0 then
                                                '10'
                                               when instr(state, 'К') > 0 then
                                                '4'
                                               when instr(state, 'Л') > 0 then
                                                '12'
                                               when instr(state, 'Н') > 0 then
                                                '10'
                                               when instr(state, 'О') > 0 then
                                                '11'
                                               when instr(state, 'П') > 0 then
                                                '3'
                                               when instr(state, 'Р') > 0 then
                                                '13'
                                               when instr(state, 'Т') > 0 then
                                                '6'
                                               when instr(state, 'Ф') > 0 then
                                                '7'
                                               else
                                                '1'
                                             end bill_state,
                                             bcid
                                        from (select T_ID           id,
                                                     sh.t_bcid      bcid,
                                                     T_CHANGEDATE   chdate,
                                                     T_NEWABCSTATUS status,
                                                     T_NEWBCSTATE   state
                                                from dvsbnrbck_dbt sh
                                               where sh.t_bcid = rec.t_bcid
                                                 and sh.t_bcstatus = chr0
                                                 AND sh.t_abcstatus = chr88
                                              union all
                                              select T_ID,
                                                     sh.t_bcid      bcid,
                                                     T_CHANGEDATE,
                                                     (select t_newabcstatus
                                                        from dvsbnrbck_dbt
                                                       where t_id = (select max(t_id)
                                                                       from dvsbnrbck_dbt
                                                                      where t_bcid = sh.t_bcid
                                                                        and t_abcstatus = chr88
                                                                        and t_id < sh.t_id)) T_NEWABCSTATUS,
                                                     T_NEWBCSTATE
                                                from dvsbnrbck_dbt sh
                                               where sh.t_bcid = rec.t_bcid
                                                 and sh.t_bcstatus = chr0
                                                 AND sh.t_abcstatus = chr0
                                               order by id))
                               where id = lastchgindate
                              )
            loop
              if (prevst <> bstate_rec.bill_state) then
                Insert into ldr_infa_cb.fct_bill_state(Bill_State,bill_code, Subject_Code, Dt, Rec_Status, Sysmoment, Ext_File)
                       values(bstate_rec.bill_state, rec.vcode, rec.remitent_subject_code, qb_dwh_utils.DateToChar(bstate_rec.chdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                       
                prevst := bstate_rec.bill_state;
                if (bstate_rec.bill_state <> '4') and (bstate_rec.rnk_last_state = 1) then
                  begin
                    select deal_code, sum_buy
                      into deal_code, sum_buy
                      from (select to_char(tick.t_dealid) || '#TCK' deal_code,
                                   to_char(bn.t_bcid) || '#BNR',
                                   tick.t_dealdate dt,
                                   qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                                   row_number() over(partition by bn.t_bcid order by tick.t_dealdate desc nulls last) rnk_last_deal
                              from dvsbanner_dbt bn
                              inner join ddl_leg_dbt leg
                                on (bn.t_bcid = leg.t_dealid)
                              inner join dvsordlnk_dbt lnk
                                on (bn.t_bcid = lnk.t_bcid)
                              inner join ddl_tick_dbt tick
                                on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                              where bn.t_bcid = rec.t_bcid
                                and leg.t_legid = n0 and leg.t_legkind = n1
                                and tick.t_bofficekind = n141 and tick.t_dealtype = n12401 -- покупка векселя
                                and tick.t_dealdate <= in_date
                            )
                      where rnk_last_deal = n1;  -- последняя сделка
                    exception
                      when no_data_found then
                        deal_code := '-1';
                    end;

                  for i in 0 ..  in_date - bstate_rec.chdate
                  loop
                    insert into ldr_infa_cb.fct_securityamount(amount, account_code, security_code, deal_code, sec_portfolio_code, lot_num, dt, rec_status, sysmoment, ext_file)
                           values (case when  bstate_rec.bill_state in ('4') then '0' else '1' end, bstate_rec.acc, rec.vcode, deal_code, rec.portfolio, '-1', qb_dwh_utils.DateToChar(bstate_rec.chdate + i), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                           
                    insert into ldr_infa_cb.fct_sec_sell_result(deal_code, security_code, sec_portfolio_code, purchase_amount, sell_amount, sum_of_disposal, lot_num, dt, rec_status, sysmoment, ext_file)
                          values (deal_code, rec.vcode, rec.portfolio, sum_buy, '0', '0', '-1', qb_dwh_utils.DateToChar(bstate_rec.chdate + i), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;
                  end loop;
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '4', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '5', qb_dwh_utils.NumberToChar(rec.discount, 2), qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '6', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '7', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '8', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                  insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                        values(deal_code, rec.vcode, '-1', '9', '0', qb_dwh_utils.DateToChar(bstate_rec.chdate), qb_dwh_utils.DateToChar(in_date), qb_dwh_utils.DateToChar(bstate_rec.chdate),  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                        
                end if;
              end if;
            end loop;
          -- корректировка ЭПС
          for receps in (SELECT sec_code,
                                   qb_dwh_utils.NumberToChar(round(sum(t_rest), 2), 2) sumrest,
                                   qb_dwh_utils.DateToChar(t_restdate) restdate,
                                   qb_dwh_utils.DateToChar(lead(t_restdate - 1, 1, in_date) over (partition by sec_code order by t_restdate)) next_date,
                                   t_restdate
                              from (select to_char(cacc.t_docid) || '#BNR' sec_code,
                                           acc.t_accountid,
                                           acc.t_account,
                                           rd.t_rest,
                                           rd.t_restdate,
                                           row_number() over(partition by cacc.t_docid order by rd.t_restdate desc) rnk
                                      from DMCACCDOC_DBT cacc
                                     inner join daccount_dbt acc
                                        on (cacc.t_chapter = acc.t_chapter and
                                           cacc.t_account = acc.t_account and
                                           cacc.t_currency = acc.t_code_currency)
                                     inner join drestdate_dbt rd
                                        on (acc.t_accountid = rd.t_accountid)
                                     where cacc.t_catnum in (select v.value
                                                               from qb_dwh_const4exp c
                                                              inner join qb_dwh_const4exp_val v
                                                                 on (c.id = v.id)
                                                              where c.name = cCATEXP_DBILL)
                                       and cacc.t_dockind = n164
                                       and cacc.t_docid = rec.t_bcid
                                       and rd.t_restdate <= in_date
                                       and rd.t_rest <> n0)
                              group by sec_code, t_restdate)
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(tick.t_dealid) || '#TCK' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               tick.t_dealdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by tick.t_dealdate desc nulls last) rnk_last_deal
                          from dvsbanner_dbt bn
                          inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                          inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                          inner join ddl_tick_dbt tick
                            on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                          where bn.t_bcid = rec.t_bcid
                            and leg.t_legid = n0 and leg.t_legkind = n1
                            and tick.t_bofficekind = n141 and tick.t_dealtype = n12401 -- покупка векселя
                            and tick.t_dealdate <= receps.t_restdate
                        )
                  where rnk_last_deal = n1;  -- последняя сделка
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, receps.sec_code, '-1', '1', receps.sumrest, receps.restdate, receps.next_date, receps.restdate,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;
            end loop;
            -- Корректировка ОКУ
            for lrec in(with dates as (
                          select distinct rd.t_lnkdate lnkdate
                            from ddlreslnk_dbt rd
                           where rd.t_parentid = rec.t_bcid
                             and rd.t_type = n2
                             and rd.t_reservesubkind in (select v.value
                                                           from qb_dwh_const4exp c
                                                          inner join qb_dwh_const4exp_val v
                                                             on (c.id = v.id)
                                                          where c.name = cRES_SUBKIND)
                             and rd.t_lnkdate <= in_date),
                               res as (
                          select lnkdate,
                                 lead(lnkdate - 1, 1, in_date) over (order by lnkdate) nxt_date,
                                 nvl( ( select * from (select r0.t_reserveamount
                                         from ddlreslnk_dbt r0
                                        where r0.t_parentid = rec.t_bcid
                                          and r0.t_type = n2
                                          and r0.t_reservesubkind = n0
                                          order by r0.t_id desc) where rownum = 1
                                          ), 0) res0,
                                 nvl( ( select * from (select r5.t_reserveamount
                                         from ddlreslnk_dbt r5
                                        where r5.t_parentid = rec.t_bcid
                                          and r5.t_type = n2
                                          and r5.t_reservesubkind = n5
                                          order by r5.t_id desc) where rownum = 1
                                          ), 0) res5

                            from dates)
                        select qb_dwh_utils.NumberToChar(res5 - res0, 2) as c2oku,
                               qb_dwh_utils.DateToChar(lnkdate) bd,
                               qb_dwh_utils.DateToChar(nxt_date) ed,
                               lnkdate
                          from res
                         where (res5 - res0) <> n0
                      )
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(tick.t_dealid) || '#TCK' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               tick.t_dealdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by tick.t_dealdate desc nulls last) rnk_last_deal
                          from dvsbanner_dbt bn
                          inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                          inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                          inner join ddl_tick_dbt tick
                            on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                          where bn.t_bcid = rec.t_bcid
                            and leg.t_legid = n0 and leg.t_legkind = n1
                            and tick.t_bofficekind = n141 and tick.t_dealtype = n12401 -- покупка векселя
                            and tick.t_dealdate <= lrec.lnkdate
                        )
                  where rnk_last_deal = n1;  -- последняя сделка
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, rec.vcode, '-1', '2', lrec.c2oku, lrec.bd, lrec.ed, lrec.bd,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
            end loop;
            -- Корректировка РПБУ
            for lrec in(SELECT qb_dwh_utils.NumberToChar(res0_sum, 2) c2rpbu,
                               qb_dwh_utils.DateToChar(res0_lnkdate) bd,
                               qb_dwh_utils.DateToChar(nxt_date) ed,
                               res0_lnkdate lnkdate
                          from (select bn.t_bcid,
                                       nvl(res0.t_reserveamount, 0) res0_sum,
                                       nvl(res0.t_lnkdate, in_date) res0_lnkdate,
                                       lead(res0.t_lnkdate - 1, 1, in_date) over (order by res0.t_lnkdate) nxt_date
                                  from dvsbanner_dbt bn
                                  left join ddlreslnk_dbt res0
                                    on (bn.t_bcid = res0.t_parentid and res0.t_type = 2 and
                                       res0.t_reservesubkind = 0 and res0.t_lnkdate <= in_date)

                                  where bn.t_bcid = rec.t_bcid
                                  )
                         where res0_sum <> n0)
            loop
              begin
                select deal_code
                  into deal_code
                  from (select to_char(tick.t_dealid) || '#TCK' deal_code,
                               to_char(bn.t_bcid) || '#BNR',
                               tick.t_dealdate dt,
                               qb_dwh_utils.NumberToChar(leg.t_principal, 2) sum_buy,
                               row_number() over(partition by bn.t_bcid order by tick.t_dealdate desc nulls last) rnk_last_deal
                          from dvsbanner_dbt bn
                          inner join ddl_leg_dbt leg
                            on (bn.t_bcid = leg.t_dealid)
                          inner join dvsordlnk_dbt lnk
                            on (bn.t_bcid = lnk.t_bcid)
                          inner join ddl_tick_dbt tick
                            on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                          where bn.t_bcid = rec.t_bcid
                            and leg.t_legid = n0 and leg.t_legkind = n1
                            and tick.t_bofficekind = n141 and tick.t_dealtype = n12401 -- покупка векселя
                            and tick.t_dealdate <= lrec.lnkdate
                        )
                  where rnk_last_deal = n1;  -- последняя сделка
              exception
                when no_data_found then
                  deal_code := '-1';
              end;
              insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
                  values(deal_code, rec.vcode, '-1', '3', lrec.c2rpbu, lrec.bd, lrec.ed, lrec.bd,  dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
            end loop;

          end if;
          -- Вставка в ASS_ACCOUNTCECURITY
          for acrec in (select distinct '0000#IBSOXXX#' || case
                                                              when (acc.t_userfield4 is null) or
                                                                  (acc.t_userfield4 = chr(0)) or
                                                                  (acc.t_userfield4 = chr(1)) or
                                                                  (acc.t_userfield4 like '0x%') then
                                                                acc.t_account
                                                              else
                                                                acc.t_userfield4
                                                            end account_code,
            --> 2020-05-15 AS
                                        /* '0000#SOFR#' || */ cat.t_code ||
                                          case when catacc.t_catnum in (235, 464) and templ.t_value4 = 1  THEN
                                                '_П'
                                               when catacc.t_catnum in (235, 464) and templ.t_value4 = 2  THEN
                                                '_Д'
                                               when catacc.t_catnum = 1492 and templ.t_value1 >= 0 THEN
                                                 (select '#' || t_code from dllvalues_dbt where t_list = n3503 and t_element = templ.t_value1)
                                               else
                                                 null
                                          end roleaccount_deal_code,
                                          case when catacc.t_activatedate < vdt then
                                                 vdt
                                               else
                                                 catacc.t_activatedate
                                          end dt
                          from dmcaccdoc_dbt catacc
                         inner join daccount_dbt acc
                            on (catacc.t_chapter = acc.t_chapter and catacc.t_account = acc.t_account and catacc.t_currency = acc.t_code_currency)
                         inner join dmccateg_dbt cat
                            on (catacc.t_catid = cat.t_id)
                          left join dmctempl_dbt templ
                            on (catacc.t_catid = templ.t_catid and catacc.t_templnum = templ.t_number)
                         where t_dockind = n164 and catacc.t_docid = rec.t_bcid
                           and catacc.t_chapter in (select v.value
                                                      from qb_dwh_const4exp c
                                                     inner join qb_dwh_const4exp_val v
                                                        on (c.id = v.id)
                                                     where c.name = cACC_CHAPTERS)
                           and exists (select 1 from dfininstr_dbt fi where fi.t_fiid = catacc.t_currency and fi.t_fi_kind = n1)
                           and catacc.t_activatedate <
                               decode(catacc.t_disablingdate,
                                      emptDate,
                                      maxDate,
                                      catacc.t_disablingdate)
                           and exists (select 1 from daccount_dbt a where a.t_chapter = catacc.t_chapter and a.t_account = catacc.t_account and a.t_code_currency = catacc.t_currency)
                         order by roleaccount_deal_code, dt)
          loop
            insert into ldr_infa_cb.ass_accountsecurity(account_code,security_code, roleaccount_deal_code, dt,rec_status, sysmoment, ext_file)
                   values (acrec.account_code, rec.vcode, acrec.roleaccount_deal_code, qb_dwh_utils.DateToChar(acrec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end loop;
          if (rec.portfolio is not null) then
            insert into ldr_infa_cb.ass_sec_portfolio(security_code, sec_portfolio_code, dt,rec_status, sysmoment, ext_file)
                   values(rec.vcode, rec.portfolio, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          -- вставка в FCT_SECURITY_ATTR
          insert into ldr_infa_cb.fct_security_attr
             (SELECT distinct rec.vcode,
                     nvl(rec.portfolio, '-1'),
                     TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                     null number_value,
                     null date_value,
                     substr(nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name), 1, 250) string_value,
                     substr(nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name), 1, 550) value,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) ) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                FROM DOBJATCOR_DBT AC
               INNER JOIN DOBJGROUP_DBT GR
                  ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
               inner join dobjattr_dbt at
                  on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                     ac.t_attrid = at.t_attrid)
               WHERE AC.T_OBJECTTYPE = n24
                 and gr.t_type = chr88
                 and gr.t_groupid <> n101
                 and ac.t_object = lpad(to_char(rec.t_bcid), 10, '0')
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) <= in_date
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) >= rec.dt
              UNION ALL
              select distinct rec.vcode,
                     rec.portfolio,
                     code code_securtity_attr,
                     case
                       when type in (0, 1, 2, 3, 4, 25) then
                        noteval
                       else
                        null
                     end number_value,
                     case
                       when type in (9, 10) then
                        noteval
                       else
                        null
                     end date_value,
                     case
                       when type in (7, 12) then
                        substr(noteval, 1, 250)
                       else
                        null
                     end string_value,
                     substr(noteval, 1, 550) value,
                     qb_dwh_utils.DateToChar(t_date) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                from (SELECT to_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                             UPPER(TRIM(NK.T_NAME)) NAME,
                             nk.t_notetype type,
                             case nk.t_notetype
                               when 0 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getInt(nt.t_text), 0)
                               when 1 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getLong(nt.t_text), 0)
                               when 2 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 3 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 4 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 7 then
                                Rsb_Struct.getString(nt.t_text)
                               when 9 then
                                qb_dwh_utils.DateToChar(Rsb_Struct.getDate(nt.t_text))
                               when 10 then
                                qb_dwh_utils.DateTimeToChar(Rsb_Struct.getTime(nt.t_text))
                               when 12 then
                                Rsb_Struct.getChar(nt.t_text)
                               when 25 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getMoney(nt.t_text), 2)
                               else
                                null
                             end noteval,
                             decode(nt.t_date, emptDate, rec.dt, nt.t_date) t_date,
                             nt.t_documentid
                        FROM DNOTETEXT_DBT NT
                       INNER JOIN DNOTEKIND_DBT NK
                          ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND
                             NT.T_NOTEKIND = NK.T_NOTEKIND)
                       WHERE NT.T_OBJECTTYPE  =  n24
                         and nt.t_documentid = lpad(to_char(rec.t_bcid), 10, '0')
                         and decode(nt.t_date, emptDate, rec.dt, nt.t_date) <= in_date
                         and decode(nt.t_date, emptDate, rec.dt, nt.t_date) >= rec.dt)
                     );
commit;                     
          -- вставка в FCT_SECURITY_ATTR_MULTY
          insert into ldr_infa_cb.fct_security_attr_multi
             (SELECT distinct rec.vcode,
                     nvl(rec.portfolio, '-1'),
                     TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                     null number_value,
                     null date_value,
                     substr(nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name), 1, 250) string_value,
                     substr(nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name), 1, 550) value,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate)) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                FROM DOBJATCOR_DBT AC
               INNER JOIN DOBJGROUP_DBT GR
                  ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
               inner join dobjattr_dbt at
                  on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                     ac.t_attrid = at.t_attrid)
               WHERE AC.T_OBJECTTYPE = n24
                 and gr.t_type = chr0
                 and ac.t_object = lpad(to_char(rec.t_bcid), 10, '0')
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) <= in_date
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) >= rec.dt
                     );
commit;                     
          insert into ldr_infa_cb.fct_security_check
              (SELECT distinct rec.vcode,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate)) dt_check,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate)) dt_redefinition,
                     nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) check_result,
                     null check_reason,
                     qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate)) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                FROM DOBJATCOR_DBT AC
               INNER JOIN DOBJGROUP_DBT GR
                  ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
               inner join dobjattr_dbt at
                  on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                     ac.t_attrid = at.t_attrid)
               WHERE AC.T_OBJECTTYPE = n24
                 and gr.t_type = chr88
                 and gr.t_groupid = n101
                 and ac.t_object = lpad(to_char(rec.t_bcid), 10, '0')
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) <= in_date
                 and decode(ac.t_validfromdate, emptDate, rec.dt, ac.t_validfromdate) >= rec.dt
                 );
commit;                 
       end loop;
    else
      -- Прочие ц/б
      select to_char(fiid) || '#FIN',
             substr(trim(fi.t_name), 1, 250),
             substr(trim(fi.t_definition), 1, 50),
             -- qb_dwh_utils.DateToChar(decode(fi.t_issued, emptDate, firstDate, fi.t_issued)),
             -- 2020-09-02 AS алгоритм определения даты выпуска, когда в открых источниках нет данных
             /* согласован Исаковой Н. */
             case
               when  decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate) <=
                     decode(fi.t_issued, emptDate, maxDate, fi.t_issued)
                and  decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate) <=
                     decode(av.t_begplacementdate, emptDate, maxDate, av.t_begplacementdate) then
                 decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)
               when  decode(fi.t_issued, emptDate, maxDate, fi.t_issued) <=
                     decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate)
                and  decode(fi.t_issued, emptDate, maxDate, fi.t_issued) <=
                     decode(av.t_begplacementdate, emptDate, maxDate, av.t_begplacementdate) then
                 fi.t_issued
               else
                 av.t_begplacementdate
             end date_is,
             qb_dwh_utils.NumberToChar(fi.t_facevalue),
             --case when fi.t_fiid in (220, 1924, 1416, 1536, 2007, 1406, 1722, 1660, 2369, 1596, 1555, 2212, 2148, 5369, 5567, 9556, 7969, 16163, 11860, 11790, 14705) then        -- !!!!!!!! Для эмитентов по этим бумагам  нет выгружаемого кода БИСКВИТ
             --       '-1'
             --     else
                     qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                 qb_dwh_utils.System_IBSO,
                                                 1,
                                                 fi.t_issuer)
             --end
             ,
             pfi.t_iso_number,
             --(select max(tick.t_dealdate) from ddl_tick_dbt tick where tick.t_pfi = fi.t_fiid),
             case
               when  decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate) <=
                     decode(fi.t_issued, emptDate, maxDate, fi.t_issued)
                and  decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate) <=
                     decode(av.t_begplacementdate, emptDate, maxDate, av.t_begplacementdate) then
                 decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)
               when  decode(fi.t_issued, emptDate, maxDate, fi.t_issued) <=
                     decode(av.t_incirculationdate, emptDate, maxDate, av.t_incirculationdate)
                and  decode(fi.t_issued, emptDate, maxDate, fi.t_issued) <=
                     decode(av.t_begplacementdate, emptDate, maxDate, av.t_begplacementdate) then
                 fi.t_issued
               else
                 av.t_begplacementdate
             end mindate,
             av.t_lsin,
             av.t_subordinated,
             av.t_nkdbase_kind
        into vcode, vname, vnames, date_is, vnomin, codeis, ficode, vdt, vregnum, issub, nkdbase
        from dfininstr_dbt fi
       inner join davoiriss_dbt av
          on (fi.t_fiid = av.t_fiid)
        left join dfininstr_dbt pfi
          on (fi.t_facevaluefi = pfi.t_fiid)
       where fi.t_fiid = fiid
         and rownum = n1;

         dateis := qb_dwh_utils.DateToChar(date_is);

       procbase := case nkdbase
                         when 0 then
                            '365'
                         when 1 then
                            '30/360'
                         when 2 then
                            '360/0'
                         when 3 then
                            '30/365'
                         when 4 then
                            '366'
                         when 5 then
                            '30/366'
                         when 6 then
                            'Act/по_купонным_периодам'
                         when 7 then
                            'Act/365L'
                         when 8 then
                            'Act/364'
                         when 9 then
                            '30E/360'
                         when 10 then
                            '30/360_ISDA'
                         when 11 then
                            'Act/Act_ICMA'
                         else
                           '-1'
                       end;
      procbase := '9999#SOFRXXX#' || procbase;

      -- Вставка в DET_FINSTR
      Insert into ldr_infa_cb.det_finstr(finstr_code, finstr_name, finstr_name_s,typefinstr, dt, rec_status,sysmoment, ext_file)
             values (vcode, vname, vnames, 2, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      -- Вставка в f
      stype := case when InConst(cSECKIND_STOCK, avoirkind) then           -- акция
                  1
               when InConst(cSECKIND_BILL, avoirkind)  then                -- вексель
                  3
               when InConst(cSECKIND_UIT, avoirkind)  then                 -- ПИФ
                  5
               when InConst(cSECKIND_BOND, avoirkind) then                 -- облигация
                  2
               when InConst(cSECKIND_RECEIPT, avoirkind) then              -- депозитарная расписка
                  6
               else
                  null
               end;
      Insert into ldr_infa_cb.det_security(typesecurity, code, date_issue, nominal, regnum, finstrsecurity_finstr_code, issuer_code, underwriter_code, finstrcurnom_finstr_code,
                  procbase, dt, rec_status, sysmoment, ext_file)
             values (stype, vcode, dateis, vnomin, vregnum, vcode, codeis, '-1', ficode, procbase, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE/*, vregnum*/);
commit;             
      if InConst(cSECKIND_STOCK, avoirkind) then
        -- Вставка в DET_STOCK
        for rec in (select to_char(fi.t_avoirkind) typestock,
                           to_char(fi.t_fiid) || '#FIN' backofficecode,
                           av.t_isin isinoldcode,
                           null isinnewcode,
                           av.t_lsin regnum,
                           case when fi.t_settlement_code = 1 then
                                  '1'
                                when fi.t_settlement_code = 0 then
                                  '2'
                                else
                                  '-1'
                           end issueform,
                           av.t_lsin secissue,
                           qb_dwh_utils.DateToChar(decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)) secissueregdate,
                           to_char(av.t_qty) secissuevolume
                      from dfininstr_dbt fi
                     inner join davoiriss_dbt av
                        on (fi.t_fiid = av.t_fiid)
                     where fi.t_fiid = fiid)
        loop
          insert into ldr_infa_cb.det_stock(typestock, backofficecode, isinoldcode, isinnewcode, regnum, issueform, secissue, secissueregdate, secissuevolume,
                                        security_code, dt, rec_status, sysmoment, ext_file)
                 values (rec.typestock, rec.backofficecode, rec.isinoldcode, rec.isinnewcode, rec.regnum, rec.issueform, rec.secissue, rec.secissueregdate, rec.secissuevolume,
                         vcode, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                         
        end loop;
      elsif InConst(cSECKIND_BOND, avoirkind) then
        -- Вставка в DET_BOND
        for rec in (select case when fi.t_avoirkind = 21 then
                                  '6'
                                when fi.t_avoirkind = 24 then
                                  '3'
                                when fi.t_avoirkind = 25 then
                                  '6'
                                when fi.t_avoirkind = 28 then
                                  '1'
                                when fi.t_avoirkind = 38 then
                                  '6'
                                when fi.t_avoirkind = 40 then
                                  '4'
                                when fi.t_avoirkind = 42 then
                                  '5'
                                when fi.t_avoirkind = 43 then
                                  '8'
                                when fi.t_avoirkind = 50 then
                                  '6'
                                else
                                  '0'
                           end typebond,
                           to_char(fi.t_fiid) || '#FIN' backofficecode,
                           av.t_isin isinoldcode,
                           null isinnewcode,
                           av.t_lsin regnum,
                           case when fi.t_settlement_code = 1 then
                                  '1'
                                when fi.t_settlement_code = 0 then
                                  '2'
                                else
                                  '0'
                           end issueform,
                           av.t_lsin secissue,
                           qb_dwh_utils.DateToChar(decode(av.t_begplacementdate, emptDate, firstDate, av.t_begplacementdate)) secissueregdate,
                           to_char(av.t_qty) secissuevolume,
                           qb_dwh_utils.DateToChar(decode(fi.t_drawingdate, emptDate, firstDate, fi.t_drawingdate)) maturitydate
                      from dfininstr_dbt fi
                     inner join davoiriss_dbt av
                        on (fi.t_fiid = av.t_fiid)
                     where fi.t_fiid = fiid)
        loop
          insert into ldr_infa_cb.det_bond(typebond, backofficecode, isinoldcode, isinnewcode, regnum, issueform, secissue, secissueregdate, secissuevolume, maturitydate,
                                        security_code, dt, rec_status, sysmoment, ext_file)
                 values (rec.typebond, rec.backofficecode, rec.isinoldcode, rec.isinnewcode, rec.regnum, rec.issueform, rec.secissue, rec.secissueregdate, rec.secissuevolume, rec.maturitydate,
                         vcode, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                         
        end loop;
      elsif InConst(cSECKIND_UIT, avoirkind) then
        -- Вставка в DET_UIT
        for rec in (select case when inv.t_type = 1 then
                                  '1'
                                when inv.t_type = 2 then
                                  '2'
                                when inv.t_type = 0 then
                                  '3'
                                when inv.t_type = 3 then
                                  '4'
                                else
                                  '0'
                           end typeuit,
                           to_char(fi.t_fiid) || '#FIN' backofficecode,
                           av.t_lsin regnum,
                           qb_dwh_utils.DateToChar(decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)) regdate,
                           null maturitydate,
                           qb_dwh_utils.DateToChar(decode(inv.t_formperiodstart, emptDate, to_date('01011980','ddmmyyyy'), inv.t_formperiodstart)) begindate,
                           qb_dwh_utils.DateToChar(decode(inv.t_formperiodend, emptDate, to_date('01013001','ddmmyyyy'), inv.t_formperiodend)) enddate
                      from dfininstr_dbt fi
                     inner join davoiriss_dbt av
                        on (fi.t_fiid = av.t_fiid)
                     inner join davrinvst_dbt inv
                        on (fi.t_fiid = inv.t_fiid)
                     where fi.t_fiid = fiid)
        loop
          insert into ldr_infa_cb.det_uit(typeuit, backofficecode, regnum,security_code, regdate, maturitydate, begindate, enddate, dt, rec_status, sysmoment, ext_file)
                 values (rec.typeuit, rec.backofficecode, rec.regnum, vcode, rec.regdate, rec.maturitydate, rec.begindate, rec.enddate, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        end loop;
      elsif InConst(cSECKIND_RECEIPT, avoirkind) then
        -- Вставка в DET_RECEIPT
        for rec in (select case when fi.t_avoirkind = 45 then
                                  1
                                when fi.t_avoirkind = 46 then
                                  2
                                when fi.t_avoirkind = 47 then
                                  3
                                when fi.t_avoirkind = 49 then
                                  4
                           end typereceipt,
                           to_char(fi.t_fiid) || '#FIN' backofficecode,
                           av.t_isin isincode,
                           av.t_lsin regnum,
                           (select case
                                     when t_fi_kind = 2 then
                                       to_number(t_fiid) || '#FIN'
                                     when t_fi_kind = 1 then
                                       t_iso_number
                                     else
                                       to_number(t_fiid) || '-1'
                                   end
                             from dfininstr_dbt where t_fiid = fi.t_parentfi) base_finstr_code,
                           qb_dwh_utils.DateToChar(decode(av.t_incirculationdate, emptDate, firstDate, av.t_incirculationdate)) secissueregdate,
                           to_char(av.t_qty) secissuevolume,
                           to_char(av.t_numbasefi) basesecurityvolume
                      from dfininstr_dbt fi
                     inner join davoiriss_dbt av
                        on (fi.t_fiid = av.t_fiid)
                     left join davoiriss_dbt base_av
                        on (fi.t_parentfi = base_av.t_fiid)
                     where fi.t_fiid = fiid)
        loop
          insert into ldr_infa_cb.det_receipt(typereceipt, backofficecode, isincode, regnum, base_finstr_code, secissueregdate, secissuevolume, basesecurityvolume,security_code, dt, rec_status, sysmoment, ext_file)
                 values (rec.typereceipt, rec.backofficecode, rec.isincode, rec.regnum, rec.base_finstr_code, rec.secissueregdate, rec.secissuevolume, rec.basesecurityvolume, vcode, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        end loop;
      end if;
      -- Вставка в FCT_SECURITY_QUOT
      select case when (count(1) > 0) then
                    '1'
                  else
                    '0'
             end
        into quot
        from dobjatcor_dbt ac
       where ac.t_objecttype = 12
         and ac.t_groupid = 18
         and in_date between ac.t_validfromdate and ac.t_validtodate
         and ac.t_attrid = 1
         and to_number(ac.t_object) = fiid;
      insert into ldr_infa_cb.fct_security_quot(is_reval,security_code, dt, rec_status,sysmoment,ext_file)
             values(quot, vcode, qb_dwh_utils.DateToChar(vdt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      -- Вставка в FCT_SECNOMINAL
      for rec in (select distinct *
                    from (
                  select nh.t_fiid fiid,
                         decode(nh.T_BEGDATE, emptDate, firstDate, nh.T_BEGDATE) chdate,
                         nh.T_FACEVALUE nominal
                    from dv_fi_facevalue_hist nh
                    where nh.t_fiid = fiid
                  union all
                  select wr.t_fiid,
                         decode(wr.t_drawingdate, emptDate, firstDate, wr.t_drawingdate),
                         rsb_fiinstr.FI_GetNominalOnDate(wr.t_fiid, wr.t_drawingdate)
                    from dfiwarnts_dbt wr
                   where t_ispartial = chr88
                     and wr.t_fiid = fiid)
                   order by chdate)
      loop
        insert into ldr_infa_cb.fct_secnominal(nominal,security_code, dt, rec_status, sysmoment,ext_file)
               values (qb_dwh_utils.NumberToChar(round(rec.nominal, 9), 9), vcode, qb_dwh_utils.DateToChar(rec.chdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      end loop;
      -- Вставка в ASS_ACCOUNTCECURITY
      for acrec in (select distinct '0000#IBSOXXX#' || case
                                                          when (acc.t_userfield4 is null) or
                                                              (acc.t_userfield4 = chr(0)) or
                                                              (acc.t_userfield4 = chr(1)) or
                                                              (acc.t_userfield4 like '0x%') then
                                                            acc.t_account
                                                          else
                                                            acc.t_userfield4
                                                        end account_code,
                                    /* '0000#SOFR#' || */ cat.t_code ||
                                          case when catacc.t_catnum in (235, 464) and templ.t_value4 = 1  THEN
                                                '_П'
                                               when catacc.t_catnum in (235, 464) and templ.t_value4 = 2  THEN
                                                '_Д'
                                               when catacc.t_catnum = 1492 and templ.t_value1 >= 0 THEN
                                                 (select '#' || t_code from dllvalues_dbt where t_list = n3503 and t_element = templ.t_value1)
                                               else
                                                 null
                                          end roleaccount_deal_code,
                                    case when catacc.t_activatedate < vdt then
                                           vdt
                                         else
                                           catacc.t_activatedate
                                    end dt
                                    --decode(catacc.t_activatedate, emptDate, vdt, catacc.t_activatedate) dt
                      from dmcaccdoc_dbt catacc
                     inner join daccount_dbt acc
                        on (catacc.t_chapter = acc.t_chapter and catacc.t_account = acc.t_account and catacc.t_currency = acc.t_code_currency)
                     inner join dmccateg_dbt cat
                        on (catacc.t_catid = cat.t_id)
                     left join dmctempl_dbt templ
                       on (catacc.t_catid = templ.t_catid and catacc.t_templnum = templ.t_number)
                     where catacc.t_fiid = fiid
                       and catacc.t_chapter in (select v.value
                                                  from qb_dwh_const4exp c
                                                 inner join qb_dwh_const4exp_val v
                                                    on (c.id = v.id)
                                                 where c.name = cACC_CHAPTERS)
                       and exists (select 1 from dfininstr_dbt fi where fi.t_fiid = catacc.t_currency and fi.t_fi_kind = n1)
                       and catacc.t_activatedate <
                           decode(catacc.t_disablingdate,
                                  emptDate,
                                  maxDate,
                                  catacc.t_disablingdate)
                       and exists (select 1 from daccount_dbt a where a.t_chapter = catacc.t_chapter and a.t_account = catacc.t_account and a.t_code_currency = catacc.t_currency)
                     order by roleaccount_deal_code, dt)
      loop
        insert into ldr_infa_cb.ass_accountsecurity(account_code,security_code, roleaccount_deal_code, dt, rec_status, sysmoment, ext_file)
               values (acrec.account_code, vcode, acrec.roleaccount_deal_code, qb_dwh_utils.DateToChar(acrec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      end loop;

    insert into ldr_infa_cb.fct_security_attr
      SELECT distinct to_char(t_fiid) || '#FIN',
             nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio
                           where security_code = vcode
                             and to_date(dt, 'dd-mm-yyyy') <= decode(t_dateredemption, emptDate, vdt, t_dateredemption))
                   where rnk = n1), '-1') portf,
             'DATE_OFFER',
             null,
             qb_dwh_utils.DateToChar(decode(t_dateredemption, emptDate, vdt, t_dateredemption)),
             null,
             qb_dwh_utils.DateToChar(decode(t_dateredemption, emptDate, vdt, t_dateredemption)),
             qb_dwh_utils.DateToChar(vdt),
             dwhRecStatus,
             dwhSysMoment,
             dwhEXT_FILE
        FROM doffers_dbt
       WHERE t_fiid = fiid;



      -- вставка в FCT_SECURITY_ATTR
      insert into ldr_infa_cb.fct_security_attr
          (SELECT distinct vcode,
                  nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio
                           where security_code = vcode
                             and to_date(dt, 'dd-mm-yyyy') <= decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate))
                   where rnk = n1), '-1') portf,
                 TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                 null number_value,
                 null date_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) string_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) value,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
           INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           WHERE AC.T_OBJECTTYPE = n12
             and gr.t_type = chr88
             and gr.t_groupid <> n62
             and ac.t_object = lpad(to_char(fiid), 10, '0')
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) <= in_date
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) >= vdt
          UNION ALL --ТЗ, табл.2, п.3 Ипотечное покрытие
          SELECT distinct vcode,
                  nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio
                           where security_code = vcode
                             and to_date(dt, 'dd-mm-yyyy') <= decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate))
                   where rnk = n1), '-1') portf,
                 TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                 null number_value,
                 null date_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) string_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) value,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
           INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           WHERE AC.T_OBJECTTYPE = n12
             and gr.t_type = chr88
             and gr.t_groupid = n101
             and ac.t_object = lpad(to_char(fiid), 10, '0')
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) <= in_date
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) >= vdt
             and BIQ_7477_78 = 1
          UNION ALL
          select distinct vcode,
                 nvl((select sec_portfolio_code
                   from (select sec_portfolio_code,
                                row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                           from ldr_infa_cb.ass_sec_portfolio
                          where security_code = vcode
                            and to_date(dt, 'dd-mm-yyyy') <= t_date )
                  where rnk = n1), '-1') portf,
                 code code_securtity_attr,
                 case
                   when type in (0, 1, 2, 3, 4, 25) then
                    noteval
                   else
                    null
                 end number_value,
                 case
                   when type in (9, 10) then
                    noteval
                   else
                    null
                 end date_value,
                 case
                   when type in (7, 12) then
                    substr(noteval, 1, 250)
                   else
                    null
                 end string_value,
                 substr(noteval, 1, 250) value,
                 qb_dwh_utils.DateToChar(t_date) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from (SELECT to_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                         UPPER(TRIM(NK.T_NAME)) NAME,
                         nk.t_notetype type,
                         case nk.t_notetype
                           when 0 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getInt(nt.t_text), 0)
                           when 1 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getLong(nt.t_text), 0)
                           when 2 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                           when 3 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                           when 4 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                           when 7 then
                            Rsb_Struct.getString(nt.t_text)
                           when 9 then
                            qb_dwh_utils.DateToChar(Rsb_Struct.getDate(nt.t_text))
                           when 10 then
                            qb_dwh_utils.DateTimeToChar(Rsb_Struct.getTime(nt.t_text))
                           when 12 then
                                Rsb_Struct.getChar(nt.t_text)
                           when 25 then
                            qb_dwh_utils.NumberToChar(Rsb_Struct.getMoney(nt.t_text), 2)
                           else
                            null
                         end noteval,
                         decode(nt.t_date, emptDate, vdt, nt.t_date) t_date,
                         nt.t_documentid
                    FROM DNOTETEXT_DBT NT
                   INNER JOIN DNOTEKIND_DBT NK
                      ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND
                         NT.T_NOTEKIND = NK.T_NOTEKIND)
                   WHERE NT.T_OBJECTTYPE  =  n12
                     and nt.t_documentid = lpad(to_char(fiid), 10, '0')
                     and decode(nt.t_date, emptDate, vdt, nt.t_date) <= in_date
                     and decode(nt.t_date, emptDate, vdt, nt.t_date) >= vdt)
                 );
commit;                 
      -- вставка в FCT_SECURITY_ATTR_MULTI
      insert into ldr_infa_cb.fct_security_attr_multi
          (SELECT distinct vcode,
                 nvl((select sec_portfolio_code
                   from (select sec_portfolio_code,
                                row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                           from ldr_infa_cb.ass_sec_portfolio
                          where security_code = vcode
                            and to_date(dt, 'dd-mm-yyyy') <= decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate))
                  where rnk = n1), '-1') portf,
                 TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_security_attr,
                 null number_value,
                 null date_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) string_value,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) value,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
           INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           WHERE AC.T_OBJECTTYPE = n12
             and gr.t_type = chr0
             and ac.t_object = lpad(to_char(fiid), 10, '0')
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) <= in_date
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) >= vdt
                 );
commit;                 
      insert into ldr_infa_cb.fct_security_check
          (SELECT distinct vcode,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt_check,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt_redefinition,
                 nvl(replace(replace(at.t_fullname, chr(0)), chr(1)), at.t_name) check_result,
                 null check_reason,
                 qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate)) dt,
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
           INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           WHERE AC.T_OBJECTTYPE = n12
             and gr.t_type = chr88
             and gr.t_groupid = n62
             and ac.t_object = lpad(to_char(fiid), 10, '0')
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) <= in_date
             and decode(ac.t_validfromdate, emptDate, vdt, ac.t_validfromdate) >= vdt
             );
commit;             
       -- вставка в FCT_SECREPAYSCHEDULE
       for rec in ( select t.fiid,
                           t.code,
                           t.typeschedule,
                           t.typerepaysec,
                           t.begindate_tbl,
                           t.begindate_calc,
                           case
                             when t.begindate_tbl > begindate_calc then
                              t.begindate_tbl
                             else
                              t.begindate_calc
                           end begindate,
                           t.enddate,
                           t.proc_rate,
                           t.proc_sum
                      from (select wr.t_fiid fiid,
                                   to_char(wr.t_id) code,
                                   case
                                     when wr.t_ispartial = chr(0) then
                                      '1'
                                     when wr.t_ispartial = chr(88) then
                                      '2'
                                   end typeschedule,
                                   case
                                     when wr.t_ispartial = chr(0) then
                                      '1'
                                     when wr.t_ispartial = chr(88) then
                                      '2'
                                   end typerepaysec,
                                   wr.t_firstdate begindate_tbl,
                                   lag(decode(wr.t_drawingdate,
                                              emptDate,
                                              vdt,
                                              wr.t_drawingdate) + 1,
                                       1, vdt) over(partition by wr.t_fiid, wr.t_ispartial order by wr.t_fiid, wr.t_ispartial, decode(wr.t_drawingdate, emptDate, vdt, wr.t_drawingdate)) begindate_calc,
                                   decode(wr.t_drawingdate, emptDate, vdt, wr.t_drawingdate) enddate,
                                   case
                                     when wr.t_incomerate = 0 then
                                       nvl((select h.t_incomerate
                                              from dflrhist_dbt h
                                             where h.t_fiwarntid = wr.t_id and rownum = 1), 0)
                                     else
                                       wr.t_incomerate
                                   end proc_rate,
                                   wr.t_incomevolume proc_sum
                              from dfiwarnts_dbt wr
                             inner join davoiriss_dbt av
                                on (wr.t_fiid = av.t_fiid)
                             where wr.t_fiid = fiid
                               and wr.t_drawingdate > emptDate
                               and  (round(wr.t_incomerate, 3) <> n0 or round(wr.t_incomevolume, 3) <> n0)) t)
       loop
         if (rec.begindate <= rec.enddate) then
           insert into ldr_infa_cb.fct_secrepayschedule(code, typeschedule, typerepaysec, begindate, enddate, proc_rate, proc_sum,security_code, dt, rec_status, sysmoment, ext_file)
           values (rec.code, rec.typeschedule, rec.typerepaysec, qb_dwh_utils.DateToChar(rec.begindate) , qb_dwh_utils.DateToChar(rec.enddate), qb_dwh_utils.NumberToChar(round(rec.proc_rate, 3), 3), qb_dwh_utils.NumberToChar(round(rec.proc_sum, 3), 3),
                   vcode,  dateis, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
         end if;
       end loop;
       insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
              values(vcode,
                     nvl((select sec_portfolio_code
                            from (select sec_portfolio_code,
                                         row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                                    from ldr_infa_cb.ass_sec_portfolio
                                   where security_code = vcode
                                     and to_date(dt, 'dd-mm-yyyy') <= in_date )
                           where rnk = n1), '-1'),
                     'IS_SUBORDINATED',
                     null,                               -- number_value
                     null,                               -- date_value
                     decode(issub, chr88, 'ДА', 'НЕТ'),  -- string_value
                     decode(issub, chr88, 'ДА', 'НЕТ'),  -- value
                     qb_dwh_utils.DateToChar(vdt),
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE);
commit;                     
    end if; -- Прочие ц/б
    -- Выгрузка в FCT_FINSTR_RATE
    for rec in (select * from (
                select fir.t_iso_number FINSTR_DENUMERATOR_FINSTR_CODE,
                       qb_dwh_utils.NumberToChar(rh.t_rate/power(10, rh.t_point), rh.t_point) FINSTR_RATE,
                       to_char(rh.t_scale) FINSTR_SCALE,
                       rd.t_type TYPE_FINSTR_RATE_TYPE_RATE_COD,
                       qb_dwh_utils.DateToChar(decode(rh.t_sincedate, emptDate, firstDate, rh.t_sincedate)) dt,
                       rh.t_sincedate
                  from dfininstr_dbt fi
                 inner join dratedef_dbt rd
                    on (fi.t_fiid = rd.t_otherfi)
                 inner join dfininstr_dbt fir
                    on (fir.t_fiid = rd.t_fiid)
                 inner join dratehist_dbt rh
                   on (rd.t_rateid = rh.t_rateid)
                 where fi.t_fiid = fiid
               union all
                select fir.t_iso_number FINSTR_NUMERATOR_FINSTR_CODE,
                       qb_dwh_utils.NumberToChar(rd.t_rate/power(10, rd.t_point), rd.t_point) FINSTR_RATE,
                       to_char(rd.t_scale) FINSTR_SCALE,
                       rd.t_type TYPE_FINSTR_RATE_TYPE_RATE_COD,
                       qb_dwh_utils.DateToChar(decode(rd.t_sincedate, emptDate, firstDate, rd.t_sincedate)) dt,
                       rd.t_sincedate
                  from dfininstr_dbt fi
                 inner join dratedef_dbt rd
                    on (fi.t_fiid = rd.t_otherfi)
                 inner join dfininstr_dbt fir
                    on (fir.t_fiid = rd.t_fiid)
                 where fi.t_fiid = fiid
               order by TYPE_FINSTR_RATE_TYPE_RATE_COD, t_sincedate)
               where to_date(dt,'dd-mm-yyyy') <= in_date)
    loop
          insert into ldr_infa_cb.fct_finstr_rate(finstr_numerator_finstr_code, finstr_denumerator_finstr_code, finstr_rate, finstr_scale, type_finstr_rate_type_rate_cod, dt,rec_status,sysmoment, ext_file)
                 values (vcode, rec.finstr_denumerator_finstr_code, rec.finstr_rate, rec.finstr_scale, rec.type_finstr_rate_type_rate_cod, rec.dt, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
    end loop;
    -- Вставка в FCT_SECEXCHANGE
    for rec in (select distinct ob.t_code CODE,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                    qb_dwh_utils.System_IBSO,
                                                     1,
                                                     2) EXCHANGE_CODE,
                       qb_dwh_utils.DateToChar(case when ob.t_bankdate < vdt then
                                                    vdt
                                               else
                                                    ob.t_bankdate
                                               end) dt
                 from dobjcode_dbt ob
                 where ob.t_objecttype = n9 and ob.t_codekind = n11 and ob.t_objectid = fiid /*and ob.t_state = n0*/)
    loop

      insert into ldr_infa_cb.fct_secexchange(code,security_code, exchange_code, dt, rec_status, sysmoment, ext_file)
             values(rec.code, vcode, rec.exchange_code, rec.dt, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;
    end loop;
  end;

  ------------------------------------------------------
  -- Выгрузка данных по ценным бумагам
  ------------------------------------------------------
  procedure export_Secur(in_department in number,
                         in_date       in date,
                         procid        in number) is
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
    cntFIID      pls_integer := 0;
    acc_code     varchar2(250);
    portf        varchar2(2);
    cnt_chglot   pls_integer;
    cnt          pls_integer;
--    uf4          varchar2(250);

  begin
    -- Установим начало выгрузки ц/б
    startevent(cEvent_EXPORT_Secur, procid, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             QB_DWH_EXPORT.cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDT, in_date);

    qb_dwh_export.InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка данных по ц/б',
                         2,
                         null,
                         null);

  -- Удалим лоты по которым мог измениться счет
  delete from qb_dwh_const4exp_val cv where cv.id = 24; commit;-- очистим список лотов по которым мог измениться счет
  insert into qb_dwh_const4exp_val
    (select 24,
            t.t_sumid
       from pkl_portfolio_accounts t
      inner join dpmwrtsum_dbt s
         on t.t_sumid = s.t_sumid
      where (select count(1)
               from (select 1
                       from v_scwrthistex
                      where t.t_sumid = t_sumid
                      group by t_portfolio)) > 1);
commit;                      
  delete from pkl_portfolio_accounts where t_sumid in (select cv.value
                                                         from qb_dwh_const4exp_val cv
                                                        where cv.id = 24);
commit;                                                        

  -- очистим времянку с лотами
  delete from dwh_histsum_tmp; commit;
  -- выбор не нулевых лотов по которым требуется выгрузка
  insert into dwh_histsum_tmp(sumid)
    (select distinct t_sumid
       from (select t.t_sumid,
                    t.t_changedate,
                    t.t_instance,
                    max(t.t_instance) over(partition by t.t_sumid, t.t_changedate) maxinstance,
                    max(t.t_changedate) over(partition by t.t_sumid) maxchangedate,
                    t.t_amount
               from v_scwrthistex t
              where t.t_changedate <= in_date
                and t.t_party = n_1     -- лоты банка
                and t.t_state in (select v.value
                                    from qb_dwh_const4exp c
                                   inner join qb_dwh_const4exp_val v
                                      on (c.id = v.id)
                                   where c.name = cLOT_STATE))
      where t_instance = maxinstance
        --and t_changedate = maxchangedate -- iSupport 533994
        and maxchangedate >= minDate -- последнее изменение лота входит в выгружаемый период
        and t_amount > n0            -- ненулевые лоты
     );
commit;     
  -->
  EXECUTE IMMEDIATE 'select * from dwh_histsum_tmp for update'; -- до окончания выгрузки ничего менять нельзя
  call_scwrthistex(in_date);
  EXECUTE IMMEDIATE 'select * from DWH_scwrthistex_TMP for update'; -- до окончания выгрузки ничего менять нельзя
  --<
  qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка остатков по прочим ц/б',
                     2,
                     null,
                     null);

  -- Цикл по отобранным лотам
  for rec in (with /* dlot as (select sumid from dwh_histsum_tmp ),
              lot0 as( select v.T_FIID,
                                    v.T_SUMID,
                                    v.T_PORTFOLIO,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_COST,
                                    v.T_STATE,
                                    v.T_PARENT,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.T_CORRINTTOEIR c2eps,
                                    decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                    v.T_RESERVAMOUNT c2rpbu,
                                    v.T_INCOMERESERV c2rpbu_nkd
                               from  v_scwrthistex v
                               where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                 and v.T_CHANGEDATE <= in_date
                                ),
                                */
              lot0 as (      select /*+ PARALLEL(4) */
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_PORTFOLIO,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_COST,
                                    v.T_STATE,
                                    v.T_PARENT,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                      ),
              lot as (select lot0.*,
                                           lag(t_state, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_state,
                                           lag(t_amount, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_amount,
                                           lag(t_portfolio, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_portfolio
                                      from lot0
                                      where t_instance = maxinstance),
              f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                         from lot where t_amount <> prev_amount or t_portfolio <> prev_portfolio or t_state <> prev_state),
              flot as (select f0lot.*,
                              case when nxt_change_date = t_changedate then
                                t_changedate
                              else
                                nxt_change_date - 1
                              end next_change_date,
                              case when nxt_change_date = t_changedate then
                                1
                              else
                                nxt_change_date - t_changedate
                              end cnt_days,
                              first_value(t_sum) over(partition by t_fiid, t_sumid order by t_changedate, t_instance) sum_buy,
                              first_value(t_amount) over(partition by t_fiid, t_sumid order by t_changedate, t_instance) cnt_buy
                         from f0lot),
              llot as (select flot.*,
                              ac.t_account acnt,
                              qb_dwh_utils.NumberTochar(flot.t_amount, 0) camount,
                              to_char(flot.t_fiid) || '#FIN' cfiid,
                              to_char(t_dealid) || '#TCK' cdealid,
                              (select sum(lnk.t_sumsale)
                                 from dpmwrtlnk_dbt lnk
                                where lnk.t_buyid = flot.t_sumid
                                  and lnk.t_createdate <= flot.t_changedate) sum_sale
                         from flot
                         left join pkl_portfolio_accounts ac
                           on (flot.t_sumid = ac.t_sumid and flot.t_state = ac.t_state and flot.t_changedate = ac.t_date and flot.t_parent = ac.t_parent)
                        )

              select t_fiid,
                     t_portfolio,
                     t_sumid,
                     t_sum,
                     qb_dwh_utils.NumberToChar(round(sum_buy, 2), 2) sum_buy,
                     cnt_buy,
                     qb_dwh_utils.NumberToChar(nvl(round(sum_sale, 2), 0), 2) sum_sale,
                     qb_dwh_utils.NumberToChar(round((cnt_buy - t_amount) * sum_buy / cnt_buy, 2), 2) sum_disp,
                     camount,
                     acnt,
                     cfiid,
                     cdealid,
                     llot.cnt_days,
                     llot.t_changedate,
                     llot.next_change_date,
                     llot.t_state,
                     llot.t_parent,
                     c2eps,
                     c2oku,
                     c2rpbu,
                     c2rpbu_nkd
                from llot
              )
    loop
      if (rec.acnt is null or rec.acnt = '-1') then --30.03.22 добавим переопределение для -1

        select count(*)
          into cnt_chglot
          from qb_dwh_const4exp_val cv
         where cv.id = 24
           and cv.value = rec.t_sumid;
        if (cnt_chglot > 0) then
          -- очистим лоты по которым мог измениться счет
          delete from pkl_portfolio_accounts pa where pa.t_sumid = rec.t_sumid; commit;
        end if;
        acc_code := qb_dwh_export_secur.GetAccountByLot(p_sumid => rec.t_sumid, p_date => rec.t_changedate);
        if (acc_code <> '-1') then
          begin
          insert into pkl_portfolio_accounts(t_sumid, t_account, t_date, t_state, t_parent)
                 values(rec.t_sumid, acc_code, rec.t_changedate, rec.t_state, rec.t_parent);
commit;                 
          exception
            when Dup_Val_On_Index then
              null;
          end;
        end if;
      else
        acc_code := rec.acnt;
      end if;
      if (rec.camount > 0) then -- iSupport 533994
        portf    := qb_dwh_export_secur.GetPortfolioMSFO(fiid => rec.t_fiid, portf => rec.t_portfolio, acc => acc_code , cdate => rec.t_changedate , sumid => rec.t_sumid );
        if (acc_code <> '-1') then
          acc_code := '0000#IBSOXXX#' || qb_dwh_utils.GetAccountUF4(acc => acc_code);
        end if;
        for drec in  (select level Next_day from dual
                                     connect by level <= rec.cnt_days )
        loop
          insert into ldr_infa_cb.fct_securityamount(amount, account_code, security_code, deal_code, sec_portfolio_code, lot_num, dt, rec_status, sysmoment,ext_file)
                values (rec.camount, acc_code, rec.cfiid, rec.cdealid, portf, qb_dwh_utils.NumberToChar(rec.t_sumid, 0), to_char(rec.t_changedate + drec.next_day - 1, 'dd-mm-yyyy'), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                
          insert into ldr_infa_cb.fct_sec_sell_result(deal_code, security_code, sec_portfolio_code, purchase_amount, sell_amount, sum_of_disposal, lot_num, dt, rec_status, sysmoment, ext_file)
                values (rec.cdealid, rec.cfiid, portf, rec.sum_buy, rec.sum_sale, rec.sum_disp, qb_dwh_utils.NumberToChar(rec.t_sumid, 0), to_char(rec.t_changedate + drec.next_day - 1, 'dd-mm-yyyy'), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                
          null;
        end loop;
        --commit;
      end if;
    end loop;

   -- Вставка в ass_sec_portfolio

   insert into ldr_infa_cb.ass_sec_portfolio(security_code, sec_portfolio_code, dt,rec_status, sysmoment, ext_file)
            with g as (select t.security_code,
                              t.sec_portfolio_code,
                              t.dt,
                              sum(to_number(t.amount)) lsum
                         from ldr_infa_cb.fct_securityamount t
                        group by t.security_code, t.sec_portfolio_code, t.dt),
                 r as (select g.*,
                              row_number() over (partition by security_code, sec_portfolio_code, dt order by to_number(lsum) desc) rn
                         from g),
                 f as (select r.*, lag (sec_portfolio_code, 1, 0) over( partition by security_code, sec_portfolio_code order by to_date(dt, 'dd-mm-yyyy')) prev_portf
                         from r where rn = 1 )
            select security_code,
                  sec_portfolio_code,
                  dt,
                  dwhRecStatus,
                  dwhSysMoment,
                  dwhEXT_FILE
             from f
            where f.sec_portfolio_code <> prev_portf;
commit;
    qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка общих данных по  ц/б',
                       2,
                       null,
                       null);


    for rec in (select fi.t_fiid, fi.t_avoirkind, 1 bill_kind                      -- собственные векселя
                  from dfininstr_dbt fi
                 where fi.t_avoirkind = n5
                    and exists ( select 1
                                   from dvsbanner_dbt bn
                                   inner join ddl_leg_dbt leg
                                      on ( bn.t_bcid = leg.t_dealid)
                                   inner join dvsordlnk_dbt lnk
                                      on (bn.t_bcid = lnk.t_bcid)
                                   inner join ddl_order_dbt ord
                                      on (lnk.t_contractid = ord.t_contractid and lnk.t_dockind = ord.t_dockind)
                                    where bn.t_fiid = fi.t_fiid and
                                      leg.t_legid = n0 and leg.t_legkind = n1)
                union all
                select fi.t_fiid, fi.t_avoirkind, 2 bill_kind                     -- учтенные векселя
                                  from dfininstr_dbt fi
                                 where fi.t_avoirkind = n5
                                    and exists ( select 1
                                                   from dvsbanner_dbt bn
                                                   inner join ddl_leg_dbt leg
                                                      on ( bn.t_bcid = leg.t_dealid)
                                                   inner join dvsordlnk_dbt lnk
                                                      on (bn.t_bcid = lnk.t_bcid)
                                                   inner join ddl_tick_dbt tick
                                                      on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                                                    where bn.t_fiid = fi.t_fiid and
                                                      leg.t_legid = n0 and leg.t_legkind = n1)
                union all
                select fi.t_fiid, fi.t_avoirkind, 0 bill_kind                        -- прочие ц/б
                                  from dfininstr_dbt fi
                                 where (((fi.t_fi_kind = n2
                                   and fi.t_issys = chr0
                                   and fi.t_avoirkind in (select v.value
                                                            from qb_dwh_const4exp c
                                                           inner join qb_dwh_const4exp_val v
                                                              on (c.id = v.id)
                                                           where c.name = cSECKIND_ALL)   -- виды ц/б для выгрузки прибиваем гвоздями как указано в ТЗ
                                   and fi.t_avoirkind <> 48 -- корзина ц/б
                                   )
                                    or (fi.t_fiid in ( select fi2.t_parentfi from dfininstr_dbt fi2  where fi2.t_avoirkind in (select v.value
                                                                                                                                from qb_dwh_const4exp c
                                                                                                                               inner join qb_dwh_const4exp_val v
                                                                                                                                  on (c.id = v.id)
                                                                                                                               where c.name = cSECKIND_RECEIPT)) and fi.t_fi_kind = 2))
                                   and (exists ( select 1
                                                  from ddl_tick_dbt tick
                                                 where tick.t_pfi = fi.t_fiid )))
                union
                select distinct fi.t_fiid, fi.t_avoirkind, decode(fi.t_avoirkind, 5, 1, 0) bill_kind     -- ц/б в корзине РЕПО
                                  from ddl_tick_ens_dbt b
                                 inner join dfininstr_dbt fi
                                    on (b.t_fiid = fi.t_fiid)
                                  inner join ddl_tick_dbt t
                                    on (b.t_dealid = t.t_dealid)
                                 where t.t_dealdate <= in_date
                union   --дополнение FIID для FCT_SEC_RATING                  
                select distinct avr.t_fiid, fi.t_avoirkind, decode(fi.t_avoirkind, 5, 1, 0) bill_kind
                                from sofr_rating_ratingshistory rh
                                inner join sofr_rating_listratings rl
                                      on (rh.rating_id = rl.rating_id)
                                inner join davoiriss_dbt avr
                                      on (rh.isin = avr.t_isin)
                                inner join dfininstr_dbt fi
                                      on (avr.t_fiid = fi.t_fiid)  
              
                 )
    loop
      cntFIID := cntFIID + 1;
      -- Запишем ценную бумагу по которой начата операция выгрузки
      qb_bp_utils.SetAttrValue(EventID, cFIID, rec.t_fiid, cntFIID);
      begin
        export_Secur(rec.t_fiid,
                     rec.t_avoirkind,
                     rec.bill_kind,
                     in_date,
                     dwhRecStatus,
                     dwhDT,
                     dwhSysMoment,
                     dwhEXT_FILE);
        --commit;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                               0,
                               cFIID,
                               rec.t_fiid);
      end;
    end loop;
    begin
      qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочников',
                   2,
                   null,
                   null);

    -- Выгрузка справочников
      -- Вставка в DET_EXCHANGE
      for rec in (with m as (select rd.t_market_place
                             from dratedef_dbt rd
                             group by rd.t_market_place)
                select
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                   qb_dwh_utils.System_IBSO,
                                                   1,
                                                   m.t_market_place) code,
                       pt.t_name name
                  from m
                 inner join dparty_dbt pt
                    on (m.t_market_place = pt.t_partyid)
                   )
      loop
        begin
          insert into ldr_infa_cb.det_exchange(code, name, dt, rec_status, sysmoment, ext_file)
                 values(rec.code, rec.name, qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        exception
          when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                                 0,
                                 null,
                                 null);
        end;
      end loop;
      -- Вставка в DET_TYPE_RATE
      for rec in (select to_char(rt.t_type) type_rate_code,
                         rt.t_typename type_rate_name
                    from dratetype_dbt rt)
      loop
        begin
          insert into ldr_infa_cb.det_type_rate(type_rate_code, type_rate_name, dt, rec_status, sysmoment, ext_file)
                 values(rec.type_rate_code, rec.type_rate_name, qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        exception
         when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                                 0,
                                 null,
                                 null);
       end;
      end loop;
      -- Вставка в ASS_RATE_EXCHANGE
      for rec in (select distinct to_char(rd.t_type) type_rate_code,
                         qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     rd.t_market_place) exchange_code
                    from dratedef_dbt rd
                   where rd.t_market_place is not null and rd.t_market_place > n0
                  )
      loop
        begin
          insert into ldr_infa_cb.ass_rate_exchange(type_rate_code, exchange_code, dt, rec_status, sysmoment, ext_file)
                 values(rec.type_rate_code, rec.exchange_code, dwhDT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        exception
         when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                                 0,
                                 null,
                                 null);
       end;
      end loop;
    --Вставка в det_security_type_711
    insert into ldr_infa_cb.det_security_type_711(code_type, name_type, dt, sysmoment, rec_status, ext_file)
           values ('-1', 'Не определен', qb_dwh_utils.DateToChar(firstDate), dwhSysMoment, dwhRecStatus, dwhEXT_FILE);
commit;           
    --Вставка в DET_PROCBASE

    insert into ldr_infa_cb.det_procbase(code, name, days_year, days_month, sign_31, first_day, last_day, null_mainsum, type_prc_charge)
    select '9999#SOFRXXX#360/0', 'Календарь/360', 360, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#366/0', 'Календарь/Календарь', 365, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#360#S', 'Календарь/360 (сложный %%)', 360, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#366#S', 'Календарь/Календарь (сложный %%)', 365, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30/365', '30/365', 365, 30 , 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#-1', 'Не определено', 365, 1, 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#360_F', 'Календарь/360 c начислением за первый день', 360, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#1', 'Ежедневная процентная ставка', 1, 1, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#366', 'Календарь/Календарь', 366, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#365', 'Календарь/365', 365, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#360', 'Календарь/360', 360, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30/360', '30/360', 360, 30, 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#31/360', '30/360 с учетом 31 числа, если кредит лежит неполный месяц', 360, 30, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30/366', '30/Календарь', 366, 30, 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#31/366', '30/Календарь с учетом 31 числа, если кредит лежит неполный месяц', 366, 30, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#Act/по_купонным_периодам', 'В году по куп. периодам, в мес. по календарю', 0, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#Act/365L', 'В году по оконч.куп.пер, в мес. по календарю', 365, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#Act/364', '364 дня в году, в месяце по календарю', 364, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30E/360', '360 дней в году, 30 в месяце (Eurobond)', 360, 30, 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#Act/Act_ICMA', 'Actual/Actual (ICMA)', 366, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30/360_ISDA', 'Календарь/360', 360, 30, 0, 1, 0, 0, 0
      from dual;
commit;      
    update ldr_infa_cb.det_procbase pb
       set pb.dt = qb_dwh_utils.DateToChar(firstDate),
           pb.sysmoment = dwhSysMoment,
           pb.rec_status = dwhRecStatus,
           pb.ext_file = dwhEXT_FILE;
commit;           



      qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка счетов по ц/б',
                         2,
                         null,
                         null);

      -- Вставка в DET_ROLEACCOUNT_DEAL
--> AS 2020-05-12
/*      for rec in (select distinct id, '0000#SOFR#' || t_code as rolecode, name */
      for rec in (with fi as (select /*+ materialize */ t_fiid
                                                         from dfininstr_dbt fi
                                                       where ( fi.t_fi_kind = n2 and exists (select 1
                                                                                                  from ddl_tick_dbt
                                                                                                 where t_pfi = fi.t_fiid))
                                                                or ( fi.t_fiid in (select fi2.t_parentfi
                                                                                     from dfininstr_dbt fi2
                                                                                    where fi2.t_avoirkind in
                                                                                        (select v.value
                                                                                           from qb_dwh_const4exp  c
                                                                                             inner join qb_dwh_const4exp_val v on (c.id = v.id)
                                                                                          where c.name = cseckind_receipt)) and fi.t_fi_kind = 2))
                  select /*+ PARALLEL(4)*/ distinct id, t_code as rolecode, name
                    from (select /*+ ordered index(CATACC DMCACCDOC_DBT_IDX4)*/  cat.t_id id, cat.t_code, cat.t_name name
                            from dmcaccdoc_dbt catacc
                           inner join dmccateg_dbt cat
                              on (catacc.t_catid = cat.t_id)
                           where t_dockind = n164
                             and catacc.t_activatedate <
                                 decode(catacc.t_disablingdate,
                                        emptDate,
                                        maxDate,
                                        catacc.t_disablingdate)
                          union all
                          select cat.t_id, cat.t_code, cat.t_name
                            from dmccateg_dbt cat
                           where 
                            exists (select /*+ index(catacc DMCACCDOC_DBT_USR6)*/ 1
                                      from dmcaccdoc_dbt catacc
                                      join fi on fi.t_fiid = catacc.t_fiid
                                     where catacc.t_catid = cat.t_id
                                           /*and exists (select t_fiid
                                                        from dfininstr_dbt fi
                                                       where fi.t_fiid = catacc.t_fiid
                                                           and ( ( fi.t_fi_kind = n2 and exists (select 1
                                                                                                  from ddl_tick_dbt
                                                                                                 where t_pfi = fi.t_fiid))
                                                                or ( fi.t_fiid in (select fi2.t_parentfi
                                                                                     from dfininstr_dbt fi2
                                                                                    where fi2.t_avoirkind in
                                                                                        (select v.value
                                                                                           from qb_dwh_const4exp  c
                                                                                             inner join qb_dwh_const4exp_val v on (c.id = v.id)
                                                                                          where c.name = cseckind_receipt)) and fi.t_fi_kind = 2)))*/
                                           and catacc.t_activatedate <
                                               decode (catacc.t_disablingdate,
                                                       emptdate, 
                                                       maxdate,
                                                       catacc.t_disablingdate))
                            union all
                            ---
                            select /*+ leading(cat ) */ cat.t_id + n5000  id,
                                   cat.t_code||(select '#' || t_code from dllvalues_dbt where t_list = n3503 and t_element = templ.t_value1) as rolecode,
                                   substr(cat.t_name || ' ( ' ||  lv.t_name || ' )', 1,250) as name
                              from dmcaccdoc_dbt catacc
                             inner join dmccateg_dbt cat
                                    on (catacc.t_catid = cat.t_id)
                             inner join dllvalues_dbt lv
                                    on (lv.t_list = n3503)
                             left join dmctempl_dbt templ
                                on (catacc.t_catid = templ.t_catid and catacc.t_templnum = templ.t_number)
                             where  CAT.T_NUMBER in (n1492) -- catacc.t_catnum in (n1492) для  1492 работает 
                               and templ.t_value1 >= 0 and rownum = 1
                                        )
                   union all
                   /*
                   select -1, '0000#SOFR#Начисл.ПДД, УВ_П', 'Начисленный процентный доход УВ' from dual
                   union all
                   select -2, '0000#SOFR#Начисл.ПДД, УВ_Д', 'Начисленный дисконтный доход УВ' from dual
                   union all
                   select -3, '0000#SOFR#Начисл.ПДД, ц/б_П', 'Начисленный процентный доход ц/б' from dual
                   union all
                   select -4, '0000#SOFR#Начисл.ПДД, ц/б_Д', 'Начисленный дисконтный доход ц/б' from dual
                   union all
                   select -4 - rownum, '0000#SOFR#' || cat.t_code || '#' || lv.t_code roleaccount_deal_code,
                   */
                   select -1, 'Начисл.ПДД, УВ_П', 'Начисленный процентный доход УВ' from dual
                   union all
                   select -2, 'Начисл.ПДД, УВ_Д', 'Начисленный дисконтный доход УВ' from dual
                   union all
                   select -3, 'Начисл.ПДД, ц/б_П', 'Начисленный процентный доход ц/б' from dual
                   union all
                   select -4, 'Начисл.ПДД, ц/б_Д', 'Начисленный дисконтный доход ц/б' from dual
                   --union all
                   --select -5, 'Просроч_вексель#ПД', 'Просроченный вексель' from dual -- по этой строке дубль по ключу CODE, DT
                   union all
                   select -4 - rownum, lv.t_code roleaccount_deal_code,
                          substr(cat.t_name || ' ( ' ||  lv.t_name || ' )', 1,250)
                     from dmccateg_dbt cat
                    inner join dllvalues_dbt lv
                       on (lv.t_list = n3503)
                    where cat.t_number = n1492
                  )
      loop
        begin
          insert into ldr_infa_cb.det_roleaccount_deal(code, name, orole_code, dt, rec_status, sysmoment, ext_file)
                 values(rec.rolecode, rec.name,'0', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        exception
         when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                                 0,
                                 null,
                                 null);
       end;
      end loop;
      -- Вставка в DET_SEC_PORTFOLIO
      begin
        insert into ldr_infa_cb.det_sec_portfolio(code, name, dt, rec_status, sysmoment, ext_file)
               values('1', 'Ценные бумаги, которые удерживаются до срока погашения', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
        insert into ldr_infa_cb.det_sec_portfolio(code, name, dt, rec_status, sysmoment, ext_file)
               values('2', 'Ценные бумаги, которые используются для удержания и торговли', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
        insert into ldr_infa_cb.det_sec_portfolio(code, name, dt, rec_status, sysmoment, ext_file)
               values('3', 'Ценные бумаги, которые используются для торговли', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      exception
       when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                               0,
                               null,
                               null);
      end;
      -- Вставка в FCT_SECURITYAMMOUNT
    end;
    -- Вставка в DET_SECURITY_ATTR
    qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка атрибутов ц/б' ,
                       2,
                       null,
                       null);

    begin
      insert into ldr_infa_cb.det_security_attr
          (SELECT DISTINCT TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE,
                          UPPER(TRIM(GR.T_NAME)) NAME,
                          DECODE(GR.T_TYPE, CHR(88), '0', '1') MULTYVALUE,
                          qb_dwh_utils.DateToChar(firstDate),
                          dwhRecStatus,
                          dwhSysMoment,
                          dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
          INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
          WHERE AC.T_OBJECTTYPE IN (n12, n24)
            and  not (gr.t_groupid = n62 and gr.t_objecttype = n12)
            and  not (gr.t_groupid = n101 and gr.t_objecttype = n24)
          UNION ALL
          SELECT DISTINCT TO_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                          UPPER(TRIM(NK.T_NAME)) NAME,
                          '0' MULTYVALUE,
                          qb_dwh_utils.DateToChar(firstDate),
                          dwhRecStatus,
                          dwhSysMoment,
                          dwhEXT_FILE
            FROM DNOTETEXT_DBT NT
          INNER JOIN DNOTEKIND_DBT NK
              ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND NT.T_NOTEKIND = NK.T_NOTEKIND)
          WHERE NT.T_OBJECTTYPE IN (n12, n24)
          union all
          select 'DATE_OFFER',
                 'Дата оферты',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'IS_SUBORDINATED',
                 'Признак субординированности ценной бумаги',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'IS_PROBLEM_RESTRUCTURING',
                 'Признак проблемной реструктуризации',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'DATE_PROBLEM_RESTRUCTURING',
                 'Дата переноса остатка на б/счет 50505',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'COUNTRY',
                 'Страна резидентности эмитента',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'COUNTRY_SO',
                 'Страновая оценка',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'ID_DIASOFT',
                 'ИД Клиента в АС Diasoft Fa#',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'ID_SOFR',
                 'ИД Клиента в СОФР',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'IS_ACTIVE_MARKET',
                 'Признак активного рынка',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'LEVEL_HIERARCHY',
                 'Уровень иерархии справедливой стоимости',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          );
commit;          
    exception
     when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    -- Добавление атрибутов по проблемной реструктиризации
    insert into ldr_infa_cb.fct_security_attr
      (SELECT security_code,
              nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio sp
                           where sp.security_code = security_code
                             and to_date(sp.dt, 'dd-mm-yyyy') <= in_date)
                   where rnk = 1),
                  '-1') portf,
              'IS_PROBLEM_RESTRUCTURING' CODE_security_attr,
              null number_value,
              null date_value,
              'X' string_value,
              'X' value,
              qb_dwh_utils.DateToChar(min(rdate)) dt,
              dwhRecStatus,
              dwhSysMoment,
              dwhEXT_FILE
         from (select /*+ index(rd, DRESTDATE_DBT_IDX0) */
                ca.account_code,
                ca.security_code,
                acc.t_accountid accid,
                rd.t_restdate rdate,
                decode(acc.t_code_currency, 0, rd.t_rest, rd.t_restcurrency) rest,
                row_number() over(partition by rd.t_accountid, rd.t_restcurrency order by rd.t_restdate desc nulls last) rnk
                 from ldr_infa_cb.ass_accountsecurity ca
                inner join daccount_dbt acc
                   on (substr(ca.account_code, 14, 50) = acc.t_userfield4)
                inner join drestdate_dbt rd
                   on (acc.t_accountid = rd.t_accountid and
                      acc.t_code_currency = rd.t_restcurrency)
                where substr(acc.t_account, 14, 5) = v50505
                  and rd.t_restdate <= in_date)
        where rnk = n1
          and rest <> n0
        group by security_code);
commit;
    insert into ldr_infa_cb.fct_security_attr
      (SELECT security_code,
              nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio sp
                           where sp.security_code = security_code
                             and to_date(sp.dt, 'dd-mm-yyyy') <= in_date)
                   where rnk = 1),
                  '-1') portf,
              'DATE_PROBLEM_RESTRUCTURING' CODE_security_attr,
              null number_value,
              qb_dwh_utils.DateToChar(min(rdate)) date_value,
              null string_value,
              qb_dwh_utils.DateToChar(min(rdate)) value,
              qb_dwh_utils.DateToChar(min(rdate)) dt,
              dwhRecStatus,
              dwhSysMoment,
              dwhEXT_FILE
         from (select /*+ index(rd, DRESTDATE_DBT_IDX0) */
                ca.account_code,
                ca.security_code,
                acc.t_accountid accid,
                rd.t_restdate rdate,
                decode(acc.t_code_currency, 0, rd.t_rest, rd.t_restcurrency) rest,
                row_number() over(partition by rd.t_accountid, rd.t_restcurrency order by rd.t_restdate desc nulls last) rnk
                 from ldr_infa_cb.ass_accountsecurity ca
                inner join daccount_dbt acc
                   on (substr(ca.account_code, 14, 50) = case
                                                            when (acc.t_userfield4 is null) or
                                                                (acc.t_userfield4 = chr(0)) or
                                                                (acc.t_userfield4 = chr(1)) or
                                                                (acc.t_userfield4 like '0x%') then
                                                              acc.t_account
                                                            else
                                                              acc.t_userfield4
                                                         end)
                inner join drestdate_dbt rd
                   on (acc.t_accountid = rd.t_accountid and
                      acc.t_code_currency = rd.t_restcurrency)
                where substr(ca.account_code, 14, 5) = v50505
                  and rd.t_restdate <= in_date)
        where rnk = n1
          and rest <> n0
        group by security_code);
commit;
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при добавлении атрибутов по проблемной реструктиризации: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;

    begin
    for rec in (with iss as(
                select finstr_code,
                       dt,
                       portf,
                       issuer_code,
                       num_code,
                       str_pref,
                       num_code code101,
                       case
                         when pcis.t_partyid is not null then
                              pcis.t_partyid
                         when pcis1.t_partyid is not null then
                              pcis1.t_partyid
                         when pcis_bis.t_partyid is not null then
                              pcis_bis.t_partyid
                         when pcis1_bis.t_partyid is not null then
                              pcis1_bis.t_partyid
                       end partyid
                       --nvl(pcis.t_partyid, pcis1.t_partyid) partyid
                  from (
                select distinct code finstr_code,
                       dt,
                       nvl((select sec_portfolio_code
                                           from (select sec_portfolio_code,
                                                        row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                                                   from ldr_infa_cb.ass_sec_portfolio sp
                                                  where sp.security_code = code
                                                    and to_date(sp.dt, 'dd-mm-yyyy') <= in_date)
                                          where rnk = 1),
                                         '-1') portf,
                       issuer_code, regexp_replace(issuer_code, '(^.*IBSOXXX#)(\d*)(.*$)','\2') num_code,
                       case when regexp_like(issuer_code, '#banks$') then
                             '00#Б#'
                            when regexp_like(issuer_code, '#cust_corp$') then
                             '00#Y#'
                            when regexp_like(issuer_code, '#person$') then
                             '00#P#'
                            else
                             '!!!'
                            end ||
                       regexp_replace(issuer_code, '(^.*BISQUIT#)(\d*)(.*$)','\2') num_code2,
                       regexp_replace(issuer_code, '(^.*IBSOXXX#)(\d*)(.*$)','\3') str_pref
                  from ldr_infa_cb.det_security
                 where issuer_code <> '-1'
                ) ds
                  left join dpartcode_dbt pcis
                    on (num_code = pcis.t_code and pcis.t_codekind = 101 and pcis.t_state = 0)
                  left join dpartcode_dbt pcis1
                    on (num_code = pcis1.t_code and pcis1.t_codekind = 1101 and pcis1.t_state = 0)
                  left join dpartcode_dbt pcis_bis
                    on (num_code2 = pcis_bis.t_code and pcis_bis.t_codekind = 101 and pcis_bis.t_state = 0)
                  left join dpartcode_dbt pcis1_bis
                    on (num_code2 = pcis1_bis.t_code and pcis1_bis.t_codekind = 1101 and pcis1_bis.t_state = 0)
                 ),
                 iss_c as (select iss.finstr_code
                                 ,iss.dt
                                 ,iss.portf
                                 ,iss.partyid
                                 ,decode(pt.t_nrcountry, chr(1), (select distinct t_country from dadress_dbt adr where adr.t_partyid = iss.partyid and rownum = 1), pt.t_nrcountry) country
                  from iss
                   left join dadress_dbt adr
                     on (iss.partyid = adr.t_partyid and adr.t_type = n1)

                  inner join dparty_dbt pt
                    on (iss.partyid = pt.t_partyid)
                )
                select iss_c.*,
                       iss_c.country || ' ' || c.t_name code_name,
                       replace(c.t_riskclass, chr(0)) risk,
                       pc.t_code code102
                  from iss_c
                left join dcountry_dbt c
                  on (iss_c.country = c.t_codelat3)
                left join dpartcode_dbt pc
                  on (iss_c.partyid = pc.t_partyid and pc.t_codekind = n102 and pc.t_state = n0)
              )
    loop
      if rec.code_name is not null then
        insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
               values(rec.finstr_code,
                      rec.portf,
                      'COUNTRY',
                      null,          -- number_value
                      null,          -- date_value
                      rec.code_name, -- string_value
                      rec.code_name, -- value
                      rec.dt,
                      dwhRecStatus,
                      dwhSysMoment,
                      dwhEXT_FILE);
commit;                      
      end if;
      if rec.risk is not null then
        insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
               values(rec.finstr_code,
                      rec.portf,
                      'COUNTRY_SO',
                      rec.risk,      -- number_value
                      null,          -- date_value
                      null,          -- string_value
                      rec.risk,      -- value
                      rec.dt,
                      dwhRecStatus,
                      dwhSysMoment,
                      dwhEXT_FILE);
commit;                      
      end if;
      if rec.code102 is not null then
        insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
               values(rec.finstr_code,
                      rec.portf,
                      'ID_DIASOFT',
                      null,          -- number_value
                      null,          -- date_value
                      rec.code102, -- string_value
                      rec.code102, -- value
                      rec.dt,
                      dwhRecStatus,
                      dwhSysMoment,
                      dwhEXT_FILE);
commit;                      
      end if;
      if rec.partyid is not null then
        insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
               values(rec.finstr_code,
                      rec.portf,
                      'ID_SOFR',
                      qb_dwh_utils.NumberToChar(rec.partyid, 0),          -- number_value
                      null,          -- date_value
                      null,          -- string_value
                      qb_dwh_utils.NumberToChar(rec.partyid, 0),          -- value
                      rec.dt,
                      dwhRecStatus,
                      dwhSysMoment,
                      dwhEXT_FILE);
commit;                      
      end if;
    end loop;

    for rec in (select code,
                       r1.sdate1,
                       r1001.sdate1001,
                       case when r1.sdate1 is not null and r1001.sdate1001 is null then
                              'ДА'
                            else
                              'НЕТ'
                       end IS_ACTIVE_MARKET,
                       case when r1001.sdate1001 is not null then
                              '2'
                            else
                              '1'
                       end LEVEL_HIERARCHY,
                       dt,
                       nvl((select sec_portfolio_code
                              from (select sec_portfolio_code,
                                           row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                                      from ldr_infa_cb.ass_sec_portfolio sp
                                     where sp.security_code = code
                                       and to_date(sp.dt, 'dd-mm-yyyy') <= in_date)
                             where rnk = n1),
                            '-1') portf
                  from ldr_infa_cb.det_security
                 inner join dfininstr_dbt fi
                    on (to_number(regexp_replace(code, '^(\d*)(#FIN)$', '\1')) = fi.t_fiid)
                  left join (select fiid, max(sdate) sdate1
                               from (select rd.t_type rtype,
                                            rd.t_otherfi fiid,
                                            rd.t_sincedate sdate
                                       from dratedef_dbt rd
                                      where rd.t_type = n1
                                     union all
                                     select rd.t_type rtype,
                                            rd.t_otherfi fiid,
                                            rh.t_sincedate
                                       from dratedef_dbt rd
                                       inner join dratehist_dbt rh
                                         on (rd.t_rateid = rh.t_rateid)
                                      where rd.t_type = n1)
                                where sdate <= in_date
                                group by fiid) r1
                     on (fi.t_fiid = r1.fiid)
                  left join (select fiid, max(sdate) sdate1001
                               from (select rd.t_type rtype,
                                            rd.t_otherfi fiid,
                                            rd.t_sincedate sdate
                                       from dratedef_dbt rd
                                      where rd.t_type = n1001
                                     union all
                                     select rd.t_type rtype,
                                            rd.t_otherfi fiid,
                                            rh.t_sincedate
                                       from dratedef_dbt rd
                                       inner join dratehist_dbt rh
                                         on (rd.t_rateid = rh.t_rateid)
                                      where rd.t_type = n1001)
                                where sdate <= in_date
                                group by fiid) r1001
                      on (fi.t_fiid = r1001.fiid)
                  where code like '%#FIN'
                    and (r1.sdate1 is not null or r1001.sdate1001 is not null)
                 order by fi.t_fiid
                )
    loop
      insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
             values(rec.code,
                    rec.portf,
                    'IS_ACTIVE_MARKET',
                    null,                          -- number_value
                    null,                          -- date_value
                    rec.is_active_market,          -- string_value
                    rec.is_active_market,          -- value
                    rec.dt,
                    dwhRecStatus,
                    dwhSysMoment,
                    dwhEXT_FILE);
commit;                    
      insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
             values(rec.code,
                    rec.portf,
                    'LEVEL_HIERARCHY',
                    rec.level_hierarchy,           -- number_value
                    null,                          -- date_value
                    null,                          -- string_value
                    rec.level_hierarchy,           -- value
                    rec.dt,
                    dwhRecStatus,
                    dwhSysMoment,
                    dwhEXT_FILE);
commit;                    
    end loop;
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при добавлении доп.атрибутов: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;

    -- Вставка характеристик лотов
    qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка характеристик лотов',
                       2,
                       null,
                       null);
    begin
      insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
      (select * from (
              with /*dlot as (select sumid from dwh_histsum_tmp  ),
                            lot0 as( select v.T_FIID,
                                                  v.T_SUMID,
                                                  v.T_CHANGEDATE,
                                                  v.T_TIME,
                                                  v.T_AMOUNT,
                                                  v.T_SUM,
                                                  v.T_DEALID,
                                                  v.T_INSTANCE,
                                                  max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                  v.T_CORRINTTOEIR c2eps,
                                                  decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                                  v.T_RESERVAMOUNT c2rpbu,
                                                  v.T_INCOMERESERV c2rpbu_nkd
                                             from  v_scwrthistex v
                                             where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                               and v.T_CHANGEDATE <= in_date
                                              ),*/
                             lot0 as (select /*+ PARALLEL(4) */
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                                      ),
                            lot as (select lot0.*,
                                           lag(c2eps, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_c2eps
                                      from lot0
                                      where t_instance = maxinstance),
                            f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                       from lot where c2eps <> prev_c2eps),
                            flot as (select f0lot.*,
                                            case when nxt_change_date = t_changedate then
                                              t_changedate
                                            else
                                              nxt_change_date - 1
                                            end next_change_date,
                                            case when nxt_change_date = t_changedate then
                                              1
                                            else
                                              nxt_change_date - t_changedate
                                            end cnt_days
                                       from f0lot),
                            llot as (select flot.*,
                                            to_char(flot.t_fiid) || '#FIN' cfiid,
                                            to_char(t_dealid) || '#TCK' cdealid
                                       from flot
                                      )
                            select cdealid,
                                   cfiid,
                                   to_char(t_sumid),
                                   '1' adjtype,
                                   qb_dwh_utils.NumberToChar(c2eps, 2) sum,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                                   qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                                   dwhRecStatus,
                                   dwhSysMoment,
                                   dwhEXT_FILE
                              from llot --where cfiid = '1172#FIN' --!!!!!!!!!!!!
                              )
      union all
      select * from (
              with /* dlot as (select sumid from dwh_histsum_tmp  ),
                            lot0 as( select v.T_FIID,
                                                  v.T_SUMID,
                                                  v.T_CHANGEDATE,
                                                  v.T_TIME,
                                                  v.T_AMOUNT,
                                                  v.T_SUM,
                                                  v.T_DEALID,
                                                  v.T_INSTANCE,
                                                  max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                  v.T_CORRINTTOEIR c2eps,
                                                  decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                                  v.T_RESERVAMOUNT c2rpbu,
                                                  v.T_INCOMERESERV c2rpbu_nkd
                                             from  v_scwrthistex v
                                             where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                               and v.T_CHANGEDATE <= in_date
                                              ),*/
                             lot0 as (select /*+ PARALLEL(4) */
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                                      ),
                            lot as (select lot0.*,
                                           lag(c2oku, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_c2oku
                                      from lot0
                                      where t_instance = maxinstance),
                            f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                       from lot where c2oku <> prev_c2oku),
                            flot as (select f0lot.*,
                                            case when nxt_change_date = t_changedate then
                                              t_changedate
                                            else
                                              nxt_change_date - 1
                                            end next_change_date,
                                            case when nxt_change_date = t_changedate then
                                              1
                                            else
                                              nxt_change_date - t_changedate
                                            end cnt_days
                                       from f0lot),
                            llot as (select flot.*,
                                            to_char(flot.t_fiid) || '#FIN' cfiid,
                                            to_char(t_dealid) || '#TCK' cdealid
                                       from flot
                                      )
                            select cdealid,
                                   cfiid,
                                   to_char(t_sumid) sumid,
                                   '2' adjtype,
                                   qb_dwh_utils.NumberToChar(c2oku, 2) sum,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                                   qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                                   dwhRecStatus,
                                   dwhSysMoment,
                                   dwhEXT_FILE
                              from llot --where cfiid = '1172#FIN'
                              )
      union all
      select * from (
              with /* dlot as (select sumid from dwh_histsum_tmp  ),
                            lot0 as( select v.T_FIID,
                                                  v.T_SUMID,
                                                  v.T_CHANGEDATE,
                                                  v.T_TIME,
                                                  v.T_AMOUNT,
                                                  v.T_SUM,
                                                  v.T_DEALID,
                                                  v.T_INSTANCE,
                                                  max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                  v.T_CORRINTTOEIR c2eps,
                                                  decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                                  v.T_RESERVAMOUNT c2rpbu,
                                                  v.T_INCOMERESERV c2rpbu_nkd
                                             from  v_scwrthistex v
                                             where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                               and v.T_CHANGEDATE <= in_date
                                              ),*/
                             lot0 as (select /*+ PARALLEL(4) */
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                                      ),
                            lot as (select lot0.*,
                                           lag(c2rpbu, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_c2rpbu
                                      from lot0
                                      where t_instance = maxinstance),
                            f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                       from lot where c2rpbu <> prev_c2rpbu),
                            flot as (select f0lot.*,
                                            case when nxt_change_date = t_changedate then
                                              t_changedate
                                            else
                                              nxt_change_date - 1
                                            end next_change_date,
                                            case when nxt_change_date = t_changedate then
                                              1
                                            else
                                              nxt_change_date - t_changedate
                                            end cnt_days
                                       from f0lot),
                            llot as (select flot.*,
                                            to_char(flot.t_fiid) || '#FIN' cfiid,
                                            to_char(t_dealid) || '#TCK' cdealid
                                       from flot
                                      )
                            select cdealid,
                                   cfiid,
                                   to_char(t_sumid) sumid,
                                   '3' adjtype,
                                   qb_dwh_utils.NumberToChar(c2rpbu, 2) sum,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                                   qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                                   dwhRecStatus,
                                   dwhSysMoment,
                                   dwhEXT_FILE
                              from llot --where cfiid = '1172#FIN'
                              )

      union all
      select * from (
              with /* dlot as (select sumid from dwh_histsum_tmp  ),
                            lot0 as( select v.T_FIID,
                                                  v.T_SUMID,
                                                  v.T_CHANGEDATE,
                                                  v.T_TIME,
                                                  v.T_AMOUNT,
                                                  v.T_SUM,
                                                  v.T_DEALID,
                                                  v.T_INSTANCE,
                                                  max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                  v.T_CORRINTTOEIR c2eps,
                                                  decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                                  v.T_RESERVAMOUNT c2rpbu,
                                                  v.T_INCOMERESERV c2rpbu_nkd
                                             from  v_scwrthistex v
                                             where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                               and v.T_CHANGEDATE <= in_date
                                              ),*/
                             lot0 as (select /*+ PARALLEL(4) */
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                                      ),
                            lot as (select lot0.*,
                                           lag(c2rpbu_nkd, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_c2rpbu_nkd
                                      from lot0
                                      where t_instance = maxinstance),
                            f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                       from lot where c2rpbu_nkd <> prev_c2rpbu_nkd),
                            flot as (select f0lot.*,
                                            case when nxt_change_date = t_changedate then
                                              t_changedate
                                            else
                                              nxt_change_date - 1
                                            end next_change_date,
                                            case when nxt_change_date = t_changedate then
                                              1
                                            else
                                              nxt_change_date - t_changedate
                                            end cnt_days
                                       from f0lot),
                            llot as (select flot.*,
                                            to_char(flot.t_fiid) || '#FIN' cfiid,
                                            to_char(t_dealid) || '#TCK' cdealid
                                       from flot
                                      )
                            select cdealid,
                                   cfiid,
                                   to_char(t_sumid) sumid,
                                   '10' adjtype,
                                   qb_dwh_utils.NumberToChar(c2rpbu_nkd, 2) sum,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                                   qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                                   dwhRecStatus,
                                   dwhSysMoment,
                                   dwhEXT_FILE
                              from llot --where cfiid = '1172#FIN'
                              )
      );
commit;      
    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
       (select * from (
        with /* dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                              max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(bonus, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_bonus
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where bonus <> prev_bonus),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '8' adjtype,
                             qb_dwh_utils.NumberToChar(bonus, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(begbonus, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_begbonus

                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where begbonus <> prev_begbonus),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '7' adjtype,
                             qb_dwh_utils.NumberToChar(begbonus, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(nkd, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_nkd
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where nkd <> prev_nkd),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '4' adjtype,
                             qb_dwh_utils.NumberToChar(nkd, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(discount, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_discount
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where discount <> prev_discount),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '5' adjtype,
                             qb_dwh_utils.NumberToChar(discount, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(interest, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_interest
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where interest <> prev_interest),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '6' adjtype,
                             qb_dwh_utils.NumberToChar(interest, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(over, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_over
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where over <> prev_over),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '9' adjtype,
                             qb_dwh_utils.NumberToChar(over, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot));
commit;                        
      -- Вставка дисконта к номиналу на дату покупки
      insert into ldr_infa_cb.fct_sec_adjustment
        (deal_code,
         finstr_code,
         lot_num,
         adjustment_type,
         amount,
         dt_begin,
         dt_end,
         dt,
         rec_status,
         sysmoment,
         ext_file)
        (select *
           from (with /*dlot as (select sumid
                                 from dwh_histsum_tmp),
                                                        lot0 as (select v.t_fiid,
                                                                        v.t_sumid,
                                                                        v.t_portfolio,
                                                                        v.t_changedate,
                                                                        v.t_time,
                                                                        v.t_amount,
                                                                        v.t_sum,
                                                                        v.t_cost,
                                                                        v.t_state,
                                                                        v.t_parent,
                                                                        v.t_dealid,
                                                                        v.t_instance,
                                                                        max(v.t_instance) over(partition by v.t_sumid, v.t_changedate) maxinstance,
                                                                        v.t_corrinttoeir c2eps,
                                                                        decode(v.t_portfolio,
                                                                               5,
                                                                               v.t_correstreserve,
                                                                               v.t_estreserve) c2oku,
                                                                        v.t_reservamount c2rpbu,
                                                                        v.t_incomereserv c2rpbu_nkd
                                                                   from v_scwrthistex v
                                                                  where exists
                                                                  (select 1
                                                                           from dlot
                                                                          where sumid =
                                                                                v.t_sumid)
                                                                    and v.t_changedate <=
                                                                        in_date), */
                                                     lot0 as (      select /*+ PARALLEL(4) */
                                                                           v.T_FIID,
                                                                           v.T_SUMID,
                                                                           v.T_PORTFOLIO,
                                                                           v.T_CHANGEDATE,
                                                                           v.T_TIME,
                                                                           v.T_AMOUNT,
                                                                           v.T_SUM,
                                                                           v.T_COST,
                                                                           v.T_STATE,
                                                                           v.T_PARENT,
                                                                           v.T_DEALID,
                                                                           v.T_INSTANCE,
                                                                           max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                                           v.c2eps,
                                                                           v.c2oku,
                                                                           v.c2rpbu,
                                                                           v.c2rpbu_nkd,
                                                                           v.begdiscount
                                                                      from DWH_scwrthistex_TMP v
                                                                     where v.T_CHANGEDATE <= in_date
                                                                                               ),
                                                                                  lot as (select lot0.*,
                                                                                                  lag(t_amount,
                                                                                                      1,
                                                                                                      0) over(partition by t_sumid order by t_changedate, t_instance) prev_amount,
                                                                                                  lag(begdiscount, --15.04.2022 добавлена проверка изменений дисконта, т.к. он может меняться при неизменном количестве ЦБ 
                                                                                                      1,
                                                                                                      0) over(partition by t_sumid order by t_changedate, t_instance) prev_begdiscount    
                                                                                             from lot0
                                                                                            where t_instance = maxinstance), 
                                                                                                                           f0lot as (select lot.*,
                                                                                                                                 lead(t_changedate,
                                                                                                                                      1,
                                                                                                                                      in_date + 1) over(partition by t_sumid order by t_changedate, t_instance) nxt_change_date
                                                                                                                            from lot
                                                                                                                           where t_amount <> prev_amount or begdiscount <> prev_begdiscount ), 
                                                                                                                                                            flot as (select f0lot.*,
                                                                                                                                                               case
                                                                                                                                                                 when nxt_change_date =
                                                                                                                                                                      t_changedate then
                                                                                                                                                                  t_changedate
                                                                                                                                                                 else
                                                                                                                                                                  nxt_change_date - 1
                                                                                                                                                               end next_change_date,
                                                                                                                                                               case
                                                                                                                                                                 when nxt_change_date =
                                                                                                                                                                      t_changedate then
                                                                                                                                                                  1
                                                                                                                                                                 else
                                                                                                                                                                  nxt_change_date -
                                                                                                                                                                  t_changedate
                                                                                                                                                               end cnt_days,
                                                                                                                                                               first_value(t_sum) over(partition by t_fiid, t_sumid order by t_changedate, t_instance) sum_buy,
                                                                                                                                                               first_value(t_amount) over(partition by t_fiid, t_sumid order by t_changedate, t_instance) cnt_buy
                                                                                                                                                          from f0lot), llot as (select flot.*,
                                                                                                                                                                                       qb_dwh_utils.numbertochar(flot.t_amount,
                                                                                                                                                                                                                 0) camount,
                                                                                                                                                                                       to_char(flot.t_fiid) ||
                                                                                                                                                                                       '#FIN' cfiid,
                                                                                                                                                                                       to_char(t_dealid) ||
                                                                                                                                                                                       '#TCK' cdealid,
                                                                                                                                                                                       (select sum(lnk.t_sumsale)
                                                                                                                                                                                          from dpmwrtlnk_dbt lnk
                                                                                                                                                                                         where lnk.t_buyid =
                                                                                                                                                                                               flot.t_sumid
                                                                                                                                                                                           and lnk.t_createdate <=
                                                                                                                                                                                               flot.t_changedate) sum_sale
                                                                                                                                                                                  from flot)

                  select cdealid,
                         cfiid,
                         to_char(t_sumid) sumid,
                         '11' adjtype,
                         --qb_dwh_utils.numbertochar(round((fi.t_facevalue -
                         --                                sum_buy / cnt_buy) * t_amount,
                         --                                2),
                         --                          2) discont_buy,
                         qb_dwh_utils.NumberToChar(round(llot.begdiscount, 2), 2) discont_buy, -- iSupport#532380
                         qb_dwh_utils.datetochar(llot.t_changedate) bd,
                         qb_dwh_utils.datetochar(llot.next_change_date) ed,
                         qb_dwh_utils.datetochar(llot.t_changedate) dt,
                         dwhRecStatus,
                         dwhSysMoment,
                         dwhEXT_FILE
                    from llot
                   inner join dfininstr_dbt fi
                      on (llot.t_fiid = fi.t_fiid)
                   where round(llot.begdiscount, 2) <> 0
                   )
         );
commit;
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при добавлении характеристик лотов: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;

    -- Вставка гашений номинала для облигаций, по которым в СОФР нет ни одного гашения.
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Генерация гашений номинала по облигациям',
                   2,
                   null,
                   null);
    begin
    insert into ldr_infa_cb.fct_secrepayschedule(code,typeschedule, typerepaysec, begindate, enddate, proc_rate, proc_sum, security_code, dt, rec_status, sysmoment, ext_file)
      select to_char((select max(to_number(code)) from ldr_infa_cb.fct_secrepayschedule) + rownum) code,
             '2',
             '2',
             b.secissueregdate begidate,
             b.maturitydate enddate,
             '0' proc_rate,
             qb_dwh_utils.NumberToChar(to_number(ds.nominal), 3) procsum,
             b.security_code,
             b.dt,
             dwhRecStatus,
             dwhSysMoment,
             dwhEXT_FILE
        from ldr_infa_cb.det_bond  b
       inner join ldr_infa_cb.det_security ds
          on (b.backofficecode = ds.code)
        left join  ldr_infa_cb.fct_secrepayschedule s
          on (b.security_code = s.security_code and s.typeschedule = v2)
       where s.code is null;
commit;       
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при добавлении списка гашений: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;

    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Очиста данных по дате открытия ц/б',
                   2,
                   null,
                   null);

    begin
    --  Очистка лишних записей всего 107 шт.
    delete from ldr_infa_cb.ass_sec_portfolio where security_code not in (select code from ldr_infa_cb.det_security ds);commit;
    delete from ldr_infa_cb.fct_securityamount where security_code not in (select code from ldr_infa_cb.det_security ds);commit;
    -- Удаляем иоторияю по векселям которые на данный момент погашены
    /*
    delete from ldr_infa_cb.fct_securityamount s
     where s.security_code in (select bill_code
                                 from (select bs.*,
                                              row_number() over(partition by bs.bill_code order by bs.dt desc) rnk
                                         from ldr_infa_cb.fct_bill_state bs
                                        order by bill_code, dt)
                                where (rnk = 1 and bill_state = 4));  -- последний статус векселя - погашен
    */
    -- Удалим курсы действующие до открытия ц/б
    delete from ldr_infa_cb.fct_finstr_rate dr
     where exists( select 1
                     from ldr_infa_cb.fct_finstr_rate r
                    inner join ldr_infa_cb.det_security s
                       on (r.finstr_numerator_finstr_code = s.code)
                    where to_date(r.dt,'dd-mm-yyyy') < to_date(s.dt,'dd-mm-yyyy')
                      and  r.finstr_numerator_finstr_code = dr.finstr_numerator_finstr_code
                      and  r.finstr_denumerator_finstr_code = dr.finstr_denumerator_finstr_code
                      and r.dt = dr.dt);
commit;
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при очистке подготовленнмх данных: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;
    select count(*)
      into cnt
      from qb_dwh_const4exp_val t 
     where t.id = n26 and t.value = n1;
    if (cnt > 0 and BIQ_7477_78 = 1) then
      -- Выгрузка данных по BIQ 7477/7478
      begin
        insert into ldr_infa_cb.det_rating_type
          (code,
           subject_code,
           name_eng,
           name_rus,
           official_name,
           object_type,
           rating_scale_name,
           credit_rating,
           currency,
           scale,
           period,
           dt_change,
           dt,
           rec_status,
           sysmoment,
           ext_file)
          select distinct '9999#SOFRXXX#' || la.code_name code,
                 '9999#SOFRXXX#' || la.agency_eng || '#CUST_CORP' subject_code,
                 la.fullname_eng name_eng,
                 la.fullname_rus name_rus,
                 la.official_name official_name,
                 case
                   when la.for_instrument = '1' then
                    '0'
                   when la.for_company = '1' then
                    '1'
                   when la.for_instrument = '0' and la.for_company = '0' then
                    '2'
                 end object_type,
                 la.scale_type_name rating_scale_name,
                 case
                   when la.is_credit = '0' then
                    '0'
                   when la.is_credit = '1' then
                    '1'
                 end credit_rating,
                 case
                   when (la.currency_type = 'N' or la.currency_type is null)  then
                    '0'
                   when la.currency_type = 'L' then
                    '1'
                   when la.currency_type = 'F' then
                    '2'
                 end currency,
                 case
                   when la.scale_type = 'N' then
                    '0'
                   when (la.scale_type = 'I' or la.scale_type is null) then
                    '1'
                 end scale,
                 case
                   when la.term_type is null then
                    '0'
                   when la.term_type = 'S' then
                    '1'
                   when la.term_type = 'L' then
                    '2'
                 end period,
                 qb_dwh_utils.datetochar(la.sysmoment) dt_change,
                 qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
                 '0' rec_status,
                 dwhSysmoment,
                 dwhExt_File
            from sofr_rating_listratings la;
commit;            
            
        insert into ldr_infa_cb.det_rating(code,
                                           rating_type_code,
                                           name,
                                           dt_change,
                                           dt,
                                           rec_status,
                                           sysmoment,
                                           ext_file)
        select '9999#SOFRXXX#' || rl.code_name || '#' || rh.prev code,
               '9999#SOFRXXX#' || rl.code_name rating_type_code,
               rh.prev name,
               qb_dwh_utils.DateToChar(max(rh.sysmoment)) dt_change,
               qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
               '0',
               dwhSysmoment,
               dwhExt_File
           from sofr_rating_ratingshistory rh
          inner join sofr_rating_listratings rl
             on (rh.rating_id = rl.rating_id)
          where rh.prev is not null
          group by rh.prev, rh.Rating_Id, rl.code_name
        union
        select distinct
               '9999#SOFRXXX#' || rl.code_name || '#' || rh.last code,
               '9999#SOFRXXX#' || rl.code_name rating_type_code,
               rh.last name,
               qb_dwh_utils.DateToChar(max(rh.sysmoment)) dt_change,
               qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
               '0',
               dwhSysmoment,
               dwhExt_File
           from sofr_rating_ratingshistory rh
          inner join sofr_rating_listratings rl
             on (rh.rating_id = rl.rating_id)
          where rh.last is not null
          group by rh.last, rh.Rating_Id, rl.code_name
          order by rating_type_code, code;
commit;          

        insert into ldr_infa_cb.fct_sec_rating(security_code,
                                               rating_code,
                                               --rating_type,
                                               dt,
                                               rec_status,
                                               sysmoment,
                                               ext_file)
        select distinct to_char(avr.t_fiid) || '#FIN'  SECURITY_CODE,
               '9999#SOFRXXX#' || rl.code_name || '#' || rh.last RATING_CODE,
      --         '9999#SOFRXXX#' || rl.code_name RATING_TYPE,
               qb_dwh_utils.DateToChar(rh.last_dt) dt,
               '0' rec_status,
               dwhsysmoment,
               dwhext_file
          from sofr_rating_ratingshistory rh
          inner join sofr_rating_listratings rl
             on (rh.rating_id = rl.rating_id)
          inner join davoiriss_dbt avr
             on (rh.isin = avr.t_isin);
commit;
        insert into ldr_infa_cb.det_subject
          (typesubject,
           code_subject,
           dt_reg,
           inn,
           system_code,
           department_code,
           country_code_num,
           dt,
           rec_status,
           sysmoment,
           ext_file)
          select '2' typesubject,
                 '9999#SOFRXXX#' || rl.agency_eng || '#CUST_CORP' code_subject,
                 null dt_reg,
                 null inn,
                 'SOFRXXX' system_code,
                 '0000' department_code,
                 '-1' country_code_num,
                 --qb_dwh_utils.datetochar(max(rl.sysmoment)) dt,
                 qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
                 '0',
                 dwhSysmoment,
                 dwhExt_File
            from sofr_rating_listratings rl
           group by rl.agency_eng
           order by rl.agency_eng;
commit;
          insert into ldr_infa_cb.det_juridic_person
            (juridic_person_name_s,
             juridic_person_name,
             dt_registration,
             note,
             subject_code,
             okved_code,
             okato_code,
             dt,
             rec_status,
             sysmoment,
             ext_file)
            select rl.agency_eng juridyc_person_name_s,
                   rl.agency_eng juridyc_person_name,
                   null dt_registration,
                   null note,
                   '9999#SOFRXXX#' || rl.agency_eng || '#CUST_CORP' subject_code,
                   '-1' okved_code,
                   '-1' okato_code,
                   --qb_dwh_utils.datetochar(max(rl.sysmoment)) dt,
                   qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
                   '0',
                   dwhSysmoment,
                   dwhExt_File
              from sofr_rating_listratings rl
             group by rl.agency_eng
             order by rl.agency_eng;
commit;
        insert into ldr_infa_cb.fct_securityrisk
          select distinct
                 case
                   when at.t_attrid = 1 then
                    '9999#SOFRXXX#1'
                   when at.t_attrid = 2 then
                    '9999#SOFRXXX#2'
                   when at.t_attrid = 3 then
                    '9999#SOFRXXX#3'
                   when at.t_attrid = 4 then
                    '9999#SOFRXXX#4'
                   when at.t_attrid = 5 then
                    '9999#SOFRXXX#5'
                 end riskcat_code,
                 to_char(fi.t_fiid) || case
                   when fi.t_avoirkind = 5 then
                    '#BNR'
                   else
                    '#FIN'
                 end security_code,
                 --replace(replace(replace(regexp_substr(at.t_fullname, '\(.*\)'), ')'),
                 --                '('),
                 --        '%') reserve_rate,
                 nvl ( replace(rsb_struct.getString(nt.t_text), chr(0), ''), 0) reserve_rate,
                 null ground,
                 qb_dwh_utils.datetochar(decode(ac.t_validfromdate,
                                                to_date('01010001', 'ddmmyyyy'),
                                                to_date('01011980', 'ddmmyyyy'),
                                                ac.t_validfromdate)) dt,
                 cCODE_TYPERISK RISKCAT_CODE_TYPERISK,
                 '0' rec_status,
                 dwhSysmoment,
                 dwhExt_file
            from dobjatcor_dbt ac
           inner join dobjgroup_dbt gr
              on (ac.t_objecttype = gr.t_objecttype and ac.t_groupid = gr.t_groupid)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           inner join dfininstr_dbt fi
              on (ac.t_object = lpad(to_char(fi.t_fiid), 10, '0'))
           left join dnotetext_dbt nt
              on (nt.t_objecttype = 12 and nt.t_notekind = 3 and ac.t_validfromdate between nt.t_date and nt.t_validtodate
                  and nt.t_documentid = ac.t_object)
           where ac.t_objecttype = 12 -- объект ценная бумага
             and gr.t_type = chr(88)
             and gr.t_groupid = 13 -- категория качества
             and exists (select 1
                    from ldr_infa_cb.det_finstr df
                   where df.finstr_code = to_char(fi.t_fiid) || case
                           when fi.t_avoirkind = 5 then
                            '#BNR'
                           else
                            '#FIN'
                         end)
             and BIQ_7477_78 = 1
             ;
commit;

        insert into ldr_infa_cb.det_kindprocrate(code,
                                                 name,
                                                 dt,
                                                 rec_status,
                                                 sysmoment,
                                                 ext_file)
        select '9999#SOFRXXX#1#FIXED',
               'Фиксированная ставка по гашениям купонов',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#1#FLOAT',
               'Плавающая ставка  по гашениям купонов',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#2#FIXED',
               'Фиксированная ставка по гашениям облигаций',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#2#FLOAT',
               'Плавающая ставка  по гашениям облигаций',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#3#FIXED',
               'Фиксированная ставка по гашениям векселей',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#3#FLOAT',
               'Плавающая ставка по гашениям векселей',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual;
commit;          
        insert into ldr_infa_cb.det_subkindprocrate(code,
                                                    name,
                                                    counts,
                                                    period,
                                                    dt,
                                                    rec_status,
                                                    sysmoment,
                                                    ext_file)
                    select '-1',
                           'Не определено',
                           '-1',
                           '-1',
                           qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
                           '0',
                           dwhSysmoment,
                           dwhExt_File
                           from dual ;
commit;
        insert into ldr_infa_cb.fct_procrate_security(procsum,
                                                      procrate,
                                                      dt_next_overvalue,
                                                      security_code,
                                                      kindprocrate_code,
                                                      procbase_code,
                                                      subkindprocrate_code,
                                                      dt,
                                                      rec_status,
                                                      sysmoment,
                                                      ext_file)
        select distinct
               qb_dwh_utils.NumberToChar(procsum, 4) procsum,
               qb_dwh_utils.NumberToChar(procrate, 4) procrate,
               dt_next_overvalue,
               security_code,
               kindprocrate_code,
               procbase_code,
               subkindprocrate_code,
               dt,
               '0' rec_status,
               dwhSysmoment,
               dwhExt_File
          from (
        select wr.t_incomevolume procsum,
               RSI_RSB_FIINSTR.CalcNKD_Ex_NoRound(wr.t_fiid, wr.t_drawingdate, 1.0, 1, 0, 0) first_calc,
               qb_dwh_utils.NumberToChar(case
                                           when wr.t_incomerate > 0 then
                                             wr.t_incomerate
                                           when h.t_incomerate > 0 then
                                             h.t_incomerate
                                           else
                                             round(RSI_RSB_FIInstr.FI_ReturnIncomeRate(), 4)
                                         end ) procrate,
               qb_dwh_utils.DateToChar(decode(lead(wr.t_drawingdate, 1, null) over (partition by wr.t_fiid order by wr.t_drawingdate), emptdate, firstdate, wr.t_drawingdate)) DT_NEXT_OVERVALUE,
               to_char(fi.t_fiid) || '#FIN' SECURITY_CODE,
               '9999#SOFRXXX#1#FIXED' KINDPROCRATE_CODE,  -- гашение купона по фиксированной ставке
               '9999#SOFRXXX#-1' PROCBASE_CODE,
               '-1' SUBKINDPROCRATE_CODE,
               qb_dwh_utils.DateToChar(decode(wr.t_drawingdate,emptdate, firstdate, wr.t_drawingdate)) dt
          from dfiwarnts_dbt wr
          inner join dfininstr_dbt fi
             on (wr.t_fiid = fi.t_fiid)
          left join dflrhist_dbt h
            on (wr.t_id = h.t_fiwarntid)

          where wr.t_ispartial = chr(0)
            and exists (select 1
                          from ldr_infa_cb.det_finstr df
                         where df.finstr_code = to_char(fi.t_fiid) || '#FIN')
        union all
        select wr.t_incomevolume procsum,
               RSI_RSB_FIINSTR.CalcNKD_Ex_NoRound(wr.t_fiid, wr.t_drawingdate, 1.0, 1, 0, 0) first_calc,
               qb_dwh_utils.NumberToChar(case
                                           when wr.t_incomerate > 0 then
                                             wr.t_incomerate
                                           when h.t_incomerate > 0 then
                                             h.t_incomerate
                                           else
                                             round(RSI_RSB_FIInstr.FI_ReturnIncomeRate(), 4)
                                         end ) procrate,
               qb_dwh_utils.DateToChar(lead(wr.t_drawingdate, 1, null) over (partition by wr.t_fiid order by wr.t_drawingdate)) DT_NEXT_OVERVALUE,
               to_char(fi.t_fiid) || '#FIN' SECURITY_CODE,
               '9999#SOFRXXX#2#' || case
                                      when h.t_fiwarntid is not null then
                                          'FLOAT'
                                        else
                                          'FIXED'
                                    end KINDPROCRATE_CODE,  -- гашение облигации
               '9999#SOFRXXX#' || case av.t_nkdbase_kind
                         when 0 then
                            '365'
                         when 1 then
                            '30/360'
                         when 2 then
                            '360/0'
                         when 3 then
                            '30/365'
                         when 4 then
                            '366'
                         when 5 then
                            '30/366'
                         when 6 then
                            'Act/по_купонным_периодам'
                         when 7 then
                            'Act/365L'
                         when 8 then
                            'Act/364'
                         when 9 then
                            '30E/360'
                         when 10 then
                            '30/360_ISDA'
                         when 11 then
                            'Act/Act_ICMA'
                         else
                           '-1'
                       end procbase_code,
               '-1' SUBKINDPROCRATE_CODE,
               qb_dwh_utils.DateToChar(decode(wr.t_drawingdate,emptdate, firstdate, wr.t_drawingdate)) dt
           from dfiwarnts_dbt wr
          inner join dfininstr_dbt fi
             on (wr.t_fiid = fi.t_fiid)
          inner join davoiriss_dbt av
             on (fi.t_fiid =  av.t_fiid)
          left join dflrhist_dbt h
            on (wr.t_id = h.t_fiwarntid)
          where wr.t_ispartial = chr(88)
            and exists (select 1
                          from ldr_infa_cb.det_finstr df
                         where df.finstr_code = to_char(fi.t_fiid) || '#FIN')
        union all
                          select round(bnin.t_perc, 9) procsum,
                                 null first_calc,
                                 case when leg.t_formula = 1 then
                                   qb_dwh_utils.NumberToChar(leg.t_price/power(10, leg.t_point), 3)
                                 else
                                   null
                                 end procrate,
                                 null DT_NEXT_OVERVALUE,
                                 to_char(bn.t_bcid) || '#BNR' security_code,
                                 '9999#SOFRXXX#3#' || case
                                                        when leg.t_typepercent = 0 then
                                                            'FIXED'
                                                          else
                                                            'FLOAT'
                                                      end KINDPROCRATE_CODE,  -- гашение векселя
                                 '9999#SOFRXXX#' || case
                                   when leg.t_basis = 4 then
                                     'Act/Act_ICMA'
                                   when leg.t_basis = 1 then
                                     '30/360'
                                   when leg.t_basis = 2 then
                                     '360'
                                   when leg.t_basis = 8 then
                                     '365'
                                   when leg.t_basis = 1001 then
                                     '31/360'
                                 end procbase_code,
                                 '-1' SUBKINDPROCRATE_CODE,
                                 qb_dwh_utils.DateToChar(case when decode(bn.t_registrationdate,emptDate,maxDate, bn.t_registrationdate) < decode(bn.t_issuedate,emptDate,maxDate, bn.t_issuedate) then
                                                                 decode(bn.t_registrationdate,emptDate,firstDate, bn.t_registrationdate)
                                                              else
                                                                decode(bn.t_issuedate,emptDate,firstDate, bn.t_issuedate)
                                                         end) dt

                            from dvsbanner_dbt bn
                            left join ddl_leg_dbt leg
                              on (bn.t_bcid = leg.t_dealid and leg.t_legid = 0 and leg.t_legkind = 1)
                            left join dfininstr_dbt pfi
                              on (leg.t_pfi = pfi.t_fiid)
                            left join dpartcode_dbt pc
                              on pc.t_partyid = bn.t_issuer and pc.t_codekind = 101
                            left join dpartcode_dbt pc1
                              on pc1.t_partyid = bn.t_holder and pc1.t_codekind = 101
                            left join dvsincome_dbt bnin
                              on (bn.t_bcid = bnin.t_bcid and bnin.t_incometype = 9)
                           where leg.t_formula = 1
                             and exists (select 1
                                           from ldr_infa_cb.det_finstr df
                                          where df.finstr_code = to_char(bn.t_bcid) || '#BNR')

        );
commit;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке данных по BIQ 7477: ' || SQLERRM,
                               0,
                               null,
                               null);
      end;
    end if;
    -- Выгрузка данных по BIQ 7477/7478

    --Завершим выгрузку ценных бумаг
    qb_bp_utils.EndEvent(EventID, null);
    --commit;
  end;

  procedure export_Secur_9996(in_department in number,
                         in_date       in date,
                         procid        in number) is
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
    cntFIID      pls_integer := 0;
    acc_code     varchar2(250);
    portf        varchar2(2);
    cnt_chglot   pls_integer;
    cnt          pls_integer;
--    uf4          varchar2(250);

  begin
    -- Установим начало выгрузки ц/б
    startevent(cEvent_EXPORT_Secur, procid, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             QB_DWH_EXPORT.cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDT, in_date);

    qb_dwh_export.InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка данных по ц/б (BIQ-9996)',
                         2,
                         null,
                         null);

  -- Удалим лоты по которым мог измениться счет
  delete from qb_dwh_const4exp_val cv where cv.id = 24;commit; -- очистим список лотов по которым мог измениться счет
  insert into qb_dwh_const4exp_val
    (select /*+ parallel(4) ordered */ 24,
            t.t_sumid
       from pkl_portfolio_accounts t
      inner join dpmwrtsum_dbt s
         on t.t_sumid = s.t_sumid
      where (select count(1)
               from (select 1
                       from v_scwrthistex
                      where t.t_sumid = t_sumid
                      group by t_portfolio)) > 1);
commit;                      
  delete from pkl_portfolio_accounts where t_sumid in (select cv.value
                                                         from qb_dwh_const4exp_val cv
                                                        where cv.id = 24);
commit;
  -- очистим времянку с лотами
  delete from dwh_histsum_tmp; commit;
  -- выбор не нулевых лотов по которым требуется выгрузка
  insert into dwh_histsum_tmp(sumid)
    (select /*+ parallel(4) */ distinct t_sumid
       from (select /*+ full(t) */ t.t_sumid,
                    t.t_changedate,
                    t.t_instance,
                    max(t.t_instance) over(partition by t.t_sumid, t.t_changedate) maxinstance,
                    max(t.t_changedate) over(partition by t.t_sumid) maxchangedate,
                    t.t_amount
               from v_scwrthistex t
              where t.t_changedate <= in_date
                and t.t_party = n_1     -- лоты банка
                and t.t_state in (select v.value
                                    from qb_dwh_const4exp c
                                   inner join qb_dwh_const4exp_val v
                                      on (c.id = v.id)
                                   where c.name = cLOT_STATE))
      where t_instance = maxinstance
        --and t_changedate = maxchangedate -- iSupport 533994
        and maxchangedate >= minDate -- последнее изменение лота входит в выгружаемый период
        and t_amount > n0            -- ненулевые лоты
     );
commit;     
  -->
  EXECUTE IMMEDIATE 'select * from dwh_histsum_tmp for update'; -- до окончания выгрузки ничего менять нельзя
  call_scwrthistex(in_date);
  EXECUTE IMMEDIATE 'select * from DWH_scwrthistex_TMP for update'; -- до окончания выгрузки ничего менять нельзя
  --<
  qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка остатков по прочим ц/б (BIQ-9996)',
                     2,
                     null,
                     null);

  -- Цикл по отобранным лотам
  for rec in (with /* dlot as (select sumid from dwh_histsum_tmp ),
              lot0 as( select v.T_FIID,
                                    v.T_SUMID,
                                    v.T_PORTFOLIO,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_COST,
                                    v.T_STATE,
                                    v.T_PARENT,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.T_CORRINTTOEIR c2eps,
                                    decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                    v.T_RESERVAMOUNT c2rpbu,
                                    v.T_INCOMERESERV c2rpbu_nkd
                               from  v_scwrthistex v
                               where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                 and v.T_CHANGEDATE <= in_date
                                ),
                                */
              lot0 as (      select 
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_PORTFOLIO,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_COST,
                                    v.T_STATE,
                                    v.T_PARENT,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                      ),
              lot as (select lot0.*,
                                           lag(t_state, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_state,
                                           lag(t_amount, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_amount,
                                           lag(t_portfolio, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_portfolio
                                      from lot0
                                      where t_instance = maxinstance),
              f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                         from lot where t_amount <> prev_amount or t_portfolio <> prev_portfolio or t_state <> prev_state),
              flot as (select f0lot.*,
                              case when nxt_change_date = t_changedate then
                                t_changedate
                              else
                                nxt_change_date - 1
                              end next_change_date,
                              case when nxt_change_date = t_changedate then
                                1
                              else
                                nxt_change_date - t_changedate
                              end cnt_days,
                              first_value(t_sum) over(partition by t_fiid, t_sumid order by t_changedate, t_instance) sum_buy,
                              first_value(t_amount) over(partition by t_fiid, t_sumid order by t_changedate, t_instance) cnt_buy
                         from f0lot),
              llot as (select flot.*,
                              ac.t_account acnt,
                              qb_dwh_utils.NumberTochar(flot.t_amount, 0) camount,
                              to_char(flot.t_fiid) || '#FIN' cfiid,
                              to_char(t_dealid) || '#TCK' cdealid,
                              (select sum(lnk.t_sumsale)
                                 from dpmwrtlnk_dbt lnk
                                where lnk.t_buyid = flot.t_sumid
                                  and lnk.t_createdate <= flot.t_changedate) sum_sale
                         from flot
                         left join pkl_portfolio_accounts ac
                           on (flot.t_sumid = ac.t_sumid and flot.t_state = ac.t_state and flot.t_changedate = ac.t_date and flot.t_parent = ac.t_parent)
                        )

              select /*+ PARALLEL(4) */ t_fiid,
                     t_portfolio,
                     t_sumid,
                     t_sum,
                     qb_dwh_utils.NumberToChar(round(sum_buy, 2), 2) sum_buy,
                     cnt_buy,
                     qb_dwh_utils.NumberToChar(nvl(round(sum_sale, 2), 0), 2) sum_sale,
                     qb_dwh_utils.NumberToChar(round((cnt_buy - t_amount) * sum_buy / cnt_buy, 2), 2) sum_disp,
                     camount,
                     acnt,
                     cfiid,
                     cdealid,
                     llot.cnt_days,
                     llot.t_changedate,
                     llot.next_change_date,
                     llot.t_state,
                     llot.t_parent,
                     c2eps,
                     c2oku,
                     c2rpbu,
                     c2rpbu_nkd
                from llot
              )
    loop
      if (rec.acnt is null or rec.acnt = '-1') then --30.03.22 добавим переопределение для -1

        select count(*)
          into cnt_chglot
          from qb_dwh_const4exp_val cv
         where cv.id = 24
           and cv.value = rec.t_sumid;
        if (cnt_chglot > 0) then
          -- очистим лоты по которым мог измениться счет
          delete from pkl_portfolio_accounts pa where pa.t_sumid = rec.t_sumid; commit;
        end if;
        acc_code := qb_dwh_export_secur.GetAccountByLot(p_sumid => rec.t_sumid, p_date => rec.t_changedate);
        if (acc_code <> '-1') then
          begin
          insert into pkl_portfolio_accounts(t_sumid, t_account, t_date, t_state, t_parent)
                 values(rec.t_sumid, acc_code, rec.t_changedate, rec.t_state, rec.t_parent);
commit;                 
          exception
            when Dup_Val_On_Index then
              null;
          end;
        end if;
      else
        acc_code := rec.acnt;
      end if;
      if (rec.camount > 0) then -- iSupport 533994
        portf    := qb_dwh_export_secur.GetPortfolioMSFO(fiid => rec.t_fiid, portf => rec.t_portfolio, acc => acc_code , cdate => rec.t_changedate , sumid => rec.t_sumid );
        if (acc_code <> '-1') then
          acc_code := '0000#IBSOXXX#' || qb_dwh_utils.GetAccountUF4(acc => acc_code);
        end if;
        for drec in  (select level Next_day from dual
                                     connect by level <= rec.cnt_days )
        loop
          insert into ldr_infa_cb.fct_securityamount(amount, account_code, security_code, deal_code, sec_portfolio_code, lot_num, dt, rec_status, sysmoment,ext_file)
                values (rec.camount, acc_code, rec.cfiid, rec.cdealid, portf, qb_dwh_utils.NumberToChar(rec.t_sumid, 0), to_char(rec.t_changedate + drec.next_day - 1, 'dd-mm-yyyy'), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                
          insert into ldr_infa_cb.fct_sec_sell_result(deal_code, security_code, sec_portfolio_code, purchase_amount, sell_amount, sum_of_disposal, lot_num, dt, rec_status, sysmoment, ext_file)
                values (rec.cdealid, rec.cfiid, portf, rec.sum_buy, rec.sum_sale, rec.sum_disp, qb_dwh_utils.NumberToChar(rec.t_sumid, 0), to_char(rec.t_changedate + drec.next_day - 1, 'dd-mm-yyyy'), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                
          null;
        end loop;
        --commit;
      end if;
    end loop;

   -- Вставка в ass_sec_portfolio

   insert into ldr_infa_cb.ass_sec_portfolio(security_code, sec_portfolio_code, dt,rec_status, sysmoment, ext_file)
            with g as (select t.security_code,
                              t.sec_portfolio_code,
                              t.dt,
                              sum(to_number(t.amount)) lsum
                         from ldr_infa_cb.fct_securityamount t
                        group by t.security_code, t.sec_portfolio_code, t.dt),
                 r as (select g.*,
                              row_number() over (partition by security_code, sec_portfolio_code, dt order by to_number(lsum) desc) rn
                         from g),
                 f as (select r.*, lag (sec_portfolio_code, 1, 0) over( partition by security_code, sec_portfolio_code order by to_date(dt, 'dd-mm-yyyy')) prev_portf
                         from r where rn = 1 )
            select security_code,
                  sec_portfolio_code,
                  dt,
                  dwhRecStatus,
                  dwhSysMoment,
                  dwhEXT_FILE
             from f
            where f.sec_portfolio_code <> prev_portf;
commit;                       

    qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка общих данных по  ц/б',
                       2,
                       null,
                       null);


    for rec in (select fi.t_fiid, fi.t_avoirkind, 1 bill_kind                      -- собственные векселя
                  from dfininstr_dbt fi
                 where fi.t_avoirkind = n5
                    and exists ( select 1
                                   from dvsbanner_dbt bn
                                   inner join ddl_leg_dbt leg
                                      on ( bn.t_bcid = leg.t_dealid)
                                   inner join dvsordlnk_dbt lnk
                                      on (bn.t_bcid = lnk.t_bcid)
                                   inner join ddl_order_dbt ord
                                      on (lnk.t_contractid = ord.t_contractid and lnk.t_dockind = ord.t_dockind)
                                    where bn.t_fiid = fi.t_fiid and
                                      leg.t_legid = n0 and leg.t_legkind = n1)
                union all
                select fi.t_fiid, fi.t_avoirkind, 2 bill_kind                     -- учтенные векселя
                                  from dfininstr_dbt fi
                                 where fi.t_avoirkind = n5
                                    and exists ( select 1
                                                   from dvsbanner_dbt bn
                                                   inner join ddl_leg_dbt leg
                                                      on ( bn.t_bcid = leg.t_dealid)
                                                   inner join dvsordlnk_dbt lnk
                                                      on (bn.t_bcid = lnk.t_bcid)
                                                   inner join ddl_tick_dbt tick
                                                      on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                                                    where bn.t_fiid = fi.t_fiid and
                                                      leg.t_legid = n0 and leg.t_legkind = n1)
                union all
                select fi.t_fiid, fi.t_avoirkind, 0 bill_kind                        -- прочие ц/б
                                  from dfininstr_dbt fi
                                 where ((fi.t_fi_kind = n2
                                   and fi.t_issys = chr0
                                   and fi.t_avoirkind in (select v.value
                                                            from qb_dwh_const4exp c
                                                           inner join qb_dwh_const4exp_val v
                                                              on (c.id = v.id)
                                                           where c.name = cSECKIND_ALL)   -- виды ц/б для выгрузки прибиваем гвоздями как указано в ТЗ
                                   and fi.t_avoirkind <> 48 -- корзина ц/б
                                   )
                                    or (fi.t_fiid in ( select fi2.t_parentfi from dfininstr_dbt fi2  where fi2.t_avoirkind in (select v.value
                                                                                                                                from qb_dwh_const4exp c
                                                                                                                               inner join qb_dwh_const4exp_val v
                                                                                                                                  on (c.id = v.id)
                                                                                                                               where c.name = cSECKIND_RECEIPT)) and fi.t_fi_kind = 2))
                                   and (exists ( select 1
                                                  from ddl_tick_dbt tick
                                                 where tick.t_pfi = fi.t_fiid ))
                union
                select distinct fi.t_fiid, fi.t_avoirkind, decode(fi.t_avoirkind, 5, 1, 0) bill_kind     -- ц/б в корзине РЕПО
                                  from ddl_tick_ens_dbt b
                                 inner join dfininstr_dbt fi
                                    on (b.t_fiid = fi.t_fiid)
                                  inner join ddl_tick_dbt t
                                    on (b.t_dealid = t.t_dealid)
                                 where t.t_dealdate <= in_date
                union   --дополнение FIID для FCT_SEC_RATING                  
                select distinct avr.t_fiid, fi.t_avoirkind, decode(fi.t_avoirkind, 5, 1, 0) bill_kind
                                from sofr_rating_ratingshistory rh
                                inner join sofr_rating_listratings rl
                                      on (rh.rating_id = rl.rating_id)
                                inner join davoiriss_dbt avr
                                      on (rh.isin = avr.t_isin)
                                inner join dfininstr_dbt fi
                                      on (avr.t_fiid = fi.t_fiid)  
              
                 )
    loop
      cntFIID := cntFIID + 1;
      -- Запишем ценную бумагу по которой начата операция выгрузки
      qb_bp_utils.SetAttrValue(EventID, cFIID, rec.t_fiid, cntFIID);
      begin
        export_Secur_9996(rec.t_fiid,
                     rec.t_avoirkind,
                     rec.bill_kind,
                     in_date,
                     dwhRecStatus,
                     dwhDT,
                     dwhSysMoment,
                     dwhEXT_FILE);
        --commit;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                               0,
                               cFIID,
                               rec.t_fiid);
      end;
    end loop;
    begin
      qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочников',
                   2,
                   null,
                   null);

    -- Выгрузка справочников
      -- Вставка в DET_EXCHANGE
      for rec in (with m as (select rd.t_market_place
                             from dratedef_dbt rd
                             group by rd.t_market_place)
                select
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                   qb_dwh_utils.System_IBSO,
                                                   1,
                                                   m.t_market_place) code,
                       pt.t_name name
                  from m
                 inner join dparty_dbt pt
                    on (m.t_market_place = pt.t_partyid)
                   )
      loop
        begin
          insert into ldr_infa_cb.det_exchange(code, name, dt, rec_status, sysmoment, ext_file)
                 values(rec.code, rec.name, qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        exception
          when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                                 0,
                                 null,
                                 null);
        end;
      end loop;
      -- Вставка в DET_TYPE_RATE
      for rec in (select to_char(rt.t_type) type_rate_code,
                         rt.t_typename type_rate_name
                    from dratetype_dbt rt)
      loop
        begin
          insert into ldr_infa_cb.det_type_rate(type_rate_code, type_rate_name, dt, rec_status, sysmoment, ext_file)
                 values(rec.type_rate_code, rec.type_rate_name, qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        exception
         when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                                 0,
                                 null,
                                 null);
       end;
      end loop;
      -- Вставка в ASS_RATE_EXCHANGE
      for rec in (select distinct to_char(rd.t_type) type_rate_code,
                         qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     rd.t_market_place) exchange_code
                    from dratedef_dbt rd
                   where rd.t_market_place is not null and rd.t_market_place > n0
                  )
      loop
        begin
          insert into ldr_infa_cb.ass_rate_exchange(type_rate_code, exchange_code, dt, rec_status, sysmoment, ext_file)
                 values(rec.type_rate_code, rec.exchange_code, dwhDT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        exception
         when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                                 0,
                                 null,
                                 null);
       end;
      end loop;
    --Вставка в det_security_type_711
    insert into ldr_infa_cb.det_security_type_711(code_type, name_type, dt, sysmoment, rec_status, ext_file)
           values ('-1', 'Не определен', qb_dwh_utils.DateToChar(firstDate), dwhSysMoment, dwhRecStatus, dwhEXT_FILE);
commit;           
    --Вставка в DET_PROCBASE

    insert into ldr_infa_cb.det_procbase(code, name, days_year, days_month, sign_31, first_day, last_day, null_mainsum, type_prc_charge)
    select '9999#SOFRXXX#360/0', 'Календарь/360', 360, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#366/0', 'Календарь/Календарь', 365, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#360#S', 'Календарь/360 (сложный %%)', 360, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#366#S', 'Календарь/Календарь (сложный %%)', 365, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30/365', '30/365', 365, 30 , 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#-1', 'Не определено', 365, 1, 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#360_F', 'Календарь/360 c начислением за первый день', 360, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#1', 'Ежедневная процентная ставка', 1, 1, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#366', 'Календарь/Календарь', 366, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#365', 'Календарь/365', 365, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#360', 'Календарь/360', 360, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30/360', '30/360', 360, 30, 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#31/360', '30/360 с учетом 31 числа, если кредит лежит неполный месяц', 360, 30, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30/366', '30/Календарь', 366, 30, 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#31/366', '30/Календарь с учетом 31 числа, если кредит лежит неполный месяц', 366, 30, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#Act/по_купонным_периодам', 'В году по куп. периодам, в мес. по календарю', 0, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#Act/365L', 'В году по оконч.куп.пер, в мес. по календарю', 365, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#Act/364', '364 дня в году, в месяце по календарю', 364, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30E/360', '360 дней в году, 30 в месяце (Eurobond)', 360, 30, 0, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#Act/Act_ICMA', 'Actual/Actual (ICMA)', 366, 31, 1, 1, 0, 0, 0
      from dual
    union all
    select '9999#SOFRXXX#30/360_ISDA', 'Календарь/360', 360, 30, 0, 1, 0, 0, 0
      from dual;
commit;      
    update ldr_infa_cb.det_procbase pb
       set pb.dt = qb_dwh_utils.DateToChar(firstDate),
           pb.sysmoment = dwhSysMoment,
           pb.rec_status = dwhRecStatus,
           pb.ext_file = dwhEXT_FILE;
commit;


      qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка счетов по ц/б',
                         2,
                         null,
                         null);

      -- Вставка в DET_ROLEACCOUNT_DEAL
--> AS 2020-05-12
/*      for rec in (select distinct id, '0000#SOFR#' || t_code as rolecode, name */
      for rec in (with fi as (select /*+ materialize*/  t_fiid 
                                   from dfininstr_dbt fi
                                   where (fi.t_fi_kind = n2
                                     and exists (select 1 from ddl_tick_dbt where t_pfi = fi.t_fiid ))
                                      or (fi.t_fiid in ( select fi2.t_parentfi from dfininstr_dbt fi2  where fi2.t_avoirkind in (select v.value
                                                                                                                                   from qb_dwh_const4exp c
                                                                                                                                  inner join qb_dwh_const4exp_val v
                                                                                                                                     on (c.id = v.id)
                                                                                                                                  where c.name = cSECKIND_RECEIPT)) and fi.t_fi_kind = 2)
                                   )
                  select /*+ PARALLEL(4)*/ distinct id, t_code as rolecode, name
                    from (select  cat.t_id id, cat.t_code, cat.t_name name
                            from dmccateg_dbt cat
                            where exists
                              (select /*+ index(CATACC DMCACCDOC_DBT_IDX4)*/ 1 from dmcaccdoc_dbt catacc
                                  where catacc.t_catid = cat.t_id
                                   and  t_dockind = n164
                                   and catacc.t_activatedate <
                                       decode(catacc.t_disablingdate,
                                              emptDate,
                                              maxDate,
                                              catacc.t_disablingdate))
                          union all
                          select  cat.t_id, cat.t_code, cat.t_name
                            from dmccateg_dbt cat 
                           where exists( select /*+  index(catacc DMCACCDOC_DBT_USR6)*/ 1  
                                  from dmcaccdoc_dbt catacc
                                  join fi on catacc.t_fiid = fi.t_fiid
                            where catacc.t_catid = cat.t_id
                                /*  and catacc.t_fiid in
                                (select  t_fiid 
                                   from dfininstr_dbt fi
                                   where (fi.t_fi_kind = n2
                                     and exists (select 1 from ddl_tick_dbt where t_pfi = fi.t_fiid ))
                                      or (fi.t_fiid in ( select fi2.t_parentfi from dfininstr_dbt fi2  where fi2.t_avoirkind in (select v.value
                                                                                                                                   from qb_dwh_const4exp c
                                                                                                                                  inner join qb_dwh_const4exp_val v
                                                                                                                                     on (c.id = v.id)
                                                                                                                                  where c.name = cSECKIND_RECEIPT)) and fi.t_fi_kind = 2)
                                   )    */
                             and catacc.t_activatedate <
                                 decode(catacc.t_disablingdate,
                                        emptDate,
                                        maxDate,
                                        catacc.t_disablingdate))
                            union all
                            ---
                            select /*+ leading(cat ) */ cat.t_id + n5000  id,
                                   cat.t_code||(select '#' || t_code from dllvalues_dbt where t_list = n3503 and t_element = templ.t_value1) as rolecode,
                                   substr(cat.t_name || ' ( ' ||  lv.t_name || ' )', 1,250) as name
                              from dmcaccdoc_dbt catacc
                              inner join dmccateg_dbt cat
                                     on (catacc.t_catid = cat.t_id)
                              inner join dllvalues_dbt lv
                                     on (lv.t_list = n3503)
                              left join dmctempl_dbt templ
                                 on (catacc.t_catid = templ.t_catid and catacc.t_templnum = templ.t_number)
                             where CAT.T_NUMBER in (n1492) --  catacc.t_catnum in (n1492) для  1492 работает
                              and templ.t_value1 >= 0 and rownum = 1
                                        )
                   union all
                   /*
                   select -1, '0000#SOFR#Начисл.ПДД, УВ_П', 'Начисленный процентный доход УВ' from dual
                   union all
                   select -2, '0000#SOFR#Начисл.ПДД, УВ_Д', 'Начисленный дисконтный доход УВ' from dual
                   union all
                   select -3, '0000#SOFR#Начисл.ПДД, ц/б_П', 'Начисленный процентный доход ц/б' from dual
                   union all
                   select -4, '0000#SOFR#Начисл.ПДД, ц/б_Д', 'Начисленный дисконтный доход ц/б' from dual
                   union all
                   select -4 - rownum, '0000#SOFR#' || cat.t_code || '#' || lv.t_code roleaccount_deal_code,
                   */
                   select -1, 'Начисл.ПДД, УВ_П', 'Начисленный процентный доход УВ' from dual
                   union all
                   select -2, 'Начисл.ПДД, УВ_Д', 'Начисленный дисконтный доход УВ' from dual
                   union all
                   select -3, 'Начисл.ПДД, ц/б_П', 'Начисленный процентный доход ц/б' from dual
                   union all
                   select -4, 'Начисл.ПДД, ц/б_Д', 'Начисленный дисконтный доход ц/б' from dual
                   --union all
                   --select -5, 'Просроч_вексель#ПД', 'Просроченный вексель' from dual -- по этой строке дубль по ключу CODE, DT
                   union all
                   select -4 - rownum, lv.t_code roleaccount_deal_code,
                          substr(cat.t_name || ' ( ' ||  lv.t_name || ' )', 1,250)
                     from dmccateg_dbt cat
                    inner join dllvalues_dbt lv
                       on (lv.t_list = n3503)
                    where cat.t_number = n1492
                  )
      loop
        begin
          insert into ldr_infa_cb.det_roleaccount_deal(code, name, orole_code, dt, rec_status, sysmoment, ext_file)
                 values(rec.rolecode, rec.name,'0', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        exception
         when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                                 0,
                                 null,
                                 null);
       end;
      end loop;
      -- Вставка в DET_SEC_PORTFOLIO
      begin
        insert into ldr_infa_cb.det_sec_portfolio(code, name, dt, rec_status, sysmoment, ext_file)
               values('1', 'Ценные бумаги, которые удерживаются до срока погашения', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
        insert into ldr_infa_cb.det_sec_portfolio(code, name, dt, rec_status, sysmoment, ext_file)
               values('2', 'Ценные бумаги, которые используются для удержания и торговли', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
        insert into ldr_infa_cb.det_sec_portfolio(code, name, dt, rec_status, sysmoment, ext_file)
               values('3', 'Ценные бумаги, которые используются для торговли', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      exception
       when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                               0,
                               null,
                               null);
      end;
      -- Вставка в FCT_SECURITYAMMOUNT
    end;
    -- Вставка в DET_SECURITY_ATTR
    qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка атрибутов ц/б' ,
                       2,
                       null,
                       null);

    begin
      insert into ldr_infa_cb.det_security_attr
          (SELECT DISTINCT TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE,
                          UPPER(TRIM(GR.T_NAME)) NAME,
                          DECODE(GR.T_TYPE, CHR(88), '0', '1') MULTYVALUE,
                          qb_dwh_utils.DateToChar(firstDate),
                          dwhRecStatus,
                          dwhSysMoment,
                          dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
          INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
          WHERE AC.T_OBJECTTYPE IN (n12, n24)
            and  not (gr.t_groupid = n62 and gr.t_objecttype = n12)
            and  not (gr.t_groupid = n101 and gr.t_objecttype = n24)
          UNION ALL
          SELECT DISTINCT TO_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                          UPPER(TRIM(NK.T_NAME)) NAME,
                          '0' MULTYVALUE,
                          qb_dwh_utils.DateToChar(firstDate),
                          dwhRecStatus,
                          dwhSysMoment,
                          dwhEXT_FILE
            FROM DNOTETEXT_DBT NT
          INNER JOIN DNOTEKIND_DBT NK
              ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND NT.T_NOTEKIND = NK.T_NOTEKIND)
          WHERE NT.T_OBJECTTYPE IN (n12, n24)
          union all
          select 'DATE_OFFER',
                 'Дата оферты',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'IS_SUBORDINATED',
                 'Признак субординированности ценной бумаги',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'IS_PROBLEM_RESTRUCTURING',
                 'Признак проблемной реструктуризации',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'DATE_PROBLEM_RESTRUCTURING',
                 'Дата переноса остатка на б/счет 50505',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'COUNTRY',
                 'Страна резидентности эмитента',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'COUNTRY_SO',
                 'Страновая оценка',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'ID_DIASOFT',
                 'ИД Клиента в АС Diasoft Fa#',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'ID_SOFR',
                 'ИД Клиента в СОФР',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'IS_ACTIVE_MARKET',
                 'Признак активного рынка',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          union all
          select 'LEVEL_HIERARCHY',
                 'Уровень иерархии справедливой стоимости',
                 '0',
                 qb_dwh_utils.DateToChar(firstDate),
                 dwhRecStatus,
                 dwhSysMoment,
                 dwhEXT_FILE
            from dual
          );
commit;          
    exception
     when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    -- Добавление атрибутов по проблемной реструктиризации
    insert into ldr_infa_cb.fct_security_attr
      (SELECT security_code,
              nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio sp
                           where sp.security_code = security_code
                             and to_date(sp.dt, 'dd-mm-yyyy') <= in_date)
                   where rnk = 1),
                  '-1') portf,
              'IS_PROBLEM_RESTRUCTURING' CODE_security_attr,
              null number_value,
              null date_value,
              'X' string_value,
              'X' value,
              qb_dwh_utils.DateToChar(min(rdate)) dt,
              dwhRecStatus,
              dwhSysMoment,
              dwhEXT_FILE
         from (select /*+ index(rd, DRESTDATE_DBT_IDX0) */
                ca.account_code,
                ca.security_code,
                acc.t_accountid accid,
                rd.t_restdate rdate,
                decode(acc.t_code_currency, 0, rd.t_rest, rd.t_restcurrency) rest,
                row_number() over(partition by rd.t_accountid, rd.t_restcurrency order by rd.t_restdate desc nulls last) rnk
                 from ldr_infa_cb.ass_accountsecurity ca
                inner join daccount_dbt acc
                   on (substr(ca.account_code, 14, 50) = acc.t_userfield4)
                inner join drestdate_dbt rd
                   on (acc.t_accountid = rd.t_accountid and
                      acc.t_code_currency = rd.t_restcurrency)
                where substr(acc.t_account, 14, 5) = v50505
                  and rd.t_restdate <= in_date)
        where rnk = n1
          and rest <> n0
        group by security_code);
commit;
    insert into ldr_infa_cb.fct_security_attr
      (SELECT security_code,
              nvl((select sec_portfolio_code
                    from (select sec_portfolio_code,
                                 row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                            from ldr_infa_cb.ass_sec_portfolio sp
                           where sp.security_code = security_code
                             and to_date(sp.dt, 'dd-mm-yyyy') <= in_date)
                   where rnk = 1),
                  '-1') portf,
              'DATE_PROBLEM_RESTRUCTURING' CODE_security_attr,
              null number_value,
              qb_dwh_utils.DateToChar(min(rdate)) date_value,
              null string_value,
              qb_dwh_utils.DateToChar(min(rdate)) value,
              qb_dwh_utils.DateToChar(min(rdate)) dt,
              dwhRecStatus,
              dwhSysMoment,
              dwhEXT_FILE
         from (select /*+ index(rd, DRESTDATE_DBT_IDX0) */
                ca.account_code,
                ca.security_code,
                acc.t_accountid accid,
                rd.t_restdate rdate,
                decode(acc.t_code_currency, 0, rd.t_rest, rd.t_restcurrency) rest,
                row_number() over(partition by rd.t_accountid, rd.t_restcurrency order by rd.t_restdate desc nulls last) rnk
                 from ldr_infa_cb.ass_accountsecurity ca
                inner join daccount_dbt acc
                   on (substr(ca.account_code, 14, 50) = case
                                                            when (acc.t_userfield4 is null) or
                                                                (acc.t_userfield4 = chr(0)) or
                                                                (acc.t_userfield4 = chr(1)) or
                                                                (acc.t_userfield4 like '0x%') then
                                                              acc.t_account
                                                            else
                                                              acc.t_userfield4
                                                         end)
                inner join drestdate_dbt rd
                   on (acc.t_accountid = rd.t_accountid and
                      acc.t_code_currency = rd.t_restcurrency)
                where substr(ca.account_code, 14, 5) = v50505
                  and rd.t_restdate <= in_date)
        where rnk = n1
          and rest <> n0
        group by security_code);
commit;
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при добавлении атрибутов по проблемной реструктиризации: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;

    begin
    for rec in (with iss as(
                select finstr_code,
                       dt,
                       portf,
                       issuer_code,
                       num_code,
                       str_pref,
                       num_code code101,
                       case
                         when pcis.t_partyid is not null then
                              pcis.t_partyid
                         when pcis1.t_partyid is not null then
                              pcis1.t_partyid
                         when pcis_bis.t_partyid is not null then
                              pcis_bis.t_partyid
                         when pcis1_bis.t_partyid is not null then
                              pcis1_bis.t_partyid
                       end partyid
                       --nvl(pcis.t_partyid, pcis1.t_partyid) partyid
                  from (
                select distinct code finstr_code,
                       dt,
                       nvl((select sec_portfolio_code
                                           from (select sec_portfolio_code,
                                                        row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                                                   from ldr_infa_cb.ass_sec_portfolio sp
                                                  where sp.security_code = code
                                                    and to_date(sp.dt, 'dd-mm-yyyy') <= in_date)
                                          where rnk = 1),
                                         '-1') portf,
                       issuer_code, regexp_replace(issuer_code, '(^.*IBSOXXX#)(\d*)(.*$)','\2') num_code,
                       case when regexp_like(issuer_code, '#banks$') then
                             '00#Б#'
                            when regexp_like(issuer_code, '#cust_corp$') then
                             '00#Y#'
                            when regexp_like(issuer_code, '#person$') then
                             '00#P#'
                            else
                             '!!!'
                            end ||
                       regexp_replace(issuer_code, '(^.*BISQUIT#)(\d*)(.*$)','\2') num_code2,
                       regexp_replace(issuer_code, '(^.*IBSOXXX#)(\d*)(.*$)','\3') str_pref
                  from ldr_infa_cb.det_security
                 where issuer_code <> '-1'
                ) ds
                  left join dpartcode_dbt pcis
                    on (num_code = pcis.t_code and pcis.t_codekind = 101 and pcis.t_state = 0)
                  left join dpartcode_dbt pcis1
                    on (num_code = pcis1.t_code and pcis1.t_codekind = 1101 and pcis1.t_state = 0)
                  left join dpartcode_dbt pcis_bis
                    on (num_code2 = pcis_bis.t_code and pcis_bis.t_codekind = 101 and pcis_bis.t_state = 0)
                  left join dpartcode_dbt pcis1_bis
                    on (num_code2 = pcis1_bis.t_code and pcis1_bis.t_codekind = 1101 and pcis1_bis.t_state = 0)
                 ),
                 iss_c as (select iss.finstr_code
                                 ,iss.dt
                                 ,iss.portf
                                 ,iss.partyid
                                 ,decode(pt.t_nrcountry, chr(1), (select distinct t_country from dadress_dbt adr where adr.t_partyid = iss.partyid and rownum = 1), pt.t_nrcountry) country
                  from iss
                   left join dadress_dbt adr
                     on (iss.partyid = adr.t_partyid and adr.t_type = n1)

                  inner join dparty_dbt pt
                    on (iss.partyid = pt.t_partyid)
                )
                select iss_c.*,
                       iss_c.country || ' ' || c.t_name code_name,
                       replace(c.t_riskclass, chr(0)) risk,
                       pc.t_code code102
                  from iss_c
                left join dcountry_dbt c
                  on (iss_c.country = c.t_codelat3)
                left join dpartcode_dbt pc
                  on (iss_c.partyid = pc.t_partyid and pc.t_codekind = n102 and pc.t_state = n0)
              )
    loop
      if rec.code_name is not null then
        insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
               values(rec.finstr_code,
                      rec.portf,
                      'COUNTRY',
                      null,          -- number_value
                      null,          -- date_value
                      rec.code_name, -- string_value
                      rec.code_name, -- value
                      rec.dt,
                      dwhRecStatus,
                      dwhSysMoment,
                      dwhEXT_FILE);
commit;                      
      end if;
      if rec.risk is not null then
        insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
               values(rec.finstr_code,
                      rec.portf,
                      'COUNTRY_SO',
                      rec.risk,      -- number_value
                      null,          -- date_value
                      null,          -- string_value
                      rec.risk,      -- value
                      rec.dt,
                      dwhRecStatus,
                      dwhSysMoment,
                      dwhEXT_FILE);
commit;                      
      end if;
      if rec.code102 is not null then
        insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
               values(rec.finstr_code,
                      rec.portf,
                      'ID_DIASOFT',
                      null,          -- number_value
                      null,          -- date_value
                      rec.code102, -- string_value
                      rec.code102, -- value
                      rec.dt,
                      dwhRecStatus,
                      dwhSysMoment,
                      dwhEXT_FILE);
commit;                      
      end if;
      if rec.partyid is not null then
        insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
               values(rec.finstr_code,
                      rec.portf,
                      'ID_SOFR',
                      qb_dwh_utils.NumberToChar(rec.partyid, 0),          -- number_value
                      null,          -- date_value
                      null,          -- string_value
                      qb_dwh_utils.NumberToChar(rec.partyid, 0),          -- value
                      rec.dt,
                      dwhRecStatus,
                      dwhSysMoment,
                      dwhEXT_FILE);
commit;                      
      end if;
    end loop;

    for rec in (select code,
                       r1.sdate1,
                       r1001.sdate1001,
                       case when r1.sdate1 is not null and r1001.sdate1001 is null then
                              'ДА'
                            else
                              'НЕТ'
                       end IS_ACTIVE_MARKET,
                       case when r1001.sdate1001 is not null then
                              '2'
                            else
                              '1'
                       end LEVEL_HIERARCHY,
                       dt,
                       nvl((select sec_portfolio_code
                              from (select sec_portfolio_code,
                                           row_number() over(order by to_date(dt, 'dd-mm-yyyy') desc) as rnk
                                      from ldr_infa_cb.ass_sec_portfolio sp
                                     where sp.security_code = code
                                       and to_date(sp.dt, 'dd-mm-yyyy') <= in_date)
                             where rnk = n1),
                            '-1') portf
                  from ldr_infa_cb.det_security
                 inner join dfininstr_dbt fi
                    on (to_number(regexp_replace(code, '^(\d*)(#FIN)$', '\1')) = fi.t_fiid)
                  left join (select fiid, max(sdate) sdate1
                               from (select rd.t_type rtype,
                                            rd.t_otherfi fiid,
                                            rd.t_sincedate sdate
                                       from dratedef_dbt rd
                                      where rd.t_type = n1
                                     union all
                                     select rd.t_type rtype,
                                            rd.t_otherfi fiid,
                                            rh.t_sincedate
                                       from dratedef_dbt rd
                                       inner join dratehist_dbt rh
                                         on (rd.t_rateid = rh.t_rateid)
                                      where rd.t_type = n1)
                                where sdate <= in_date
                                group by fiid) r1
                     on (fi.t_fiid = r1.fiid)
                  left join (select fiid, max(sdate) sdate1001
                               from (select rd.t_type rtype,
                                            rd.t_otherfi fiid,
                                            rd.t_sincedate sdate
                                       from dratedef_dbt rd
                                      where rd.t_type = n1001
                                     union all
                                     select rd.t_type rtype,
                                            rd.t_otherfi fiid,
                                            rh.t_sincedate
                                       from dratedef_dbt rd
                                       inner join dratehist_dbt rh
                                         on (rd.t_rateid = rh.t_rateid)
                                      where rd.t_type = n1001)
                                where sdate <= in_date
                                group by fiid) r1001
                      on (fi.t_fiid = r1001.fiid)
                  where code like '%#FIN'
                    and (r1.sdate1 is not null or r1001.sdate1001 is not null)
                 order by fi.t_fiid
                )
    loop
      insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
             values(rec.code,
                    rec.portf,
                    'IS_ACTIVE_MARKET',
                    null,                          -- number_value
                    null,                          -- date_value
                    rec.is_active_market,          -- string_value
                    rec.is_active_market,          -- value
                    rec.dt,
                    dwhRecStatus,
                    dwhSysMoment,
                    dwhEXT_FILE);
commit;                    
      insert into ldr_infa_cb.fct_security_attr(security_code, sec_portfolio_code, code_security_attr, number_value, date_value, string_value, value, dt, rec_status, sysmoment, ext_file)
             values(rec.code,
                    rec.portf,
                    'LEVEL_HIERARCHY',
                    rec.level_hierarchy,           -- number_value
                    null,                          -- date_value
                    null,                          -- string_value
                    rec.level_hierarchy,           -- value
                    rec.dt,
                    dwhRecStatus,
                    dwhSysMoment,
                    dwhEXT_FILE);
commit;                    
    end loop;
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при добавлении доп.атрибутов: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;

    -- Вставка характеристик лотов
    qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка характеристик лотов',
                       2,
                       null,
                       null);
    begin
      insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
      (select * from (
              with /*dlot as (select sumid from dwh_histsum_tmp  ),
                            lot0 as( select v.T_FIID,
                                                  v.T_SUMID,
                                                  v.T_CHANGEDATE,
                                                  v.T_TIME,
                                                  v.T_AMOUNT,
                                                  v.T_SUM,
                                                  v.T_DEALID,
                                                  v.T_INSTANCE,
                                                  max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                  v.T_CORRINTTOEIR c2eps,
                                                  decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                                  v.T_RESERVAMOUNT c2rpbu,
                                                  v.T_INCOMERESERV c2rpbu_nkd
                                             from  v_scwrthistex v
                                             where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                               and v.T_CHANGEDATE <= in_date
                                              ),*/
                             lot0 as (select /*+ PARALLEL(4) */
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                                      ),
                            lot as (select lot0.*,
                                           lag(c2eps, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_c2eps
                                      from lot0
                                      where t_instance = maxinstance),
                            f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                       from lot where c2eps <> prev_c2eps),
                            flot as (select f0lot.*,
                                            case when nxt_change_date = t_changedate then
                                              t_changedate
                                            else
                                              nxt_change_date - 1
                                            end next_change_date,
                                            case when nxt_change_date = t_changedate then
                                              1
                                            else
                                              nxt_change_date - t_changedate
                                            end cnt_days
                                       from f0lot),
                            llot as (select flot.*,
                                            to_char(flot.t_fiid) || '#FIN' cfiid,
                                            to_char(t_dealid) || '#TCK' cdealid
                                       from flot
                                      )
                            select cdealid,
                                   cfiid,
                                   to_char(t_sumid),
                                   '1' adjtype,
                                   qb_dwh_utils.NumberToChar(c2eps, 2) sum,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                                   qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                                   dwhRecStatus,
                                   dwhSysMoment,
                                   dwhEXT_FILE
                              from llot --where cfiid = '1172#FIN' --!!!!!!!!!!!!
                              )
      union all
      select * from (
              with /* dlot as (select sumid from dwh_histsum_tmp  ),
                            lot0 as( select v.T_FIID,
                                                  v.T_SUMID,
                                                  v.T_CHANGEDATE,
                                                  v.T_TIME,
                                                  v.T_AMOUNT,
                                                  v.T_SUM,
                                                  v.T_DEALID,
                                                  v.T_INSTANCE,
                                                  max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                  v.T_CORRINTTOEIR c2eps,
                                                  decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                                  v.T_RESERVAMOUNT c2rpbu,
                                                  v.T_INCOMERESERV c2rpbu_nkd
                                             from  v_scwrthistex v
                                             where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                               and v.T_CHANGEDATE <= in_date
                                              ),*/
                             lot0 as (select /*+ PARALLEL(4) */
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                                      ),
                            lot as (select lot0.*,
                                           lag(c2oku, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_c2oku
                                      from lot0
                                      where t_instance = maxinstance),
                            f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                       from lot where c2oku <> prev_c2oku),
                            flot as (select f0lot.*,
                                            case when nxt_change_date = t_changedate then
                                              t_changedate
                                            else
                                              nxt_change_date - 1
                                            end next_change_date,
                                            case when nxt_change_date = t_changedate then
                                              1
                                            else
                                              nxt_change_date - t_changedate
                                            end cnt_days
                                       from f0lot),
                            llot as (select flot.*,
                                            to_char(flot.t_fiid) || '#FIN' cfiid,
                                            to_char(t_dealid) || '#TCK' cdealid
                                       from flot
                                      )
                            select cdealid,
                                   cfiid,
                                   to_char(t_sumid) sumid,
                                   '2' adjtype,
                                   qb_dwh_utils.NumberToChar(c2oku, 2) sum,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                                   qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                                   dwhRecStatus,
                                   dwhSysMoment,
                                   dwhEXT_FILE
                              from llot --where cfiid = '1172#FIN'
                              )
      union all
      select * from (
              with /* dlot as (select sumid from dwh_histsum_tmp  ),
                            lot0 as( select v.T_FIID,
                                                  v.T_SUMID,
                                                  v.T_CHANGEDATE,
                                                  v.T_TIME,
                                                  v.T_AMOUNT,
                                                  v.T_SUM,
                                                  v.T_DEALID,
                                                  v.T_INSTANCE,
                                                  max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                  v.T_CORRINTTOEIR c2eps,
                                                  decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                                  v.T_RESERVAMOUNT c2rpbu,
                                                  v.T_INCOMERESERV c2rpbu_nkd
                                             from  v_scwrthistex v
                                             where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                               and v.T_CHANGEDATE <= in_date
                                              ),*/
                             lot0 as (select /*+ PARALLEL(4) */
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                                      ),
                            lot as (select lot0.*,
                                           lag(c2rpbu, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_c2rpbu
                                      from lot0
                                      where t_instance = maxinstance),
                            f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                       from lot where c2rpbu <> prev_c2rpbu),
                            flot as (select f0lot.*,
                                            case when nxt_change_date = t_changedate then
                                              t_changedate
                                            else
                                              nxt_change_date - 1
                                            end next_change_date,
                                            case when nxt_change_date = t_changedate then
                                              1
                                            else
                                              nxt_change_date - t_changedate
                                            end cnt_days
                                       from f0lot),
                            llot as (select flot.*,
                                            to_char(flot.t_fiid) || '#FIN' cfiid,
                                            to_char(t_dealid) || '#TCK' cdealid
                                       from flot
                                      )
                            select cdealid,
                                   cfiid,
                                   to_char(t_sumid) sumid,
                                   '3' adjtype,
                                   qb_dwh_utils.NumberToChar(c2rpbu, 2) sum,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                                   qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                                   dwhRecStatus,
                                   dwhSysMoment,
                                   dwhEXT_FILE
                              from llot --where cfiid = '1172#FIN'
                              )

      union all
      select * from (
              with /* dlot as (select sumid from dwh_histsum_tmp  ),
                            lot0 as( select v.T_FIID,
                                                  v.T_SUMID,
                                                  v.T_CHANGEDATE,
                                                  v.T_TIME,
                                                  v.T_AMOUNT,
                                                  v.T_SUM,
                                                  v.T_DEALID,
                                                  v.T_INSTANCE,
                                                  max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                  v.T_CORRINTTOEIR c2eps,
                                                  decode(v.T_PORTFOLIO, 5, v.T_CORRESTRESERVE, v.T_ESTRESERVE) c2oku,
                                                  v.T_RESERVAMOUNT c2rpbu,
                                                  v.T_INCOMERESERV c2rpbu_nkd
                                             from  v_scwrthistex v
                                             where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                               and v.T_CHANGEDATE <= in_date
                                              ),*/
                             lot0 as (select /*+ PARALLEL(4) */
                                    v.T_FIID,
                                    v.T_SUMID,
                                    v.T_CHANGEDATE,
                                    v.T_TIME,
                                    v.T_AMOUNT,
                                    v.T_SUM,
                                    v.T_DEALID,
                                    v.T_INSTANCE,
                                    max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                    v.c2eps,
                                    v.c2oku,
                                    v.c2rpbu,
                                    v.c2rpbu_nkd
                               from DWH_scwrthistex_TMP v
                              where v.T_CHANGEDATE <= in_date
                                      ),
                            lot as (select lot0.*,
                                           lag(c2rpbu_nkd, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_c2rpbu_nkd
                                      from lot0
                                      where t_instance = maxinstance),
                            f0lot as (select lot.*,
                                            lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                       from lot where c2rpbu_nkd <> prev_c2rpbu_nkd),
                            flot as (select f0lot.*,
                                            case when nxt_change_date = t_changedate then
                                              t_changedate
                                            else
                                              nxt_change_date - 1
                                            end next_change_date,
                                            case when nxt_change_date = t_changedate then
                                              1
                                            else
                                              nxt_change_date - t_changedate
                                            end cnt_days
                                       from f0lot),
                            llot as (select flot.*,
                                            to_char(flot.t_fiid) || '#FIN' cfiid,
                                            to_char(t_dealid) || '#TCK' cdealid
                                       from flot
                                      )
                            select cdealid,
                                   cfiid,
                                   to_char(t_sumid) sumid,
                                   '10' adjtype,
                                   qb_dwh_utils.NumberToChar(c2rpbu_nkd, 2) sum,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                                   qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                                   qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                                   dwhRecStatus,
                                   dwhSysMoment,
                                   dwhEXT_FILE
                              from llot --where cfiid = '1172#FIN'
                              )
      );
commit;      
    insert into ldr_infa_cb.fct_sec_adjustment(deal_code, finstr_code, lot_num, adjustment_type, amount, dt_begin, dt_end, dt, rec_status, sysmoment, ext_file)
       (select * from (
        with /* dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                              max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(bonus, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_bonus
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where bonus <> prev_bonus),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '8' adjtype,
                             qb_dwh_utils.NumberToChar(bonus, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(begbonus, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_begbonus

                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where begbonus <> prev_begbonus),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '7' adjtype,
                             qb_dwh_utils.NumberToChar(begbonus, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(nkd, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_nkd
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where nkd <> prev_nkd),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '4' adjtype,
                             qb_dwh_utils.NumberToChar(nkd, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(discount, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_discount
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where discount <> prev_discount),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '5' adjtype,
                             qb_dwh_utils.NumberToChar(discount, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(interest, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_interest
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where interest <> prev_interest),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '6' adjtype,
                             qb_dwh_utils.NumberToChar(interest, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot)
        union all
        select * from (
        with /*dlot as (select sumid from dwh_histsum_tmp  ),
                      lot0 as( select v.T_FIID,
                                            v.T_SUMID,
                                            v.T_CHANGEDATE,
                                            v.T_TIME,
                                            v.T_AMOUNT,
                                            v.T_SUM,
                                            v.T_DEALID,
                                            v.T_INSTANCE,
                                            max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                            v.T_NKDAMOUNT    nkd,
                                            v.T_DISCOUNTINCOME discount,
                                            v.T_INTERESTINCOME interest,
                                            v.T_BEGBONUS begbonus,
                                            v.T_BONUS bonus,
                                            v.T_OVERAMOUNT over
                                       from  v_scwrthistex v
                                       where exists  (select 1 from dlot where sumid = v.T_SUMID)
                                         and v.T_CHANGEDATE <= in_date
                                        ),*/
                      lot0 as (select /*+ PARALLEL(4) */
                             v.T_FIID,
                             v.T_SUMID,
                             v.T_CHANGEDATE,
                             v.T_TIME,
                             v.T_AMOUNT,
                             v.T_SUM,
                             v.T_DEALID,
                             v.T_INSTANCE,
                             max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                             v.nkd,
                             v.discount,
                             v.interest,
                             v.begbonus,
                             v.bonus,
                             v.over
                        from DWH_scwrthistex_TMP v
                       where v.T_CHANGEDATE <= in_date
                               ),
                      lot as (select lot0.*,
                                     lag(over, 1, 0) over( partition by t_sumid order by t_changedate, t_instance) prev_over
                                from lot0
                                where t_instance = maxinstance),
                      f0lot as (select lot.*,
                                      lead(t_changedate , 1, in_date + 1 ) over( partition by t_sumid order by t_changedate, t_instance)  nxt_change_date
                                 from lot where over <> prev_over),
                      flot as (select f0lot.*,
                                      case when nxt_change_date = t_changedate then
                                        t_changedate
                                      else
                                        nxt_change_date - 1
                                      end next_change_date,
                                      case when nxt_change_date = t_changedate then
                                        1
                                      else
                                        nxt_change_date - t_changedate
                                      end cnt_days
                                 from f0lot),
                      llot as (select flot.*,
                                      qb_dwh_utils.NumberTochar(flot.bonus, 0) cbonus,
                                      to_char(flot.t_fiid) || '#FIN' cfiid,
                                      to_char(t_dealid) || '#TCK' cdealid
                                 from flot
                                )
                      select cdealid,
                             cfiid,
                             to_char(t_sumid),
                             '9' adjtype,
                             qb_dwh_utils.NumberToChar(over, 2) sum,
                             qb_dwh_utils.DateToChar(llot.t_changedate) bd,
                             qb_dwh_utils.DateToChar(llot.next_change_date) ed,
                             qb_dwh_utils.DateToChar(llot.t_changedate) dt,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE
                        from llot));
commit;                        
      -- Вставка дисконта к номиналу на дату покупки
      insert into ldr_infa_cb.fct_sec_adjustment
        (deal_code,
         finstr_code,
         lot_num,
         adjustment_type,
         amount,
         dt_begin,
         dt_end,
         dt,
         rec_status,
         sysmoment,
         ext_file)
        (select *
           from (with /*dlot as (select sumid
                                 from dwh_histsum_tmp),
                                                        lot0 as (select v.t_fiid,
                                                                        v.t_sumid,
                                                                        v.t_portfolio,
                                                                        v.t_changedate,
                                                                        v.t_time,
                                                                        v.t_amount,
                                                                        v.t_sum,
                                                                        v.t_cost,
                                                                        v.t_state,
                                                                        v.t_parent,
                                                                        v.t_dealid,
                                                                        v.t_instance,
                                                                        max(v.t_instance) over(partition by v.t_sumid, v.t_changedate) maxinstance,
                                                                        v.t_corrinttoeir c2eps,
                                                                        decode(v.t_portfolio,
                                                                               5,
                                                                               v.t_correstreserve,
                                                                               v.t_estreserve) c2oku,
                                                                        v.t_reservamount c2rpbu,
                                                                        v.t_incomereserv c2rpbu_nkd
                                                                   from v_scwrthistex v
                                                                  where exists
                                                                  (select 1
                                                                           from dlot
                                                                          where sumid =
                                                                                v.t_sumid)
                                                                    and v.t_changedate <=
                                                                        in_date), */
                                                     lot0 as (      select /*+ PARALLEL(4) */
                                                                           v.T_FIID,
                                                                           v.T_SUMID,
                                                                           v.T_PORTFOLIO,
                                                                           v.T_CHANGEDATE,
                                                                           v.T_TIME,
                                                                           v.T_AMOUNT,
                                                                           v.T_SUM,
                                                                           v.T_COST,
                                                                           v.T_STATE,
                                                                           v.T_PARENT,
                                                                           v.T_DEALID,
                                                                           v.T_INSTANCE,
                                                                           max(v.T_INSTANCE) over (partition by v.T_SUMID, v.T_CHANGEDATE) maxinstance,
                                                                           v.c2eps,
                                                                           v.c2oku,
                                                                           v.c2rpbu,
                                                                           v.c2rpbu_nkd,
                                                                           v.begdiscount
                                                                      from DWH_scwrthistex_TMP v
                                                                     where v.T_CHANGEDATE <= in_date
                                                                                               ),
                                                                                  lot as (select lot0.*,
                                                                                                  lag(t_amount,
                                                                                                      1,
                                                                                                      0) over(partition by t_sumid order by t_changedate, t_instance) prev_amount,
                                                                                                  lag(begdiscount, --15.04.2022 добавлена проверка изменений дисконта, т.к. он может меняться при неизменном количестве ЦБ 
                                                                                                      1,
                                                                                                      0) over(partition by t_sumid order by t_changedate, t_instance) prev_begdiscount    
                                                                                             from lot0
                                                                                            where t_instance = maxinstance), 
                                                                                                                           f0lot as (select lot.*,
                                                                                                                                 lead(t_changedate,
                                                                                                                                      1,
                                                                                                                                      in_date + 1) over(partition by t_sumid order by t_changedate, t_instance) nxt_change_date
                                                                                                                            from lot
                                                                                                                           where t_amount <> prev_amount or begdiscount <> prev_begdiscount ), 
                                                                                                                                                            flot as (select f0lot.*,
                                                                                                                                                               case
                                                                                                                                                                 when nxt_change_date =
                                                                                                                                                                      t_changedate then
                                                                                                                                                                  t_changedate
                                                                                                                                                                 else
                                                                                                                                                                  nxt_change_date - 1
                                                                                                                                                               end next_change_date,
                                                                                                                                                               case
                                                                                                                                                                 when nxt_change_date =
                                                                                                                                                                      t_changedate then
                                                                                                                                                                  1
                                                                                                                                                                 else
                                                                                                                                                                  nxt_change_date -
                                                                                                                                                                  t_changedate
                                                                                                                                                               end cnt_days,
                                                                                                                                                               first_value(t_sum) over(partition by t_fiid, t_sumid order by t_changedate, t_instance) sum_buy,
                                                                                                                                                               first_value(t_amount) over(partition by t_fiid, t_sumid order by t_changedate, t_instance) cnt_buy
                                                                                                                                                          from f0lot), llot as (select flot.*,
                                                                                                                                                                                       qb_dwh_utils.numbertochar(flot.t_amount,
                                                                                                                                                                                                                 0) camount,
                                                                                                                                                                                       to_char(flot.t_fiid) ||
                                                                                                                                                                                       '#FIN' cfiid,
                                                                                                                                                                                       to_char(t_dealid) ||
                                                                                                                                                                                       '#TCK' cdealid,
                                                                                                                                                                                       (select sum(lnk.t_sumsale)
                                                                                                                                                                                          from dpmwrtlnk_dbt lnk
                                                                                                                                                                                         where lnk.t_buyid =
                                                                                                                                                                                               flot.t_sumid
                                                                                                                                                                                           and lnk.t_createdate <=
                                                                                                                                                                                               flot.t_changedate) sum_sale
                                                                                                                                                                                  from flot)

                  select cdealid,
                         cfiid,
                         to_char(t_sumid) sumid,
                         '11' adjtype,
                         --qb_dwh_utils.numbertochar(round((fi.t_facevalue -
                         --                                sum_buy / cnt_buy) * t_amount,
                         --                                2),
                         --                          2) discont_buy,
                         qb_dwh_utils.NumberToChar(round(llot.begdiscount, 2), 2) discont_buy, -- iSupport#532380
                         qb_dwh_utils.datetochar(llot.t_changedate) bd,
                         qb_dwh_utils.datetochar(llot.next_change_date) ed,
                         qb_dwh_utils.datetochar(llot.t_changedate) dt,
                         dwhRecStatus,
                         dwhSysMoment,
                         dwhEXT_FILE
                    from llot
                   inner join dfininstr_dbt fi
                      on (llot.t_fiid = fi.t_fiid)
                   where round(llot.begdiscount, 2) <> 0
                   )
         );
commit;
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при добавлении характеристик лотов: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;

    -- Вставка гашений номинала для облигаций, по которым в СОФР нет ни одного гашения.
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Генерация гашений номинала по облигациям',
                   2,
                   null,
                   null);
    begin
    insert into ldr_infa_cb.fct_secrepayschedule(code,typeschedule, typerepaysec, begindate, enddate, proc_rate, proc_sum, security_code, dt, rec_status, sysmoment, ext_file)
      select to_char((select max(to_number(code)) from ldr_infa_cb.fct_secrepayschedule) + rownum) code,
             '2',
             '2',
             b.secissueregdate begidate,
             b.maturitydate enddate,
             '0' proc_rate,
             qb_dwh_utils.NumberToChar(to_number(ds.nominal), 3) procsum,
             b.security_code,
             b.dt,
             dwhRecStatus,
             dwhSysMoment,
             dwhEXT_FILE
        from ldr_infa_cb.det_bond  b
       inner join ldr_infa_cb.det_security ds
          on (b.backofficecode = ds.code)
        left join  ldr_infa_cb.fct_secrepayschedule s
          on (b.security_code = s.security_code and s.typeschedule = v2)
       where s.code is null;
commit;       
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при добавлении списка гашений: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;

    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Очиста данных по дате открытия ц/б',
                   2,
                   null,
                   null);

    begin
    --  Очистка лишних записей всего 107 шт.
    delete from ldr_infa_cb.ass_sec_portfolio where security_code not in (select code from ldr_infa_cb.det_security ds);commit;
    delete from ldr_infa_cb.fct_securityamount where security_code not in (select code from ldr_infa_cb.det_security ds);commit;
    -- Удаляем иоторияю по векселям которые на данный момент погашены
    /*
    delete from ldr_infa_cb.fct_securityamount s
     where s.security_code in (select bill_code
                                 from (select bs.*,
                                              row_number() over(partition by bs.bill_code order by bs.dt desc) rnk
                                         from ldr_infa_cb.fct_bill_state bs
                                        order by bill_code, dt)
                                where (rnk = 1 and bill_state = 4));  -- последний статус векселя - погашен
    */
    -- Удалим курсы действующие до открытия ц/б
    delete from ldr_infa_cb.fct_finstr_rate dr
     where exists( select 1
                     from ldr_infa_cb.fct_finstr_rate r
                    inner join ldr_infa_cb.det_security s
                       on (r.finstr_numerator_finstr_code = s.code)
                    where to_date(r.dt,'dd-mm-yyyy') < to_date(s.dt,'dd-mm-yyyy')
                      and  r.finstr_numerator_finstr_code = dr.finstr_numerator_finstr_code
                      and  r.finstr_denumerator_finstr_code = dr.finstr_denumerator_finstr_code
                      and r.dt = dr.dt);
commit;                      
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при очистке подготовленнмх данных: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;
    select count(*)
      into cnt
      from qb_dwh_const4exp_val t 
     where t.id = n26 and t.value = n1;
    if (cnt > 0 and BIQ_7477_78 = 1) then
      -- Выгрузка данных по BIQ 7477/7478
      begin
        insert into ldr_infa_cb.det_rating_type
          (code,
           subject_code,
           name_eng,
           name_rus,
           official_name,
           object_type,
           rating_scale_name,
           credit_rating,
           currency,
           scale,
           period,
           dt_change,
           dt,
           rec_status,
           sysmoment,
           ext_file)
          select distinct '9999#SOFRXXX#' || la.code_name code,
                 '9999#SOFRXXX#' || la.agency_eng || '#CUST_CORP' subject_code,
                 la.fullname_eng name_eng,
                 la.fullname_rus name_rus,
                 la.official_name official_name,
                 case
                   when la.for_instrument = '1' then
                    '0'
                   when la.for_company = '1' then
                    '1'
                   when la.for_instrument = '0' and la.for_company = '0' then
                    '2'
                 end object_type,
                 la.scale_type_name rating_scale_name,
                 case
                   when la.is_credit = '0' then
                    '0'
                   when la.is_credit = '1' then
                    '1'
                 end credit_rating,
                 case
                   when (la.currency_type = 'N' or la.currency_type is null)  then
                    '0'
                   when la.currency_type = 'L' then
                    '1'
                   when la.currency_type = 'F' then
                    '2'
                 end currency,
                 case
                   when la.scale_type = 'N' then
                    '0'
                   when (la.scale_type = 'I' or la.scale_type is null) then
                    '1'
                 end scale,
                 case
                   when la.term_type is null then
                    '0'
                   when la.term_type = 'S' then
                    '1'
                   when la.term_type = 'L' then
                    '2'
                 end period,
                 qb_dwh_utils.datetochar(la.sysmoment) dt_change,
                 qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
                 '0' rec_status,
                 dwhSysmoment,
                 dwhExt_File
            from sofr_rating_listratings la;
commit;            
        insert into ldr_infa_cb.det_rating(code,
                                           rating_type_code,
                                           name,
                                           dt_change,
                                           dt,
                                           rec_status,
                                           sysmoment,
                                           ext_file)
        select '9999#SOFRXXX#' || rl.code_name || '#' || rh.prev code,
               '9999#SOFRXXX#' || rl.code_name rating_type_code,
               rh.prev name,
               qb_dwh_utils.DateToChar(max(rh.sysmoment)) dt_change,
               qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
               '0',
               dwhSysmoment,
               dwhExt_File
           from sofr_rating_ratingshistory rh
          inner join sofr_rating_listratings rl
             on (rh.rating_id = rl.rating_id)
          where rh.prev is not null
          group by rh.prev, rh.Rating_Id, rl.code_name
        union
        select distinct
               '9999#SOFRXXX#' || rl.code_name || '#' || rh.last code,
               '9999#SOFRXXX#' || rl.code_name rating_type_code,
               rh.last name,
               qb_dwh_utils.DateToChar(max(rh.sysmoment)) dt_change,
               qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
               '0',
               dwhSysmoment,
               dwhExt_File
           from sofr_rating_ratingshistory rh
          inner join sofr_rating_listratings rl
             on (rh.rating_id = rl.rating_id)
          where rh.last is not null
          group by rh.last, rh.Rating_Id, rl.code_name
          order by rating_type_code, code;
commit;
        insert into ldr_infa_cb.fct_sec_rating(security_code,
                                               rating_code,
                                               --rating_type,
                                               dt,
                                               rec_status,
                                               sysmoment,
                                               ext_file)
        select distinct to_char(avr.t_fiid) || '#FIN'  SECURITY_CODE,
               '9999#SOFRXXX#' || rl.code_name || '#' || rh.last RATING_CODE,
      --         '9999#SOFRXXX#' || rl.code_name RATING_TYPE,
               qb_dwh_utils.DateToChar(rh.last_dt) dt,
               '0' rec_status,
               dwhsysmoment,
               dwhext_file
          from sofr_rating_ratingshistory rh
          inner join sofr_rating_listratings rl
             on (rh.rating_id = rl.rating_id)
          inner join davoiriss_dbt avr
             on (rh.isin = avr.t_isin);
commit;
        insert into ldr_infa_cb.det_subject
          (typesubject,
           code_subject,
           dt_reg,
           inn,
           system_code,
           department_code,
           country_code_num,
           dt,
           rec_status,
           sysmoment,
           ext_file)
          select '2' typesubject,
                 '9999#SOFRXXX#' || rl.agency_eng || '#CUST_CORP' code_subject,
                 null dt_reg,
                 null inn,
                 'SOFRXXX' system_code,
                 '0000' department_code,
                 '-1' country_code_num,
                 --qb_dwh_utils.datetochar(max(rl.sysmoment)) dt,
                 qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
                 '0',
                 dwhSysmoment,
                 dwhExt_File
            from sofr_rating_listratings rl
           group by rl.agency_eng
           order by rl.agency_eng;
commit;
          insert into ldr_infa_cb.det_juridic_person
            (juridic_person_name_s,
             juridic_person_name,
             dt_registration,
             note,
             subject_code,
             okved_code,
             okato_code,
             dt,
             rec_status,
             sysmoment,
             ext_file)
            select rl.agency_eng juridyc_person_name_s,
                   rl.agency_eng juridyc_person_name,
                   null dt_registration,
                   null note,
                   '9999#SOFRXXX#' || rl.agency_eng || '#CUST_CORP' subject_code,
                   '-1' okved_code,
                   '-1' okato_code,
                   --qb_dwh_utils.datetochar(max(rl.sysmoment)) dt,
                   qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
                   '0',
                   dwhSysmoment,
                   dwhExt_File
              from sofr_rating_listratings rl
             group by rl.agency_eng
             order by rl.agency_eng;
commit;
        insert into ldr_infa_cb.fct_securityrisk
          select distinct
                 case
                   when at.t_attrid = 1 then
                    '9999#SOFRXXX#1'
                   when at.t_attrid = 2 then
                    '9999#SOFRXXX#2'
                   when at.t_attrid = 3 then
                    '9999#SOFRXXX#3'
                   when at.t_attrid = 4 then
                    '9999#SOFRXXX#4'
                   when at.t_attrid = 5 then
                    '9999#SOFRXXX#5'
                 end riskcat_code,
                 to_char(fi.t_fiid) || case
                   when fi.t_avoirkind = 5 then
                    '#BNR'
                   else
                    '#FIN'
                 end security_code,
                 --replace(replace(replace(regexp_substr(at.t_fullname, '\(.*\)'), ')'),
                 --                '('),
                 --        '%') reserve_rate,
                 nvl ( replace(rsb_struct.getString(nt.t_text), chr(0), ''), 0) reserve_rate,
                 null ground,
                 qb_dwh_utils.datetochar(decode(ac.t_validfromdate,
                                                to_date('01010001', 'ddmmyyyy'),
                                                to_date('01011980', 'ddmmyyyy'),
                                                ac.t_validfromdate)) dt,
                 cCODE_TYPERISK RISKCAT_CODE_TYPERISK,
                 '0' rec_status,
                 dwhSysmoment,
                 dwhExt_file
            from dobjatcor_dbt ac
           inner join dobjgroup_dbt gr
              on (ac.t_objecttype = gr.t_objecttype and ac.t_groupid = gr.t_groupid)
           inner join dobjattr_dbt at
              on (ac.t_objecttype = at.t_objecttype and ac.t_groupid = at.t_groupid and
                 ac.t_attrid = at.t_attrid)
           inner join dfininstr_dbt fi
              on (ac.t_object = lpad(to_char(fi.t_fiid), 10, '0'))
           left join dnotetext_dbt nt
              on (nt.t_objecttype = 12 and nt.t_notekind = 3 and ac.t_validfromdate between nt.t_date and nt.t_validtodate
                  and nt.t_documentid = ac.t_object)
           where ac.t_objecttype = 12 -- объект ценная бумага
             and gr.t_type = chr(88)
             and gr.t_groupid = 13 -- категория качества
             and exists (select 1
                    from ldr_infa_cb.det_finstr df
                   where df.finstr_code = to_char(fi.t_fiid) || case
                           when fi.t_avoirkind = 5 then
                            '#BNR'
                           else
                            '#FIN'
                         end)
             and BIQ_7477_78 = 1
             ;
commit;

        insert into ldr_infa_cb.det_kindprocrate(code,
                                                 name,
                                                 dt,
                                                 rec_status,
                                                 sysmoment,
                                                 ext_file)
        select '9999#SOFRXXX#1#FIXED',
               'Фиксированная ставка по гашениям купонов',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#1#FLOAT',
               'Плавающая ставка  по гашениям купонов',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#2#FIXED',
               'Фиксированная ставка по гашениям облигаций',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#2#FLOAT',
               'Плавающая ставка  по гашениям облигаций',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#3#FIXED',
               'Фиксированная ставка по гашениям векселей',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual
        union all
        select '9999#SOFRXXX#3#FLOAT',
               'Плавающая ставка по гашениям векселей',
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
               '0',
               dwhSysmoment,
               dwhExt_File
          from dual;
commit;          
        insert into ldr_infa_cb.det_subkindprocrate(code,
                                                    name,
                                                    counts,
                                                    period,
                                                    dt,
                                                    rec_status,
                                                    sysmoment,
                                                    ext_file)
                    select '-1',
                           'Не определено',
                           '-1',
                           '-1',
                           qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')),
                           '0',
                           dwhSysmoment,
                           dwhExt_File
                           from dual ;
commit;
        insert into ldr_infa_cb.fct_procrate_security(procsum,
                                                      procrate,
                                                      dt_next_overvalue,
                                                      security_code,
                                                      kindprocrate_code,
                                                      procbase_code,
                                                      subkindprocrate_code,
                                                      dt,
                                                      rec_status,
                                                      sysmoment,
                                                      ext_file)
        select distinct
               qb_dwh_utils.NumberToChar(procsum, 4) procsum,
               qb_dwh_utils.NumberToChar(procrate, 4) procrate,
               dt_next_overvalue,
               security_code,
               kindprocrate_code,
               procbase_code,
               subkindprocrate_code,
               dt,
               '0' rec_status,
               dwhSysmoment,
               dwhExt_File
          from (
        select wr.t_incomevolume procsum,
               RSI_RSB_FIINSTR.CalcNKD_Ex_NoRound(wr.t_fiid, wr.t_drawingdate, 1.0, 1, 0, 0) first_calc,
               qb_dwh_utils.NumberToChar(case
                                           when wr.t_incomerate > 0 then
                                             wr.t_incomerate
                                           when h.t_incomerate > 0 then
                                             h.t_incomerate
                                           else
                                             round(RSI_RSB_FIInstr.FI_ReturnIncomeRate(), 4)
                                         end ) procrate,
               qb_dwh_utils.DateToChar(decode(lead(wr.t_drawingdate, 1, null) over (partition by wr.t_fiid order by wr.t_drawingdate), emptdate, firstdate, wr.t_drawingdate)) DT_NEXT_OVERVALUE,
               to_char(fi.t_fiid) || '#FIN' SECURITY_CODE,
               '9999#SOFRXXX#1#FIXED' KINDPROCRATE_CODE,  -- гашение купона по фиксированной ставке
               '9999#SOFRXXX#-1' PROCBASE_CODE,
               '-1' SUBKINDPROCRATE_CODE,
               qb_dwh_utils.DateToChar(decode(wr.t_drawingdate,emptdate, firstdate, wr.t_drawingdate)) dt
          from dfiwarnts_dbt wr
          inner join dfininstr_dbt fi
             on (wr.t_fiid = fi.t_fiid)
          left join dflrhist_dbt h
            on (wr.t_id = h.t_fiwarntid)

          where wr.t_ispartial = chr(0)
            and exists (select 1
                          from ldr_infa_cb.det_finstr df
                         where df.finstr_code = to_char(fi.t_fiid) || '#FIN')
        union all
        select wr.t_incomevolume procsum,
               RSI_RSB_FIINSTR.CalcNKD_Ex_NoRound(wr.t_fiid, wr.t_drawingdate, 1.0, 1, 0, 0) first_calc,
               qb_dwh_utils.NumberToChar(case
                                           when wr.t_incomerate > 0 then
                                             wr.t_incomerate
                                           when h.t_incomerate > 0 then
                                             h.t_incomerate
                                           else
                                             round(RSI_RSB_FIInstr.FI_ReturnIncomeRate(), 4)
                                         end ) procrate,
               qb_dwh_utils.DateToChar(lead(wr.t_drawingdate, 1, null) over (partition by wr.t_fiid order by wr.t_drawingdate)) DT_NEXT_OVERVALUE,
               to_char(fi.t_fiid) || '#FIN' SECURITY_CODE,
               '9999#SOFRXXX#2#' || case
                                      when h.t_fiwarntid is not null then
                                          'FLOAT'
                                        else
                                          'FIXED'
                                    end KINDPROCRATE_CODE,  -- гашение облигации
               '9999#SOFRXXX#' || case av.t_nkdbase_kind
                         when 0 then
                            '365'
                         when 1 then
                            '30/360'
                         when 2 then
                            '360/0'
                         when 3 then
                            '30/365'
                         when 4 then
                            '366'
                         when 5 then
                            '30/366'
                         when 6 then
                            'Act/по_купонным_периодам'
                         when 7 then
                            'Act/365L'
                         when 8 then
                            'Act/364'
                         when 9 then
                            '30E/360'
                         when 10 then
                            '30/360_ISDA'
                         when 11 then
                            'Act/Act_ICMA'
                         else
                           '-1'
                       end procbase_code,
               '-1' SUBKINDPROCRATE_CODE,
               qb_dwh_utils.DateToChar(decode(wr.t_drawingdate,emptdate, firstdate, wr.t_drawingdate)) dt
           from dfiwarnts_dbt wr
          inner join dfininstr_dbt fi
             on (wr.t_fiid = fi.t_fiid)
          inner join davoiriss_dbt av
             on (fi.t_fiid =  av.t_fiid)
          left join dflrhist_dbt h
            on (wr.t_id = h.t_fiwarntid)
          where wr.t_ispartial = chr(88)
            and exists (select 1
                          from ldr_infa_cb.det_finstr df
                         where df.finstr_code = to_char(fi.t_fiid) || '#FIN')
        union all
                          select round(bnin.t_perc, 9) procsum,
                                 null first_calc,
                                 case when leg.t_formula = 1 then
                                   qb_dwh_utils.NumberToChar(leg.t_price/power(10, leg.t_point), 3)
                                 else
                                   null
                                 end procrate,
                                 null DT_NEXT_OVERVALUE,
                                 to_char(bn.t_bcid) || '#BNR' security_code,
                                 '9999#SOFRXXX#3#' || case
                                                        when leg.t_typepercent = 0 then
                                                            'FIXED'
                                                          else
                                                            'FLOAT'
                                                      end KINDPROCRATE_CODE,  -- гашение векселя
                                 '9999#SOFRXXX#' || case
                                   when leg.t_basis = 4 then
                                     'Act/Act_ICMA'
                                   when leg.t_basis = 1 then
                                     '30/360'
                                   when leg.t_basis = 2 then
                                     '360'
                                   when leg.t_basis = 8 then
                                     '365'
                                   when leg.t_basis = 1001 then
                                     '31/360'
                                 end procbase_code,
                                 '-1' SUBKINDPROCRATE_CODE,
                                 qb_dwh_utils.DateToChar(case when decode(bn.t_registrationdate,emptDate,maxDate, bn.t_registrationdate) < decode(bn.t_issuedate,emptDate,maxDate, bn.t_issuedate) then
                                                                 decode(bn.t_registrationdate,emptDate,firstDate, bn.t_registrationdate)
                                                              else
                                                                decode(bn.t_issuedate,emptDate,firstDate, bn.t_issuedate)
                                                         end) dt

                            from dvsbanner_dbt bn
                            left join ddl_leg_dbt leg
                              on (bn.t_bcid = leg.t_dealid and leg.t_legid = 0 and leg.t_legkind = 1)
                            left join dfininstr_dbt pfi
                              on (leg.t_pfi = pfi.t_fiid)
                            left join dpartcode_dbt pc
                              on pc.t_partyid = bn.t_issuer and pc.t_codekind = 101
                            left join dpartcode_dbt pc1
                              on pc1.t_partyid = bn.t_holder and pc1.t_codekind = 101
                            left join dvsincome_dbt bnin
                              on (bn.t_bcid = bnin.t_bcid and bnin.t_incometype = 9)
                           where leg.t_formula = 1
                             and exists (select 1
                                           from ldr_infa_cb.det_finstr df
                                          where df.finstr_code = to_char(bn.t_bcid) || '#BNR')

        );
commit;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке данных по BIQ 7477: ' || SQLERRM,
                               0,
                               null,
                               null);
      end;
    end if;
    -- Выгрузка данных по BIQ 7477/7478

    --Завершим выгрузку ценных бумаг
    qb_bp_utils.EndEvent(EventID, null);
    --commit;
  end;

  procedure  add2Fct_Securitydeal(legid in ddl_leg_dbt.t_id%type,
                                  dealid in ddl_tick_dbt.t_dealid%type,
                                  deal_code in varchar2,
                                  dwhs in varchar2,
                                  dwhm in varchar2,
                                  dwhf in varchar2 ) is
  begin
    for legrec in (select  case when tick.t_placement = chr(88) then                                           -- выпуск
                               '3'
                             when tick.t_dealtype = 12431 then                                                 -- мена
                               '5'
                             when tick.t_dealtype = 12430 or tick.t_bofficekind = 142 then                     -- погашение УВ
                               '4'
                             when rsb_secur.IsBuy(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then       -- покупка
                               case when deal_code like '%#2' then -- вторая часть обратного РЕПО
                                      '2'
                                    else
                                      '1'
                               end
                             when rsb_secur.IsSale(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then      -- продажа
                               case when deal_code like '%#2' then -- вторая часть прямого РЕПО
                                      '1'
                                    else
                                      '2'
                               end
                             when rsb_secur.IsAvrWrtIn(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then  -- ввод
                               '7'
                             when rsb_secur.IsAvrWrtOut(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then -- вывод
                               '8'
                             when rsb_secur.IsRet_Issue(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then -- погашение
                               '4'
                             else
                               '0'
                         end typesecdeal,
                         leg.t_price sec_price,
                         null sec_proc,
                         leg.t_price rate,
                         leg.t_scale scale,
                         leg.t_nkd couponyield, -- Должно заполнятся только для облигаций
                         (select fi.t_ccy from dfininstr_dbt fi where fi.t_fiid = leg.t_cfi) code_currency,
                         leg.t_totalcost deal_amount,
                         leg.t_principal amount,
                         case when rsb_secur.IsSale(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 0 then
                                case when deal_code like '%TCK#2' then -- вторая часть прямого РЕПО
                                       (select fi.t_iso_number from dfininstr_dbt fi where fi.t_fiid = leg.t_cfi)
                                     else
                                       to_char(tick.t_pfi) || decode(fi.t_avoirkind, 5, '#BNR','#FIN')
                                end
                              else
                                case when deal_code like '%TCK#2' then -- вторая часть прямого РЕПО
                                       to_char(tick.t_pfi) || decode(fi.t_avoirkind, 5, '#BNR','#FIN')
                                     else
                                       (select fi.t_iso_number from dfininstr_dbt fi where fi.t_fiid = leg.t_cfi)
                                end
                         end finstrbuy_finstr_code,
                         case when rsb_secur.IsBuy(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 0 then
                                case when deal_code like '%TCK#2' then -- вторая часть обратного РЕПО
                                       (select fi.t_iso_number from dfininstr_dbt fi where fi.t_fiid = leg.t_cfi)
                                     else
                                        to_char(tick.t_pfi) || decode(fi.t_avoirkind, 5, '#BNR','#FIN')
                                end
                              else
                                case when deal_code like '%TCK#2' then -- вторая часть обратного РЕПО
                                        to_char(tick.t_pfi) || decode(fi.t_avoirkind, 5, '#BNR','#FIN')
                                     else
                                        (select fi.t_iso_number from dfininstr_dbt fi where fi.t_fiid = leg.t_cfi)
                                end
                         end finstrsel_finstr_code,
                         exch_code EXCHANGE_CODE
                         
                  from ddl_leg_dbt leg
                 inner join ddl_tick_dbt tick
                    on (tick.t_dealid = leg.t_dealid and tick.t_dealid = dealid) -- DEF-20402, code review, part2
                 inner join doprkoper_dbt opr
                    on (tick.t_dealtype = opr.t_kind_operation)
                 inner join dfininstr_dbt fi
                   on (tick.t_pfi = fi.t_fiid)
                 where leg.t_id = legid)
    loop
      insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
             values(legrec.typesecdeal, qb_dwh_utils.NumberToChar(Round(legrec.sec_price, 14), 14), legrec.sec_proc, qb_dwh_utils.NumberToChar(Round(legrec.rate, 14), 14), qb_dwh_utils.NumberToChar(Round(legrec.scale, 0), 0), qb_dwh_utils.NumberToChar(Round(legrec.couponyield, 3), 3), legrec.code_currency, qb_dwh_utils.NumberToChar(Round(legrec.deal_amount, 3), 3), qb_dwh_utils.NumberToChar(Round(legrec.amount, 0), 0),  deal_code, legrec.finstrbuy_finstr_code, legrec.finstrsel_finstr_code, legrec.exchange_code, dwhs, dwhm, dwhf);
commit;             
    end loop;

  end;


  procedure  add2Fct_Securitydeal_basket(legid in ddl_leg_dbt.t_id%type,
                                         dealid in ddl_tick_dbt.t_dealid%type,
                                         deal_code in varchar2,
                                         totalcost in ddl_tick_ens_dbt.t_totalcost%type,
                                         principal in ddl_tick_ens_dbt.t_principal%type,
                                         nkd       in ddl_tick_ens_dbt.t_nkd%type,
                                         code_fi   in varchar2,
                                         id_cur    in dfininstr_dbt.t_fiid%type,
                                         dwhs in varchar2,
                                         dwhm in varchar2,
                                         dwhf in varchar2 ) is
            iso_number varchar2(30);
  begin
    select fi.t_iso_number into iso_number from dfininstr_dbt fi where fi.t_fiid = id_cur; -- DEF-20402, code review
    for legrec in (select  case when tick.t_placement = chr(88) then                                           -- выпуск
                               '3'
                             when tick.t_dealtype = 12431 then                                                 -- мена
                               '5'
                             when tick.t_dealtype = 12430 or tick.t_bofficekind = 142 then                     -- погашение УВ
                               '4'
                             when rsb_secur.IsBuy(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then       -- покупка
                               case when deal_code like '%#2' then -- вторая часть обратного РЕПО
                                      '2'
                                    else
                                      '1'
                               end
                             when rsb_secur.IsSale(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then      -- продажа
                               case when deal_code like '%#2' then -- вторая часть прямого РЕПО
                                      '1'
                                    else
                                      '2'
                               end
                             when rsb_secur.IsAvrWrtIn(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then  -- ввод
                               '7'
                             when rsb_secur.IsAvrWrtOut(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then -- вывод
                               '8'
                             when rsb_secur.IsRet_Issue(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then -- погашение
                               '4'
                             else
                               '0'
                         end typesecdeal,
                         leg.t_price sec_price,
                         null sec_proc,
                         leg.t_price rate,
                         leg.t_scale scale,
                         nkd couponyield, -- Должно заполнятся только для облигаций
                         (select fi.t_ccy from dfininstr_dbt fi where fi.t_fiid = id_cur) code_currency,
                         totalcost deal_amount,
                         principal amount,
                         case when rsb_secur.IsSale(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 0 then
                                case when deal_code like '%#2' then -- вторая часть прямого РЕПО
                                       iso_number
                                     else
                                       code_fi
                                end
                              else
                                case when deal_code like '%#2' then -- вторая часть прямого РЕПО
                                       code_fi
                                     else
                                       iso_number
                                end
                         end finstrbuy_finstr_code,
                         case when rsb_secur.IsBuy(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 0 then
                                case when deal_code like '%#2' then -- вторая часть обратного РЕПО
                                       iso_number
                                     else
                                        code_fi
                                end
                              else
                                case when deal_code like '%#2' then -- вторая часть обратного РЕПО
                                        code_fi
                                     else
                                        iso_number
                                end
                         end finstrsel_finstr_code,
                         exch_code EXCHANGE_CODE
                  from ddl_leg_dbt leg
                 inner join ddl_tick_dbt tick
                    on (tick.t_dealid = leg.t_dealid and tick.t_dealid = dealid) -- DEF-20402, code review
                 inner join doprkoper_dbt opr
                    on (tick.t_dealtype = opr.t_kind_operation)
                where leg.t_id = legid)
    loop
      insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
             values(legrec.typesecdeal, qb_dwh_utils.NumberToChar(Round(legrec.sec_price, 14), 14), legrec.sec_proc, qb_dwh_utils.NumberToChar(Round(legrec.rate, 14), 14), qb_dwh_utils.NumberToChar(Round(legrec.scale, 0), 0), qb_dwh_utils.NumberToChar(Round(legrec.couponyield, 3), 3), legrec.code_currency, qb_dwh_utils.NumberToChar(Round(legrec.deal_amount, 3), 3), qb_dwh_utils.NumberToChar(Round(legrec.amount, 0), 0),  deal_code, legrec.finstrbuy_finstr_code, legrec.finstrsel_finstr_code, legrec.exchange_code, dwhs, dwhm, dwhf);
commit;             
    end loop;

  end;



  procedure  add2Fct_Repaydeal(legid in ddl_leg_dbt.t_id%type,
                               dealid in ddl_tick_dbt.t_dealid%type,
                               dwhs in varchar2,
                               dwhm in varchar2,
                               dwhf in varchar2 ) is
  begin
    for legrec in (select case when fi_pfi.t_avoirkind = 5 then
                                 '3'
                               when ((tick.t_bofficekind = 117 and tick.t_dealtype in (2021, 2023, 2025, 12021)) or
                                     (tick.t_bofficekind = 4832 and tick.t_dealtype = 2051)) then                    -- полное гашение
                                 '3'
                               when (tick.t_bofficekind = 117 and tick.t_dealtype in (2027, 12027)) then             -- частичное гашение
                                 '2'
                               when ((tick.t_bofficekind = 117 and tick.t_dealtype in (2022, 2024, 2026, 12022)) or
                                     (tick.t_bofficekind = 4832 and tick.t_dealtype = 2052)) then                    -- гашение купона
                                 '1'
                               else
                                 '-1'
                          end typerepay,
                          '1' typeowner,
                          case when fi_pfi.t_fi_kind = 1 then
                            fi_pfi.t_iso_number
                               when fi_pfi.t_fi_kind = 2 then
                            to_char(tick.t_pfi) || decode(fi_pfi.t_avoirkind, 5, '#BNR','#FIN')
                               else
                            '-1'
                          end  security_code,
                          fi_cfi.t_ccy currency_finstr_code,
                          case when tick.t_Number_Coupon <> chr(1) then
                            tick.t_number_coupon
                          else
                            null
                          end coupon_number,
                          null nominal_proc,
                          leg.t_principal amount,
                          leg.t_totalcost value,
                          tick.t_dealid || '#TCK' deal_code
                     from ddl_leg_dbt leg
                    inner join ddl_tick_dbt tick
                       on (tick.t_dealid = dealid)
                    inner join doprkoper_dbt opr
                       on (tick.t_dealtype = opr.t_kind_operation)
                    inner join dfininstr_dbt fi_pfi
                       on (tick.t_pfi = fi_pfi.t_fiid)
                    inner join dfininstr_dbt fi_cfi
                       on (leg.t_cfi = fi_cfi.t_fiid)
                   where leg.t_id = legid)
    loop
      insert into ldr_infa_cb.fct_repaydeal(typerepay, typeowner, security_code, currency_finstr_code, coupon_number, nominal_proc, amount, value, deal_code, rec_status, sysmoment, ext_file)
             values(legrec.typerepay, legrec.typeowner, legrec.security_code, legrec.currency_finstr_code, legrec.coupon_number, legrec.nominal_proc, qb_dwh_utils.NumberToChar(round(legrec.amount, 0), 0), qb_dwh_utils.NumberToChar(round(legrec.value, 2), 2), legrec.deal_code, dwhs, dwhm, dwhf);
commit;             
    end loop;
  end;

  function GetAccountsDeal (p_bofficekind in ddl_tick_dbt.t_bofficekind%type,
                            p_dealid in ddl_tick_dbt.t_dealid%type) return acc_deal_tt pipelined is
   acc_deal_r acc_deal_t;
  begin
    for dacc_rec in ( select distinct *
                    from (with dp as (select p_bofficekind rec_bofficekind,
                                             p_dealid      rec_dealid
                                        from dual), dacnt as (select acctrn.t_account_payer,
                                                                     --acc_p.t_userfield4 uf4_p,
                                                                     case
                                                                        when (acc_p.t_userfield4 is null) or
                                                                            (acc_p.t_userfield4 = chr(0)) or
                                                                            (acc_p.t_userfield4 = chr(1)) or
                                                                            (acc_p.t_userfield4 like '0x%') then
                                                                          acc_p.t_account
                                                                        else
                                                                          acc_p.t_userfield4
                                                                     end uf4_p,
                                                                     acctrn.t_account_receiver,
                                                                     --acc_r.t_userfield4 uf4_r,
                                                                     case
                                                                        when (acc_r.t_userfield4 is null) or
                                                                            (acc_r.t_userfield4 = chr(0)) or
                                                                            (acc_r.t_userfield4 = chr(1)) or
                                                                            (acc_r.t_userfield4 like '0x%') then
                                                                          acc_r.t_account
                                                                        else
                                                                          acc_r.t_userfield4
                                                                     end uf4_r,
                                                                     acctrn.t_department,
                                                                     dealkind,
                                                                     dealid
                                                                from dacctrn_dbt acctrn,
                                                                     (select /*+LEADING(grdeal) INDEX(grdeal ddlgrdeal_dbt_idx1)*/
                                                                       grdoc.t_docid    as acctrnid,
                                                                       grdeal.t_dockind dealkind,
                                                                       grdeal.t_docid   dealid
                                                                        from ddlgrdeal_dbt grdeal,
                                                                             ddlgrdoc_dbt  grdoc
                                                                       where exists
                                                                       (select 1
                                                                                from dp
                                                                               where dp.rec_bofficekind =
                                                                                     grdeal.t_dockind
                                                                                 and dp.rec_dealid =
                                                                                     grdeal.t_docid)
                                                                         and grdoc.t_grdealid =
                                                                             grdeal.t_id
                                                                         and grdoc.t_dockind = n1
                                                                      union all
                                                                      select oprdocs.t_acctrnid as acctrnid,
                                                                             opr.t_dockind,
                                                                             to_number(opr.t_documentid)
                                                                        from doproper_dbt opr,
                                                                             doprdocs_dbt oprdocs
                                                                       where exists (select 1
                                                                                       from dp
                                                                                      where dp.rec_bofficekind = opr.t_dockind
                                                                                        and lpad(to_char(dp.rec_dealid), 34, 0) = opr.t_documentid)
                                                                         and oprdocs.t_id_operation =
                                                                             opr.t_id_operation
                                                                         and oprdocs.t_dockind = n1) q,
                                                                     dfininstr_dbt pfi,
                                                                     dfininstr_dbt rfi,
                                                                     daccount_dbt acc_p,
                                                                     daccount_dbt acc_r
                                                               where acctrn.t_acctrnid = q.acctrnid
                                                                 and acctrn.t_accountid_payer = acc_p.t_accountid
                                                                 and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                                 and pfi.t_fiid =
                                                                     acctrn.t_fiid_payer
                                                                 and rfi.t_fiid =
                                                                     acctrn.t_fiid_receiver
                                                                 and acctrn.t_state = n1
                                                                 and acctrn.t_chapter in
                                                                     (select v.value
                                                                        from qb_dwh_const4exp c
                                                                       inner join qb_dwh_const4exp_val v
                                                                          on (c.id = v.id)
                                                                       where c.name = cACC_CHAPTERS)
                                                                 and pfi.t_fi_kind = n1
                                                                 and rfi.t_fi_kind = n1)
                           select distinct dp.t_name || '#IBSOXXX#' || dacnt.uf4_p acc,
                                         /*  'XXXX#SOFR#' || */ nvl(cat_pd.t_code, cat_po.t_code) cat_code,
                                           nvl(acd_pd.t_catid, acd_po.t_catid) cat_id,
                                           nvl(cat_pd.t_name, cat_po.t_name) cat_name,
                                           nvl(acd_pd.t_activatedate, acd_po.t_activatedate) cat_date,
                                           dacnt.t_department
                             from dacnt
                            inner join ddp_dep_dbt dp -- филиал счета
                               on (dacnt.t_department = dp.t_code)
                             left join dmcaccdoc_dbt acd_pd -- счет плательщика по сделке
                               on (dacnt.t_account_payer = acd_pd.t_account and
                                  acd_pd.t_dockind = dacnt.dealkind and
                                  acd_pd.t_docid = dacnt.dealid)
                             left join dmccateg_dbt cat_pd -- категория по счету плательщика по сделке
                               on (acd_pd.t_catid = cat_pd.t_id)
                             left join dmcaccdoc_dbt acd_po -- счет плательщика общесистемный
                               on (dacnt.t_account_payer = acd_po.t_account and
                                  acd_po.t_iscommon = chr88)
                             left join dmccateg_dbt cat_po -- категория по общесистемному счету плтательщика
                               on (acd_po.t_catid = cat_po.t_id)
                           union all
                           select *
                             from (with dp as (select p_bofficekind rec_bofficekind,
                                                      p_dealid      rec_dealid
                                                 from dual), dacnt as (select acctrn.t_account_payer,
                                                                              --acc_p.t_userfield4 uf4_p,
                                                                              case
                                                                                when (acc_p.t_userfield4 is null) or
                                                                                    (acc_p.t_userfield4 = chr(0)) or
                                                                                    (acc_p.t_userfield4 = chr(1)) or
                                                                                    (acc_p.t_userfield4 like '0x%') then
                                                                                  acc_p.t_account
                                                                                else
                                                                                  acc_p.t_userfield4
                                                                              end uf4_p,
                                                                              acctrn.t_account_receiver,
                                                                              --acc_r.t_userfield4 uf4_r,
                                                                              case
                                                                                when (acc_r.t_userfield4 is null) or
                                                                                    (acc_r.t_userfield4 = chr(0)) or
                                                                                    (acc_r.t_userfield4 = chr(1)) or
                                                                                    (acc_r.t_userfield4 like '0x%') then
                                                                                  acc_r.t_account
                                                                                else
                                                                                  acc_r.t_userfield4
                                                                              end uf4_r,
                                                                              acctrn.t_department,
                                                                              dealkind,
                                                                              dealid
                                                                         from dacctrn_dbt acctrn,
                                                                              (select /*+LEADING(grdeal) INDEX(grdeal ddlgrdeal_dbt_idx1)*/
                                                                                grdoc.t_docid    as acctrnid,
                                                                                grdeal.t_dockind dealkind,
                                                                                grdeal.t_docid   dealid
                                                                                 from ddlgrdeal_dbt grdeal,
                                                                                      ddlgrdoc_dbt  grdoc
                                                                                where exists
                                                                                (select 1
                                                                                         from dp
                                                                                        where dp.rec_bofficekind =
                                                                                              grdeal.t_dockind
                                                                                          and dp.rec_dealid =
                                                                                              grdeal.t_docid)
                                                                                  and grdoc.t_grdealid =
                                                                                      grdeal.t_id
                                                                                  and grdoc.t_dockind = n1
                                                                               union all
                                                                               select oprdocs.t_acctrnid as acctrnid,
                                                                                      opr.t_dockind,
                                                                                      to_number(opr.t_documentid)
                                                                                 from doproper_dbt opr,
                                                                                      doprdocs_dbt oprdocs
                                                                                where exists (select 1
                                                                                                from dp
                                                                                               where dp.rec_bofficekind = opr.t_dockind
                                                                                                 and lpad(to_char(dp.rec_dealid), 34, 0) = opr.t_documentid)
                                                                                  and oprdocs.t_id_operation =
                                                                                      opr.t_id_operation
                                                                                  and oprdocs.t_dockind = n1) q,
                                                                              dfininstr_dbt pfi,
                                                                              dfininstr_dbt rfi,
                                                                              daccount_dbt acc_p,
                                                                              daccount_dbt acc_r
                                                                        where acctrn.t_acctrnid =
                                                                              q.acctrnid
                                                                          and acctrn.t_accountid_payer = acc_p.t_accountid
                                                                          and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                                          and pfi.t_fiid =
                                                                              acctrn.t_fiid_payer
                                                                          and rfi.t_fiid =
                                                                              acctrn.t_fiid_receiver
                                                                          and acctrn.t_state = n1
                                                                          and acctrn.t_chapter in
                                                                              ( select v.value
                                                                                 from qb_dwh_const4exp c
                                                                                inner join qb_dwh_const4exp_val v
                                                                                   on (c.id = v.id)
                                                                                where c.name =
                                                                                      cACC_CHAPTERS)
                                                                          and pfi.t_fi_kind = n1
                                                                          and rfi.t_fi_kind = n1)
                                    select distinct dp.t_name || '#IBSOXXX#' ||
                                                    dacnt.uf4_r,
/*                                                    'XXXX#SOFR#' ||*/
                                                    nvl(cat_rd.t_code, cat_ro.t_code),
                                                    nvl(acd_rd.t_catid, acd_ro.t_catid) catid_receiver,
                                                    nvl(cat_rd.t_name, cat_ro.t_name) cat_name,
                                                    nvl(acd_rd.t_activatedate,
                                                        acd_ro.t_activatedate) catdate,
                                                    dacnt.t_department
                                      from dacnt
                                     inner join ddp_dep_dbt dp
                                        on (dacnt.t_department = dp.t_code)
                                      left join dmcaccdoc_dbt acd_rd
                                        on (dacnt.t_account_receiver = acd_rd.t_account and
                                           acd_rd.t_dockind = dacnt.dealkind and
                                           acd_rd.t_docid = dacnt.dealid)
                                      left join dmccateg_dbt cat_rd
                                        on (acd_rd.t_catid = cat_rd.t_id)
                                      left join dmcaccdoc_dbt acd_ro
                                        on (dacnt.t_account_receiver = acd_ro.t_account and
                                           acd_ro.t_iscommon = chr88)
                                      left join dmccateg_dbt cat_ro
                                        on (acd_ro.t_catid = cat_ro.t_id)))
            )
      loop
        acc_deal_r.acc_code  := dacc_rec.acc;
        acc_deal_r.deal_code := null;
        acc_deal_r.cat_code  := dacc_rec.cat_code;
        acc_deal_r.cat_name  := dacc_rec.cat_name;
        acc_deal_r.cat_date  := dacc_rec.cat_date;
        pipe row (acc_deal_r);
      end loop;
  end;

  function GetSumPart2(pdealid in number, pfiid in number) return number is
    vsum number(30, 2);
  begin
    with d1 as
         (select t.t_fiid,
                 t.t_dealid,
                 round(sum(t.t_totalcost * decode(t.t_kind, 0, 1, -1)), 2) totalcost -- сумма
            from ddl_tick_ens_dbt t
           where t.t_dealid = pdealid
            group by t.t_dealid, t.t_fiid
          ),
        d2 as
          (select d1.*,
                  round(leg2.t_totalcost, 2) part2,
                  round(d1.totalcost / leg0.t_totalcost * leg2.t_totalcost, 2) partly_cost,
              --    last_value(d1.t_fiid) over(ORDER BY d1.t_fiid RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_fiid -- DEF-20402, code review, part2 
                  last_value(d1.t_fiid) over() last_fiid 
             from d1
            inner join dfininstr_dbt fi
               on (d1.t_fiid = fi.t_fiid)
            inner join ddl_leg_dbt leg0
               on (d1.t_dealid = leg0.t_dealid and leg0.t_legkind = 0)
            inner join ddl_leg_dbt leg2
               on (d1.t_dealid = leg2.t_dealid and leg2.t_legkind = 2)
          ),
        d3 as
         (select d2.*,
                 round(sum(d2.partly_cost) over(order by d2.t_fiid rows between unbounded preceding and current row), 2) acum_sum -- сумма с нарастающим итогом
            from d2)
        select --d3.*,
               case
                 when d3.t_fiid = d3.last_fiid then
                  d3.part2 - d3.acum_sum + d3.partly_cost
                 else
                  d3.partly_cost
               end partly_cost_c
          into vsum
          from d3
        where t_fiid = pfiid;
    return vsum;
  end;

  -- вернет количество, стоимость, период для ценной бумаги в корзине на требуемую дату
  function GetParmsSecurity(pdealid in number, pfiid in number, pdate in date) return sec_basket is
    sec_basket_rec sec_basket;
    vsum number(30, 2);
  begin
    vsum := round(GetSumPart2(pdealid, pfiid), 2);
    select *
      into sec_basket_rec
      from (select t.t_fiid fiid,
                   to_char(t.t_fiid) || decode(fi.t_avoirkind, 5, '#BNR', '#FIN') fi_code,
                   t.t_date bd,
                   lead(t.t_date - 1,
                        1,
                        to_date('31129999','ddmmyyyy')) over(partition by t.t_fiid order by t_date) ed,
                   leg0.t_maturity date1,
                   leg2.t_maturity date2,
                 --  round(sum(t.t_totalcost * decode(t.t_kind, 0, 1, -1))
                 --        over(partition by t.t_fiid order by t_date rows between unbounded
                 --             preceding and current row),
                 --        2) totalcost, -- реальная сумма
                   round(sum(t.t_principal * decode(t.t_kind, 0, 1, -1)) over(partition by t.t_fiid order by t_date rows between unbounded preceding and current row) *
                     first_value(t.t_totalcost) over(partition by t.t_fiid order by t.t_date)  /
                     first_value(t.t_principal) over(partition by t.t_fiid order by t.t_date), 2) totalcost, -- рассчитанная сумма по первоначальной стоимости одной ц/б
                   round(sum(t.t_principal * decode(t.t_kind, 0, 1, -1)) over(partition by t.t_fiid order by t_date rows between unbounded preceding and current row), 0) cnt, -- количество
                   round(sum(t.t_nkd * decode(t.t_kind, 0, 1, -1)) over(partition by t.t_fiid order by t_date rows between unbounded preceding and current row), 2) nkd, -- НКД
                   to_char(t.t_dealid) || '#TCK' main_deal,
                   to_char(t.t_dealid) || '#' || to_char(pdate, 'ddmmyyyy') || '#' ||
                   to_char(t.t_fiid) || decode(fi.t_avoirkind, 5, '#BNR', '#FIN') ||
                   '#TCK' part_deal,
                   leg0.t_id legid0,
                   leg2.t_id legid2,
                   t.t_dealid dealid,
                   t.t_costfiid costfiid,
                   vsum
              from ddl_tick_ens_dbt t
             inner join dfininstr_dbt fi
                on (t.t_fiid = fi.t_fiid)
             inner join ddl_leg_dbt leg0
                on (t.t_dealid = leg0.t_dealid and leg0.t_legkind = 0)
             inner join ddl_leg_dbt leg2
                on (t.t_dealid = leg2.t_dealid and leg2.t_legkind = 2)
             where t.t_dealid = pdealid
               and t.t_fiid = pfiid)
     where pdate between bd and ed;
    return sec_basket_rec;
  end;

  function GetCountCBTotal(pdealid in number, pdate in date) return number is
    tcnt number(30);
  begin
    select sum(t_principal * decode(t.t_kind, 0, 1, -1))
      into tcnt
      from ddl_tick_ens_dbt t
     where t.t_dealid = pdealid
       and t.t_date <= pdate;
    return tcnt;
  end;

  procedure export_Deals(in_department in number,
                         in_date       in date,
                         procid        in number) is
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
    cnt          pls_integer;
    sec_basket_rec sec_basket;
    totalcnt number(30);

  begin
    -- Установим начало выгрузки сделок
    startevent(cEvent_EXPORT_Deals, procid, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             QB_DWH_EXPORT.cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDT, in_date);

    qb_dwh_export.InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка данных по сделкам',
                         2,
                         null,
                         null);
    -- сделки с собственными векселями

    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с СВ',
                   2,
                   null,
                   null);
    for rec in (select to_char(ord.t_contractid) || '#ORD' code,
                       ord.t_contractid,
                       ord.t_dockind,
                       dp.t_name department_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             decode( ord.t_contractor, -1, 1, ord.t_contractor)) subject_code,

                       case when ord.t_dockind = 109 then
                              '13'
                            when ord.t_dockind = 110 then
                              '27'
                            when ord.t_dockind = 112 then
                              '13'
                            else
                              '-1'
                       end dealtype,
                       ord.t_ordernumber docnum,
                       '0' is_interior,
                       leg.t_start begindate,
                       leg.t_maturity enddate,
                       null note,
                       ord.t_signdate dt,
                       ord.t_createdate,
                       case when ord.t_dockind = 109 then -- выпуск
                              '3'
                            when ord.t_dockind = 113 then -- новация
                              '6'
                            when ord.t_dockind = 112 then -- !!!!!!!!!!!!!!!!! задал вопрос
                              '10'
                            when ord.t_dockind = 110 then -- гашение
                              '4'
                       end typesecdeal,
                       leg.t_price sec_price,
                       leg.t_scale scale,
                       leg.t_principal principal,
                       leg.t_totalcost totalcost,
                       to_char(bn.t_bcid) || '#BNR' finstr_code,
                       fi_cfi.t_ccy currency_finstr_code,
                       fi_cfi.t_iso_number currency_num_code,
                       row_number() over (partition by ord.t_contractid order by leg.t_id) rnk,
                       count(*) over (partition by ord.t_contractid) cnt
                  from dvsbanner_dbt bn
                  inner join ddl_leg_dbt leg
                    on (bn.t_bcid = leg.t_dealid)
                  inner join dvsordlnk_dbt lnk
                    on (bn.t_bcid = lnk.t_bcid)
                 inner join ddl_order_dbt ord
                    on (lnk.t_contractid = ord.t_contractid and lnk.t_dockind = ord.t_dockind)
                 inner join ddp_dep_dbt dp
                    on (ord.t_department = dp.t_code)
                 left join dfininstr_dbt fi_cfi
                    on (leg.t_cfi = fi_cfi.t_fiid)
                 where leg.t_legid = n0 and leg.t_legkind = n1
                   and ord.t_signdate <= in_date)
    loop
      begin
      if (rec.rnk = 1) then
        insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
               values (rec.code, rec.department_code, rec.subject_code, rec.dealtype, rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      end if;
      if (rec.dealtype = '13') then
        if (rec.cnt > 1) then
          if (rec.rnk = 1) then
            insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
                   values (rec.typesecdeal, '-1', null, '-1', '-1', null, rec.currency_finstr_code, '-1', null, rec.code, '-1', '-1', '-1', dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          insert into ldr_infa_cb.fct_secdeal_finstr(sec_price, sec_proc, rate, scale, couponyield, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, deal_amount, dt, rec_status, sysmoment, ext_file)
                 values (qb_dwh_utils.NumberToChar(rec.sec_price), null, qb_dwh_utils.NumberToChar(Round(rec.sec_price, 14), 14), qb_dwh_utils.NumberToChar(round(rec.scale, 0), 0), null, qb_dwh_utils.NumberToChar(1, 0), rec.code, rec.finstr_code, rec.finstr_code, qb_dwh_utils.NumberToChar(rec.cnt, 0), qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
          insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
                 values (rec.typesecdeal, qb_dwh_utils.NumberToChar(rec.sec_price), null, qb_dwh_utils.NumberToChar(Round(rec.sec_price, 14), 14), qb_dwh_utils.NumberToChar(round(rec.scale, 0), 0), null, rec.currency_finstr_code, qb_dwh_utils.NumberToChar(round(rec.principal, 3), 3), qb_dwh_utils.NumberToChar(1, 0), rec.code, rec.finstr_code, rec.finstr_code, '-1', dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        end if;
      elsif rec.dealtype = '27' then
        if (rec.cnt > 1) then
          if (rec.rnk = 1) then
            insert into ldr_infa_cb.fct_repaydeal(typerepay, typeowner, security_code, currency_finstr_code, coupon_number, nominal_proc, amount, value, deal_code, rec_status, sysmoment, ext_file)
                   values('3', '1', '-1', '-1', null, null, '-1', '-1', rec.code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          insert into ldr_infa_cb.fct_secdeal_finstr(sec_price, sec_proc, rate, scale, couponyield, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, deal_amount, dt, rec_status, sysmoment, ext_file)
                 values ('3', null, '1', '-1', qb_dwh_utils.NumberToChar(round(rec.principal, 2), 2), '1', rec.code, rec.finstr_code, rec.currency_num_code, qb_dwh_utils.NumberToChar(rec.cnt, 0), qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
          insert into ldr_infa_cb.fct_repaydeal(typerepay, typeowner, security_code, currency_finstr_code, coupon_number, nominal_proc, amount, value, deal_code, rec_status, sysmoment, ext_file)
                 values('3', '1', rec.finstr_code, rec.currency_finstr_code, null, null, '1', qb_dwh_utils.NumberToChar(round(rec.principal, 2), 2), rec.code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        end if;
      end if;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке данных по сделке с СВ: ' || SQLERRM,
                               0,
                               cDeal,
                               rec.t_contractid);
      end;

      -- Добавление счетов по сделкам с собственными векселями
      for dacc_rec in (select distinct *
                          from (with dacnt as (select acctrn.t_account_payer,
                                                      --acc_p.t_userfield4 uf4_p,
                                                      case
                                                        when (acc_p.t_userfield4 is null) or
                                                            (acc_p.t_userfield4 = chr(0)) or
                                                            (acc_p.t_userfield4 = chr(1)) or
                                                            (acc_p.t_userfield4 like '0x%') then
                                                          acc_p.t_account
                                                        else
                                                          acc_p.t_userfield4
                                                      end uf4_p,
                                                      acctrn.t_account_receiver,
                                                      --acc_r.t_userfield4 uf4_r,
                                                      case
                                                        when (acc_r.t_userfield4 is null) or
                                                            (acc_r.t_userfield4 = chr(0)) or
                                                            (acc_r.t_userfield4 = chr(1)) or
                                                            (acc_r.t_userfield4 like '0x%') then
                                                          acc_r.t_account
                                                        else
                                                          acc_r.t_userfield4
                                                      end uf4_r,
                                                      acctrn.t_department
                                                 from dacctrn_dbt acctrn,
                                                      (select oprdocs.t_acctrnid as acctrnid
                                                         from doproper_dbt opr,
                                                              doprdocs_dbt oprdocs
                                                        where opr.t_dockind = rec.t_dockind
                                                          and opr.t_documentid =
                                                              lpad(to_char(rec.t_contractid), 10, 0)
                                                          and oprdocs.t_id_operation = opr.t_id_operation
                                                          and oprdocs.t_dockind = 1) q,
                                                      dfininstr_dbt pfi,
                                                      dfininstr_dbt rfi,
                                                      daccount_dbt acc_p,
                                                      daccount_dbt acc_r
                                                where acctrn.t_acctrnid = q.acctrnid
                                                  and acctrn.t_accountid_payer =  acc_p.t_accountid
                                                  and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                  and pfi.t_fiid = acctrn.t_fiid_payer
                                                  and rfi.t_fiid = acctrn.t_fiid_receiver
                                                  and acctrn.t_state = 1
                                                  and acctrn.t_chapter in (select v.value
                                                                             from qb_dwh_const4exp c
                                                                            inner join qb_dwh_const4exp_val v
                                                                               on (c.id = v.id)
                                                                            where c.name = cACC_CHAPTERS)
                                                  and pfi.t_fi_kind = 1
                                                  and rfi.t_fi_kind = 1)
                                 select distinct dp.t_name || '#IBSOXXX#' || dacnt.uf4_p acc,
                                                 --'XXXX#SOFR#' ||
                                                 nvl(cat_pd.t_code, cat_po.t_code) cat_code,
                                                 nvl(acd_pd.t_catid, acd_po.t_catid) cat_id,
                                                 nvl(cat_pd.t_name, cat_po.t_name) cat_name,
                                                 nvl(acd_pd.t_activatedate, acd_po.t_activatedate) cat_date,
                                                 case when acd_pd.t_activatedate is not null then acd_pd.t_disablingdate else acd_po.t_disablingdate end cat_enddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                 dacnt.t_department
                                   from dacnt
                                  inner join ddp_dep_dbt dp -- филиал счета
                                     on (dacnt.t_department = dp.t_code)
                                   left join dmcaccdoc_dbt acd_pd -- счет плательщика по сделке
                                     on (dacnt.t_account_payer = acd_pd.t_account and
                                        acd_pd.t_dockind = rec.t_dockind and
                                        acd_pd.t_docid = rec.t_contractid)
                                   left join dmccateg_dbt cat_pd -- категория по счету плательщика по сделке
                                     on (acd_pd.t_catid = cat_pd.t_id)
                                   left join dmcaccdoc_dbt acd_po -- счет плательщика общесистемный
                                     on (dacnt.t_account_payer = acd_po.t_account and
                                        acd_po.t_iscommon = chr88)
                                   left join dmccateg_dbt cat_po -- категория по общесистемному счету плтательщика
                                     on (acd_po.t_catid = cat_po.t_id)
                                 union all
                                 select *
                                   from (with dacnt as (select acctrn.t_account_payer,
                                                               --acc_p.t_userfield4 uf4_p,
                                                               case
                                                                  when (acc_p.t_userfield4 is null) or
                                                                      (acc_p.t_userfield4 = chr(0)) or
                                                                      (acc_p.t_userfield4 = chr(1)) or
                                                                      (acc_p.t_userfield4 like '0x%') then
                                                                    acc_p.t_account
                                                                  else
                                                                    acc_p.t_userfield4
                                                               end uf4_p,
                                                               acctrn.t_account_receiver,
                                                               --acc_r.t_userfield4 uf4_r,
                                                               case
                                                                  when (acc_r.t_userfield4 is null) or
                                                                      (acc_r.t_userfield4 = chr(0)) or
                                                                      (acc_r.t_userfield4 = chr(1)) or
                                                                      (acc_r.t_userfield4 like '0x%') then
                                                                    acc_r.t_account
                                                                  else
                                                                    acc_r.t_userfield4
                                                               end uf4_r,
                                                               acctrn.t_department
                                                          from dacctrn_dbt acctrn,
                                                               (select oprdocs.t_acctrnid as acctrnid
                                                                  from doproper_dbt opr,
                                                                       doprdocs_dbt oprdocs
                                                                 where opr.t_dockind = rec.t_dockind
                                                                   and opr.t_documentid =
                                                                       lpad(to_char(rec.t_contractid), 10, 0)
                                                                   and oprdocs.t_id_operation =
                                                                       opr.t_id_operation
                                                                   and oprdocs.t_dockind = n1) q,
                                                               dfininstr_dbt pfi,
                                                               dfininstr_dbt rfi,
                                                               daccount_dbt acc_p,
                                                               daccount_dbt acc_r
                                                         where acctrn.t_acctrnid = q.acctrnid
                                                           and acctrn.t_accountid_payer =  acc_p.t_accountid
                                                           and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                           and pfi.t_fiid = acctrn.t_fiid_payer
                                                           and rfi.t_fiid = acctrn.t_fiid_receiver
                                                           and acctrn.t_state = n1
                                                           and acctrn.t_chapter in (select v.value
                                                                                      from qb_dwh_const4exp c
                                                                                     inner join qb_dwh_const4exp_val v
                                                                                        on (c.id = v.id)
                                                                                     where c.name = cACC_CHAPTERS)
                                                           and pfi.t_fi_kind = n1
                                                           and rfi.t_fi_kind = n1)
                                          select distinct dp.t_name || '#IBSOXXX#' || dacnt.uf4_r,
                                                          --'XXXX#SOFR#' ||
                                                          nvl(cat_rd.t_code, cat_ro.t_code),
                                                          nvl(acd_rd.t_catid, acd_ro.t_catid) catid_receiver,
                                                          nvl(cat_rd.t_name, cat_ro.t_name) cat_name,
                                                          nvl(acd_rd.t_activatedate, acd_ro.t_activatedate) catdate,
                                                          case when acd_rd.t_activatedate is not null then acd_rd.t_disablingdate else acd_ro.t_disablingdate end catenddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                          dacnt.t_department
                                            from dacnt
                                           inner join ddp_dep_dbt dp
                                              on (dacnt.t_department = dp.t_code)
                                            left join dmcaccdoc_dbt acd_rd
                                              on (dacnt.t_account_receiver = acd_rd.t_account and
                                                 acd_rd.t_dockind = rec.t_dockind and
                                                 acd_rd.t_docid = rec.t_contractid)
                                            left join dmccateg_dbt cat_rd
                                              on (acd_rd.t_catid = cat_rd.t_id)
                                            left join dmcaccdoc_dbt acd_ro
                                              on (dacnt.t_account_receiver = acd_ro.t_account and
                                                 acd_ro.t_iscommon = chr88)
                                            left join dmccateg_dbt cat_ro
                                              on (acd_ro.t_catid = cat_ro.t_id)
                                           ))
                        )
      loop
        -- Вставка в ass_accountdeal
        begin
        if (dacc_rec.cat_id is not null) then
          begin
            insert into ldr_infa_cb.ass_accountdeal(account_code, deal_code, roleaccount_deal_code, dt, rec_status, sysmoment, ext_file, dt_end) -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                   values(dacc_rec.acc, rec.code, dacc_rec.cat_code, qb_dwh_utils.DateToChar(dacc_rec.cat_date), dwhRecStatus, dwhSysMoment, dwhEXT_FILE, case when dacc_rec.cat_enddate = to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END) else qb_dwh_utils.DateToChar(dacc_rec.cat_enddate-1) end);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
          begin
            insert into ldr_infa_cb.det_roleaccount_deal(code, name, orole_code, dt, rec_status, sysmoment, ext_file)
                   values (dacc_rec.cat_code, dacc_rec.cat_name, '0', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
        end if;
        exception
          when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке счета по сделке с СВ: ' || SQLERRM,
                                 0,
                                 cDeal,
                                 rec.t_contractid);
        end;
      end loop;
      --commit;
    end loop;

    -- сделки с учтенными векселями
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с УВ',
                   2,
                   null,
                   null);



    for rec in (select to_char(tick.t_dealid) || '#TCK' code,
                       '0000' department_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             decode(tick.t_partyid, -1, 1, tick.t_partyid)) subject_code,

                       case when tick.t_bofficekind = 141 then
                              '13'
                            when tick.t_bofficekind = 142 then
                              '27'
                            when tick.t_bofficekind = 143 then
                              '13'
                            else
                              '-1'
                       end dealtype,
                       case when tick.t_bofficekind = 141 then
                              case when tick.t_dealtype = 12401 then
                                     '1'
                                   when tick.t_dealtype = 12411 then
                                     '2'
                                   when tick.t_dealtype = 12431 then
                                     '5'
                                   else
                                     '0'
                              end
                            when tick.t_bofficekind = 142 then
                              '4'
                            when tick.t_bofficekind = 143 then
                              '10' -- залог
                       end typesecdeal,
                       tick.t_dealcode docnum,
                       '0' is_interior,
                       decode(leg.t_start, emptDate, tick.t_dealdate, leg.t_start) begindate,
                       decode(leg.t_maturity, emptDate, tick.t_dealdate, leg.t_maturity) enddate,
                       tick.t_comment note,
                       tick.t_dealdate dt,
                       leg.t_id legid,
                       tick.t_dealid dealid,
                       tick.t_bofficekind bofficekind,
                       tick.t_dealdate,
                       leg.t_price sec_price,
                       leg.t_totalcost totalcost,
                       leg.t_principal principal,
                       null sec_proc,
                       leg.t_totalcost rate,
                       1 scale,
                       null couponyield,
                       leg.t_principal deal_amount,
                       1 amount,
                       case when tick.t_bofficekind = 141 and tick.t_dealtype = 12411 then
                         fi.t_iso_number
                       else
                         to_char(bn.t_bcid) || '#BNR'
                       end finstrbuy_finstr_code,
                       case when tick.t_bofficekind = 141 and tick.t_dealtype = 12401 then
                         fi.t_iso_number
                       else
                         to_char(bn.t_bcid) || '#BNR'
                       end finstrsel_finstr_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             tick.t_marketid) exchange_code,
                       to_char(bn.t_bcid) || '#BNR' finstr_code,
                       fi.t_ccy currency_finstr_code,
                       fi.t_iso_number currency_num_code,
                       row_number() over (partition by tick.t_dealid order by leg.t_id) rnk,
                       count(*) over (partition by tick.t_dealid) cnt
                  from dvsbanner_dbt bn
                  inner join ddl_leg_dbt leg
                    on (bn.t_bcid = leg.t_dealid)
                  inner join dvsordlnk_dbt lnk
                    on (bn.t_bcid = lnk.t_bcid)
                  inner join ddl_tick_dbt tick
                    on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                  inner join dfininstr_dbt fi
                    on (leg.t_cfi = fi.t_fiid)
                  where leg.t_legid = n0 and leg.t_legkind = n1
                    and tick.t_bofficekind in (select v.value
                                                 from qb_dwh_const4exp c
                                                inner join qb_dwh_const4exp_val v
                                                   on (c.id = v.id)
                                                where c.name = cDEALSKIND_DBILL)
                    and tick.t_dealdate <= in_date)
    loop
      begin
      if (rec.rnk = 1) then
        insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
               values (rec.code, rec.department_code, rec.subject_code, rec.dealtype, rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      end if;
      if rec.dealtype = '13' then
        if(rec.cnt > 1) then
          if (rec.rnk = 1) then
            insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
                   values (rec.typesecdeal, '-1', null, '-1', '-1', null, rec.currency_finstr_code, '-1', '-1', rec.code, '-1', '-1', rec.exchange_code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          insert into ldr_infa_cb.fct_secdeal_finstr(sec_price, sec_proc, rate, scale, couponyield, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, deal_amount, dt, rec_status, sysmoment, ext_file)
                 values (qb_dwh_utils.NumberToChar(rec.sec_price), qb_dwh_utils.NumberToChar(rec.sec_proc), qb_dwh_utils.NumberToChar(Round(rec.rate, 14), 14), qb_dwh_utils.NumberToChar(round(rec.scale, 0), 0), null, qb_dwh_utils.NumberToChar(round(rec.amount,0), 0), rec.code, rec.finstrbuy_finstr_code, rec.finstrsel_finstr_code, qb_dwh_utils.NumberToChar(rec.cnt, 0), qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
          insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
                   values (rec.typesecdeal, qb_dwh_utils.NumberToChar(rec.sec_price), qb_dwh_utils.NumberToChar(rec.sec_proc), qb_dwh_utils.NumberToChar(Round(rec.rate, 14), 14), qb_dwh_utils.NumberToChar(round(rec.scale, 0), 0), null, rec.currency_finstr_code, qb_dwh_utils.NumberToChar(round(rec.deal_amount, 3), 3), qb_dwh_utils.NumberToChar(round(rec.amount,0), 0), rec.code, rec.finstrbuy_finstr_code, rec.finstrsel_finstr_code, rec.exchange_code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
        end if;
      elsif rec.dealtype = '27' then
        if (rec.cnt > 1) then
          if (rec.rnk = 1) then
            insert into ldr_infa_cb.fct_repaydeal(typerepay, typeowner, security_code, currency_finstr_code, coupon_number, nominal_proc, amount, value, deal_code, rec_status, sysmoment, ext_file)
                   values('3', '1', '-1', '-1', null, null, '-1', '-1', rec.code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          insert into ldr_infa_cb.fct_secdeal_finstr(sec_price, sec_proc, rate, scale, couponyield, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, deal_amount, dt, rec_status, sysmoment, ext_file)
                 values ('3', null, '1', '-1', qb_dwh_utils.NumberToChar(round(rec.totalcost, 2), 2), qb_dwh_utils.NumberToChar(round(rec.principal, 0), 0), rec.code, rec.finstr_code, rec.currency_num_code, qb_dwh_utils.NumberToChar(rec.cnt, 0), qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
          insert into ldr_infa_cb.fct_repaydeal(typerepay, typeowner, security_code, currency_finstr_code, coupon_number, nominal_proc, amount, value, deal_code, rec_status, sysmoment, ext_file)
                values('3', '1', rec.finstr_code, rec.currency_finstr_code, null, null, qb_dwh_utils.NumberToChar(round(rec.principal, 0), 0), qb_dwh_utils.NumberToChar(round(rec.totalcost, 2), 2), rec.code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                
        end if;
      end if;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке данных по сделке с УВ: ' || SQLERRM,
                               0,
                               cDeal,
                               rec.dealid);
      end;

      -- Добавление счетов по сделкам с УВ
      for dacc_rec in (select distinct *
                          from (with dacnt as (select acctrn.t_account_payer,
                                                      --acc_p.t_userfield4 uf4_p,
                                                      case
                                                        when (acc_p.t_userfield4 is null) or
                                                            (acc_p.t_userfield4 = chr(0)) or
                                                            (acc_p.t_userfield4 = chr(1)) or
                                                            (acc_p.t_userfield4 like '0x%') then
                                                          acc_p.t_account
                                                        else
                                                          acc_p.t_userfield4
                                                      end uf4_p,
                                                      acctrn.t_account_receiver,
                                                      --acc_r.t_userfield4 uf4_r,
                                                      case
                                                        when (acc_r.t_userfield4 is null) or
                                                            (acc_r.t_userfield4 = chr(0)) or
                                                            (acc_r.t_userfield4 = chr(1)) or
                                                            (acc_r.t_userfield4 like '0x%') then
                                                          acc_r.t_account
                                                        else
                                                          acc_r.t_userfield4
                                                      end uf4_r,
                                                      acctrn.t_department
                                                 from dacctrn_dbt acctrn,
                                                      (select oprdocs.t_acctrnid as acctrnid
                                                         from doproper_dbt opr,
                                                              doprdocs_dbt oprdocs
                                                        where opr.t_dockind = rec.bofficekind
                                                          and opr.t_documentid =
                                                              lpad(to_char(rec.dealid), 34, 0)
                                                          and oprdocs.t_id_operation = opr.t_id_operation
                                                          and oprdocs.t_dockind = 1) q,
                                                      dfininstr_dbt pfi,
                                                      dfininstr_dbt rfi,
                                                      daccount_dbt acc_p,
                                                      daccount_dbt acc_r
                                                where acctrn.t_acctrnid = q.acctrnid
                                                  and acctrn.t_accountid_payer =  acc_p.t_accountid
                                                  and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                  and pfi.t_fiid = acctrn.t_fiid_payer
                                                  and rfi.t_fiid = acctrn.t_fiid_receiver
                                                  and acctrn.t_state = 1
                                                  and acctrn.t_chapter in (select v.value
                                                                             from qb_dwh_const4exp c
                                                                            inner join qb_dwh_const4exp_val v
                                                                               on (c.id = v.id)
                                                                            where c.name = cACC_CHAPTERS)
                                                  and pfi.t_fi_kind = 1
                                                  and rfi.t_fi_kind = 1)
                                 select distinct dp.t_name || '#IBSOXXX#' || dacnt.uf4_p acc,
                                                 --'XXXX#SOFR#' ||
                                                 nvl(cat_pd.t_code, cat_po.t_code) cat_code,
                                                 nvl(acd_pd.t_catid, acd_po.t_catid) cat_id,
                                                 nvl(cat_pd.t_name, cat_po.t_name) cat_name,
                                                 nvl(acd_pd.t_activatedate, acd_po.t_activatedate) cat_date,
                                                 case when acd_pd.t_activatedate is not null then acd_pd.t_disablingdate else acd_po.t_disablingdate end cat_enddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                 dacnt.t_department
                                   from dacnt
                                  inner join ddp_dep_dbt dp -- филиал счета
                                     on (dacnt.t_department = dp.t_code)
                                   left join dmcaccdoc_dbt acd_pd -- счет плательщика по сделке
                                     on (dacnt.t_account_payer = acd_pd.t_account and
                                        acd_pd.t_dockind = rec.bofficekind and
                                        acd_pd.t_docid = rec.dealid)
                                   left join dmccateg_dbt cat_pd -- категория по счету плательщика по сделке
                                     on (acd_pd.t_catid = cat_pd.t_id)
                                   left join dmcaccdoc_dbt acd_po -- счет плательщика общесистемный
                                     on (dacnt.t_account_payer = acd_po.t_account and
                                        acd_po.t_iscommon = chr88)
                                   left join dmccateg_dbt cat_po -- категория по общесистемному счету плтательщика
                                     on (acd_po.t_catid = cat_po.t_id)
                                 union all
                                 select *
                                   from (with dacnt as (select acctrn.t_account_payer,
                                                               --acc_p.t_userfield4 uf4_p,
                                                               case
                                                                  when (acc_p.t_userfield4 is null) or
                                                                      (acc_p.t_userfield4 = chr(0)) or
                                                                      (acc_p.t_userfield4 = chr(1)) or
                                                                      (acc_p.t_userfield4 like '0x%') then
                                                                    acc_p.t_account
                                                                  else
                                                                    acc_p.t_userfield4
                                                               end uf4_p,
                                                               acctrn.t_account_receiver,
                                                               --acc_r.t_userfield4 uf4_r,
                                                               case
                                                                  when (acc_r.t_userfield4 is null) or
                                                                      (acc_r.t_userfield4 = chr(0)) or
                                                                      (acc_r.t_userfield4 = chr(1)) or
                                                                      (acc_r.t_userfield4 like '0x%') then
                                                                    acc_r.t_account
                                                                  else
                                                                    acc_r.t_userfield4
                                                               end uf4_r,
                                                               acctrn.t_department
                                                          from dacctrn_dbt acctrn,
                                                               (select oprdocs.t_acctrnid as acctrnid
                                                                  from doproper_dbt opr,
                                                                       doprdocs_dbt oprdocs
                                                                 where opr.t_dockind = rec.bofficekind
                                                                   and opr.t_documentid =
                                                                       lpad(to_char(rec.dealid), 34, 0)
                                                                   and oprdocs.t_id_operation =
                                                                       opr.t_id_operation
                                                                   and oprdocs.t_dockind = n1) q,
                                                               dfininstr_dbt pfi,
                                                               dfininstr_dbt rfi,
                                                               daccount_dbt acc_p,
                                                               daccount_dbt acc_r
                                                         where acctrn.t_acctrnid = q.acctrnid
                                                           and acctrn.t_accountid_payer = acc_p.t_accountid
                                                           and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                           and pfi.t_fiid = acctrn.t_fiid_payer
                                                           and rfi.t_fiid = acctrn.t_fiid_receiver
                                                           and acctrn.t_state = n1
                                                           and acctrn.t_chapter in (select v.value
                                                                                      from qb_dwh_const4exp c
                                                                                     inner join qb_dwh_const4exp_val v
                                                                                        on (c.id = v.id)
                                                                                     where c.name = cACC_CHAPTERS)
                                                           and pfi.t_fi_kind = n1
                                                           and rfi.t_fi_kind = n1)
                                          select distinct dp.t_name || '#IBSOXXX#' || dacnt.uf4_r,
                                                          --'XXXX#SOFR#' ||
                                                          nvl(cat_rd.t_code, cat_ro.t_code),
                                                          nvl(acd_rd.t_catid, acd_ro.t_catid) catid_receiver,
                                                          nvl(cat_rd.t_name, cat_ro.t_name) cat_name,
                                                          nvl(acd_rd.t_activatedate, acd_ro.t_activatedate) catdate,
                                                          case when acd_rd.t_activatedate is not null then acd_rd.t_disablingdate else acd_ro.t_disablingdate end catenddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                          dacnt.t_department
                                            from dacnt
                                           inner join ddp_dep_dbt dp
                                              on (dacnt.t_department = dp.t_code)
                                            left join dmcaccdoc_dbt acd_rd
                                              on (dacnt.t_account_receiver = acd_rd.t_account and
                                                 acd_rd.t_dockind = rec.bofficekind and
                                                 acd_rd.t_docid = rec.dealid)
                                            left join dmccateg_dbt cat_rd
                                              on (acd_rd.t_catid = cat_rd.t_id)
                                            left join dmcaccdoc_dbt acd_ro
                                              on (dacnt.t_account_receiver = acd_ro.t_account and
                                                 acd_ro.t_iscommon = chr88)
                                            left join dmccateg_dbt cat_ro
                                              on (acd_ro.t_catid = cat_ro.t_id)
                                           ))
                        )
      loop
        begin
        -- Вставка в ass_accountdeal
        if (dacc_rec.cat_id is not null) then
          begin
            insert into ldr_infa_cb.ass_accountdeal(account_code, deal_code, roleaccount_deal_code, dt, rec_status, sysmoment, ext_file, dt_end) -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                   values(dacc_rec.acc, rec.code, dacc_rec.cat_code, qb_dwh_utils.DateToChar(dacc_rec.cat_date), dwhRecStatus, dwhSysMoment, dwhEXT_FILE, case when dacc_rec.cat_enddate = to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END) else qb_dwh_utils.DateToChar(dacc_rec.cat_enddate-1) end);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
          begin
            insert into ldr_infa_cb.det_roleaccount_deal(code, name, orole_code, dt, rec_status, sysmoment, ext_file)
                   values (dacc_rec.cat_code, dacc_rec.cat_name, '0', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
        end if;
        exception
          when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке счета по сделке с УВ: ' || SQLERRM,
                                 0,
                                 cDeal,
                                 rec.dealid);
        end;
      end loop;
      --commit;
    end loop;

    -- сделки с прочими ц/б
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с прочими ц/б',
                   2,
                   null,
                   null);

    for rec in (select to_char(tick.t_dealid) || '#TCK' code,
                       '0000' department_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             decode(tick.t_partyid, -1, 1, tick.t_partyid)) subject_code,

                       case when tick.t_bofficekind = 101 then
                              case when (leg0.t_id is not null) and (leg2.t_id is not null)  then -- есть два транша -  сделка РЕПО
                                     case when rsb_secur.IsSale(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then -- продажа с обратным выкупом (прямое РЕПО)
                                          '15'
                                         when rsb_secur.IsBuy(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then -- покупка с обратной продажей (прямое РЕПО)
                                          '12'
                                         else
                                          '-1' -- эта ветка не должна работать
                                     end
                                   else
                                     '13'  -- Покупка/Продажа
                              end
                            when tick.t_bofficekind in (117, 4832) then
                              '27'
                            when tick.t_bofficekind in (127, 4831) then
                              '24'
                            when tick.t_bofficekind = 4830 then
                              '13'
                            else
                              '-1'
                       end dealtype,
                       tick.t_dealcode docnum,
                       '0' is_interior,
                       decode(leg0.t_start, emptDate, tick.t_dealdate, leg0.t_start) begindate,
                       case when nvl(tkchn.t_OldMaturity2, leg2.t_maturity) is not null then
                              decode(nvl(tkchn.t_OldMaturity2, leg2.t_maturity), emptDate, tick.t_dealdate, nvl(tkchn.t_OldMaturity2, leg2.t_maturity))
                            else
                              decode(leg0.t_maturity, emptDate, tick.t_dealdate, leg0.t_maturity)
                       end enddate,
                       tick.t_comment note,
                       tick.t_dealdate dt,
                       leg0.t_id ledid_0,
                       leg2.t_id ledid_2,
                       tick.t_dealid dealid,
                       tick.t_dealdate,
                       tick.t_bofficekind bofficekind,
                       case when (select t_partyid
                                    from ddp_dep_dbt
                                   where t_parentcode = 0
                                     and t_nodetype = 1
                                     and t_status = 2) = fi.t_issuer then
                              '0'
                             else
                              '1'
                       end is_our_cb,
                       leg0.t_incomerate incomerate,
                       leg0.t_totalcost deal_amount1,
                       leg0.t_principal amount1
                  from ddl_tick_dbt tick
                 inner join dfininstr_dbt fi
                    on (tick.t_pfi = fi.t_fiid)
                 inner join doprkoper_dbt opr
                    on (tick.t_dealtype = opr.t_kind_operation)
                  left join ddl_leg_dbt leg0
                    on (tick.t_dealid = leg0.t_dealid and leg0.t_legkind = n0)
                  left join ddl_leg_dbt leg2
                    on (tick.t_dealid = leg2.t_dealid and leg2.t_legkind = n2)
                  left join dsptkchng_dbt tkchn
                    on tkchn.t_id = (select min(s_tkchn.t_id) from dsptkchng_dbt s_tkchn where s_tkchn.t_dealid = tick.t_dealid)
                 where tick.t_dealdate <= in_date
                   and tick.t_bofficekind  in (select v.value
                                                 from qb_dwh_const4exp c
                                                inner join qb_dwh_const4exp_val v
                                                   on (c.id = v.id)
                                                where c.name = cDEALSKIND_SEC)
                   and tick.t_clientid = n_1
                   and fi.t_avoirkind in (select v.value
                                            from qb_dwh_const4exp c
                                           inner join qb_dwh_const4exp_val v
                                              on (c.id = v.id)
                                           where c.name = cSECKIND_ALL))
    loop
      begin
      insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
             values (rec.code, rec.department_code, rec.subject_code, rec.dealtype, rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      if (rec.dealtype = '15') or (rec.dealtype = '12') then -- Прямое или обратное РЕПО
        select count(*)
          into cnt
          from ddl_tick_ens_dbt t
         where t.t_dealid = rec.dealid;
        if (cnt = 0) then
          -- Сделка с одной ц/б
          insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
                 values (rec.code || '#1', rec.department_code, rec.subject_code, '13', rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.begindate), rec.note, qb_dwh_utils.DateToChar(rec.begindate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          add2Fct_Securitydeal(rec.ledid_0,
                               rec.dealid,
                               rec.code || '#1',
                               dwhRecStatus,
                               dwhSysMoment,
                               dwhEXT_FILE);
          insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
                 values (rec.code || '#2', rec.department_code, rec.subject_code, '13', rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.enddate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.enddate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          add2Fct_Securitydeal(rec.ledid_2,
                               rec.dealid,
                               rec.code || '#2',
                               dwhRecStatus,
                               dwhSysMoment,
                               dwhEXT_FILE);
          if rec.dealtype = '15' then
              insert into ldr_infa_cb.fct_repodeal(typedealasgmt, deal_code, proc_rate, proc_base, rec_status, sysmoment, ext_file, typedirect)
                     values(rec.is_our_cb, rec.code, qb_dwh_utils.NumberToChar(rec.incomerate), null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE, '1');
commit;
          end if;
          if rec.dealtype = '12' then
              insert into ldr_infa_cb.fct_repodeal_reverse(typedealasgmt, deal_code, proc_rate, proc_base, rec_status, sysmoment, ext_file, typedirect)
                     values(rec.is_our_cb, rec.code, qb_dwh_utils.NumberToChar(rec.incomerate), null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE, '2');
commit;                     
          end if;
          insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                 values(rec.code, rec.code || '#1', 'REPO', qb_dwh_utils.DateToChar(rec.t_dealdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                 values(rec.code, rec.code || '#2', 'REPO', qb_dwh_utils.DateToChar(rec.t_dealdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
        -- сделка с корзиной ц/б
          -- перенесено из тестового скрипта
          -- цикл по dealid датам сделки в которые менялся состав корзины
          insert into ldr_infa_cb.fct_securitydeal(typesecdeal, sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code,deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, deal_fee, extra_costs, rec_status, sysmoment, ext_file)
                 values('7', qb_dwh_utils.NumberToChar(0, 14), '0', qb_dwh_utils.NumberToChar(0, 14), '0', '0', '-1', rec.deal_amount1, rec.amount1, rec.code, '-1', '-1', '-1',  null, null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 

          for date_rec in (select chg_date bd,
                                  lead(chg_date - 1, 1, to_date('31129999','ddmmyyyy')) over(order by chg_date) ed
                             from (select distinct t_date chg_date
                                     from ddl_tick_ens_dbt b
                                    where b.t_dealid = rec.dealid
                                    order by chg_date)
                                   order by bd)
          loop
            -- цикл по ценным бумагам в корзине
            for sec_rec in (select distinct t_fiid
                              from ddl_tick_ens_dbt b
                             where b.t_dealid = rec.dealid)
            loop
              -- опеределим количество и стоимость ц/б на дату
              sec_basket_rec := GetParmsSecurity(rec.dealid, sec_rec.t_fiid, date_rec.bd);
              if (sec_basket_rec.cnt > 0) then
                insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
                   values(sec_basket_rec.part_code || '#1', rec.department_code, rec.subject_code, '13', rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(sec_basket_rec.date1), qb_dwh_utils.DateToChar(sec_basket_rec.date1), rec.note, qb_dwh_utils.DateToChar(sec_basket_rec.date1), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
                insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                   values(sec_basket_rec.main_code, sec_basket_rec.part_code || '#1','REPO', qb_dwh_utils.DateToChar(date_rec.bd), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
                insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
                   values(sec_basket_rec.part_code || '#2', rec.department_code, rec.subject_code, '13', rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(sec_basket_rec.date2), qb_dwh_utils.DateToChar(sec_basket_rec.date2), rec.note, qb_dwh_utils.DateToChar(sec_basket_rec.date2), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
                insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                   values(sec_basket_rec.main_code, sec_basket_rec.part_code || '#2','REPO', qb_dwh_utils.DateToChar(date_rec.bd), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
                add2Fct_Securitydeal_basket(sec_basket_rec.legid0,
                                                sec_basket_rec.dealid,
                                                sec_basket_rec.part_code || '#1',
                                                sec_basket_rec.totalcost,
                                                sec_basket_rec.cnt,
                                                sec_basket_rec.nkd,
                                                sec_basket_rec.fi_code,
                                                sec_basket_rec.costfiid,
                                                dwhRecStatus,
                                                dwhSysMoment,
                                                dwhEXT_FILE);
                add2Fct_Securitydeal_basket(sec_basket_rec.legid2,
                                                sec_basket_rec.dealid,
                                                sec_basket_rec.part_code || '#2',
                                                sec_basket_rec.sump2,
                                                sec_basket_rec.cnt,
                                                sec_basket_rec.nkd,
                                                sec_basket_rec.fi_code,
                                                sec_basket_rec.costfiid,
                                                dwhRecStatus,
                                                dwhSysMoment,
                                                dwhEXT_FILE);
                if (date_rec.ed < to_date('31129999','ddmmyyyy') ) then --если было иземение состава корзины закрываем добавленнные части
                  insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                     values(sec_basket_rec.main_code, sec_basket_rec.part_code || '#1','REPO', qb_dwh_utils.DateToChar(date_rec.ed + 1), '1', dwhSysMoment, dwhEXT_FILE);
commit;                     
                  insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                     values(sec_basket_rec.main_code, sec_basket_rec.part_code || '#2','REPO', qb_dwh_utils.DateToChar(date_rec.ed + 1), '1', dwhSysMoment, dwhEXT_FILE);
commit;                     
                end if;
              end if;
            end loop;
            totalcnt := GetCountCBTotal(rec.dealid, date_rec.bd);
            insert into ldr_infa_cb.fct_deal_indicator(deal_code,
                                                    deal_attr_code,
                                                    currency_curr_code_txt,
                                                    measurement_unit_code,
                                                    number_value,
                                                    date_value,
                                                    string_value,
                                                    dt,
                                                    rec_status,
                                                    sysmoment,
                                                    ext_file)
               values (to_char(rec.dealid) || '#TCK',
                       'BASKET_AMOUNT',
                       '-1',
                       '-1',
                       qb_dwh_utils.NumberToChar(totalcnt, 0), -- общее количество бумаг по сделке
                       null,
                       null,
                       qb_dwh_utils.DateToChar(date_rec.bd),
                       dwhRecStatus,
                       dwhSysMoment,
                       dwhEXT_FILE);
commit;
          end loop;
          -- перенесено из тестового скрипта

          if rec.dealtype = '15' then
            insert into ldr_infa_cb.fct_repodeal(typedealasgmt, deal_code, proc_rate, proc_base, rec_status, sysmoment, ext_file, typedirect)
                     values(rec.is_our_cb, rec.code, qb_dwh_utils.NumberToChar(rec.incomerate), null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE, '3');
commit;
          end if;
          if rec.dealtype = '12' then
            insert into ldr_infa_cb.fct_repodeal_reverse(typedealasgmt, deal_code, proc_rate, proc_base, rec_status, sysmoment, ext_file, typedirect)
                   values(rec.is_our_cb, rec.code, qb_dwh_utils.NumberToChar(rec.incomerate), null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE, '3');
commit;                   
          end if;
        end if;
        -- Добавление графиков
        insert into ldr_infa_cb.fct_repayschedule_dm(typeschedule,
                                                    eventsum,
                                                    dealsum,
                                                    code,
                                                    finstramount,
                                                    movingdirection,
                                                    finstr_code,
                                                    deal_code,
                                                    typerepay_code,
                                                    dt,
                                                    rec_status,
                                                    sysmoment,
                                                    ext_file)
         (select '1' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#PAYFIRST#PLAN' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(decode(rq.t_plandate, emptDate, firstDate, rq.t_plandate)) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n1 and rq.t_type = n2 and rownum = n1
          union all
          select '3' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#PAYFIRST#FACT' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_factdate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n1 and rq.t_type = n2 and rownum = n1
          union all
          select '1' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#SUPFIRST#PLAN' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_plandate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n1 and rq.t_type = n8 and rownum = n1
          union all
          select '3' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#SUPFIRST#FACT' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_factdate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n1 and rq.t_type = n8 and rownum = n1
          union all
          select '1' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#PAYSEC#PLAN' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_plandate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n2 and rq.t_type = n2 and rownum = n1
          union all
          select '3' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#PAYSEC#FACT' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_factdate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n2 and rq.t_type = n2 and rownum = n1
          union all
          select '1' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#SUPSEC#PLAN' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_plandate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n2 and rq.t_type = n8 and rownum = n1
          union all
          select '3' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#SUPSEC#FACT' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_factdate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n2 and rq.t_type = n8 and rownum = n1
          );
commit;

      end if;
      if rec.dealtype = '13' then
        add2Fct_Securitydeal(rec.ledid_0,
                             rec.dealid,
                             rec.code,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE);
      elsif rec.dealtype = '27' then
        add2Fct_Repaydeal(rec.ledid_0,
                             rec.dealid,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE ) ;
      end if;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке данных по сделке с цб: ' || SQLERRM,
                               0,
                               cDeal,
                               rec.dealid);
      end;

      -- Добавление счетов по сделкам с прочими ц/б

      for dacc_rec in ( select *
                          from (select distinct dp.t_name || '#IBSOXXX#' || sacc.uf4 acc,
/*                                                'XXXX#SOFR#' || */
                                                nvl(nvl(cat_rd.t_code, cat_ro.t_code),cat_leg.t_code) catcode,
                                                nvl(nvl(acd_rd.t_catid, acd_ro.t_catid), acd_leg.t_catid) catid,
                                                nvl(nvl(cat_rd.t_name, cat_ro.t_name), cat_leg.t_name) catname,
                                                nvl(nvl(acd_rd.t_activatedate, acd_ro.t_activatedate), acd_leg.t_activatedate) catdate,
                                                case when acd_rd.t_activatedate is not null then acd_rd.t_disablingdate
                                                     when acd_ro.t_activatedate is not null then acd_ro.t_disablingdate
                                                     else acd_leg.t_disablingdate
                                                end catenddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                sacc.t_department
                                  from (with dp as (select rec.bofficekind   rec_bofficekind,
                                                           rec.dealid rec_dealid
                                                      from dual), dacnt as (select acctrn.t_account_payer,
                                                                                   --acc_p.t_userfield4 uf4_p,
                                                                                   case
                                                                                      when (acc_p.t_userfield4 is null) or
                                                                                          (acc_p.t_userfield4 = chr(0)) or
                                                                                          (acc_p.t_userfield4 = chr(1)) or
                                                                                          (acc_p.t_userfield4 like '0x%') then
                                                                                        acc_p.t_account
                                                                                      else
                                                                                        acc_p.t_userfield4
                                                                                   end uf4_p,
                                                                                   acctrn.t_account_receiver,
                                                                                   acctrn.t_department,
                                                                                   dealkind,
                                                                                   dealid
                                                                              from dacctrn_dbt acctrn,
                                                                                   (select /*+LEADING(grdeal) INDEX(grdeal ddlgrdeal_dbt_idx1)*/
                                                                                     grdoc.t_docid    as acctrnid,
                                                                                     grdeal.t_dockind dealkind,
                                                                                     grdeal.t_docid   dealid
                                                                                      from ddlgrdeal_dbt grdeal,
                                                                                           ddlgrdoc_dbt  grdoc
                                                                                     where exists
                                                                                     (select 1
                                                                                              from dp
                                                                                             where dp.rec_bofficekind =
                                                                                                   grdeal.t_dockind
                                                                                               and dp.rec_dealid =
                                                                                                   grdeal.t_docid)
                                                                                       and grdoc.t_grdealid =
                                                                                           grdeal.t_id
                                                                                       and grdoc.t_dockind = n1
                                                                                    union all
                                                                                    select oprdocs.t_acctrnid as acctrnid,
                                                                                           opr.t_dockind,
                                                                                           to_number(opr.t_documentid)
                                                                                      from doproper_dbt opr,
                                                                                           doprdocs_dbt oprdocs
                                                                                     where exists
                                                                                     (select 1
                                                                                              from dp
                                                                                             where dp.rec_bofficekind =
                                                                                                   opr.t_dockind
                                                                                               and lpad(to_char(dp.rec_dealid),
                                                                                                        34,
                                                                                                        0) =
                                                                                                   opr.t_documentid)
                                                                                       and oprdocs.t_id_operation =
                                                                                           opr.t_id_operation
                                                                                       and oprdocs.t_dockind = n1) q,
                                                                                   dfininstr_dbt pfi,
                                                                                   dfininstr_dbt rfi,
                                                                                   daccount_dbt  acc_p
                                                                             where acctrn.t_acctrnid =
                                                                                   q.acctrnid
                                                                               and acctrn.t_accountid_payer = acc_p.t_accountid
                                                                               and pfi.t_fiid =
                                                                                   acctrn.t_fiid_payer
                                                                               and rfi.t_fiid =
                                                                                   acctrn.t_fiid_receiver
                                                                               and acctrn.t_state = 1
                                                                               and acctrn.t_chapter in
                                                                                   (select v.value
                                                                                      from qb_dwh_const4exp c
                                                                                     inner join qb_dwh_const4exp_val v
                                                                                        on (c.id = v.id)
                                                                                     where c.name =
                                                                                           cACC_CHAPTERS)
                                                                               and pfi.t_fi_kind = n1
                                                                               and rfi.t_fi_kind = n1)
                                         select dacnt.t_account_payer acc,
                                                dacnt.uf4_p uf4,
                                                dacnt.t_department,
                                                dacnt.dealkind,
                                                dacnt.dealid
                                           from dacnt
                                         union all
                                         select *
                                           from (with dp as (select rec.bofficekind   rec_bofficekind,
                                                                    rec.dealid rec_dealid
                                                               from dual), dacnt as (select acctrn.t_account_payer,
                                                                                            acctrn.t_account_receiver,
                                                                                            --acc_r.t_userfield4 uf4_r,
                                                                                            case
                                                                                              when (acc_r.t_userfield4 is null) or
                                                                                                  (acc_r.t_userfield4 = chr(0)) or
                                                                                                  (acc_r.t_userfield4 = chr(1)) or
                                                                                                  (acc_r.t_userfield4 like '0x%') then
                                                                                                acc_r.t_account
                                                                                              else
                                                                                                acc_r.t_userfield4
                                                                                            end uf4_r,
                                                                                            acctrn.t_department,
                                                                                            dealkind,
                                                                                            dealid
                                                                                       from dacctrn_dbt acctrn,
                                                                                            (select /*+LEADING(grdeal) INDEX(grdeal ddlgrdeal_dbt_idx1)*/
                                                                                              grdoc.t_docid    as acctrnid,
                                                                                              grdeal.t_dockind dealkind,
                                                                                              grdeal.t_docid   dealid
                                                                                               from ddlgrdeal_dbt grdeal,
                                                                                                    ddlgrdoc_dbt  grdoc
                                                                                              where exists
                                                                                              (select 1
                                                                                                       from dp
                                                                                                      where dp.rec_bofficekind =
                                                                                                            grdeal.t_dockind
                                                                                                        and dp.rec_dealid =
                                                                                                            grdeal.t_docid)
                                                                                                and grdoc.t_grdealid =
                                                                                                    grdeal.t_id
                                                                                                and grdoc.t_dockind = n1
                                                                                             union all
                                                                                             select oprdocs.t_acctrnid as acctrnid,
                                                                                                    opr.t_dockind,
                                                                                                    to_number(opr.t_documentid)
                                                                                               from doproper_dbt opr,
                                                                                                    doprdocs_dbt oprdocs
                                                                                              where exists
                                                                                              (select 1
                                                                                                       from dp
                                                                                                      where dp.rec_bofficekind =
                                                                                                            opr.t_dockind
                                                                                                        and lpad(to_char(dp.rec_dealid),
                                                                                                                 34,
                                                                                                                 0) =
                                                                                                            opr.t_documentid)
                                                                                                and oprdocs.t_id_operation =
                                                                                                    opr.t_id_operation
                                                                                                and oprdocs.t_dockind = n1) q,
                                                                                            dfininstr_dbt pfi,
                                                                                            dfininstr_dbt rfi,
                                                                                            daccount_dbt acc_r
                                                                                      where acctrn.t_acctrnid =
                                                                                            q.acctrnid
                                                                                        and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                                                        and pfi.t_fiid =
                                                                                            acctrn.t_fiid_payer
                                                                                        and rfi.t_fiid =
                                                                                            acctrn.t_fiid_receiver
                                                                                        and acctrn.t_state = n1
                                                                                        and acctrn.t_chapter in
                                                                                            (select v.value
                                                                                               from qb_dwh_const4exp c
                                                                                              inner join qb_dwh_const4exp_val v
                                                                                                 on (c.id = v.id)
                                                                                              where c.name =
                                                                                                    cACC_CHAPTERS)
                                                                                        and pfi.t_fi_kind = n1
                                                                                        and rfi.t_fi_kind = n1)
                                                  select distinct dacnt.t_account_receiver,
                                                                  dacnt.uf4_r,
                                                                  dacnt.t_department,
                                                                  dacnt.dealkind,
                                                                  dacnt.dealid
                                                    from dacnt)) sacc
                                                   inner join ddp_dep_dbt dp
                                                      on (sacc.t_department = dp.t_code)
                                                    left join dmcaccdoc_dbt acd_rd
                                                      on (sacc.acc = acd_rd.t_account and
                                                         acd_rd.t_dockind = sacc.dealkind and
                                                         acd_rd.t_docid = sacc.dealid)
                                                    left join dmccateg_dbt cat_rd
                                                      on (acd_rd.t_catid = cat_rd.t_id)
                                                    left join dmcaccdoc_dbt acd_ro
                                                      on (sacc.acc = acd_ro.t_account and
                                                         acd_ro.t_iscommon = chr88)
                                                    left join dmccateg_dbt cat_ro
                                                      on (acd_ro.t_catid = cat_ro.t_id)
                                                    left join ddl_leg_dbt leg
                                                      on (sacc.dealid = leg.t_dealid and leg.t_legkind = 0)
                                                    left join dmcaccdoc_dbt acd_leg
                                                      on (sacc.acc = acd_leg.t_account and
                                                         acd_leg.t_dockind = 176 and
                                                         acd_leg.t_docid = leg.t_id)
                                                    left join dmccateg_dbt cat_leg
                                                      on (acd_leg.t_catid = cat_leg.t_id)
                                )
                         where catid is not null
                        )
      loop
        -- Вставка в ass_accountdeal
        begin
          begin
            insert into ldr_infa_cb.ass_accountdeal(account_code, deal_code, roleaccount_deal_code, dt, rec_status, sysmoment, ext_file, dt_end) -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                   values(dacc_rec.acc, rec.code, dacc_rec.catcode, qb_dwh_utils.DateToChar(dacc_rec.catdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE, case when dacc_rec.catenddate = to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END) else qb_dwh_utils.DateToChar(dacc_rec.catenddate-1) end);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
          begin
            insert into ldr_infa_cb.det_roleaccount_deal(code, name, orole_code, dt, rec_status, sysmoment, ext_file)
                   values (dacc_rec.catcode, dacc_rec.catname, '0', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
        exception
          when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке счета по сделке с цб ' || SQLERRM,
                                 0,
                                 cDeal,
                                 rec.dealid);
        end;

      end loop;

      insert into ldr_infa_cb.ass_carrydeal
      (select * from (with dp as (select rec.bofficekind rec_bofficekind,
                                         rec.dealid rec_dealid
                                    from dual),
                          trn as (select acctrnid,
                                         acctrn.t_account_payer,
                                         acctrn.t_account_receiver,
                                         acctrn.t_department,
                                         acctrn.t_date_carry,
                                         dealkind,
                                         dealid
                                   from dacctrn_dbt acctrn,
                                        (select /*+LEADING(grdeal) INDEX(grdeal ddlgrdeal_dbt_idx1)*/
                                          grdoc.t_docid    as acctrnid,
                                          grdeal.t_dockind dealkind,
                                          grdeal.t_docid   dealid
                                           from ddlgrdeal_dbt grdeal,
                                                ddlgrdoc_dbt  grdoc
                                          where exists  (select 1
                                                           from dp
                                                          where dp.rec_bofficekind =
                                                                grdeal.t_dockind
                                                            and dp.rec_dealid =
                                                                grdeal.t_docid)
                                            and grdoc.t_grdealid = grdeal.t_id
                                            and grdoc.t_dockind = n1
                                         union all
                                         select oprdocs.t_acctrnid as acctrnid,
                                                opr.t_dockind,
                                                to_number(opr.t_documentid)
                                           from doproper_dbt opr,
                                                doprdocs_dbt oprdocs
                                          where exists (select 1
                                                          from dp
                                                         where dp.rec_bofficekind = opr.t_dockind
                                                           and lpad(to_char(dp.rec_dealid), 34, 0) = opr.t_documentid)
                                            and oprdocs.t_id_operation = opr.t_id_operation
                                            and oprdocs.t_dockind = n1) q
                                  where acctrn.t_acctrnid = q.acctrnid
                                    and acctrn.t_state = n1
                                    and acctrn.t_userfield4 <> chr(1)), -- исключим проводки без идентификатора БИСКВИТ
                           trn_code as ( select qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                            qb_dwh_utils.System_IBSO,
                                                            trn.t_Department,
                                                            trn.AcctrnID,
                                                            trn.Dealkind) CARRY_CODE,
                                                to_char(trn.dealid) || '#TCK' DEAL_CODE,
                                                qb_dwh_utils.DateToChar(trn.t_date_carry) DT,
                                                '0' REC_STATUS,
                                                dwhSYSMOMENT,
                                                dwhEXT_FILE
                                           from trn)
                    select * from trn_code));
commit;
      --commit;
    end loop;

    -- конвертации выпусков
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка конвертаций выпуска',
                   2,
                   null,
                   null);
    for rec in (select to_char(cm.t_documentid) || '#COM' code,
                       '0000' department_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             decode(cm.t_clientid, -1, 1, cm.t_clientid)) subject_code,
                       '13' dealtype,
                       cm.t_commcode docnum,
                       '0' is_interior,
                       cm.t_commdate begindate,
                       cm.t_commdate enddate,
                       cm.t_comment note,
                       cm.t_commdate dt,
                       to_char(cm.t_fiid) || decode(fi.t_avoirkind, 5, '#BNR', '#FIN') code_currency,
                       cm.t_documentid
                  from ddl_comm_dbt cm
                 inner join dfininstr_dbt fi
                    on (cm.t_fiid = fi.t_fiid)
                 where cm.t_commdate <= in_date
                   and cm.t_dockind = n135
                   and cm.t_operationkind = n2020)
    loop
      begin
      insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
             values (rec.code, rec.department_code, rec.subject_code, rec.dealtype, rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
             values ('9', '-1', null, '-1', '-1', null, rec.code_currency, null, null, rec.code, rec.code_currency, rec.code_currency, '-1', dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке конвертаций выпуска: ' || SQLERRM,
                               0,
                               cDeal,
                               rec.t_documentid);
      end;

    end loop;

    --BIQ-7477/7478 Выгрузка депозитных сертификатов
    if (BIQ_7477_78 = 1) then
        qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка депозитных сертификатов',
                       2,
                       null,
                       null);
        for rec in (select 
            to_char(cert.t_fiid)||'#SRT' SECURITY_CODE, 
            to_char(bnr.t_kind) TYPESERTIFICATE, --1=именной, 2 =предъявительский
            cert.t_number NUMBERSERTIFICATE,
            cert.t_series SERIESSERTIFICATE,
            qb_dwh_utils.datetochar(leg.t_closed) MATURITYDATE, --дата погашения 
            qb_dwh_utils.datetochar(leg.t_maturity) DEMANDDATE, --дата востребования в интерфейсе
            qb_dwh_utils.datetochar(leg.t_start) DEPOSITDATE, --дата внесения
            qb_dwh_utils.NumberToChar(leg.t_price/power(10, leg.t_point)) PROC_RATE,
            qb_dwh_utils.NumberToChar(bnr.t_oncallrate/power(10, leg.t_point)) AHEAD_PROC_RATE,
            --состояние
            bnr.t_bcstate SERTIFICATE_STATE, --состояние
            to_char(cert.t_fiid)||'#SRT' SERTIFICATE_CODE,
            qb_dwh_utils.datetochar(cert.t_issuedate) DT, 
            qb_dwh_utils.GetComponentCode('DET_SUBJECT', qb_dwh_utils.System_IBSO, 1, bnr.t_holder) SUBJECT_CODE, --держатель, как в векселе
            --справочник ЦБ
            qb_dwh_utils.datetochar(fi.t_issued) DATE_ISSUE,
            qb_dwh_utils.NumberToChar(fi.t_facevalue) NOMINAL,
            qb_dwh_utils.GetComponentCode('DET_SUBJECT', qb_dwh_utils.System_IBSO, 1, bnr.t_issuer) ISSUER_CODE,
            fi_nom.t_iso_number FINSTRCURNOM_FINSTR_CODE,
            fi.t_name FINSTR_NAME,
            fi.t_definition FINSTR_NAME_S
             
              from dv_ficert_dbt cert 
              inner join dvsbanner_dbt bnr on (bnr.t_bcid = cert.t_ficertid and cert.t_avoirkind= 9) --9=Депозитный сертификат
              inner join dfininstr_dbt fi on fi.t_fiid = cert.t_fiid
              inner join dfininstr_dbt fi_nom on fi_nom.t_fiid = fi.t_facevaluefi
              inner join ddl_leg_dbt leg on (leg.t_dealid = cert.t_ficertid and leg.t_start = cert.t_issuedate and leg.t_principal = cert.t_facevalue)
              left join dparty_dbt pt on pt.t_partyid = cert.t_issuer
              )
              loop
                        begin
                              insert into ldr_infa_cb.det_sertificate(SECURITY_CODE, TYPESERTIFICATE, NUMBERSERTIFICATE, SERIESSERTIFICATE, MATURITYDATE, DEMANDDATE, DEPOSITDATE, PROC_RATE, AHEAD_PROC_RATE, DT, rec_status, sysmoment, ext_file)
                              values (rec.SECURITY_CODE, rec.TYPESERTIFICATE, rec.NUMBERSERTIFICATE, rec.SERIESSERTIFICATE, rec.MATURITYDATE, rec.DEMANDDATE, rec.DEPOSITDATE, rec.PROC_RATE, rec.AHEAD_PROC_RATE, rec.DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                              
                              
                              insert into ldr_infa_cb.fct_sertificate_state(SERTIFICATE_STATE, SERTIFICATE_CODE, DT, SUBJECT_CODE, rec_status, sysmoment, ext_file)
                              values (rec.SERTIFICATE_STATE, rec.SERTIFICATE_CODE, rec.DT, rec.SUBJECT_CODE, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                              
                              
                              Insert into ldr_infa_cb.det_security(typesecurity, code, date_issue, nominal, regnum, finstrsecurity_finstr_code, issuer_code, underwriter_code, finstrcurnom_finstr_code, procbase, dt, rec_status, sysmoment, ext_file)
                              values ('4', rec.SECURITY_CODE, rec.DATE_ISSUE, rec.NOMINAL, null, rec.SECURITY_CODE, rec.ISSUER_CODE, '-1', rec.FINSTRCURNOM_FINSTR_CODE, '9999#SOFRXXX#1', rec.DATE_ISSUE, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                              
                       
                              Insert into ldr_infa_cb.det_finstr(finstr_code, finstr_name, finstr_name_s, typefinstr, dt, rec_status,sysmoment, ext_file)
                              values (rec.SECURITY_CODE, rec.FINSTR_NAME, rec.FINSTR_NAME_S, '2', rec.DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);       
commit;                              
                         
                         exception
                              when others then
                                qb_bp_utils.SetError(EventID,
                                    SQLCODE,
                                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке депозитных сертификатов: ' || SQLERRM,
                                   0,
                                   cDeal,
                                   rec.SECURITY_CODE);
                        end;

               end loop;
    end if;

    qb_bp_utils.SetError(EventID,
               '',
               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка атрибутов сделок',
               2,
               null,
               null);

    begin

    -- Заполнение категории 47 по сделкам !!!!!!!!!!!!!!!!( удалить при установке обновления по заполнению данной категории)
    insert into dobjatcor_dbt
      select 101,
             47,
             1,
             lpad(to_char(tick.t_dealid), 34, '0'),
             chr(0),
             tick.t_dealdate,
             1,
             to_date('31129999','ddmmyyyy'),
             trunc(sysdate),
             to_date('01010001 ' || to_char(sysdate, 'hh24:mi:ss'), 'ddmmyyyy hh24:mi:ss'),
             chr(88),
             null
        from ddl_tick_dbt tick
        inner join dfininstr_dbt fi
           on (tick.t_pfi = fi.t_fiid)
        where tick.t_dealdate <= in_date
          and tick.t_bofficekind  in (select v.value
                                        from qb_dwh_const4exp c
                                       inner join qb_dwh_const4exp_val v
                                          on (c.id = v.id)
                                       where c.name = cDEALSKIND_SEC)
          and tick.t_clientid = n_1
          and fi.t_avoirkind in (select v.value
                                   from qb_dwh_const4exp c
                                  inner join qb_dwh_const4exp_val v
                                     on (c.id = v.id)
                                  where c.name = cSECKIND_ALL)
          and not exists (select 1
                            from dobjatcor_dbt ac
                           where ac.t_objecttype = n101
                             and ac.t_groupid = n47
                             and ac.t_object = lpad(to_char(tick.t_dealid), 34, '0'));
commit;                             
    -- Добавление характеристики "Тест на рыночность пройден" для сделок с УВ
     insert into ldr_infa_cb.ass_deal_cat_val (deal_code, deal_cat_val_code, deal_cat_val_code_deal_cat, dt, rec_status, sysmoment, ext_file)
            (select distinct code,
                            '101C47#' || testnm,
                            '101C47',
                            qb_dwh_utils.datetochar(t_dealdate),
                            dwhRecStatus,
                            dwhSysMoment,
                            dwhEXT_FILE
              from (select to_char(tick.t_dealid) || '#TCK' code,
                           row_number() over(partition by tick.t_dealid order by leg.t_id) rnk,
                           count(*) over(partition by tick.t_dealid) cnt,
                           decode(leg.t_relativeprice, chr(88), 'Да', 'Нет') testnm,
                           leg.t_relativeprice,
                           tick.t_dealdate
                      from dvsbanner_dbt bn
                     inner join ddl_leg_dbt leg
                        on (bn.t_bcid = leg.t_dealid)
                     inner join dvsordlnk_dbt lnk
                        on (bn.t_bcid = lnk.t_bcid)
                     inner join ddl_tick_dbt tick
                        on (lnk.t_contractid = tick.t_dealid and
                           lnk.t_dockind = tick.t_bofficekind)
                     inner join dfininstr_dbt fi
                        on (leg.t_cfi = fi.t_fiid)
                     where leg.t_legid = n0
                       and leg.t_legkind = n1
                       and tick.t_bofficekind in
                           (select v.value
                              from qb_dwh_const4exp c
                             inner join qb_dwh_const4exp_val v
                                on (c.id = v.id)
                             where c.name = cDEALSKIND_DBILL)
                       and tick.t_dealdate <= in_date
                       and leg.t_relativeprice <> chr0));
commit;                       
    -- Добавление характеристики "Тест на рыночность пройден" для сделок с СВ
    insert into ldr_infa_cb.ass_deal_cat_val (deal_code, deal_cat_val_code, deal_cat_val_code_deal_cat, dt, rec_status, sysmoment, ext_file)
      select distinct to_char(ord.t_contractid) || '#ORD' code,
            '101C47#' || decode(leg.t_relativeprice, chr(88), 'Да', 'Нет') testnm,
            '101C47',
            qb_dwh_utils.datetochar(ord.t_signdate) dt,
            dwhRecStatus,
            dwhSysMoment,
            dwhEXT_FILE
       from dvsbanner_dbt bn
       inner join ddl_leg_dbt leg
         on (bn.t_bcid = leg.t_dealid)
       inner join dvsordlnk_dbt lnk
          on (bn.t_bcid = lnk.t_bcid)
       inner join ddl_order_dbt ord
          on (lnk.t_contractid = ord.t_contractid and lnk.t_dockind = ord.t_dockind)
       inner join ddp_dep_dbt dp
          on (ord.t_department = dp.t_code)
        left join dfininstr_dbt fi_cfi
          on (leg.t_cfi = fi_cfi.t_fiid)
       where leg.t_legid = n0 and leg.t_legkind = n1
         and ord.t_signdate <= in_date
         and leg.t_relativeprice <> chr0;
commit;         
    -- Заполнение справочника категорий по сделкам
    Insert into ldr_infa_cb.det_deal_cat(code_deal_cat, name_deal_cat, is_multivalued, dt, rec_status, sysmoment, ext_file)
          (SELECT DISTINCT TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_DEAL_CAT,
                          UPPER(TRIM(GR.T_NAME)) NAME_DEAL_CAT,
                          DECODE(GR.T_TYPE, CHR(88), '0', '1') IS_MULTYVALUED,
                          qb_dwh_utils.DateToChar(firstDate),
                          dwhRecStatus,
                          dwhSysMoment,
                          dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
          INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
          WHERE AC.T_OBJECTTYPE = n101);
commit;          
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке справочника категорий по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);

    end;

    begin
    -- заполнение справочника значений по категориям
    insert into ldr_infa_cb.det_deal_cat_val(deal_cat_code,
                                          code_deal_cat_val,
                                          name_deal_cat_val,
                                          dt,
                                          rec_status,
                                          sysmoment,
                                          ext_file)
           (SELECT DISTINCT TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) DEAL_CAT_CODE,
                          TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) || '#' || case when trim(atr.t_name) is null or trim(atr.t_name) = chr(1) then atr.t_nameobject else atr.t_name end code_deal_cat_val,
                          TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) || '#' || case when trim(atr.t_fullname) is null or trim(atr.t_fullname) = chr(1) then atr.t_nameobject else atr.t_fullname end name_deal_cat_val,
                          qb_dwh_utils.DateToChar(firstDate),
                          dwhRecStatus,
                          dwhSysMoment,
                          dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
          INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
          inner join dobjattr_dbt atr
             on (gr.t_objecttype = atr.t_objecttype and gr.t_groupid = atr.t_groupid)
          WHERE AC.T_OBJECTTYPE = n101);
commit;          
     -- Заполение списка значений категорий по сделкам
     insert into ldr_infa_cb.ass_deal_cat_val (deal_code, deal_cat_val_code, deal_cat_val_code_deal_cat, dt, rec_status, sysmoment, ext_file)
           (SELECT /*+ leading(tick,ac) index(tick DDL_TICK_DBT_IDX_U1) */ distinct to_char(to_number(ac.t_object)) || '#TCK' deal_code,
                         TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) || '#' || case when trim(atr.t_name) is null or trim(atr.t_name) = chr(1) then atr.t_nameobject else atr.t_name end code_deal_cat_val,
                         TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) DEAL_CAT_CODE,
                         qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, tick.t_dealdate, ac.t_validfromdate)),
                         dwhRecStatus,
                         dwhSysMoment,
                         dwhEXT_FILE
                    FROM DOBJATCOR_DBT AC
                  INNER JOIN DOBJGROUP_DBT GR
                      ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
                  inner join dobjattr_dbt atr
                     on (gr.t_objecttype = atr.t_objecttype and gr.t_groupid = atr.t_groupid and ac.t_attrid = atr.t_attrid)
                  inner join ddl_tick_dbt tick
                     on (ac.t_object = lpad(to_char(tick.t_dealid), 34, '0'))
                  WHERE AC.T_OBJECTTYPE = n101
                     and tick.t_dealdate <= in_date
                     and tick.t_clientid = n_1
                     --and exists (select 1 from ldr_infa_cb.fct_deal where code = to_char(to_number(ac.t_object)) || '#TCK' )
                     );
commit;                     
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке списка категорий по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    insert into ldr_infa_cb.det_deal_typeattr (code,
                                            name,
                                            is_money_value,
                                            data_type,
                                            dt,
                                            rec_status,
                                            sysmoment,
                                            ext_file)
           (SELECT DISTINCT TO_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                            UPPER(TRIM(NK.T_NAME)) NAME,
                            case when nk.t_notetype = 25 then
                                   '1'
                                 else
                                   '0'
                            end is_money_value,
                            case when nk.t_notetype in (0, 1, 2, 3, 4, 25) then
                                   '1'
                                 when nk.t_notetype = 9 then
                                   '2'
                                 when nk.t_notetype in (7, 12) then
                                   '3'
                                 else
                                   '0'
                            end data_type,
                            qb_dwh_utils.DateToChar(firstDate),
                            dwhRecStatus,
                            dwhSysMoment,
                            dwhEXT_FILE
              FROM DNOTETEXT_DBT NT
            INNER JOIN DNOTEKIND_DBT NK
                ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND NT.T_NOTEKIND = NK.T_NOTEKIND)
            WHERE NT.T_OBJECTTYPE = n101);
commit;            
    insert into ldr_infa_cb.det_deal_typeattr (code,
                                            name,
                                            is_money_value,
                                            data_type,
                                            dt,
                                            rec_status,
                                            sysmoment,
                                            ext_file)
       values('BASKET_AMOUNT',
             'КОЛ-ВО БУМАГ В СДЕЛКЕ РЕПО С КОРЗИНОЙ ЦБ',
             '0',
             '1',
             qb_dwh_utils.DateToChar(firstDate),
             dwhRecStatus,
             dwhSysMoment,
             dwhEXT_FILE);
commit;
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке справочника примечаний по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    insert into ldr_infa_cb.fct_deal_indicator(deal_code,
                                            deal_attr_code,
                                            currency_curr_code_txt,
                                            measurement_unit_code,
                                            number_value,
                                            date_value,
                                            string_value,
                                            dt,
                                            rec_status,
                                            sysmoment,
                                            ext_file)
           (              select distinct deal_code,
                     code deal_attr_code,
                     '-1',
                     '-1',
                     case
                       when type in (0, 1, 2, 3, 4, 25) then
                        noteval
                       else
                        null
                     end number_value,
                     case
                       when type in (9, 10) then
                        noteval
                       else
                        null
                     end date_value,
                     case
                       when type in (7, 12) then
                        substr(noteval, 1, 256)
                       else
                        null
                     end string_value,
                     qb_dwh_utils.DateToChar(t_date) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                from (SELECT to_char(tick.t_dealid) ||  '#TCK' deal_code,
                             to_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                             UPPER(TRIM(NK.T_NAME)) NAME,
                             nk.t_notetype type,
                             case nk.t_notetype
                               when 0 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getInt(nt.t_text), 0)
                               when 1 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getLong(nt.t_text), 0)
                               when 2 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 3 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 4 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 7 then
                                Rsb_Struct.getString(nt.t_text)
                               when 9 then
                                qb_dwh_utils.DateToChar(Rsb_Struct.getDate(nt.t_text))
                               when 10 then
                                qb_dwh_utils.DateTimeToChar(Rsb_Struct.getTime(nt.t_text))
                               when 12 then
                                Rsb_Struct.getChar(nt.t_text)
                               when 25 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getMoney(nt.t_text), 2)
                               else
                                null
                             end noteval,
                             decode(nt.t_date, emptDate, tick.t_dealdate, nt.t_date) t_date,
                             nt.t_documentid
                        FROM DNOTETEXT_DBT NT
                       INNER JOIN DNOTEKIND_DBT NK
                          ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND
                             NT.T_NOTEKIND = NK.T_NOTEKIND)
                       inner join ddl_tick_dbt tick
                          on (nt.t_documentid = lpad(to_char(tick.t_dealid), 34, '0'))
                       WHERE NT.T_OBJECTTYPE  =  n101
                         and decode(nt.t_date, emptDate, tick.t_dealdate, nt.t_date) <= in_date
                         and decode(nt.t_date, emptDate, tick.t_dealdate, nt.t_date) >= tick.t_dealdate
                         and tick.t_clientid = n_1
                         --and exists (select 1 from ldr_infa_cb.fct_deal where code = to_char(tick.t_dealid) || '#TCK' )
                         ));
commit;                         
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке свободных атрибутов по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    insert into ldr_infa_cb.fct_overdue_securitydeal
       (select deal_code,
               deal_fiid security_code,
               '0000#IBSOXXX#' || decode(t_userfield4, chr(1), t_account, t_userfield4) account_code,
               acc_role_code,
               qb_dwh_utils.NumberToChar(d.next_day , 0),
               qb_dwh_utils.NumberToChar(abs(inrest), 2),
               qb_dwh_utils.DateToChar(t.t_restdate + d.next_day - 1) dt,
               dwhRecStatus,
               dwhSysMoment,
               dwhEXT_FILE
          from (select distinct
                       catacc.t_account,
                       --ac.t_userfield4,
                       case
                          when (ac.t_userfield4 is null) or
                              (ac.t_userfield4 = chr(0)) or
                              (ac.t_userfield4 = chr(1)) or
                              (ac.t_userfield4 like '0x%') then
                            ac.t_account
                          else
                            ac.t_userfield4
                       end t_userfield4,
                       catacc.t_catnum,
                       nvl(decode(tick.t_dealid, null, null, to_char(tick.t_dealid) || '#TCK'), decode(tick_leg.t_dealid, null, null, to_char(tick_leg.t_dealid) || '#TCK')) deal_code,
                       nvl(decode(tick.t_dealid, null, null, to_char(tick.t_pfi) || '#FIN') , decode(tick_leg.t_dealid, null, null, to_char(tick_leg.t_pfi) || '#FIN'))  deal_fiid,
                       rd.t_accountid,
                       rd.t_restcurrency ,
                       rd.t_restdate,
                       rd.t_rest inrest,
                       row_number() over(partition by rd.t_accountid order by rd.t_restdate) frow,
                        case when max(rd.t_restdate) over(partition by rd.t_accountid) = min(rd.t_restdate) over(partition by rd.t_accountid) then
                            in_date - min(rd.t_restdate) over(partition by rd.t_accountid)
                          else
                             max(rd.t_restdate) over(partition by rd.t_accountid) -
                             min(rd.t_restdate) over(partition by rd.t_accountid)
                        end cnt_days,
                       --'0000#SOFR#' || cat.t_code acc_role_code
                       cat.t_code acc_role_code
                  from dmcaccdoc_dbt catacc
                  inner join dmccateg_dbt cat
                    on (catacc.t_catid = cat.t_id)
                  left join dmctempl_dbt templ
                    on (catacc.t_catid = templ.t_catid and catacc.t_templnum = templ.t_number)
                  left join ddl_leg_dbt leg
                    on (catacc.t_docid = leg.t_id and catacc.t_dockind = n176)
                  left join ddl_tick_dbt tick_leg
                    on (tick_leg.t_dealid = leg.t_dealid)
                  left join ddl_tick_dbt tick
                    on (catacc.t_docid = tick.t_dealid and catacc.t_dockind in (select v.value
                                                                                  from qb_dwh_const4exp c
                                                                                 inner join qb_dwh_const4exp_val v
                                                                                    on (c.id = v.id)
                                                                                 where c.name = cEXP_DOCKIND))
                  inner join daccount_dbt ac
                    on (catacc.t_chapter = ac.t_chapter and catacc.t_account = ac.t_account and catacc.t_currency = ac.t_code_currency)
                  inner join drestdate_dbt rd
                    on (ac.t_accountid = rd.t_accountid and rd.t_restcurrency  = ac.t_code_currency)
                 where ((catacc.t_catnum in (701,1244)) or
                        (catacc.t_catnum in (233, 1237, 1245, 1246, 1298, 1299) and templ.t_value3 = 1) or
                        (catacc.t_catnum in (1245, 1246) and templ.t_value1 = 1)
                       )
                   and (tick.t_dealid is not null or leg.t_id is not null)
              ) t
        inner join (select level Next_day from dual
                    connect by level <= 1000) d
          on (d.next_day <= t.cnt_days )
        left join drestdate_dbt rd
           on (rd.t_accountid = t.t_accountid and rd.t_restcurrency = t.t_restcurrency and rd.t_restdate = t.t_restdate + d.next_day - 1)
        where (t.frow = 1)
          and t.inrest <> 0
        union all
        select deal_code,
               deal_fiid security_code,
               account_code,
               acc_role_code,
               qb_dwh_utils.NumberToChar(d.next_day , 0),
               qb_dwh_utils.NumberToChar(abs(inrest), 2),
               qb_dwh_utils.DateToChar(t.t_restdate + d.next_day -1) dt,
               dwhRecStatus,
               dwhSysMoment,
               dwhEXT_FILE
          from (select distinct
                        '0000#IBSOXXX#' || case
                                              when (ac.t_userfield4 is null) or
                                                  (ac.t_userfield4 = chr(0)) or
                                                  (ac.t_userfield4 = chr(1)) or
                                                  (ac.t_userfield4 like '0x%') then
                                                ac.t_account
                                              else
                                                ac.t_userfield4
                                           end account_code,
                        acc.t_catnum,
                        to_char(lnk.t_bcid) || '#BNR' deal_fiid,
                        decode(tick_ord.t_dealid, null, null, to_char(tick_ord.t_dealid) || '#TCK') deal_code,
                        rd.t_accountid,
                        rd.t_restcurrency ,
                        rd.t_restdate,
                        rd.t_rest inrest,
                        row_number() over(partition by rd.t_accountid order by rd.t_restdate) frow,
                        case when max(rd.t_restdate) over(partition by rd.t_accountid) = min(rd.t_restdate) over(partition by rd.t_accountid) then
                            in_date - min(rd.t_restdate) over(partition by rd.t_accountid)
                          else
                             max(rd.t_restdate) over(partition by rd.t_accountid) -
                             min(rd.t_restdate) over(partition by rd.t_accountid)
                        end cnt_days,
                        --'0000#SOFR#' || cat.t_code acc_role_code
                        cat.t_code acc_role_code
                   from dmcaccdoc_dbt acc
                  inner join dmccateg_dbt cat
                     on (acc.t_catid = cat.t_id)
                   left join dmctempl_dbt templ
                     on (acc.t_catid = templ.t_catid and acc.t_templnum = templ.t_number)
                   left join dvsordlnk_dbt lnk
                     on (acc.t_docid = lnk.t_bcid and lnk.t_linkkind = n0 and lnk.t_dockind in (select v.value
                                                                                                  from qb_dwh_const4exp c
                                                                                                 inner join qb_dwh_const4exp_val v
                                                                                                    on (c.id = v.id)
                                                                                                 where c.name = cDEALSKIND_DBILL_2))
                   left join ddl_tick_dbt tick_ord
                     on (lnk.t_contractid = tick_ord.t_dealid and lnk.t_dockind = tick_ord.t_bofficekind)
                  inner join daccount_dbt ac
                     on (acc.t_chapter = ac.t_chapter and acc.t_account = ac.t_account and acc.t_currency = ac.t_code_currency)
                  inner join drestdate_dbt rd
                     on (ac.t_accountid = rd.t_accountid and rd.t_restcurrency = ac.t_code_currency)
                  where acc.t_dockind = n164
                    and (acc.t_catnum in (select v.value
                                            from qb_dwh_const4exp c
                                           inner join qb_dwh_const4exp_val v
                                              on (c.id = v.id)
                                           where c.name = cEXP_CAT_TMP2) or
                        (acc.t_catnum = n462 and templ.t_value4 in (select v.value
                                                                      from qb_dwh_const4exp c
                                                                     inner join qb_dwh_const4exp_val v
                                                                        on (c.id = v.id)
                                                                     where c.name = cEXP_CAT462_TEMPL)))
                    and tick_ord.t_dealid is not null
              ) t
        inner join (select level Next_day from dual
                    connect by level <= 1000) d
          on (d.next_day <= t.cnt_days )
        left join drestdate_dbt rd
           on (rd.t_accountid = t.t_accountid and rd.t_restcurrency = t.t_restcurrency and rd.t_restdate = t.t_restdate + d.next_day - 1)
        where (t.frow = n1)
          and t.inrest <> n0);
commit;          
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке просрочки по цб: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    insert into ldr_infa_cb.fct_dealrisk
       (select t.t_insmethod GROUND,
               t.t_reservepercent RESERVE_RATE,
               to_char(d.t_dealid) || '#TCK' DEAL_CODE,
               case
                 when t.t_reservepercent < 1 then
                   '9999#SOFRXXX#1'
                 when t.t_reservepercent < 21 then
                   '9999#SOFRXXX#2'
                 when t.t_reservepercent < 51 then
                   '9999#SOFRXXX#3'
                 when t.t_reservepercent < 100 then
                   '9999#SOFRXXX#4'
                 else
                   '9999#SOFRXXX#5'
               end RISKCAT_CODE,
               to_date('01011980', 'ddmmyyyy') DT,
               '0' REC_STATUS,
               dwhSYSMOMENT,
               dwhEXT_FILE,
               --из справочника det_typerisk: CODE_TYPERISK=254i, NAME_TYPERISK="Группы риска, классиф.элементы расчетной базы резерва"
               --группа риска = категория качества, а запись в dmm_qcateg_dbt означает наличие этого реквизита у сделки 
               cCODE_TYPERISK RISKCAT_CODE_TYPERISK 
          from dmm_qcateg_dbt t
          inner join ddl_tick_dbt d --on d.t_department = in_Department
             on t.t_dealid = d.t_dealid
          where exists (select 1
                          from ldr_infa_cb.fct_deal fd
                         where fd.code = to_char(t.t_dealid) || '#TCK')
        );
commit;        
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке групп риска по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;
    insert into ldr_infa_cb.fct_dealrisk
      (ground,
       reserve_rate,
       deal_code,
       riskcat_code,
       dt,
       rec_status,
       sysmoment,
       ext_file,
       RISKCAT_CODE_TYPERISK)
      select null ground,
             (select qb_dwh_utils.numbertochar(rsb_struct.getdouble(nt.t_text))
                from dnotetext_dbt nt
               where nt.t_objecttype = 101
                 and nt.t_notekind = case
                       when at.t_groupid = 13 then
                        3
                       when at.t_groupid = 14 then
                        6
                       when at.t_groupid = 15 then
                        8
                     end
                 and nt.t_documentid = at.t_object) reserve_rate,
             to_char(to_number(at.t_object)) || '#TCK' deal_code,
             case
               when atr.t_nameobject = '1' then
                '9999#SOFRXXX#1'
               when atr.t_nameobject = '2' then
                '9999#SOFRXXX#2'
               when atr.t_nameobject = '3' then
                '9999#SOFRXXX#3'
               when atr.t_nameobject = '4' then
                '9999#SOFRXXX#4'
               else
                '9999#SOFRXXX#5'
             end riskcat_code,
             qb_dwh_utils.datetochar(at.t_validfromdate) dt,
             '0' rec_status,
             dwhSYSMOMENT,
             dwhEXT_FILE,
             cCODE_TYPERISK RISKCAT_CODE_TYPERISK --в данном отборе только категории качества (группы риска)
        from dobjatcor_dbt at
       inner join dobjattr_dbt atr
          on (at.t_objecttype = atr.t_objecttype and at.t_groupid = atr.t_groupid and
             at.t_attrid = atr.t_attrid)
       where at.t_objecttype = 101
         and at.t_groupid in (13, 14, 15)
         and in_date between at.t_validfromdate and at.t_validtodate;
commit;
    -- Удаление атрибутов у которых дата установки атрибута меньше даты открытия ц/б
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Удаление лишних атрибутов',
                   2,
                   null,
                   null);
    delete from ldr_infa_cb.fct_security_attr atr
     where exists
     (select 1
        from ldr_infa_cb.det_security sec
       where sec.code = atr.security_code
         and to_date(sec.dt, 'dd-mm-yyyy') > to_date(atr.dt, 'dd-mm-yyyy'));
commit;
    delete from ldr_infa_cb.ass_deal_cat_val where not exists (select 1 from ldr_infa_cb.fct_deal where code = deal_code ); commit;
    delete from ldr_infa_cb.fct_deal_indicator where not exists (select 1 from ldr_infa_cb.fct_deal where code = deal_code ); commit;
    -- Очистка дублей
    delete from ldr_infa_cb.det_roleaccount_deal
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by code,  name order by code) rn
                               from ldr_infa_cb.det_roleaccount_deal
                             )
                      where rn > 1);
commit;                      
    delete from ldr_infa_cb.FCT_SEC_SELL_RESULT
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by lot_num, deal_code, security_code, dt  order by dt) rn
                               from ldr_infa_cb.FCT_SEC_SELL_RESULT
                             )
                      where rn > 1);
commit;                      
    delete from ldr_infa_cb.fct_security_attr
     where rowid in (select rowid
                       from (select rowid,
                                    code_security_attr,
                                    row_number() over (partition by security_code, code_security_attr, dt  order by dt) rn,
                                    row_number() over (partition by security_code, code_security_attr, date_value, dt  order by dt) rn_do
                               from ldr_infa_cb.fct_security_attr 
                             )
                      where (code_security_attr = 'DATE_OFFER' and rn_do > 1)
                          or (code_security_attr != 'DATE_OFFER' and rn > 1));
commit;                      
    delete from ldr_infa_cb.fct_security_attr_multi
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by security_code, sec_portfolio_code, code_security_attr, value, dt  order by dt) rn
                               from ldr_infa_cb.fct_security_attr_multi
                             )
                      where rn > 1);
commit;                      
    delete from ldr_infa_cb.det_deal_cat_val
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by deal_cat_code, code_deal_cat_val, dt  order by dt) rn
                               from ldr_infa_cb.det_deal_cat_val
                             )
                      where rn > 1);
commit;                      
    delete from ldr_infa_cb.ass_accountdeal
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by account_code, deal_code, roleaccount_deal_code, dt  order by dt) rn
                               from ldr_infa_cb.ass_accountdeal
                             )
                      where rn > 1);
commit;

    -- Установка типа доходности для СВ
    update ldr_infa_cb.det_bill
       set typeprofit = '1'
     where security_code in
           (select security_code
              from (select db.security_code,
                           round(to_number(ds.nominal,
                                           '99999999999999999999.999999999999'),
                                 2) nom_cost,
                           round(deals.bccost, 2) sale_cost,
                           ds.finstrcurnom_finstr_code nom_fi,
                           deals.ficode sale_fi
                      from ldr_infa_cb.det_bill db
                     inner join ldr_infa_cb.det_security ds
                        on (db.security_code = ds.code)
                     inner join (select bccost,
                                       bcfi,
                                       bcid,
                                       fi.t_iso_number ficode
                                  from (select bck.t_bcid bcid,
                                               lnk.t_bccost bccost,
                                               lnk.t_bccfi bcfi,
                                               row_number() over(partition by bck.t_bcid order by bck.t_id desc) rnk
                                          from dvsbnrbck_dbt bck,
                                               doprdocs_dbt  docs,
                                               dvsordlnk_dbt lnk,
                                               doproper_dbt  oper
                                         where lnk.t_dockind = oper.t_dockind
                                           and lpad(lnk.t_contractid, 10, '0') =
                                               oper.t_documentid
                                           and oper.t_dockind in (select v.value
                                                                    from qb_dwh_const4exp c
                                                                   inner join qb_dwh_const4exp_val v
                                                                      on (c.id = v.id)
                                                                   where c.name = cOPER_DOCKIND)
                                           and oper.t_id_operation =
                                               docs.t_id_operation
                                           and docs.t_dockind = n191
                                           and docs.t_documentid =
                                               lpad(bck.t_id, 10, '0')
                                           and bck.t_bcstatus = chr88
                                           and lnk.t_bcid = bck.t_bcid
                                           and bck.t_newabcstatus = n20)
                                 inner join dfininstr_dbt fi
                                    on (bcfi = fi.t_fiid)
                                 where rnk = 1) deals
                        on (to_number(regexp_replace(db.security_code, '#BNR$')) =
                           deals.bcid)
                       inner join dvsbanner_dbt bn
                         on (to_number(regexp_replace(db.security_code, '#BNR$')) =
                           bn.t_bcid)
                     where db.typeprofit = v2
                       and db.discount is null
                       and exists (select 1 from ddp_dep_dbt dp where dp.t_partyid = bn.t_issuer ))
             where nom_cost = sale_cost
               and nom_fi = sale_fi);
commit;
    -- Установка типа доходности для УВ
    update ldr_infa_cb.det_bill
       set typeprofit = '1'
     where security_code in
           (select security_code
              from (select db.security_code,
                           round(to_number(ds.nominal,
                                           '99999999999999999999.999999999999'),
                                 2) nom_cost,
                           round(deals.buy_cost, 2) buy_cost,
                           ds.finstrcurnom_finstr_code nom_fi,
                           deals.ficode buy_fi
                      from ldr_infa_cb.det_bill db
                     inner join ldr_infa_cb.det_security ds
                        on (db.security_code = ds.code)
                     left join (select buy_cost,
                                       buy_fi,
                                       bcid,
                                       fi.t_iso_number ficode
                                  from (select bn.t_bcid bcid,
                                                leg.t_principal buy_cost,
                                                leg.t_cfi buy_fi,
                                                row_number() over (partition by bn.t_bcid order by tick.t_dealid desc) rnk
                                          from  dvsbanner_dbt bn
                                          inner join ddl_leg_dbt leg
                                            on (bn.t_bcid = leg.t_dealid)
                                          inner join dvsordlnk_dbt lnk
                                            on (bn.t_bcid = lnk.t_bcid)
                                          inner join ddl_tick_dbt tick
                                            on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                                          inner join dfininstr_dbt fi
                                            on (leg.t_cfi = fi.t_fiid)
                                          where leg.t_legid = n0 and leg.t_legkind = n1
                                            and tick.t_bofficekind in (select v.value
                                                                         from qb_dwh_const4exp c
                                                                        inner join qb_dwh_const4exp_val v
                                                                           on (c.id = v.id)
                                                                        where c.name = cDEALSKIND_DBILL)
                                            and tick.t_dealtype = n12401
                                            and tick.t_dealdate <= in_date
                                           )
                                 inner join dfininstr_dbt fi
                                    on (buy_fi = fi.t_fiid)
                                 where rnk = n1) deals
                        on (to_number(regexp_replace(db.security_code, '#BNR$')) =
                           deals.bcid)
                       inner join dvsbanner_dbt bn
                         on (to_number(regexp_replace(db.security_code, '#BNR$')) =
                           bn.t_bcid)
                     where db.typeprofit = v2
                       and db.discount is null
                       and not exists (select 1 from ddp_dep_dbt dp where dp.t_partyid = bn.t_issuer )
                       )
             where nom_cost = buy_cost
               and nom_fi = buy_fi);
commit;

    --Завершим выгрузку сделок
    qb_bp_utils.EndEvent(EventID, null);
    --commit;
  end;

  procedure export_Deals_9996(in_department in number,
                         in_date       in date,
                         procid        in number) is
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
    cnt          pls_integer;
    sec_basket_rec sec_basket;
    totalcnt number(30);

  begin
    -- Установим начало выгрузки сделок
    startevent(cEvent_EXPORT_Deals, procid, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             QB_DWH_EXPORT.cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDT, in_date);

    qb_dwh_export.InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка данных по сделкам',
                         2,
                         null,
                         null);
    -- сделки с собственными векселями

    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с СВ',
                   2,
                   null,
                   null);
    for rec in (select to_char(ord.t_contractid) || '#ORD' code,
                       ord.t_contractid,
                       ord.t_dockind,
                       dp.t_name department_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             decode( ord.t_contractor, -1, 1, ord.t_contractor)) subject_code,

                       case when ord.t_dockind = 109 then
                              '13'
                            when ord.t_dockind = 110 then
                              '27'
                            when ord.t_dockind = 112 then
                              '13'
                            else
                              '-1'
                       end dealtype,
                       ord.t_ordernumber docnum,
                       '0' is_interior,
                       leg.t_start begindate,
                       leg.t_maturity enddate,
                       null note,
                       ord.t_signdate dt,
                       ord.t_createdate,
                       case when ord.t_dockind = 109 then -- выпуск
                              '3'
                            when ord.t_dockind = 113 then -- новация
                              '6'
                            when ord.t_dockind = 112 then -- !!!!!!!!!!!!!!!!! задал вопрос
                              '10'
                            when ord.t_dockind = 110 then -- гашение
                              '4'
                       end typesecdeal,
                       leg.t_price sec_price,
                       leg.t_scale scale,
                       leg.t_principal principal,
                       leg.t_totalcost totalcost,
                       to_char(bn.t_bcid) || '#BNR' finstr_code,
                       fi_cfi.t_ccy currency_finstr_code,
                       fi_cfi.t_iso_number currency_num_code,
                       row_number() over (partition by ord.t_contractid order by leg.t_id) rnk,
                       count(*) over (partition by ord.t_contractid) cnt
                  from dvsbanner_dbt bn
                  inner join ddl_leg_dbt leg
                    on (bn.t_bcid = leg.t_dealid)
                  inner join dvsordlnk_dbt lnk
                    on (bn.t_bcid = lnk.t_bcid)
                 inner join ddl_order_dbt ord
                    on (lnk.t_contractid = ord.t_contractid and lnk.t_dockind = ord.t_dockind)
                 inner join ddp_dep_dbt dp
                    on (ord.t_department = dp.t_code)
                 left join dfininstr_dbt fi_cfi
                    on (leg.t_cfi = fi_cfi.t_fiid)
                 where leg.t_legid = n0 and leg.t_legkind = n1
                   and ord.t_signdate <= in_date)
    loop
      begin
      if (rec.rnk = 1) then
        insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
               values (rec.code, rec.department_code, rec.subject_code, rec.dealtype, rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      end if;
      if (rec.dealtype = '13') then
        if (rec.cnt > 1) then
          if (rec.rnk = 1) then
            insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
                   values (rec.typesecdeal, '-1', null, '-1', '-1', null, rec.currency_finstr_code, '-1', null, rec.code, '-1', '-1', '-1', dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          insert into ldr_infa_cb.fct_secdeal_finstr(sec_price, sec_proc, rate, scale, couponyield, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, deal_amount, dt, rec_status, sysmoment, ext_file)
                 values (qb_dwh_utils.NumberToChar(rec.sec_price), null, qb_dwh_utils.NumberToChar(Round(rec.sec_price, 14), 14), qb_dwh_utils.NumberToChar(round(rec.scale, 0), 0), null, qb_dwh_utils.NumberToChar(1, 0), rec.code, rec.finstr_code, rec.finstr_code, qb_dwh_utils.NumberToChar(rec.cnt, 0), qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
          insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
                 values (rec.typesecdeal, qb_dwh_utils.NumberToChar(rec.sec_price), null, qb_dwh_utils.NumberToChar(Round(rec.sec_price, 14), 14), qb_dwh_utils.NumberToChar(round(rec.scale, 0), 0), null, rec.currency_finstr_code, qb_dwh_utils.NumberToChar(round(rec.principal, 3), 3), qb_dwh_utils.NumberToChar(1, 0), rec.code, rec.finstr_code, rec.finstr_code, '-1', dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        end if;
      elsif rec.dealtype = '27' then
        if (rec.cnt > 1) then
          if (rec.rnk = 1) then
            insert into ldr_infa_cb.fct_repaydeal(typerepay, typeowner, security_code, currency_finstr_code, coupon_number, nominal_proc, amount, value, deal_code, rec_status, sysmoment, ext_file)
                   values('3', '1', '-1', '-1', null, null, '-1', '-1', rec.code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          insert into ldr_infa_cb.fct_secdeal_finstr(sec_price, sec_proc, rate, scale, couponyield, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, deal_amount, dt, rec_status, sysmoment, ext_file)
                 values ('3', null, '1', '-1', qb_dwh_utils.NumberToChar(round(rec.principal, 2), 2), '1', rec.code, rec.finstr_code, rec.currency_num_code, qb_dwh_utils.NumberToChar(rec.cnt, 0), qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
          insert into ldr_infa_cb.fct_repaydeal(typerepay, typeowner, security_code, currency_finstr_code, coupon_number, nominal_proc, amount, value, deal_code, rec_status, sysmoment, ext_file)
                 values('3', '1', rec.finstr_code, rec.currency_finstr_code, null, null, '1', qb_dwh_utils.NumberToChar(round(rec.principal, 2), 2), rec.code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        end if;
      end if;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке данных по сделке с СВ: ' || SQLERRM,
                               0,
                               cDeal,
                               rec.t_contractid);
      end;

      -- Добавление счетов по сделкам с собственными векселями
      for dacc_rec in (select distinct *
                          from (with dacnt as (select acctrn.t_account_payer,
                                                      --acc_p.t_userfield4 uf4_p,
                                                      case
                                                        when (acc_p.t_userfield4 is null) or
                                                            (acc_p.t_userfield4 = chr(0)) or
                                                            (acc_p.t_userfield4 = chr(1)) or
                                                            (acc_p.t_userfield4 like '0x%') then
                                                          acc_p.t_account
                                                        else
                                                          acc_p.t_userfield4
                                                      end uf4_p,
                                                      acctrn.t_account_receiver,
                                                      --acc_r.t_userfield4 uf4_r,
                                                      case
                                                        when (acc_r.t_userfield4 is null) or
                                                            (acc_r.t_userfield4 = chr(0)) or
                                                            (acc_r.t_userfield4 = chr(1)) or
                                                            (acc_r.t_userfield4 like '0x%') then
                                                          acc_r.t_account
                                                        else
                                                          acc_r.t_userfield4
                                                      end uf4_r,
                                                      acctrn.t_department
                                                 from dacctrn_dbt acctrn,
                                                      (select oprdocs.t_acctrnid as acctrnid
                                                         from doproper_dbt opr,
                                                              doprdocs_dbt oprdocs
                                                        where opr.t_dockind = rec.t_dockind
                                                          and opr.t_documentid =
                                                              lpad(to_char(rec.t_contractid), 10, 0)
                                                          and oprdocs.t_id_operation = opr.t_id_operation
                                                          and oprdocs.t_dockind = 1) q,
                                                      dfininstr_dbt pfi,
                                                      dfininstr_dbt rfi,
                                                      daccount_dbt acc_p,
                                                      daccount_dbt acc_r
                                                where acctrn.t_acctrnid = q.acctrnid
                                                  and acctrn.t_accountid_payer =  acc_p.t_accountid
                                                  and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                  and pfi.t_fiid = acctrn.t_fiid_payer
                                                  and rfi.t_fiid = acctrn.t_fiid_receiver
                                                  and acctrn.t_state = 1
                                                  and acctrn.t_chapter in (select v.value
                                                                             from qb_dwh_const4exp c
                                                                            inner join qb_dwh_const4exp_val v
                                                                               on (c.id = v.id)
                                                                            where c.name = cACC_CHAPTERS)
                                                  and pfi.t_fi_kind = 1
                                                  and rfi.t_fi_kind = 1)
                                 select distinct dp.t_name || '#IBSOXXX#' || dacnt.uf4_p acc,
                                                 --'XXXX#SOFR#' ||
                                                 nvl(cat_pd.t_code, cat_po.t_code) cat_code,
                                                 nvl(acd_pd.t_catid, acd_po.t_catid) cat_id,
                                                 nvl(cat_pd.t_name, cat_po.t_name) cat_name,
                                                 nvl(acd_pd.t_activatedate, acd_po.t_activatedate) cat_date,
                                                 case when acd_pd.t_activatedate is not null then acd_pd.t_disablingdate else acd_po.t_disablingdate end cat_enddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                 dacnt.t_department
                                   from dacnt
                                  inner join ddp_dep_dbt dp -- филиал счета
                                     on (dacnt.t_department = dp.t_code)
                                   left join dmcaccdoc_dbt acd_pd -- счет плательщика по сделке
                                     on (dacnt.t_account_payer = acd_pd.t_account and
                                        acd_pd.t_dockind = rec.t_dockind and
                                        acd_pd.t_docid = rec.t_contractid)
                                   left join dmccateg_dbt cat_pd -- категория по счету плательщика по сделке
                                     on (acd_pd.t_catid = cat_pd.t_id)
                                   left join dmcaccdoc_dbt acd_po -- счет плательщика общесистемный
                                     on (dacnt.t_account_payer = acd_po.t_account and
                                        acd_po.t_iscommon = chr88)
                                   left join dmccateg_dbt cat_po -- категория по общесистемному счету плтательщика
                                     on (acd_po.t_catid = cat_po.t_id)
                                 union all
                                 select *
                                   from (with dacnt as (select acctrn.t_account_payer,
                                                               --acc_p.t_userfield4 uf4_p,
                                                               case
                                                                  when (acc_p.t_userfield4 is null) or
                                                                      (acc_p.t_userfield4 = chr(0)) or
                                                                      (acc_p.t_userfield4 = chr(1)) or
                                                                      (acc_p.t_userfield4 like '0x%') then
                                                                    acc_p.t_account
                                                                  else
                                                                    acc_p.t_userfield4
                                                               end uf4_p,
                                                               acctrn.t_account_receiver,
                                                               --acc_r.t_userfield4 uf4_r,
                                                               case
                                                                  when (acc_r.t_userfield4 is null) or
                                                                      (acc_r.t_userfield4 = chr(0)) or
                                                                      (acc_r.t_userfield4 = chr(1)) or
                                                                      (acc_r.t_userfield4 like '0x%') then
                                                                    acc_r.t_account
                                                                  else
                                                                    acc_r.t_userfield4
                                                               end uf4_r,
                                                               acctrn.t_department
                                                          from dacctrn_dbt acctrn,
                                                               (select oprdocs.t_acctrnid as acctrnid
                                                                  from doproper_dbt opr,
                                                                       doprdocs_dbt oprdocs
                                                                 where opr.t_dockind = rec.t_dockind
                                                                   and opr.t_documentid =
                                                                       lpad(to_char(rec.t_contractid), 10, 0)
                                                                   and oprdocs.t_id_operation =
                                                                       opr.t_id_operation
                                                                   and oprdocs.t_dockind = n1) q,
                                                               dfininstr_dbt pfi,
                                                               dfininstr_dbt rfi,
                                                               daccount_dbt acc_p,
                                                               daccount_dbt acc_r
                                                         where acctrn.t_acctrnid = q.acctrnid
                                                           and acctrn.t_accountid_payer =  acc_p.t_accountid
                                                           and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                           and pfi.t_fiid = acctrn.t_fiid_payer
                                                           and rfi.t_fiid = acctrn.t_fiid_receiver
                                                           and acctrn.t_state = n1
                                                           and acctrn.t_chapter in (select v.value
                                                                                      from qb_dwh_const4exp c
                                                                                     inner join qb_dwh_const4exp_val v
                                                                                        on (c.id = v.id)
                                                                                     where c.name = cACC_CHAPTERS)
                                                           and pfi.t_fi_kind = n1
                                                           and rfi.t_fi_kind = n1)
                                          select distinct dp.t_name || '#IBSOXXX#' || dacnt.uf4_r,
                                                          --'XXXX#SOFR#' ||
                                                          nvl(cat_rd.t_code, cat_ro.t_code),
                                                          nvl(acd_rd.t_catid, acd_ro.t_catid) catid_receiver,
                                                          nvl(cat_rd.t_name, cat_ro.t_name) cat_name,
                                                          nvl(acd_rd.t_activatedate, acd_ro.t_activatedate) catdate,
                                                          case when acd_rd.t_activatedate is not null then acd_rd.t_disablingdate else acd_ro.t_disablingdate end catenddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                          dacnt.t_department
                                            from dacnt
                                           inner join ddp_dep_dbt dp
                                              on (dacnt.t_department = dp.t_code)
                                            left join dmcaccdoc_dbt acd_rd
                                              on (dacnt.t_account_receiver = acd_rd.t_account and
                                                 acd_rd.t_dockind = rec.t_dockind and
                                                 acd_rd.t_docid = rec.t_contractid)
                                            left join dmccateg_dbt cat_rd
                                              on (acd_rd.t_catid = cat_rd.t_id)
                                            left join dmcaccdoc_dbt acd_ro
                                              on (dacnt.t_account_receiver = acd_ro.t_account and
                                                 acd_ro.t_iscommon = chr88)
                                            left join dmccateg_dbt cat_ro
                                              on (acd_ro.t_catid = cat_ro.t_id)
                                           ))
                        )
      loop
        -- Вставка в ass_accountdeal
        begin
        if (dacc_rec.cat_id is not null) then
          begin
            insert into ldr_infa_cb.ass_accountdeal(account_code, deal_code, roleaccount_deal_code, dt, rec_status, sysmoment, ext_file, dt_end) -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                   values(dacc_rec.acc, rec.code, dacc_rec.cat_code, qb_dwh_utils.DateToChar(dacc_rec.cat_date), dwhRecStatus, dwhSysMoment, dwhEXT_FILE, case when dacc_rec.cat_enddate = to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END) else qb_dwh_utils.DateToChar(dacc_rec.cat_enddate-1) end);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
          begin
            insert into ldr_infa_cb.det_roleaccount_deal(code, name, orole_code, dt, rec_status, sysmoment, ext_file)
                   values (dacc_rec.cat_code, dacc_rec.cat_name, '0', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
        end if;
        exception
          when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке счета по сделке с СВ: ' || SQLERRM,
                                 0,
                                 cDeal,
                                 rec.t_contractid);
        end;
      end loop;
      --commit;
    end loop;

    -- сделки с учтенными векселями
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с УВ',
                   2,
                   null,
                   null);



    for rec in (select to_char(tick.t_dealid) || '#TCK' code,
                       '0000' department_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             decode(tick.t_partyid, -1, 1, tick.t_partyid)) subject_code,

                       case when tick.t_bofficekind = 141 then
                              '13'
                            when tick.t_bofficekind = 142 then
                              '27'
                            when tick.t_bofficekind = 143 then
                              '13'
                            else
                              '-1'
                       end dealtype,
                       case when tick.t_bofficekind = 141 then
                              case when tick.t_dealtype = 12401 then
                                     '1'
                                   when tick.t_dealtype = 12411 then
                                     '2'
                                   when tick.t_dealtype = 12431 then
                                     '5'
                                   else
                                     '0'
                              end
                            when tick.t_bofficekind = 142 then
                              '4'
                            when tick.t_bofficekind = 143 then
                              '10' -- залог
                       end typesecdeal,
                       tick.t_dealcode docnum,
                       '0' is_interior,
                       decode(leg.t_start, emptDate, tick.t_dealdate, leg.t_start) begindate,
                       decode(leg.t_maturity, emptDate, tick.t_dealdate, leg.t_maturity) enddate,
                       tick.t_comment note,
                       tick.t_dealdate dt,
                       leg.t_id legid,
                       tick.t_dealid dealid,
                       tick.t_bofficekind bofficekind,
                       tick.t_dealdate,
                       leg.t_price sec_price,
                       leg.t_totalcost totalcost,
                       leg.t_principal principal,
                       null sec_proc,
                       leg.t_totalcost rate,
                       1 scale,
                       null couponyield,
                       leg.t_principal deal_amount,
                       1 amount,
                       case when tick.t_bofficekind = 141 and tick.t_dealtype = 12411 then
                         fi.t_iso_number
                       else
                         to_char(bn.t_bcid) || '#BNR'
                       end finstrbuy_finstr_code,
                       case when tick.t_bofficekind = 141 and tick.t_dealtype = 12401 then
                         fi.t_iso_number
                       else
                         to_char(bn.t_bcid) || '#BNR'
                       end finstrsel_finstr_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             tick.t_marketid) exchange_code,
                       to_char(bn.t_bcid) || '#BNR' finstr_code,
                       fi.t_ccy currency_finstr_code,
                       fi.t_iso_number currency_num_code,
                       row_number() over (partition by tick.t_dealid order by leg.t_id) rnk,
                       count(*) over (partition by tick.t_dealid) cnt
                  from dvsbanner_dbt bn
                  inner join ddl_leg_dbt leg
                    on (bn.t_bcid = leg.t_dealid)
                  inner join dvsordlnk_dbt lnk
                    on (bn.t_bcid = lnk.t_bcid)
                  inner join ddl_tick_dbt tick
                    on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                  inner join dfininstr_dbt fi
                    on (leg.t_cfi = fi.t_fiid)
                  where leg.t_legid = n0 and leg.t_legkind = n1
                    and tick.t_bofficekind in (select v.value
                                                 from qb_dwh_const4exp c
                                                inner join qb_dwh_const4exp_val v
                                                   on (c.id = v.id)
                                                where c.name = cDEALSKIND_DBILL)
                    and tick.t_dealdate <= in_date)
    loop
      begin
      if (rec.rnk = 1) then
        insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
               values (rec.code, rec.department_code, rec.subject_code, rec.dealtype, rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;               
      end if;
      if rec.dealtype = '13' then
        if(rec.cnt > 1) then
          if (rec.rnk = 1) then
            insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
                   values (rec.typesecdeal, '-1', null, '-1', '-1', null, rec.currency_finstr_code, '-1', '-1', rec.code, '-1', '-1', rec.exchange_code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          insert into ldr_infa_cb.fct_secdeal_finstr(sec_price, sec_proc, rate, scale, couponyield, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, deal_amount, dt, rec_status, sysmoment, ext_file)
                 values (qb_dwh_utils.NumberToChar(rec.sec_price), qb_dwh_utils.NumberToChar(rec.sec_proc), qb_dwh_utils.NumberToChar(Round(rec.rate, 14), 14), qb_dwh_utils.NumberToChar(round(rec.scale, 0), 0), null, qb_dwh_utils.NumberToChar(round(rec.amount,0), 0), rec.code, rec.finstrbuy_finstr_code, rec.finstrsel_finstr_code, qb_dwh_utils.NumberToChar(rec.cnt, 0), qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
          insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
                   values (rec.typesecdeal, qb_dwh_utils.NumberToChar(rec.sec_price), qb_dwh_utils.NumberToChar(rec.sec_proc), qb_dwh_utils.NumberToChar(Round(rec.rate, 14), 14), qb_dwh_utils.NumberToChar(round(rec.scale, 0), 0), null, rec.currency_finstr_code, qb_dwh_utils.NumberToChar(round(rec.deal_amount, 3), 3), qb_dwh_utils.NumberToChar(round(rec.amount,0), 0), rec.code, rec.finstrbuy_finstr_code, rec.finstrsel_finstr_code, rec.exchange_code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
        end if;
      elsif rec.dealtype = '27' then
        if (rec.cnt > 1) then
          if (rec.rnk = 1) then
            insert into ldr_infa_cb.fct_repaydeal(typerepay, typeowner, security_code, currency_finstr_code, coupon_number, nominal_proc, amount, value, deal_code, rec_status, sysmoment, ext_file)
                   values('3', '1', '-1', '-1', null, null, '-1', '-1', rec.code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          end if;
          insert into ldr_infa_cb.fct_secdeal_finstr(sec_price, sec_proc, rate, scale, couponyield, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, deal_amount, dt, rec_status, sysmoment, ext_file)
                 values ('3', null, '1', '-1', qb_dwh_utils.NumberToChar(round(rec.totalcost, 2), 2), qb_dwh_utils.NumberToChar(round(rec.principal, 0), 0), rec.code, rec.finstr_code, rec.currency_num_code, qb_dwh_utils.NumberToChar(rec.cnt, 0), qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
          insert into ldr_infa_cb.fct_repaydeal(typerepay, typeowner, security_code, currency_finstr_code, coupon_number, nominal_proc, amount, value, deal_code, rec_status, sysmoment, ext_file)
                values('3', '1', rec.finstr_code, rec.currency_finstr_code, null, null, qb_dwh_utils.NumberToChar(round(rec.principal, 0), 0), qb_dwh_utils.NumberToChar(round(rec.totalcost, 2), 2), rec.code, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                
        end if;
      end if;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке данных по сделке с УВ: ' || SQLERRM,
                               0,
                               cDeal,
                               rec.dealid);
      end;

      -- Добавление счетов по сделкам с УВ
      for dacc_rec in (select distinct *
                          from (with dacnt as (select acctrn.t_account_payer,
                                                      --acc_p.t_userfield4 uf4_p,
                                                      case
                                                        when (acc_p.t_userfield4 is null) or
                                                            (acc_p.t_userfield4 = chr(0)) or
                                                            (acc_p.t_userfield4 = chr(1)) or
                                                            (acc_p.t_userfield4 like '0x%') then
                                                          acc_p.t_account
                                                        else
                                                          acc_p.t_userfield4
                                                      end uf4_p,
                                                      acctrn.t_account_receiver,
                                                      --acc_r.t_userfield4 uf4_r,
                                                      case
                                                        when (acc_r.t_userfield4 is null) or
                                                            (acc_r.t_userfield4 = chr(0)) or
                                                            (acc_r.t_userfield4 = chr(1)) or
                                                            (acc_r.t_userfield4 like '0x%') then
                                                          acc_r.t_account
                                                        else
                                                          acc_r.t_userfield4
                                                      end uf4_r,
                                                      acctrn.t_department
                                                 from dacctrn_dbt acctrn,
                                                      (select oprdocs.t_acctrnid as acctrnid
                                                         from doproper_dbt opr,
                                                              doprdocs_dbt oprdocs
                                                        where opr.t_dockind = rec.bofficekind
                                                          and opr.t_documentid =
                                                              lpad(to_char(rec.dealid), 34, 0)
                                                          and oprdocs.t_id_operation = opr.t_id_operation
                                                          and oprdocs.t_dockind = 1) q,
                                                      dfininstr_dbt pfi,
                                                      dfininstr_dbt rfi,
                                                      daccount_dbt acc_p,
                                                      daccount_dbt acc_r
                                                where acctrn.t_acctrnid = q.acctrnid
                                                  and acctrn.t_accountid_payer =  acc_p.t_accountid
                                                  and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                  and pfi.t_fiid = acctrn.t_fiid_payer
                                                  and rfi.t_fiid = acctrn.t_fiid_receiver
                                                  and acctrn.t_state = 1
                                                  and acctrn.t_chapter in (select v.value
                                                                             from qb_dwh_const4exp c
                                                                            inner join qb_dwh_const4exp_val v
                                                                               on (c.id = v.id)
                                                                            where c.name = cACC_CHAPTERS)
                                                  and pfi.t_fi_kind = 1
                                                  and rfi.t_fi_kind = 1)
                                 select distinct dp.t_name || '#IBSOXXX#' || dacnt.uf4_p acc,
                                                 --'XXXX#SOFR#' ||
                                                 nvl(cat_pd.t_code, cat_po.t_code) cat_code,
                                                 nvl(acd_pd.t_catid, acd_po.t_catid) cat_id,
                                                 nvl(cat_pd.t_name, cat_po.t_name) cat_name,
                                                 nvl(acd_pd.t_activatedate, acd_po.t_activatedate) cat_date,
                                                 case when acd_pd.t_activatedate is not null then acd_pd.t_disablingdate else acd_po.t_disablingdate end cat_enddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                 dacnt.t_department
                                   from dacnt
                                  inner join ddp_dep_dbt dp -- филиал счета
                                     on (dacnt.t_department = dp.t_code)
                                   left join dmcaccdoc_dbt acd_pd -- счет плательщика по сделке
                                     on (dacnt.t_account_payer = acd_pd.t_account and
                                        acd_pd.t_dockind = rec.bofficekind and
                                        acd_pd.t_docid = rec.dealid)
                                   left join dmccateg_dbt cat_pd -- категория по счету плательщика по сделке
                                     on (acd_pd.t_catid = cat_pd.t_id)
                                   left join dmcaccdoc_dbt acd_po -- счет плательщика общесистемный
                                     on (dacnt.t_account_payer = acd_po.t_account and
                                        acd_po.t_iscommon = chr88)
                                   left join dmccateg_dbt cat_po -- категория по общесистемному счету плтательщика
                                     on (acd_po.t_catid = cat_po.t_id)
                                 union all
                                 select *
                                   from (with dacnt as (select acctrn.t_account_payer,
                                                               --acc_p.t_userfield4 uf4_p,
                                                               case
                                                                  when (acc_p.t_userfield4 is null) or
                                                                      (acc_p.t_userfield4 = chr(0)) or
                                                                      (acc_p.t_userfield4 = chr(1)) or
                                                                      (acc_p.t_userfield4 like '0x%') then
                                                                    acc_p.t_account
                                                                  else
                                                                    acc_p.t_userfield4
                                                               end uf4_p,
                                                               acctrn.t_account_receiver,
                                                               --acc_r.t_userfield4 uf4_r,
                                                               case
                                                                  when (acc_r.t_userfield4 is null) or
                                                                      (acc_r.t_userfield4 = chr(0)) or
                                                                      (acc_r.t_userfield4 = chr(1)) or
                                                                      (acc_r.t_userfield4 like '0x%') then
                                                                    acc_r.t_account
                                                                  else
                                                                    acc_r.t_userfield4
                                                               end uf4_r,
                                                               acctrn.t_department
                                                          from dacctrn_dbt acctrn,
                                                               (select oprdocs.t_acctrnid as acctrnid
                                                                  from doproper_dbt opr,
                                                                       doprdocs_dbt oprdocs
                                                                 where opr.t_dockind = rec.bofficekind
                                                                   and opr.t_documentid =
                                                                       lpad(to_char(rec.dealid), 34, 0)
                                                                   and oprdocs.t_id_operation =
                                                                       opr.t_id_operation
                                                                   and oprdocs.t_dockind = n1) q,
                                                               dfininstr_dbt pfi,
                                                               dfininstr_dbt rfi,
                                                               daccount_dbt acc_p,
                                                               daccount_dbt acc_r
                                                         where acctrn.t_acctrnid = q.acctrnid
                                                           and acctrn.t_accountid_payer = acc_p.t_accountid
                                                           and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                           and pfi.t_fiid = acctrn.t_fiid_payer
                                                           and rfi.t_fiid = acctrn.t_fiid_receiver
                                                           and acctrn.t_state = n1
                                                           and acctrn.t_chapter in (select v.value
                                                                                      from qb_dwh_const4exp c
                                                                                     inner join qb_dwh_const4exp_val v
                                                                                        on (c.id = v.id)
                                                                                     where c.name = cACC_CHAPTERS)
                                                           and pfi.t_fi_kind = n1
                                                           and rfi.t_fi_kind = n1)
                                          select distinct dp.t_name || '#IBSOXXX#' || dacnt.uf4_r,
                                                          --'XXXX#SOFR#' ||
                                                          nvl(cat_rd.t_code, cat_ro.t_code),
                                                          nvl(acd_rd.t_catid, acd_ro.t_catid) catid_receiver,
                                                          nvl(cat_rd.t_name, cat_ro.t_name) cat_name,
                                                          nvl(acd_rd.t_activatedate, acd_ro.t_activatedate) catdate,
                                                          case when acd_rd.t_activatedate is not null then acd_rd.t_disablingdate else acd_ro.t_disablingdate end catenddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                          dacnt.t_department
                                            from dacnt
                                           inner join ddp_dep_dbt dp
                                              on (dacnt.t_department = dp.t_code)
                                            left join dmcaccdoc_dbt acd_rd
                                              on (dacnt.t_account_receiver = acd_rd.t_account and
                                                 acd_rd.t_dockind = rec.bofficekind and
                                                 acd_rd.t_docid = rec.dealid)
                                            left join dmccateg_dbt cat_rd
                                              on (acd_rd.t_catid = cat_rd.t_id)
                                            left join dmcaccdoc_dbt acd_ro
                                              on (dacnt.t_account_receiver = acd_ro.t_account and
                                                 acd_ro.t_iscommon = chr88)
                                            left join dmccateg_dbt cat_ro
                                              on (acd_ro.t_catid = cat_ro.t_id)
                                           ))
                        )
      loop
        begin
        -- Вставка в ass_accountdeal
        if (dacc_rec.cat_id is not null) then
          begin
            insert into ldr_infa_cb.ass_accountdeal(account_code, deal_code, roleaccount_deal_code, dt, rec_status, sysmoment, ext_file, dt_end) -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                   values(dacc_rec.acc, rec.code, dacc_rec.cat_code, qb_dwh_utils.DateToChar(dacc_rec.cat_date), dwhRecStatus, dwhSysMoment, dwhEXT_FILE, case when dacc_rec.cat_enddate = to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END) else qb_dwh_utils.DateToChar(dacc_rec.cat_enddate-1) end);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
          begin
            insert into ldr_infa_cb.det_roleaccount_deal(code, name, orole_code, dt, rec_status, sysmoment, ext_file)
                   values (dacc_rec.cat_code, dacc_rec.cat_name, '0', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
        end if;
        exception
          when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке счета по сделке с УВ: ' || SQLERRM,
                                 0,
                                 cDeal,
                                 rec.dealid);
        end;
      end loop;
      --commit;
    end loop;

    -- сделки с прочими ц/б
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с прочими ц/б',
                   2,
                   null,
                   null);

    for rec in (select to_char(tick.t_dealid) || '#TCK' code,
                       '0000' department_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             decode(tick.t_partyid, -1, 1, tick.t_partyid)) subject_code,

                       case when tick.t_bofficekind = 101 then
                              case when (leg0.t_id is not null) and (leg2.t_id is not null)  then -- есть два транша -  сделка РЕПО
                                     case when rsb_secur.IsSale(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then -- продажа с обратным выкупом (прямое РЕПО)
                                          '15'
                                         when rsb_secur.IsBuy(rsb_secur.get_OperationGroup(opr.t_SysTypes)) = 1 then -- покупка с обратной продажей (прямое РЕПО)
                                          '12'
                                         else
                                          '-1' -- эта ветка не должна работать
                                     end
                                   else
                                     '13'  -- Покупка/Продажа
                              end
                            when tick.t_bofficekind in (117, 4832) then
                              '27'
                            when tick.t_bofficekind in (127, 4831) then
                              '24'
                            when tick.t_bofficekind = 4830 then
                              '13'
                            else
                              '-1'
                       end dealtype,
                       tick.t_dealcode docnum,
                       '0' is_interior,
                       decode(leg0.t_start, emptDate, tick.t_dealdate, leg0.t_start) begindate,
                       case when nvl(tkchn.t_OldMaturity2, leg2.t_maturity) is not null then
                              decode(nvl(tkchn.t_OldMaturity2, leg2.t_maturity), emptDate, tick.t_dealdate, nvl(tkchn.t_OldMaturity2, leg2.t_maturity))
                            else
                              decode(leg0.t_maturity, emptDate, tick.t_dealdate, leg0.t_maturity)
                       end enddate,
                       tick.t_comment note,
                       tick.t_dealdate dt,
                       leg0.t_id ledid_0,
                       leg2.t_id ledid_2,
                       tick.t_dealid dealid,
                       tick.t_dealdate,
                       tick.t_bofficekind bofficekind,
                       case when (select t_partyid
                                    from ddp_dep_dbt
                                   where t_parentcode = 0
                                     and t_nodetype = 1
                                     and t_status = 2) = fi.t_issuer then
                              '0'
                             else
                              '1'
                       end is_our_cb,
                       leg0.t_incomerate incomerate,
                       leg0.t_totalcost deal_amount1,
                       leg0.t_principal amount1
                  from ddl_tick_dbt tick
                 inner join dfininstr_dbt fi
                    on (tick.t_pfi = fi.t_fiid)
                 inner join doprkoper_dbt opr
                    on (tick.t_dealtype = opr.t_kind_operation)
                  left join ddl_leg_dbt leg0
                    on (tick.t_dealid = leg0.t_dealid and leg0.t_legkind = n0)
                  left join ddl_leg_dbt leg2
                    on (tick.t_dealid = leg2.t_dealid and leg2.t_legkind = n2)
                  left join dsptkchng_dbt tkchn
                    on tkchn.t_id = (select min(s_tkchn.t_id) from dsptkchng_dbt s_tkchn where s_tkchn.t_dealid = tick.t_dealid)
                 where tick.t_dealdate <= in_date
                   and tick.t_bofficekind  in (select v.value
                                                 from qb_dwh_const4exp c
                                                inner join qb_dwh_const4exp_val v
                                                   on (c.id = v.id)
                                                where c.name = cDEALSKIND_SEC)
                   and tick.t_clientid = n_1
                   and fi.t_avoirkind in (select v.value
                                            from qb_dwh_const4exp c
                                           inner join qb_dwh_const4exp_val v
                                              on (c.id = v.id)
                                           where c.name = cSECKIND_ALL))
    loop
      begin
      insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
             values (rec.code, rec.department_code, rec.subject_code, rec.dealtype, rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      if (rec.dealtype = '15') or (rec.dealtype = '12') then -- Прямое или обратное РЕПО
        select count(*)
          into cnt
          from ddl_tick_ens_dbt t
         where t.t_dealid = rec.dealid;
        if (cnt = 0) then
          -- Сделка с одной ц/б
          insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
                 values (rec.code || '#1', rec.department_code, rec.subject_code, '13', rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.begindate), rec.note, qb_dwh_utils.DateToChar(rec.begindate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          add2Fct_Securitydeal(rec.ledid_0,
                               rec.dealid,
                               rec.code || '#1',
                               dwhRecStatus,
                               dwhSysMoment,
                               dwhEXT_FILE);
          insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
                 values (rec.code || '#2', rec.department_code, rec.subject_code, '13', rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.enddate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.enddate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          add2Fct_Securitydeal(rec.ledid_2,
                               rec.dealid,
                               rec.code || '#2',
                               dwhRecStatus,
                               dwhSysMoment,
                               dwhEXT_FILE);
          if rec.dealtype = '15' then
              insert into ldr_infa_cb.fct_repodeal(typedealasgmt, deal_code, proc_rate, proc_base, rec_status, sysmoment, ext_file, typedirect)
                     values(rec.is_our_cb, rec.code, qb_dwh_utils.NumberToChar(rec.incomerate), null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE, '1');
commit;
          end if;
          if rec.dealtype = '12' then
              insert into ldr_infa_cb.fct_repodeal_reverse(typedealasgmt, deal_code, proc_rate, proc_base, rec_status, sysmoment, ext_file, typedirect)
                     values(rec.is_our_cb, rec.code, qb_dwh_utils.NumberToChar(rec.incomerate), null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE, '2');
commit;                     
          end if;
          insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                 values(rec.code, rec.code || '#1', 'REPO', qb_dwh_utils.DateToChar(rec.t_dealdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
          insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                 values(rec.code, rec.code || '#2', 'REPO', qb_dwh_utils.DateToChar(rec.t_dealdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
        else
        -- сделка с корзиной ц/б
          -- перенесено из тестового скрипта
          -- цикл по dealid датам сделки в которые менялся состав корзины
          insert into ldr_infa_cb.fct_securitydeal(typesecdeal, sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code,deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, deal_fee, extra_costs, rec_status, sysmoment, ext_file)
                 values('7', qb_dwh_utils.NumberToChar(0, 14), '0', qb_dwh_utils.NumberToChar(0, 14), '0', '0', '-1', rec.deal_amount1, rec.amount1, rec.code, '-1', '-1', '-1',  null, null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;
          for date_rec in (select chg_date bd,
                                  lead(chg_date - 1, 1, to_date('31129999','ddmmyyyy')) over(order by chg_date) ed
                             from (select distinct t_date chg_date
                                     from ddl_tick_ens_dbt b
                                    where b.t_dealid = rec.dealid
                                    order by chg_date)
                                   order by bd)
          loop
            -- цикл по ценным бумагам в корзине
            for sec_rec in (select distinct t_fiid
                              from ddl_tick_ens_dbt b
                             where b.t_dealid = rec.dealid)
            loop
              -- опеределим количество и стоимость ц/б на дату
              sec_basket_rec := GetParmsSecurity(rec.dealid, sec_rec.t_fiid, date_rec.bd);
              if (sec_basket_rec.cnt > 0) then
                insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
                   values(sec_basket_rec.part_code || '#1', rec.department_code, rec.subject_code, '13', rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(sec_basket_rec.date1), qb_dwh_utils.DateToChar(sec_basket_rec.date1), rec.note, qb_dwh_utils.DateToChar(sec_basket_rec.date1), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
                insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                   values(sec_basket_rec.main_code, sec_basket_rec.part_code || '#1','REPO', qb_dwh_utils.DateToChar(date_rec.bd), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
                insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
                   values(sec_basket_rec.part_code || '#2', rec.department_code, rec.subject_code, '13', rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(sec_basket_rec.date2), qb_dwh_utils.DateToChar(sec_basket_rec.date2), rec.note, qb_dwh_utils.DateToChar(sec_basket_rec.date2), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
                insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                   values(sec_basket_rec.main_code, sec_basket_rec.part_code || '#2','REPO', qb_dwh_utils.DateToChar(date_rec.bd), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
                add2Fct_Securitydeal_basket(sec_basket_rec.legid0,
                                                sec_basket_rec.dealid,
                                                sec_basket_rec.part_code || '#1',
                                                sec_basket_rec.totalcost,
                                                sec_basket_rec.cnt,
                                                sec_basket_rec.nkd,
                                                sec_basket_rec.fi_code,
                                                sec_basket_rec.costfiid,
                                                dwhRecStatus,
                                                dwhSysMoment,
                                                dwhEXT_FILE);
                add2Fct_Securitydeal_basket(sec_basket_rec.legid2,
                                                sec_basket_rec.dealid,
                                                sec_basket_rec.part_code || '#2',
                                                sec_basket_rec.sump2,
                                                sec_basket_rec.cnt,
                                                sec_basket_rec.nkd,
                                                sec_basket_rec.fi_code,
                                                sec_basket_rec.costfiid,
                                                dwhRecStatus,
                                                dwhSysMoment,
                                                dwhEXT_FILE);
                if (date_rec.ed < to_date('31129999','ddmmyyyy') ) then --если было иземение состава корзины закрываем добавленнные части
                  insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                     values(sec_basket_rec.main_code, sec_basket_rec.part_code || '#1','REPO', qb_dwh_utils.DateToChar(date_rec.ed + 1), '1', dwhSysMoment, dwhEXT_FILE);
commit;                     
                  insert into ldr_infa_cb.ass_fct_deal(parent_code, child_code, type_deal_rel_code, dt, rec_status, sysmoment, ext_file)
                     values(sec_basket_rec.main_code, sec_basket_rec.part_code || '#2','REPO', qb_dwh_utils.DateToChar(date_rec.ed + 1), '1', dwhSysMoment, dwhEXT_FILE);
commit;                     
                end if;
              end if;
            end loop;
            totalcnt := GetCountCBTotal(rec.dealid, date_rec.bd);
            insert into ldr_infa_cb.fct_deal_indicator(deal_code,
                                                    deal_attr_code,
                                                    currency_curr_code_txt,
                                                    measurement_unit_code,
                                                    number_value,
                                                    date_value,
                                                    string_value,
                                                    dt,
                                                    rec_status,
                                                    sysmoment,
                                                    ext_file)
               values (to_char(rec.dealid) || '#TCK',
                       'BASKET_AMOUNT',
                       '-1',
                       '-1',
                       qb_dwh_utils.NumberToChar(totalcnt, 0), -- общее количество бумаг по сделке
                       null,
                       null,
                       qb_dwh_utils.DateToChar(date_rec.bd),
                       dwhRecStatus,
                       dwhSysMoment,
                       dwhEXT_FILE);
commit;
          end loop;
          -- перенесено из тестового скрипта

          if rec.dealtype = '15' then
            insert into ldr_infa_cb.fct_repodeal(typedealasgmt, deal_code, proc_rate, proc_base, rec_status, sysmoment, ext_file, typedirect)
                     values(rec.is_our_cb, rec.code, qb_dwh_utils.NumberToChar(rec.incomerate), null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE, '3');
commit;
          end if;
          if rec.dealtype = '12' then
            insert into ldr_infa_cb.fct_repodeal_reverse(typedealasgmt, deal_code, proc_rate, proc_base, rec_status, sysmoment, ext_file, typedirect)
                   values(rec.is_our_cb, rec.code, qb_dwh_utils.NumberToChar(rec.incomerate), null, dwhRecStatus, dwhSysMoment, dwhEXT_FILE, '3');
commit;                   
          end if;
        end if;
        -- Добавление графиков
        insert into ldr_infa_cb.fct_repayschedule_dm(typeschedule,
                                                    eventsum,
                                                    dealsum,
                                                    code,
                                                    finstramount,
                                                    movingdirection,
                                                    finstr_code,
                                                    deal_code,
                                                    typerepay_code,
                                                    dt,
                                                    rec_status,
                                                    sysmoment,
                                                    ext_file)
         (select '1' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#PAYFIRST#PLAN' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(decode(rq.t_plandate, emptDate, firstDate, rq.t_plandate)) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n1 and rq.t_type = n2 and rownum = n1
          union all
          select '3' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#PAYFIRST#FACT' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_factdate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n1 and rq.t_type = n2 and rownum = n1
          union all
          select '1' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#SUPFIRST#PLAN' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_plandate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n1 and rq.t_type = n8 and rownum = n1
          union all
          select '3' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#SUPFIRST#FACT' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_factdate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n1 and rq.t_type = n8 and rownum = n1
          union all
          select '1' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#PAYSEC#PLAN' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_plandate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n2 and rq.t_type = n2 and rownum = n1
          union all
          select '3' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#PAYSEC#FACT' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_factdate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n2 and rq.t_type = n2 and rownum = n1
          union all
          select '1' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#SUPSEC#PLAN' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_plandate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n2 and rq.t_type = n8 and rownum = n1
          union all
          select '3' TYPESCHEDULE,
                 null EVENTSUM,
                 null DEALSUM,
                 '0000#SOFRXXX#' || to_char(rq.t_docid) || '#SUPSEC#FACT' CODE,
                 null FINSTRAMOUNT,
                 case
                   when rq.t_kind = 0 then
                     '1'
                   when rq.t_kind = 1 then
                     '2'
                 end MOVINGDIRECTION,
                 case when fi.t_fi_kind = 2 then
                   to_char(fi.T_FIID) || '#' || decode(fi.t_avoirkind, 5, 'BNR', 'FIN')
                 else
                   fi.t_iso_number
                 end FINSTR_CODE,
                 to_char(rq.t_docid) || '#TCK' DEAL_CODE,
                 '1' TYPEREPAY_CODE,
                 qb_dwh_utils.DateToChar(rq.t_factdate) DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE
           from ddlrq_dbt rq
           inner join dfininstr_dbt fi
              on (rq.t_fiid = fi.t_fiid)
          where rq.t_dockind = rec.bofficekind and rq.t_docid  = rec.dealid
            and rq.t_dealpart = n2 and rq.t_type = n8 and rownum = n1
          );
commit;

      end if;
      if rec.dealtype = '13' then
        add2Fct_Securitydeal(rec.ledid_0,
                             rec.dealid,
                             rec.code,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE);
      elsif rec.dealtype = '27' then
        add2Fct_Repaydeal(rec.ledid_0,
                             rec.dealid,
                             dwhRecStatus,
                             dwhSysMoment,
                             dwhEXT_FILE ) ;
      end if;
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке данных по сделке с цб: ' || SQLERRM,
                               0,
                               cDeal,
                               rec.dealid);
      end;

      -- Добавление счетов по сделкам с прочими ц/б

      for dacc_rec in ( select *
                          from (select distinct dp.t_name || '#IBSOXXX#' || sacc.uf4 acc,
/*                                                'XXXX#SOFR#' || */
                                                nvl(nvl(cat_rd.t_code, cat_ro.t_code),cat_leg.t_code) catcode,
                                                nvl(nvl(acd_rd.t_catid, acd_ro.t_catid), acd_leg.t_catid) catid,
                                                nvl(nvl(cat_rd.t_name, cat_ro.t_name), cat_leg.t_name) catname,
                                                nvl(nvl(acd_rd.t_activatedate, acd_ro.t_activatedate), acd_leg.t_activatedate) catdate,
                                                case when acd_rd.t_activatedate is not null then acd_rd.t_disablingdate
                                                     when acd_ro.t_activatedate is not null then acd_ro.t_disablingdate
                                                     else acd_leg.t_disablingdate
                                                end catenddate, -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                                                sacc.t_department
                                  from (with dp as (select rec.bofficekind   rec_bofficekind,
                                                           rec.dealid rec_dealid
                                                      from dual), dacnt as (select acctrn.t_account_payer,
                                                                                   --acc_p.t_userfield4 uf4_p,
                                                                                   case
                                                                                      when (acc_p.t_userfield4 is null) or
                                                                                          (acc_p.t_userfield4 = chr(0)) or
                                                                                          (acc_p.t_userfield4 = chr(1)) or
                                                                                          (acc_p.t_userfield4 like '0x%') then
                                                                                        acc_p.t_account
                                                                                      else
                                                                                        acc_p.t_userfield4
                                                                                   end uf4_p,
                                                                                   acctrn.t_account_receiver,
                                                                                   acctrn.t_department,
                                                                                   dealkind,
                                                                                   dealid
                                                                              from dacctrn_dbt acctrn,
                                                                                   (select /*+LEADING(grdeal) INDEX(grdeal ddlgrdeal_dbt_idx1)*/
                                                                                     grdoc.t_docid    as acctrnid,
                                                                                     grdeal.t_dockind dealkind,
                                                                                     grdeal.t_docid   dealid
                                                                                      from ddlgrdeal_dbt grdeal,
                                                                                           ddlgrdoc_dbt  grdoc
                                                                                     where exists
                                                                                     (select 1
                                                                                              from dp
                                                                                             where dp.rec_bofficekind =
                                                                                                   grdeal.t_dockind
                                                                                               and dp.rec_dealid =
                                                                                                   grdeal.t_docid)
                                                                                       and grdoc.t_grdealid =
                                                                                           grdeal.t_id
                                                                                       and grdoc.t_dockind = n1
                                                                                    union all
                                                                                    select oprdocs.t_acctrnid as acctrnid,
                                                                                           opr.t_dockind,
                                                                                           to_number(opr.t_documentid)
                                                                                      from doproper_dbt opr,
                                                                                           doprdocs_dbt oprdocs
                                                                                     where exists
                                                                                     (select 1
                                                                                              from dp
                                                                                             where dp.rec_bofficekind =
                                                                                                   opr.t_dockind
                                                                                               and lpad(to_char(dp.rec_dealid),
                                                                                                        34,
                                                                                                        0) =
                                                                                                   opr.t_documentid)
                                                                                       and oprdocs.t_id_operation =
                                                                                           opr.t_id_operation
                                                                                       and oprdocs.t_dockind = n1) q,
                                                                                   dfininstr_dbt pfi,
                                                                                   dfininstr_dbt rfi,
                                                                                   daccount_dbt  acc_p
                                                                             where acctrn.t_acctrnid =
                                                                                   q.acctrnid
                                                                               and acctrn.t_accountid_payer = acc_p.t_accountid
                                                                               and pfi.t_fiid =
                                                                                   acctrn.t_fiid_payer
                                                                               and rfi.t_fiid =
                                                                                   acctrn.t_fiid_receiver
                                                                               and acctrn.t_state = 1
                                                                               and acctrn.t_chapter in
                                                                                   (select v.value
                                                                                      from qb_dwh_const4exp c
                                                                                     inner join qb_dwh_const4exp_val v
                                                                                        on (c.id = v.id)
                                                                                     where c.name =
                                                                                           cACC_CHAPTERS)
                                                                               and pfi.t_fi_kind = n1
                                                                               and rfi.t_fi_kind = n1)
                                         select dacnt.t_account_payer acc,
                                                dacnt.uf4_p uf4,
                                                dacnt.t_department,
                                                dacnt.dealkind,
                                                dacnt.dealid
                                           from dacnt
                                         union all
                                         select *
                                           from (with dp as (select rec.bofficekind   rec_bofficekind,
                                                                    rec.dealid rec_dealid
                                                               from dual), dacnt as (select acctrn.t_account_payer,
                                                                                            acctrn.t_account_receiver,
                                                                                            --acc_r.t_userfield4 uf4_r,
                                                                                            case
                                                                                              when (acc_r.t_userfield4 is null) or
                                                                                                  (acc_r.t_userfield4 = chr(0)) or
                                                                                                  (acc_r.t_userfield4 = chr(1)) or
                                                                                                  (acc_r.t_userfield4 like '0x%') then
                                                                                                acc_r.t_account
                                                                                              else
                                                                                                acc_r.t_userfield4
                                                                                            end uf4_r,
                                                                                            acctrn.t_department,
                                                                                            dealkind,
                                                                                            dealid
                                                                                       from dacctrn_dbt acctrn,
                                                                                            (select /*+LEADING(grdeal) INDEX(grdeal ddlgrdeal_dbt_idx1)*/
                                                                                              grdoc.t_docid    as acctrnid,
                                                                                              grdeal.t_dockind dealkind,
                                                                                              grdeal.t_docid   dealid
                                                                                               from ddlgrdeal_dbt grdeal,
                                                                                                    ddlgrdoc_dbt  grdoc
                                                                                              where exists
                                                                                              (select 1
                                                                                                       from dp
                                                                                                      where dp.rec_bofficekind =
                                                                                                            grdeal.t_dockind
                                                                                                        and dp.rec_dealid =
                                                                                                            grdeal.t_docid)
                                                                                                and grdoc.t_grdealid =
                                                                                                    grdeal.t_id
                                                                                                and grdoc.t_dockind = n1
                                                                                             union all
                                                                                             select oprdocs.t_acctrnid as acctrnid,
                                                                                                    opr.t_dockind,
                                                                                                    to_number(opr.t_documentid)
                                                                                               from doproper_dbt opr,
                                                                                                    doprdocs_dbt oprdocs
                                                                                              where exists
                                                                                              (select 1
                                                                                                       from dp
                                                                                                      where dp.rec_bofficekind =
                                                                                                            opr.t_dockind
                                                                                                        and lpad(to_char(dp.rec_dealid),
                                                                                                                 34,
                                                                                                                 0) =
                                                                                                            opr.t_documentid)
                                                                                                and oprdocs.t_id_operation =
                                                                                                    opr.t_id_operation
                                                                                                and oprdocs.t_dockind = n1) q,
                                                                                            dfininstr_dbt pfi,
                                                                                            dfininstr_dbt rfi,
                                                                                            daccount_dbt acc_r
                                                                                      where acctrn.t_acctrnid =
                                                                                            q.acctrnid
                                                                                        and acctrn.t_accountid_receiver = acc_r.t_accountid
                                                                                        and pfi.t_fiid =
                                                                                            acctrn.t_fiid_payer
                                                                                        and rfi.t_fiid =
                                                                                            acctrn.t_fiid_receiver
                                                                                        and acctrn.t_state = n1
                                                                                        and acctrn.t_chapter in
                                                                                            (select v.value
                                                                                               from qb_dwh_const4exp c
                                                                                              inner join qb_dwh_const4exp_val v
                                                                                                 on (c.id = v.id)
                                                                                              where c.name =
                                                                                                    cACC_CHAPTERS)
                                                                                        and pfi.t_fi_kind = n1
                                                                                        and rfi.t_fi_kind = n1)
                                                  select distinct dacnt.t_account_receiver,
                                                                  dacnt.uf4_r,
                                                                  dacnt.t_department,
                                                                  dacnt.dealkind,
                                                                  dacnt.dealid
                                                    from dacnt)) sacc
                                                   inner join ddp_dep_dbt dp
                                                      on (sacc.t_department = dp.t_code)
                                                    left join dmcaccdoc_dbt acd_rd
                                                      on (sacc.acc = acd_rd.t_account and
                                                         acd_rd.t_dockind = sacc.dealkind and
                                                         acd_rd.t_docid = sacc.dealid)
                                                    left join dmccateg_dbt cat_rd
                                                      on (acd_rd.t_catid = cat_rd.t_id)
                                                    left join dmcaccdoc_dbt acd_ro
                                                      on (sacc.acc = acd_ro.t_account and
                                                         acd_ro.t_iscommon = chr88)
                                                    left join dmccateg_dbt cat_ro
                                                      on (acd_ro.t_catid = cat_ro.t_id)
                                                    left join ddl_leg_dbt leg
                                                      on (sacc.dealid = leg.t_dealid and leg.t_legkind = 0)
                                                    left join dmcaccdoc_dbt acd_leg
                                                      on (sacc.acc = acd_leg.t_account and
                                                         acd_leg.t_dockind = 176 and
                                                         acd_leg.t_docid = leg.t_id)
                                                    left join dmccateg_dbt cat_leg
                                                      on (acd_leg.t_catid = cat_leg.t_id)
                                )
                         where catid is not null
                        )
      loop
        -- Вставка в ass_accountdeal
        begin
          begin
            insert into ldr_infa_cb.ass_accountdeal(account_code, deal_code, roleaccount_deal_code, dt, rec_status, sysmoment, ext_file, dt_end) -- KS 04.04.2022 Закрытие действующих связей должно осуществляться через поле ASS_ACCOUNTDEAL.DT_END
                   values(dacc_rec.acc, rec.code, dacc_rec.catcode, qb_dwh_utils.DateToChar(dacc_rec.catdate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE, case when dacc_rec.catenddate = to_date('01.01.0001','dd.mm.yyyy') then qb_dwh_utils.DateToChar(qb_dwh_utils.DT_END) else qb_dwh_utils.DateToChar(dacc_rec.catenddate-1) end);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
          begin
            insert into ldr_infa_cb.det_roleaccount_deal(code, name, orole_code, dt, rec_status, sysmoment, ext_file)
                   values (dacc_rec.catcode, dacc_rec.catname, '0', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                   
          exception
            when dup_val_on_Index then
              null;
          end;
        exception
          when others then
            qb_bp_utils.SetError(EventID,
                                 SQLCODE,
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке счета по сделке с цб ' || SQLERRM,
                                 0,
                                 cDeal,
                                 rec.dealid);
        end;

      end loop;

      insert into ldr_infa_cb.ass_carrydeal
      (select * from (with dp as (select rec.bofficekind rec_bofficekind,
                                         rec.dealid rec_dealid
                                    from dual),
                          trn as (select acctrnid,
                                         acctrn.t_account_payer,
                                         acctrn.t_account_receiver,
                                         acctrn.t_department,
                                         acctrn.t_date_carry,
                                         dealkind,
                                         dealid
                                   from dacctrn_dbt acctrn,
                                        (select /*+LEADING(grdeal) INDEX(grdeal ddlgrdeal_dbt_idx1)*/
                                          grdoc.t_docid    as acctrnid,
                                          grdeal.t_dockind dealkind,
                                          grdeal.t_docid   dealid
                                           from ddlgrdeal_dbt grdeal,
                                                ddlgrdoc_dbt  grdoc
                                          where exists  (select 1
                                                           from dp
                                                          where dp.rec_bofficekind =
                                                                grdeal.t_dockind
                                                            and dp.rec_dealid =
                                                                grdeal.t_docid)
                                            and grdoc.t_grdealid = grdeal.t_id
                                            and grdoc.t_dockind = n1
                                         union all
                                         select oprdocs.t_acctrnid as acctrnid,
                                                opr.t_dockind,
                                                to_number(opr.t_documentid)
                                           from doproper_dbt opr,
                                                doprdocs_dbt oprdocs
                                          where exists (select 1
                                                          from dp
                                                         where dp.rec_bofficekind = opr.t_dockind
                                                           and lpad(to_char(dp.rec_dealid), 34, 0) = opr.t_documentid)
                                            and oprdocs.t_id_operation = opr.t_id_operation
                                            and oprdocs.t_dockind = n1) q
                                  where acctrn.t_acctrnid = q.acctrnid
                                    and acctrn.t_state = n1
                                    and acctrn.t_userfield4 <> chr(1)), -- исключим проводки без идентификатора БИСКВИТ
                           trn_code as ( select qb_dwh_utils.GetComponentCode('FCT_CARRY',
                                                            qb_dwh_utils.System_IBSO,
                                                            trn.t_Department,
                                                            trn.AcctrnID,
                                                            trn.Dealkind) CARRY_CODE,
                                                to_char(trn.dealid) || '#TCK' DEAL_CODE,
                                                qb_dwh_utils.DateToChar(trn.t_date_carry) DT,
                                                '0' REC_STATUS,
                                                dwhSYSMOMENT,
                                                dwhEXT_FILE
                                           from trn)
                    select * from trn_code));
commit;
      --commit;
    end loop;

    -- конвертации выпусков
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка конвертаций выпуска',
                   2,
                   null,
                   null);
    for rec in (select to_char(cm.t_documentid) || '#COM' code,
                       '0000' department_code,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             decode(cm.t_clientid, -1, 1, cm.t_clientid)) subject_code,
                       '13' dealtype,
                       cm.t_commcode docnum,
                       '0' is_interior,
                       cm.t_commdate begindate,
                       cm.t_commdate enddate,
                       cm.t_comment note,
                       cm.t_commdate dt,
                       to_char(cm.t_fiid) || decode(fi.t_avoirkind, 5, '#BNR', '#FIN') code_currency,
                       cm.t_documentid
                  from ddl_comm_dbt cm
                 inner join dfininstr_dbt fi
                    on (cm.t_fiid = fi.t_fiid)
                 where cm.t_commdate <= in_date
                   and cm.t_dockind = n135
                   and cm.t_operationkind = n2020)
    loop
      begin
      insert into ldr_infa_cb.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
             values (rec.code, rec.department_code, rec.subject_code, rec.dealtype, rec.docnum, rec.is_interior, qb_dwh_utils.DateToChar(rec.begindate), qb_dwh_utils.DateToChar(rec.enddate), rec.note, qb_dwh_utils.DateToChar(rec.dt), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      insert into ldr_infa_cb.fct_securitydeal(typesecdeal,sec_price, sec_proc, rate, scale, couponyield, currency_finstr_code, deal_amount, amount, deal_code, finstrbuy_finstr_code, finstrsel_finstr_code, exchange_code, rec_status, sysmoment, ext_file)
             values ('9', '-1', null, '-1', '-1', null, rec.code_currency, null, null, rec.code, rec.code_currency, rec.code_currency, '-1', dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;             
      exception
        when others then
          qb_bp_utils.SetError(EventID,
                               SQLCODE,
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке конвертаций выпуска: ' || SQLERRM,
                               0,
                               cDeal,
                               rec.t_documentid);
      end;

    end loop;

    --BIQ-7477/7478 Выгрузка депозитных сертификатов
    if (BIQ_7477_78 = 1) then
        qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка депозитных сертификатов',
                       2,
                       null,
                       null);
        for rec in (select 
            to_char(cert.t_fiid)||'#SRT' SECURITY_CODE, 
            to_char(bnr.t_kind) TYPESERTIFICATE, --1=именной, 2 =предъявительский
            cert.t_number NUMBERSERTIFICATE,
            cert.t_series SERIESSERTIFICATE,
            qb_dwh_utils.datetochar(leg.t_closed) MATURITYDATE, --дата погашения 
            qb_dwh_utils.datetochar(leg.t_maturity) DEMANDDATE, --дата востребования в интерфейсе
            qb_dwh_utils.datetochar(leg.t_start) DEPOSITDATE, --дата внесения
            qb_dwh_utils.NumberToChar(leg.t_price/power(10, leg.t_point)) PROC_RATE,
            qb_dwh_utils.NumberToChar(bnr.t_oncallrate/power(10, leg.t_point)) AHEAD_PROC_RATE,
            --состояние
            bnr.t_bcstate SERTIFICATE_STATE, --состояние
            to_char(cert.t_fiid)||'#SRT' SERTIFICATE_CODE,
            qb_dwh_utils.datetochar(cert.t_issuedate) DT, 
            qb_dwh_utils.GetComponentCode('DET_SUBJECT', qb_dwh_utils.System_IBSO, 1, bnr.t_holder) SUBJECT_CODE, --держатель, как в векселе
            --справочник ЦБ
            qb_dwh_utils.datetochar(fi.t_issued) DATE_ISSUE,
            qb_dwh_utils.NumberToChar(fi.t_facevalue) NOMINAL,
            qb_dwh_utils.GetComponentCode('DET_SUBJECT', qb_dwh_utils.System_IBSO, 1, bnr.t_issuer) ISSUER_CODE,
            fi_nom.t_iso_number FINSTRCURNOM_FINSTR_CODE,
            fi.t_name FINSTR_NAME,
            fi.t_definition FINSTR_NAME_S,
            nvl2(dd.t_avoirkind, '9999#SOFRXXX#'||to_char(dd.t_fi_kind)||'#'||to_char(dd.t_avoirkind),null) as SECURITY_KIND_CODE 
              from dv_ficert_dbt cert 
              inner join dvsbanner_dbt bnr on (bnr.t_bcid = cert.t_ficertid and cert.t_avoirkind= 9) --9=Депозитный сертификат
              inner join dfininstr_dbt fi on fi.t_fiid = cert.t_fiid
              inner join dfininstr_dbt fi_nom on fi_nom.t_fiid = fi.t_facevaluefi
              inner join ddl_leg_dbt leg on (leg.t_dealid = cert.t_ficertid and leg.t_start = cert.t_issuedate and leg.t_principal = cert.t_facevalue)
              left join dparty_dbt pt on pt.t_partyid = cert.t_issuer
              left join davrkinds_dbt dd on dd.t_fi_kind = fi.t_fi_kind and dd.t_avoirkind = fi.t_avoirkind 
              )
              loop
                        begin
                              insert into ldr_infa_cb.det_sertificate(SECURITY_CODE, TYPESERTIFICATE, NUMBERSERTIFICATE, SERIESSERTIFICATE, MATURITYDATE, DEMANDDATE, DEPOSITDATE, PROC_RATE, AHEAD_PROC_RATE, DT, rec_status, sysmoment, ext_file)
                              values (rec.SECURITY_CODE, rec.TYPESERTIFICATE, rec.NUMBERSERTIFICATE, rec.SERIESSERTIFICATE, rec.MATURITYDATE, rec.DEMANDDATE, rec.DEPOSITDATE, rec.PROC_RATE, rec.AHEAD_PROC_RATE, rec.DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                              
                              insert into ldr_infa_cb.fct_sertificate_state(SERTIFICATE_STATE, SERTIFICATE_CODE, DT, SUBJECT_CODE, rec_status, sysmoment, ext_file)
                              values (rec.SERTIFICATE_STATE, rec.SERTIFICATE_CODE, rec.DT, rec.SUBJECT_CODE, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                              
                              Insert into ldr_infa_cb.det_security(typesecurity, code, date_issue, nominal, regnum, finstrsecurity_finstr_code, issuer_code, underwriter_code, finstrcurnom_finstr_code, procbase, dt, SECURITY_KIND_CODE, rec_status, sysmoment, ext_file)
                              values ('4', rec.SECURITY_CODE, rec.DATE_ISSUE, rec.NOMINAL, null, rec.SECURITY_CODE, rec.ISSUER_CODE, '-1', rec.FINSTRCURNOM_FINSTR_CODE, '9999#SOFRXXX#1', rec.DATE_ISSUE, rec.SECURITY_KIND_CODE, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                       
                              Insert into ldr_infa_cb.det_finstr(finstr_code, finstr_name, finstr_name_s, typefinstr, dt, rec_status,sysmoment, ext_file)
                              values (rec.SECURITY_CODE, rec.FINSTR_NAME, rec.FINSTR_NAME_S, '2', rec.DT, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);       
commit;                         
                         exception
                              when others then
                                qb_bp_utils.SetError(EventID,
                                    SQLCODE,
                                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке депозитных сертификатов: ' || SQLERRM,
                                   0,
                                   cDeal,
                                   rec.SECURITY_CODE);
                        end;

               end loop;
    end if;

    qb_bp_utils.SetError(EventID,
               '',
               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка атрибутов сделок',
               2,
               null,
               null);

    begin

    -- Заполнение категории 47 по сделкам !!!!!!!!!!!!!!!!( удалить при установке обновления по заполнению данной категории)
    insert into dobjatcor_dbt
      select 101,
             47,
             1,
             lpad(to_char(tick.t_dealid), 34, '0'),
             chr(0),
             tick.t_dealdate,
             1,
             to_date('31129999','ddmmyyyy'),
             trunc(sysdate),
             to_date('01010001 ' || to_char(sysdate, 'hh24:mi:ss'), 'ddmmyyyy hh24:mi:ss'),
             chr(88),
             null
        from ddl_tick_dbt tick
        inner join dfininstr_dbt fi
           on (tick.t_pfi = fi.t_fiid)
        where tick.t_dealdate <= in_date
          and tick.t_bofficekind  in (select v.value
                                        from qb_dwh_const4exp c
                                       inner join qb_dwh_const4exp_val v
                                          on (c.id = v.id)
                                       where c.name = cDEALSKIND_SEC)
          and tick.t_clientid = n_1
          and fi.t_avoirkind in (select v.value
                                   from qb_dwh_const4exp c
                                  inner join qb_dwh_const4exp_val v
                                     on (c.id = v.id)
                                  where c.name = cSECKIND_ALL)
          and not exists (select 1
                            from dobjatcor_dbt ac
                           where ac.t_objecttype = n101
                             and ac.t_groupid = n47
                             and ac.t_object = lpad(to_char(tick.t_dealid), 34, '0'));
commit;                             
    -- Добавление характеристики "Тест на рыночность пройден" для сделок с УВ
     insert into ldr_infa_cb.ass_deal_cat_val (deal_code, deal_cat_val_code, deal_cat_val_code_deal_cat, dt, rec_status, sysmoment, ext_file)
            (select distinct code,
                            '101C47#' || testnm,
                            '101C47',
                            qb_dwh_utils.datetochar(t_dealdate),
                            dwhRecStatus,
                            dwhSysMoment,
                            dwhEXT_FILE
              from (select to_char(tick.t_dealid) || '#TCK' code,
                           row_number() over(partition by tick.t_dealid order by leg.t_id) rnk,
                           count(*) over(partition by tick.t_dealid) cnt,
                           decode(leg.t_relativeprice, chr(88), 'Да', 'Нет') testnm,
                           leg.t_relativeprice,
                           tick.t_dealdate
                      from dvsbanner_dbt bn
                     inner join ddl_leg_dbt leg
                        on (bn.t_bcid = leg.t_dealid)
                     inner join dvsordlnk_dbt lnk
                        on (bn.t_bcid = lnk.t_bcid)
                     inner join ddl_tick_dbt tick
                        on (lnk.t_contractid = tick.t_dealid and
                           lnk.t_dockind = tick.t_bofficekind)
                     inner join dfininstr_dbt fi
                        on (leg.t_cfi = fi.t_fiid)
                     where leg.t_legid = n0
                       and leg.t_legkind = n1
                       and tick.t_bofficekind in
                           (select v.value
                              from qb_dwh_const4exp c
                             inner join qb_dwh_const4exp_val v
                                on (c.id = v.id)
                             where c.name = cDEALSKIND_DBILL)
                       and tick.t_dealdate <= in_date
                       and leg.t_relativeprice <> chr0));
commit;                       
    -- Добавление характеристики "Тест на рыночность пройден" для сделок с СВ
    insert into ldr_infa_cb.ass_deal_cat_val (deal_code, deal_cat_val_code, deal_cat_val_code_deal_cat, dt, rec_status, sysmoment, ext_file)
      select distinct to_char(ord.t_contractid) || '#ORD' code,
            '101C47#' || decode(leg.t_relativeprice, chr(88), 'Да', 'Нет') testnm,
            '101C47',
            qb_dwh_utils.datetochar(ord.t_signdate) dt,
            dwhRecStatus,
            dwhSysMoment,
            dwhEXT_FILE
       from dvsbanner_dbt bn
       inner join ddl_leg_dbt leg
         on (bn.t_bcid = leg.t_dealid)
       inner join dvsordlnk_dbt lnk
          on (bn.t_bcid = lnk.t_bcid)
       inner join ddl_order_dbt ord
          on (lnk.t_contractid = ord.t_contractid and lnk.t_dockind = ord.t_dockind)
       inner join ddp_dep_dbt dp
          on (ord.t_department = dp.t_code)
        left join dfininstr_dbt fi_cfi
          on (leg.t_cfi = fi_cfi.t_fiid)
       where leg.t_legid = n0 and leg.t_legkind = n1
         and ord.t_signdate <= in_date
         and leg.t_relativeprice <> chr0;
commit;         
    -- Заполнение справочника категорий по сделкам
    Insert into ldr_infa_cb.det_deal_cat(code_deal_cat, name_deal_cat, is_multivalued, dt, rec_status, sysmoment, ext_file)
          (SELECT DISTINCT TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) CODE_DEAL_CAT,
                          UPPER(TRIM(GR.T_NAME)) NAME_DEAL_CAT,
                          DECODE(GR.T_TYPE, CHR(88), '0', '1') IS_MULTYVALUED,
                          qb_dwh_utils.DateToChar(firstDate),
                          dwhRecStatus,
                          dwhSysMoment,
                          dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
          INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
          WHERE AC.T_OBJECTTYPE = n101);
commit;          
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке справочника категорий по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);

    end;

    begin
    -- заполнение справочника значений по категориям
    insert into ldr_infa_cb.det_deal_cat_val(deal_cat_code,
                                          code_deal_cat_val,
                                          name_deal_cat_val,
                                          dt,
                                          rec_status,
                                          sysmoment,
                                          ext_file)
           (SELECT DISTINCT TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) DEAL_CAT_CODE,
                          TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) || '#' || case when trim(atr.t_name) is null or trim(atr.t_name) = chr(1) then atr.t_nameobject else atr.t_name end code_deal_cat_val,
                          TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) || '#' || case when trim(atr.t_fullname) is null or trim(atr.t_fullname) = chr(1) then atr.t_nameobject else atr.t_fullname end name_deal_cat_val,
                          qb_dwh_utils.DateToChar(firstDate),
                          dwhRecStatus,
                          dwhSysMoment,
                          dwhEXT_FILE
            FROM DOBJATCOR_DBT AC
          INNER JOIN DOBJGROUP_DBT GR
              ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
          inner join dobjattr_dbt atr
             on (gr.t_objecttype = atr.t_objecttype and gr.t_groupid = atr.t_groupid)
          WHERE AC.T_OBJECTTYPE = n101);
commit;          
     -- Заполение списка значений категорий по сделкам
     insert into ldr_infa_cb.ass_deal_cat_val (deal_code, deal_cat_val_code, deal_cat_val_code_deal_cat, dt, rec_status, sysmoment, ext_file)
           (SELECT /*+ leading(tick,ac) index(tick DDL_TICK_DBT_IDX_U1) */ distinct to_char(to_number(ac.t_object)) || '#TCK' deal_code,
                         TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) || '#' || case when trim(atr.t_name) is null or trim(atr.t_name) = chr(1) then atr.t_nameobject else atr.t_name end code_deal_cat_val,
                         TO_CHAR(AC.T_OBJECTTYPE) || 'C' || TO_CHAR(AC.T_GROUPID) DEAL_CAT_CODE,
                         qb_dwh_utils.DateToChar(decode(ac.t_validfromdate, emptDate, tick.t_dealdate, ac.t_validfromdate)),
                         dwhRecStatus,
                         dwhSysMoment,
                         dwhEXT_FILE
                    FROM DOBJATCOR_DBT AC
                  INNER JOIN DOBJGROUP_DBT GR
                      ON (AC.T_OBJECTTYPE = GR.T_OBJECTTYPE AND AC.T_GROUPID = GR.T_GROUPID)
                  inner join dobjattr_dbt atr
                     on (gr.t_objecttype = atr.t_objecttype and gr.t_groupid = atr.t_groupid and ac.t_attrid = atr.t_attrid)
                  inner join ddl_tick_dbt tick
                     on (ac.t_object = lpad(to_char(tick.t_dealid), 34, '0'))
                  WHERE AC.T_OBJECTTYPE = n101
                     and tick.t_dealdate <= in_date
                     and tick.t_clientid = n_1
                     --and exists (select 1 from ldr_infa_cb.fct_deal where code = to_char(to_number(ac.t_object)) || '#TCK' )
                     );
commit;                     
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке списка категорий по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    insert into ldr_infa_cb.det_deal_typeattr (code,
                                            name,
                                            is_money_value,
                                            data_type,
                                            dt,
                                            rec_status,
                                            sysmoment,
                                            ext_file)
           (SELECT DISTINCT TO_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                            UPPER(TRIM(NK.T_NAME)) NAME,
                            case when nk.t_notetype = 25 then
                                   '1'
                                 else
                                   '0'
                            end is_money_value,
                            case when nk.t_notetype in (0, 1, 2, 3, 4, 25) then
                                   '1'
                                 when nk.t_notetype = 9 then
                                   '2'
                                 when nk.t_notetype in (7, 12) then
                                   '3'
                                 else
                                   '0'
                            end data_type,
                            qb_dwh_utils.DateToChar(firstDate),
                            dwhRecStatus,
                            dwhSysMoment,
                            dwhEXT_FILE
              FROM DNOTETEXT_DBT NT
            INNER JOIN DNOTEKIND_DBT NK
                ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND NT.T_NOTEKIND = NK.T_NOTEKIND)
            WHERE NT.T_OBJECTTYPE = n101);
commit;            
    insert into ldr_infa_cb.det_deal_typeattr (code,
                                            name,
                                            is_money_value,
                                            data_type,
                                            dt,
                                            rec_status,
                                            sysmoment,
                                            ext_file)
       values('BASKET_AMOUNT',
             'КОЛ-ВО БУМАГ В СДЕЛКЕ РЕПО С КОРЗИНОЙ ЦБ',
             '0',
             '1',
             qb_dwh_utils.DateToChar(firstDate),
             dwhRecStatus,
             dwhSysMoment,
             dwhEXT_FILE);
commit;
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке справочника примечаний по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    insert into ldr_infa_cb.fct_deal_indicator(deal_code,
                                            deal_attr_code,
                                            currency_curr_code_txt,
                                            measurement_unit_code,
                                            number_value,
                                            date_value,
                                            string_value,
                                            dt,
                                            rec_status,
                                            sysmoment,
                                            ext_file)
           (              select distinct deal_code,
                     code deal_attr_code,
                     '-1',
                     '-1',
                     case
                       when type in (0, 1, 2, 3, 4, 25) then
                        noteval
                       else
                        null
                     end number_value,
                     case
                       when type in (9, 10) then
                        noteval
                       else
                        null
                     end date_value,
                     case
                       when type in (7, 12) then
                        substr(noteval, 1, 256)
                       else
                        null
                     end string_value,
                     qb_dwh_utils.DateToChar(t_date) dt,
                     dwhRecStatus,
                     dwhSysMoment,
                     dwhEXT_FILE
                from (SELECT to_char(tick.t_dealid) ||  '#TCK' deal_code,
                             to_CHAR(NT.T_OBJECTTYPE) || 'T' || TO_CHAR(NT.T_NOTEKIND) CODE,
                             UPPER(TRIM(NK.T_NAME)) NAME,
                             nk.t_notetype type,
                             case nk.t_notetype
                               when 0 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getInt(nt.t_text), 0)
                               when 1 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getLong(nt.t_text), 0)
                               when 2 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 3 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 4 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getDouble(nt.t_text))
                               when 7 then
                                Rsb_Struct.getString(nt.t_text)
                               when 9 then
                                qb_dwh_utils.DateToChar(Rsb_Struct.getDate(nt.t_text))
                               when 10 then
                                qb_dwh_utils.DateTimeToChar(Rsb_Struct.getTime(nt.t_text))
                               when 12 then
                                Rsb_Struct.getChar(nt.t_text)
                               when 25 then
                                qb_dwh_utils.NumberToChar(Rsb_Struct.getMoney(nt.t_text), 2)
                               else
                                null
                             end noteval,
                             decode(nt.t_date, emptDate, tick.t_dealdate, nt.t_date) t_date,
                             nt.t_documentid
                        FROM DNOTETEXT_DBT NT
                       INNER JOIN DNOTEKIND_DBT NK
                          ON (NT.T_OBJECTTYPE = NK.T_OBJECTTYPE AND
                             NT.T_NOTEKIND = NK.T_NOTEKIND)
                       inner join ddl_tick_dbt tick
                          on (nt.t_documentid = lpad(to_char(tick.t_dealid), 34, '0'))
                       WHERE NT.T_OBJECTTYPE  =  n101
                         and decode(nt.t_date, emptDate, tick.t_dealdate, nt.t_date) <= in_date
                         and decode(nt.t_date, emptDate, tick.t_dealdate, nt.t_date) >= tick.t_dealdate
                         and tick.t_clientid = n_1
                         --and exists (select 1 from ldr_infa_cb.fct_deal where code = to_char(tick.t_dealid) || '#TCK' )
                         ));
commit;                         
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке свободных атрибутов по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    insert into ldr_infa_cb.fct_overdue_securitydeal
       (select deal_code,
               deal_fiid security_code,
               '0000#IBSOXXX#' || decode(t_userfield4, chr(1), t_account, t_userfield4) account_code,
               acc_role_code,
               qb_dwh_utils.NumberToChar(d.next_day , 0),
               qb_dwh_utils.NumberToChar(abs(inrest), 2),
               qb_dwh_utils.DateToChar(t.t_restdate + d.next_day - 1) dt,
               dwhRecStatus,
               dwhSysMoment,
               dwhEXT_FILE
          from (select distinct
                       catacc.t_account,
                       --ac.t_userfield4,
                       case
                          when (ac.t_userfield4 is null) or
                              (ac.t_userfield4 = chr(0)) or
                              (ac.t_userfield4 = chr(1)) or
                              (ac.t_userfield4 like '0x%') then
                            ac.t_account
                          else
                            ac.t_userfield4
                       end t_userfield4,
                       catacc.t_catnum,
                       nvl(decode(tick.t_dealid, null, null, to_char(tick.t_dealid) || '#TCK'), decode(tick_leg.t_dealid, null, null, to_char(tick_leg.t_dealid) || '#TCK')) deal_code,
                       nvl(decode(tick.t_dealid, null, null, to_char(tick.t_pfi) || '#FIN') , decode(tick_leg.t_dealid, null, null, to_char(tick_leg.t_pfi) || '#FIN'))  deal_fiid,
                       rd.t_accountid,
                       rd.t_restcurrency ,
                       rd.t_restdate,
                       rd.t_rest inrest,
                       row_number() over(partition by rd.t_accountid order by rd.t_restdate) frow,
                        case when max(rd.t_restdate) over(partition by rd.t_accountid) = min(rd.t_restdate) over(partition by rd.t_accountid) then
                            in_date - min(rd.t_restdate) over(partition by rd.t_accountid)
                          else
                             max(rd.t_restdate) over(partition by rd.t_accountid) -
                             min(rd.t_restdate) over(partition by rd.t_accountid)
                        end cnt_days,
                       --'0000#SOFR#' || cat.t_code acc_role_code
                       cat.t_code acc_role_code
                  from dmcaccdoc_dbt catacc
                  inner join dmccateg_dbt cat
                    on (catacc.t_catid = cat.t_id)
                  left join dmctempl_dbt templ
                    on (catacc.t_catid = templ.t_catid and catacc.t_templnum = templ.t_number)
                  left join ddl_leg_dbt leg
                    on (catacc.t_docid = leg.t_id and catacc.t_dockind = n176)
                  left join ddl_tick_dbt tick_leg
                    on (tick_leg.t_dealid = leg.t_dealid)
                  left join ddl_tick_dbt tick
                    on (catacc.t_docid = tick.t_dealid and catacc.t_dockind in (select v.value
                                                                                  from qb_dwh_const4exp c
                                                                                 inner join qb_dwh_const4exp_val v
                                                                                    on (c.id = v.id)
                                                                                 where c.name = cEXP_DOCKIND))
                  inner join daccount_dbt ac
                    on (catacc.t_chapter = ac.t_chapter and catacc.t_account = ac.t_account and catacc.t_currency = ac.t_code_currency)
                  inner join drestdate_dbt rd
                    on (ac.t_accountid = rd.t_accountid and rd.t_restcurrency  = ac.t_code_currency)
                 where ((catacc.t_catnum in (701,1244)) or
                        (catacc.t_catnum in (233, 1237, 1245, 1246, 1298, 1299) and templ.t_value3 = 1) or
                        (catacc.t_catnum in (1245, 1246) and templ.t_value1 = 1)
                       )
                   and (tick.t_dealid is not null or leg.t_id is not null)
              ) t
        inner join (select level Next_day from dual
                    connect by level <= 1000) d
          on (d.next_day <= t.cnt_days )
        left join drestdate_dbt rd
           on (rd.t_accountid = t.t_accountid and rd.t_restcurrency = t.t_restcurrency and rd.t_restdate = t.t_restdate + d.next_day - 1)
        where (t.frow = 1)
          and t.inrest <> 0
        union all
        select deal_code,
               deal_fiid security_code,
               account_code,
               acc_role_code,
               qb_dwh_utils.NumberToChar(d.next_day , 0),
               qb_dwh_utils.NumberToChar(abs(inrest), 2),
               qb_dwh_utils.DateToChar(t.t_restdate + d.next_day -1) dt,
               dwhRecStatus,
               dwhSysMoment,
               dwhEXT_FILE
          from (select distinct
                        '0000#IBSOXXX#' || case
                                              when (ac.t_userfield4 is null) or
                                                  (ac.t_userfield4 = chr(0)) or
                                                  (ac.t_userfield4 = chr(1)) or
                                                  (ac.t_userfield4 like '0x%') then
                                                ac.t_account
                                              else
                                                ac.t_userfield4
                                           end account_code,
                        acc.t_catnum,
                        to_char(lnk.t_bcid) || '#BNR' deal_fiid,
                        decode(tick_ord.t_dealid, null, null, to_char(tick_ord.t_dealid) || '#TCK') deal_code,
                        rd.t_accountid,
                        rd.t_restcurrency ,
                        rd.t_restdate,
                        rd.t_rest inrest,
                        row_number() over(partition by rd.t_accountid order by rd.t_restdate) frow,
                        case when max(rd.t_restdate) over(partition by rd.t_accountid) = min(rd.t_restdate) over(partition by rd.t_accountid) then
                            in_date - min(rd.t_restdate) over(partition by rd.t_accountid)
                          else
                             max(rd.t_restdate) over(partition by rd.t_accountid) -
                             min(rd.t_restdate) over(partition by rd.t_accountid)
                        end cnt_days,
                        --'0000#SOFR#' || cat.t_code acc_role_code
                        cat.t_code acc_role_code
                   from dmcaccdoc_dbt acc
                  inner join dmccateg_dbt cat
                     on (acc.t_catid = cat.t_id)
                   left join dmctempl_dbt templ
                     on (acc.t_catid = templ.t_catid and acc.t_templnum = templ.t_number)
                   left join dvsordlnk_dbt lnk
                     on (acc.t_docid = lnk.t_bcid and lnk.t_linkkind = n0 and lnk.t_dockind in (select v.value
                                                                                                  from qb_dwh_const4exp c
                                                                                                 inner join qb_dwh_const4exp_val v
                                                                                                    on (c.id = v.id)
                                                                                                 where c.name = cDEALSKIND_DBILL_2))
                   left join ddl_tick_dbt tick_ord
                     on (lnk.t_contractid = tick_ord.t_dealid and lnk.t_dockind = tick_ord.t_bofficekind)
                  inner join daccount_dbt ac
                     on (acc.t_chapter = ac.t_chapter and acc.t_account = ac.t_account and acc.t_currency = ac.t_code_currency)
                  inner join drestdate_dbt rd
                     on (ac.t_accountid = rd.t_accountid and rd.t_restcurrency = ac.t_code_currency)
                  where acc.t_dockind = n164
                    and (acc.t_catnum in (select v.value
                                            from qb_dwh_const4exp c
                                           inner join qb_dwh_const4exp_val v
                                              on (c.id = v.id)
                                           where c.name = cEXP_CAT_TMP2) or
                        (acc.t_catnum = n462 and templ.t_value4 in (select v.value
                                                                      from qb_dwh_const4exp c
                                                                     inner join qb_dwh_const4exp_val v
                                                                        on (c.id = v.id)
                                                                     where c.name = cEXP_CAT462_TEMPL)))
                    and tick_ord.t_dealid is not null
              ) t
        inner join (select level Next_day from dual
                    connect by level <= 1000) d
          on (d.next_day <= t.cnt_days )
        left join drestdate_dbt rd
           on (rd.t_accountid = t.t_accountid and rd.t_restcurrency = t.t_restcurrency and rd.t_restdate = t.t_restdate + d.next_day - 1)
        where (t.frow = n1)
          and t.inrest <> n0);
commit;          
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке просрочки по цб: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;

    begin
    insert into ldr_infa_cb.fct_dealrisk
       (select t.t_insmethod GROUND,
               t.t_reservepercent RESERVE_RATE,
               to_char(d.t_dealid) || '#TCK' DEAL_CODE,
               case
                 when t.t_reservepercent < 1 then
                   '9999#SOFRXXX#1'
                 when t.t_reservepercent < 21 then
                   '9999#SOFRXXX#2'
                 when t.t_reservepercent < 51 then
                   '9999#SOFRXXX#3'
                 when t.t_reservepercent < 100 then
                   '9999#SOFRXXX#4'
                 else
                   '9999#SOFRXXX#5'
               end RISKCAT_CODE,
               to_date('01011980', 'ddmmyyyy') DT,
               '0' REC_STATUS,
               dwhSYSMOMENT,
               dwhEXT_FILE,
               --из справочника det_typerisk: CODE_TYPERISK=254i, NAME_TYPERISK="Группы риска, классиф.элементы расчетной базы резерва"
               --группа риска = категория качества, а запись в dmm_qcateg_dbt означает наличие этого реквизита у сделки 
               cCODE_TYPERISK RISKCAT_CODE_TYPERISK 
          from dmm_qcateg_dbt t
          inner join ddl_tick_dbt d --on d.t_department = in_Department
             on t.t_dealid = d.t_dealid
          where exists (select 1
                          from ldr_infa_cb.fct_deal fd
                         where fd.code = to_char(t.t_dealid) || '#TCK')
        );
commit;        
    exception
      when others then
        qb_bp_utils.SetError(EventID,
                             SQLCODE,
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при выгрузке групп риска по сделкам: ' || SQLERRM,
                             0,
                             null,
                             null);
    end;
    insert into ldr_infa_cb.fct_dealrisk
      (ground,
       reserve_rate,
       deal_code,
       riskcat_code,
       dt,
       rec_status,
       sysmoment,
       ext_file,
       RISKCAT_CODE_TYPERISK)
      select null ground,
             (select qb_dwh_utils.numbertochar(rsb_struct.getdouble(nt.t_text))
                from dnotetext_dbt nt
               where nt.t_objecttype = 101
                 and nt.t_notekind = case
                       when at.t_groupid = 13 then
                        3
                       when at.t_groupid = 14 then
                        6
                       when at.t_groupid = 15 then
                        8
                     end
                 and nt.t_documentid = at.t_object) reserve_rate,
             to_char(to_number(at.t_object)) || '#TCK' deal_code,
             case
               when atr.t_nameobject = '1' then
                '9999#SOFRXXX#1'
               when atr.t_nameobject = '2' then
                '9999#SOFRXXX#2'
               when atr.t_nameobject = '3' then
                '9999#SOFRXXX#3'
               when atr.t_nameobject = '4' then
                '9999#SOFRXXX#4'
               else
                '9999#SOFRXXX#5'
             end riskcat_code,
             qb_dwh_utils.datetochar(at.t_validfromdate) dt,
             '0' rec_status,
             dwhSYSMOMENT,
             dwhEXT_FILE,
             cCODE_TYPERISK RISKCAT_CODE_TYPERISK --в данном отборе только категории качества (группы риска)
        from dobjatcor_dbt at
       inner join dobjattr_dbt atr
          on (at.t_objecttype = atr.t_objecttype and at.t_groupid = atr.t_groupid and
             at.t_attrid = atr.t_attrid)
       where at.t_objecttype = 101
         and at.t_groupid in (13, 14, 15)
         and in_date between at.t_validfromdate and at.t_validtodate;
commit;         

    -- Удаление атрибутов у которых дата установки атрибута меньше даты открытия ц/б
    qb_bp_utils.SetError(EventID,
                   '',
                   to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Удаление лишних атрибутов',
                   2,
                   null,
                   null);
    delete from ldr_infa_cb.fct_security_attr atr
     where exists
     (select 1
        from ldr_infa_cb.det_security sec
       where sec.code = atr.security_code
         and to_date(sec.dt, 'dd-mm-yyyy') > to_date(atr.dt, 'dd-mm-yyyy'));
commit;         

    delete from ldr_infa_cb.ass_deal_cat_val where not exists (select 1 from ldr_infa_cb.fct_deal where code = deal_code ); commit;
    delete from ldr_infa_cb.fct_deal_indicator where not exists (select 1 from ldr_infa_cb.fct_deal where code = deal_code ); commit;
    -- Очистка дублей
    delete from ldr_infa_cb.det_roleaccount_deal
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by code,  name order by code) rn
                               from ldr_infa_cb.det_roleaccount_deal
                             )
                      where rn > 1);
commit;                      
    delete from ldr_infa_cb.FCT_SEC_SELL_RESULT
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by lot_num, deal_code, security_code, dt  order by dt) rn
                               from ldr_infa_cb.FCT_SEC_SELL_RESULT
                             )
                      where rn > 1);
commit;                      
    delete from ldr_infa_cb.fct_security_attr
     where rowid in (select rowid
                       from (select rowid,
                                    code_security_attr,
                                    row_number() over (partition by security_code, code_security_attr, dt  order by dt) rn,
                                    row_number() over (partition by security_code, code_security_attr, date_value, dt  order by dt) rn_do
                               from ldr_infa_cb.fct_security_attr 
                             )
                      where (code_security_attr = 'DATE_OFFER' and rn_do > 1)
                          or (code_security_attr != 'DATE_OFFER' and rn > 1));
commit;                      
    delete from ldr_infa_cb.fct_security_attr_multi
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by security_code, sec_portfolio_code, code_security_attr, value, dt  order by dt) rn
                               from ldr_infa_cb.fct_security_attr_multi
                             )
                      where rn > 1);
commit;                      
    delete from ldr_infa_cb.det_deal_cat_val
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by deal_cat_code, code_deal_cat_val, dt  order by dt) rn
                               from ldr_infa_cb.det_deal_cat_val
                             )
                      where rn > 1);
commit;                      
    delete from ldr_infa_cb.ass_accountdeal
     where rowid in (select rowid
                       from (select rowid,
                                    row_number() over (partition by account_code, deal_code, roleaccount_deal_code, dt  order by dt) rn
                               from ldr_infa_cb.ass_accountdeal
                             )
                      where rn > 1);
commit;

    -- Установка типа доходности для СВ
    update ldr_infa_cb.det_bill
       set typeprofit = '1'
     where security_code in
           (select security_code
              from (select db.security_code,
                           round(to_number(ds.nominal,
                                           '99999999999999999999.999999999999'),
                                 2) nom_cost,
                           round(deals.bccost, 2) sale_cost,
                           ds.finstrcurnom_finstr_code nom_fi,
                           deals.ficode sale_fi
                      from ldr_infa_cb.det_bill db
                     inner join ldr_infa_cb.det_security ds
                        on (db.security_code = ds.code)
                     inner join (select bccost,
                                       bcfi,
                                       bcid,
                                       fi.t_iso_number ficode
                                  from (select bck.t_bcid bcid,
                                               lnk.t_bccost bccost,
                                               lnk.t_bccfi bcfi,
                                               row_number() over(partition by bck.t_bcid order by bck.t_id desc) rnk
                                          from dvsbnrbck_dbt bck,
                                               doprdocs_dbt  docs,
                                               dvsordlnk_dbt lnk,
                                               doproper_dbt  oper
                                         where lnk.t_dockind = oper.t_dockind
                                           and lpad(lnk.t_contractid, 10, '0') =
                                               oper.t_documentid
                                           and oper.t_dockind in (select v.value
                                                                    from qb_dwh_const4exp c
                                                                   inner join qb_dwh_const4exp_val v
                                                                      on (c.id = v.id)
                                                                   where c.name = cOPER_DOCKIND)
                                           and oper.t_id_operation =
                                               docs.t_id_operation
                                           and docs.t_dockind = n191
                                           and docs.t_documentid =
                                               lpad(bck.t_id, 10, '0')
                                           and bck.t_bcstatus = chr88
                                           and lnk.t_bcid = bck.t_bcid
                                           and bck.t_newabcstatus = n20)
                                 inner join dfininstr_dbt fi
                                    on (bcfi = fi.t_fiid)
                                 where rnk = 1) deals
                        on (to_number(regexp_replace(db.security_code, '#BNR$')) =
                           deals.bcid)
                       inner join dvsbanner_dbt bn
                         on (to_number(regexp_replace(db.security_code, '#BNR$')) =
                           bn.t_bcid)
                     where db.typeprofit = v2
                       and db.discount is null
                       and exists (select 1 from ddp_dep_dbt dp where dp.t_partyid = bn.t_issuer ))
             where nom_cost = sale_cost
               and nom_fi = sale_fi);
commit;
    -- Установка типа доходности для УВ
    update ldr_infa_cb.det_bill
       set typeprofit = '1'
     where security_code in
           (select security_code
              from (select db.security_code,
                           round(to_number(ds.nominal,
                                           '99999999999999999999.999999999999'),
                                 2) nom_cost,
                           round(deals.buy_cost, 2) buy_cost,
                           ds.finstrcurnom_finstr_code nom_fi,
                           deals.ficode buy_fi
                      from ldr_infa_cb.det_bill db
                     inner join ldr_infa_cb.det_security ds
                        on (db.security_code = ds.code)
                     left join (select buy_cost,
                                       buy_fi,
                                       bcid,
                                       fi.t_iso_number ficode
                                  from (select bn.t_bcid bcid,
                                                leg.t_principal buy_cost,
                                                leg.t_cfi buy_fi,
                                                row_number() over (partition by bn.t_bcid order by tick.t_dealid desc) rnk
                                          from  dvsbanner_dbt bn
                                          inner join ddl_leg_dbt leg
                                            on (bn.t_bcid = leg.t_dealid)
                                          inner join dvsordlnk_dbt lnk
                                            on (bn.t_bcid = lnk.t_bcid)
                                          inner join ddl_tick_dbt tick
                                            on (lnk.t_contractid = tick.t_dealid and lnk.t_dockind = tick.t_bofficekind)
                                          inner join dfininstr_dbt fi
                                            on (leg.t_cfi = fi.t_fiid)
                                          where leg.t_legid = n0 and leg.t_legkind = n1
                                            and tick.t_bofficekind in (select v.value
                                                                         from qb_dwh_const4exp c
                                                                        inner join qb_dwh_const4exp_val v
                                                                           on (c.id = v.id)
                                                                        where c.name = cDEALSKIND_DBILL)
                                            and tick.t_dealtype = n12401
                                            and tick.t_dealdate <= in_date
                                           )
                                 inner join dfininstr_dbt fi
                                    on (buy_fi = fi.t_fiid)
                                 where rnk = n1) deals
                        on (to_number(regexp_replace(db.security_code, '#BNR$')) =
                           deals.bcid)
                       inner join dvsbanner_dbt bn
                         on (to_number(regexp_replace(db.security_code, '#BNR$')) =
                           bn.t_bcid)
                     where db.typeprofit = v2
                       and db.discount is null
                       and not exists (select 1 from ddp_dep_dbt dp where dp.t_partyid = bn.t_issuer )
                       )
             where nom_cost = buy_cost
               and nom_fi = buy_fi);
commit;

    --Завершим выгрузку сделок
    qb_bp_utils.EndEvent(EventID, null);
    --commit;
  end;

  procedure export_Commissions(in_department in number,
                               in_date       in date,
                               procid        in number) is
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(10);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
  begin
    startevent(cEvent_EXPORT_Commissions, procid, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             QB_DWH_EXPORT.cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDT, in_date);

    qb_dwh_export.InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE);
    qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка данных по комиссиям',
                         2,
                         null,
                         null);
    begin
    for rec in (
      SELECT /*+ leading(fi,tick) use_nl(tick) index(tick DDL_TICK_DBT_IDXE)*/ 
        distinct cm.t_code, cm.t_name nm1, cm.t_name nm2, cm.t_name nm3
          FROM DDLCOMIS_DBT DLC
         inner join DSFCOMISS_DBT CM
            on CM.T_FEETYPE = DLC.T_FEETYPE
           AND CM.T_NUMBER = DLC.T_COMNUMBER
         inner join ddl_tick_dbt tick
            on (dlc.t_docid = tick.t_dealid)
         inner join dfininstr_dbt fi
            on (tick.t_pfi = fi.t_fiid)
         WHERE /*DLC.T_DOCKIND in (101, 4830) --(ПД. Покупка/продажа ц/б, Покупка/продажа СЭБ)
           and */CM.t_receiverid = n2          --(Получатель комиссии: биржа)
           and tick.t_dealdate <= in_date
           and tick.t_bofficekind  in (select v.value
                                         from qb_dwh_const4exp c
                                        inner join qb_dwh_const4exp_val v
                                           on (c.id = v.id)
                                        where c.name = cDEALSKIND_SEC)
           and tick.t_clientid = n_1         --(Собственные сделки)
           and fi.t_avoirkind in (select /*+ PRECOMPUTE_SUBQUERY */ v.value
                                    from qb_dwh_const4exp c
                                   inner join qb_dwh_const4exp_val v
                                      on (c.id = v.id)
                                   where c.name = cSECKIND_ALL)) 
    loop
      insert into ldr_infa_cb.det_commission 
        (code, name, fullname, shortname, type, dt, rec_status, sysmoment, ext_file)
        values (rec.t_code, rec.nm1, rec.nm2, rec.nm3, 
          '0', qb_dwh_utils.DateToChar(firstDate), 
          dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
      commit;                                   
    end loop;  
    insert into ldr_infa_cb.det_commission(code, name, fullname, shortname, type, dt, rec_status, sysmoment, ext_file)
      values ('Выпуск', 'Расходы, связанные с выпуском ценных бумаг ', 'Расходы, связанные с выпуском ценных бумаг ', 'Расходы, связанные с выпуском ц/б ', '1', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;      
    insert into ldr_infa_cb.det_comm_cat(code_cat, name_cat, dt, rec_status, sysmoment, ext_file)
      values ('Тип взимания', 'Тип взимания комиссии', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;      
    insert into ldr_infa_cb.det_comm_cat_val(code_cat_val, name_cat_val, comm_code_cat, dt, rec_status, sysmoment, ext_file)
      values ('Единовременная', 'Единовременная', 'Тип взимания', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;      
    insert into ldr_infa_cb.det_comm_cat_val(code_cat_val, name_cat_val, comm_code_cat, dt, rec_status, sysmoment, ext_file)
      values ('Периодическая', 'Периодическая', 'Тип взимания', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;
    for rec in (
      SELECT /*+ leading(fi,tick) use_nl(tick) index(tick DDL_TICK_DBT_IDXE)*/ distinct cm.t_code,
               case when cm.t_feetype = 3 then
                 'Единовременная'
               when cm.t_feetype = 1 then
                  'Периодическая'
               else
                 '-1'
               end feetype
          FROM DDLCOMIS_DBT DLC
         inner join DSFCOMISS_DBT CM
            on CM.T_FEETYPE = DLC.T_FEETYPE
           AND CM.T_NUMBER = DLC.T_COMNUMBER
         inner join ddl_tick_dbt tick
            on (dlc.t_docid = tick.t_dealid)
         inner join dfininstr_dbt fi
            on (tick.t_pfi = fi.t_fiid)
         WHERE /*DLC.T_DOCKIND in (101, 4830) --(ПД. Покупка/продажа ц/б, Покупка/продажа СЭБ)
           and */CM.t_receiverid = n2          --(Получатель комиссии: биржа)
           and tick.t_dealdate <= in_date
           and tick.t_bofficekind  in (select v.value
                                         from qb_dwh_const4exp c
                                        inner join qb_dwh_const4exp_val v
                                           on (c.id = v.id)
                                        where c.name = cDEALSKIND_SEC)
           and tick.t_clientid = n_1         --(Собственные сделки)
           and fi.t_avoirkind in (select /*+ PRECOMPUTE_SUBQUERY */ v.value
                                    from qb_dwh_const4exp c
                                   inner join qb_dwh_const4exp_val v
                                      on (c.id = v.id)
                                   where c.name = cSECKIND_ALL )
      )  
    loop
      insert into ldr_infa_cb.ass_comm_cat_val(
        commission_code, comm_code_cat, comm_code_cat_val, dt, 
        rec_status, sysmoment, ext_file) values
              (rec.t_code,
               rec.feetype,
               'Тип взимания',
               qb_dwh_utils.DateToChar(firstDate),
               dwhRecStatus,
               dwhSysMoment,
               dwhEXT_FILE);
        commit;
    end loop;    
     insert into ldr_infa_cb.ass_comm_cat_val(commission_code, comm_code_cat, comm_code_cat_val, dt, rec_status, sysmoment, ext_file)
       values ('Выпуск', 'Периодическая', 'Тип взимания', qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;       
     exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при заполнении справочника коммиссий: ' || SQLERRM,
                              0,
                              null,
                              null);
     end;
     begin
       insert into ldr_infa_cb.fct_deal_commission(
         deal_code, commission_code,com_abs, com_perc, com_min, 
         com_max, note, dt, sysmoment, rec_status, ext_file)
        SELECT /*+ leading(fi,tick) use_nl(tick) index(tick DDL_TICK_DBT_IDXE)*/  
          to_char(tick.t_dealid) || '#TCK' deal_code,
              cm.t_code commission_code,
              qb_dwh_utils.NumberToChar(round(dlc.t_sum, 2), 2) com_abs,
              null,
              null,
              null,
              null,
              qb_dwh_utils.DateToChar(dlc.t_planpaydate) dt,
              dwhSysMoment,
              dwhRecStatus,
              dwhEXT_FILE
         FROM DDLCOMIS_DBT DLC
        inner join DSFCOMISS_DBT CM
           on CM.T_FEETYPE = DLC.T_FEETYPE
          AND CM.T_NUMBER = DLC.T_COMNUMBER
        inner join ddl_tick_dbt tick
           on (dlc.t_docid = tick.t_dealid)
        inner join dfininstr_dbt fi
           on (tick.t_pfi = fi.t_fiid)
        WHERE DLC.T_DOCKIND in (select v.value
                                  from qb_dwh_const4exp c
                                 inner join qb_dwh_const4exp_val v
                                    on (c.id = v.id)
                                 where c.name = cCOM_DOCKIND) --(ПД. Покупка/продажа ц/б, Покупка/продажа СЭБ)
          and CM.t_receiverid = n2          --(Получатель комиссии: биржа)
          and tick.t_dealdate <= in_date
          and tick.t_bofficekind  in (select v.value
                                        from qb_dwh_const4exp c
                                       inner join qb_dwh_const4exp_val v
                                          on (c.id = v.id)
                                       where c.name = cDEALSKIND_SEC)
          and tick.t_clientid = n_1         --(Собственные сделки)
          and fi.t_avoirkind in (select /*+ PRECOMPUTE_SUBQUERY */ v.value
                                   from qb_dwh_const4exp c
                                  inner join qb_dwh_const4exp_val v
                                     on (c.id = v.id)
                                  where c.name = cSECKIND_ALL)
        order by tick.t_dealid, dlc.t_planpaydate ;
     commit;
     for rec in (
       SELECT to_char(fi.t_fiid) || '#FIN' fin,
              qb_dwh_utils.NumberToChar(round(dlc.t_sum, 2), 2) chsum,
              qb_dwh_utils.DateToChar(dlc.t_planpaydate) dt
         FROM DDLCOMIS_DBT DLC
        inner join DSFCOMISS_DBT CM
           on CM.T_FEETYPE = DLC.T_FEETYPE
          AND CM.T_NUMBER = DLC.T_COMNUMBER
        inner join dfininstr_dbt fi
           on fi.t_fiid = DLC.t_docid
        inner join Davrkinds_dbt k
           on k.t_fi_kind = n2
          and k.t_avoirkind = fi.t_avoirkind
        WHERE DLC.T_DOCKIND = n5 --(ПД.Анкета выпуска ЦБ)
          and DLC.t_receiverID = n2 --(Получатель комиссии: биржа)
          and dlc.t_planpaydate <= in_date
       ) 
     loop
       insert into ldr_infa_cb.fct_sec_commission(
         security_code, commission_code, commission, dt, 
         sysmoment, rec_status, ext_file)
         values (rec.fin,
              'Выпуск',
              rec.chsum,
              rec.dt,
              dwhSysMoment,
              dwhRecStatus,
              dwhEXT_FILE
         );
       commit;
     end loop;
commit;          
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при заполнении списка коммиссий: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;
    qb_bp_utils.EndEvent(EventID, null);
    --commit;
  end;

  ------------------------------------------------------
  -- Выгрузка данных по ценным бумагам
  ------------------------------------------------------
  procedure RunExport(in_Date date, procid number, export_mode number default 0) is
    vLdrClear varchar2(400);
    is_9996 number;
  begin

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
  -- EXECUTE IMMEDIATE 'ALTER sequence DQB_SEQ_PROCESS cache 50';
  
  select case when T_LINTVALUE = 88 then 1 else 0 end
  into is_9996
  from dregval_dbt where T_KEYID = (select max(T_KEYID) from DREGPARM_DBT WHERE T_NAME = 'BIQ-9996');

if is_9996 = 1 then
---9996  
    vLdrClear := nvl(RSB_Common.GetRegStrValue('РСХБ\ИНТЕГРАЦИЯ\ЦХД\TRUNCATE_LDRINFA'),'YES');
    if (export_mode in (0, 1, 2) ) then export_SecurKIND(procid); end if;
    if (export_mode in (0, 1) ) then
      if (vLdrClear = 'YES') then
        clearSecurData(1);
        commit;
      end if;
      export_Secur_9996(1, in_Date, procid);
      commit;
    end if;
    if (export_mode in (0, 2) ) then
      if (vLdrClear = 'YES') then
        clearDealsData(1);
        commit;
      end if;
      export_Deals_9996(1, in_Date, procid);
      commit;
    end if;
    if (export_mode in (0, 3) ) then
      if (vLdrClear = 'YES') then
        clearCommData(1);
        commit;
      end if;
      export_Commissions(1, in_Date, procid);
      commit;
    end if;
else
--standart
    vLdrClear := nvl(RSB_Common.GetRegStrValue('РСХБ\ИНТЕГРАЦИЯ\ЦХД\TRUNCATE_LDRINFA'),'YES');
    if (export_mode in (0, 1) ) then
      if (vLdrClear = 'YES') then
        clearSecurData(1);
        commit;
      end if;
      export_Secur(1, in_Date, procid);
      commit;
    end if;
    if (export_mode in (0, 2) ) then
      if (vLdrClear = 'YES') then
        clearDealsData(1);
        commit;
      end if;
      export_Deals(1, in_Date, procid);
      commit;
    end if;
    if (export_mode in (0, 3) ) then
      if (vLdrClear = 'YES') then
        clearCommData(1);
        commit;
      end if;
      export_Commissions(1, in_Date, procid);
      commit;
    end if;
end if;    
  end;

  procedure RunExport_9996(in_Date date, procid number, export_mode number default 0) is
    vLdrClear varchar2(400);
  begin

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
  -- EXECUTE IMMEDIATE 'ALTER sequence DQB_SEQ_PROCESS cache 50';

    vLdrClear := nvl(RSB_Common.GetRegStrValue('РСХБ\ИНТЕГРАЦИЯ\ЦХД\TRUNCATE_LDRINFA'),'YES');
    if (export_mode in (0, 1, 2) ) then export_SecurKIND(procid); end if;
    if (export_mode in (0, 1) ) then
      if (vLdrClear = 'YES') then
        clearSecurData(1);
        commit;
      end if;
      export_Secur_9996(1, in_Date, procid);
      commit;
    end if;
    if (export_mode in (0, 2) ) then
      if (vLdrClear = 'YES') then
        clearDealsData(1);
        commit;
      end if;
      export_Deals_9996(1, in_Date, procid);
      commit;
    end if;
    if (export_mode in (0, 3) ) then
      if (vLdrClear = 'YES') then
        clearCommData(1);
        commit;
      end if;
      export_Commissions(1, in_Date, procid);
      commit;
    end if;
  end;

end QB_DWH_EXPORT_SECUR;
/

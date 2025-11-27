create or replace package body QB_DWH_EXPORT_PFI is

  firstDate          constant date := to_date('01011980', 'ddmmyyyy');
  emptDate           constant date := to_date('01010001', 'ddmmyyyy');
  perpDWHDate        constant date := to_date('01013001', 'ddmmyyyy');
--  minDate            constant date := to_date('01012019', 'ddmmyyyy');
--  maxDate            constant date := to_date('31129999', 'ddmmyyyy');
  
  BIQ_8474           constant number := 1; --Рубильник для BIQ_8474. 1 = включено, оставляем нужную строку и компилируем. 
  --BIQ_8474           constant number := 0; --Рубильник для BIQ_8474. 0 = ВЫключено.

  --разовая выгрузка с 24 января 2023 года по 16 сентября 2023 года DEF-42824
  DateForStartLoadFrom24012023   constant date := 
    --to_date('11092023', 'ddmmyyyy');
    to_date('17092023', 'ddmmyyyy'); 
    
  DateForStartLoadFrom17092023    constant date := 
    to_date('15042024', 'ddmmyyyy'); 

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

  ------------------------------------------------------
  -- очистка данных по ПФИ
  ------------------------------------------------------
  procedure clearPFIData(in_Type number default 0) is
  begin
    execute immediate 'truncate table ldr_infa_pfi.det_finstr';
    execute immediate 'truncate table ldr_infa_pfi.det_contract';
    execute immediate 'truncate table ldr_infa_pfi.fct_finstr_rate';
    execute immediate 'truncate table ldr_infa_pfi.det_exchange';
    execute immediate 'truncate table ldr_infa_pfi.det_index';
    execute immediate 'truncate table ldr_infa_pfi.det_type_rate';
    execute immediate 'truncate table ldr_infa_pfi.det_procbase';
    execute immediate 'truncate table ldr_infa_pfi.det_rate';
    execute immediate 'truncate table ldr_infa_pfi.det_rate_pair';
    execute immediate 'truncate table ldr_infa_pfi.det_currency_pair';
    execute immediate 'truncate table ldr_infa_pfi.det_roleaccount_deal';
    if (in_type <> 2) then
      execute immediate 'truncate table ldr_infa_pfi.tmp_acctrn';
      execute immediate 'truncate table ldr_infa_pfi.ass_accountdeal';
      execute immediate 'truncate table ldr_infa_pfi.fct_deal_rst';
    end if;
    execute immediate 'truncate table ldr_infa_pfi.det_deal_cat';
    execute immediate 'truncate table ldr_infa_pfi.det_deal_cat_val';
    execute immediate 'truncate table ldr_infa_pfi.ass_deal_cat_val';
    execute immediate 'truncate table ldr_infa_pfi.det_deal_typeattr';
    execute immediate 'truncate table ldr_infa_pfi.fct_deal_indicator';
    execute immediate 'truncate table ldr_infa_pfi.det_typeattr';
    execute immediate 'truncate table ldr_infa_pfi.fct_subj_indicator';
    execute immediate 'truncate table ldr_infa_pfi.fct_deal';
    execute immediate 'truncate table ldr_infa_pfi.ass_contract_deal';
    execute immediate 'truncate table ldr_infa_pfi.fct_option';
    execute immediate 'truncate table ldr_infa_pfi.fct_spotforward';
    execute immediate 'truncate table ldr_infa_pfi.fct_futures';
    execute immediate 'truncate table ldr_infa_pfi.fct_fxswap';
    execute immediate 'truncate table ldr_infa_pfi.fct_irs';
    execute immediate 'truncate table ldr_infa_pfi.fct_banknote';
    --BIQ-8474
    execute immediate 'truncate table ldr_infa_pfi.fct_procrate_deal'; 
    execute immediate 'truncate table ldr_infa_pfi.det_kindprocrate';
    execute immediate 'truncate table ldr_infa_pfi.det_subkindprocrate';
    --BIQ-10007
    execute immediate 'truncate table LDR_INFA_PFI.DET_ACC_ASS_KIND';
    execute immediate 'truncate table LDR_INFA_PFI.DET_ACCOUNT_SOFR';

    --исключение для разовая выгрузка с 24 января 2023 года по 16 сентября 2023 года DEF-42824
    if (trunc(sysdate) <> trunc(DateForStartLoadFrom24012023)) then
      execute immediate 'truncate table LDR_INFA_PFI.FCT_ACCOUNT_SOFR';
    end if;
    execute immediate 'truncate table LDR_INFA_PFI.TMP_ACC306';
    execute immediate 'truncate table LDR_INFA_PFI.TMP_ACC_SUBJ_306';
    execute immediate 'truncate table LDR_INFA_PFI.ASS_DET_ACCOUNT_SOFR';
        
/*    if in_type = 0 then
      delete from DQB_BP_EVENT_ERROR_DBT;  -- Ошибки произошедьшии при событии
      delete from DQB_BP_EVENT_ATTR_DBT;   -- Аттрибуты
      delete from DQB_BP_EVENT_DBT;        -- События
    end if;*/
  end;


function hasEmptyTables
   return boolean
is
   cnt   pls_integer;
begin
   select COUNT (*)
     into cnt
     from (select 'FCT_SPOTFORWARD' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_SPOTFORWARD)
           union
           select 'FCT_OPTION' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_OPTION)
           union
           select 'FCT_FXSWAP' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_FXSWAP)
           union
           select 'FCT_FUTURES' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_FUTURES)
           union
           select 'FCT_FINSTR_RATE' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_FINSTR_RATE)
           union
           select 'FCT_DEAL_RST' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_DEAL_RST)
           union
           select 'FCT_DEAL_INDICATOR' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_DEAL_INDICATOR)
           union
           select 'DET_TYPE_RATE' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_TYPE_RATE)
           union
           select 'DET_TYPEATTR' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_TYPEATTR)
           union
           select 'DET_ROLEACCOUNT_DEAL' FL
             from DUAL
            where not exists
                     (select 1 from ldr_infa_pfi.DET_ROLEACCOUNT_DEAL)
           union
           select 'DET_RATE_PAIR' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_RATE_PAIR)
           union
           select 'DET_RATE' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_RATE)
           union
           select 'DET_INDEX' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_INDEX)
           union
           select 'DET_FINSTR' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_FINSTR)
           union
           select 'DET_EXCHANGE' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_EXCHANGE)
           union
           select 'DET_DEAL_TYPEATTR' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_DEAL_TYPEATTR)
           union
           select 'DET_DEAL_CAT_VAL' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_DEAL_CAT_VAL)
           union
           select 'DET_DEAL_CAT' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_DEAL_CAT)
           union
           select 'DET_CURRENCY_PAIR' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_CURRENCY_PAIR)
           union
           select 'DET_CONTRACT' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_CONTRACT)
           union
           select 'ASS_DEAL_CAT_VAL' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.ASS_DEAL_CAT_VAL)
           union
           select 'ASS_CONTRACT_DEAL' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.ASS_CONTRACT_DEAL)
           union
           select 'ASS_ACCOUNTDEAL' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.ASS_ACCOUNTDEAL)
           union
           select 'FCT_SUBJ_INDICATOR' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_SUBJ_INDICATOR)
           union
           select 'FCT_IRS' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_IRS)
           union
           select 'FCT_DEAL' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_DEAL)
           union
           select 'FCT_BANKNOTE' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.FCT_BANKNOTE)
           union
           select 'ASS_DET_ACCOUNT_SOFR' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.ASS_DET_ACCOUNT_SOFR)
           union
           select 'DET_ACCOUNT_SOFR' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_ACCOUNT_SOFR)
           union
           select 'DET_PROCBASE' FL
             from DUAL
            where not exists (select 1 from ldr_infa_pfi.DET_PROCBASE));

   if (cnt > 0)
   then
      return true;
   end if;

   return false;
end;

  procedure export_pfi(in_department in number,
                               in_date       in date,
                               procid        in number,
                               exp_mode      in number) is
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(20);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
    vDateLastOD date;
    vDateBeg    date;
    vSysmmnt     varchar2(20);
    prev_in_date date:=in_date-1; --rsi_rsbcalendar.getdateafterworkday(in_date,-1);
  begin
    startevent(cEvent_EXPORT_PFI, procid, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             QB_DWH_EXPORT.cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDT, in_date);

    qb_dwh_export.InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE, 2);
    qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка данных по ПФИ',
                         2,
                         null,
                         null);
    begin
           -- Выгрузка в DET_FINSTR
      qb_bp_utils.SetError(EventID,
                           '',
                           to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справчника ФИ',
                           2,
                           null,
                           null);
      Insert into ldr_infa_pfi.det_finstr(finstr_code, finstr_name, finstr_name_s, typefinstr, dt, rec_status, sysmoment, ext_file)
      (
      -- Биржевые опционы
      select '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN' FINSTR_CODE,
             fi.t_name FINSTR_NAME,
             substr(fi.t_definition,1,50) FINSTR_NAME_S,
             '2' TYPEFINSTR,
             qb_dwh_utils.DateToChar(decode(fi.t_issued, to_date('01010001','ddmmyyyy'), to_date('01011980','ddmmyyyy'), fi.t_issued)) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from dfininstr_dbt fi
        where fi.t_fi_kind = 4
          and fi.t_avoirkind = 2
          and (exists (select 1
                         from ddvdeal_dbt dv
                        where dv.t_date <= in_date
                          and dv.t_client = -1         -- исключаем клиентские сделки
                          and dv.t_istrust = chr(0)
                          and dv.t_fiid = fi.t_fiid)or
               exists (select 1
                         from dfideriv_dbt fdr
                        where fdr.t_incirculationdate <= in_date
                          and fdr.t_fiid = fi.t_fiid)or
               exists (select 1
                         from dfideriv_dbt fdr
                        where fdr.t_incirculationdate <= in_date
                          and fdr.t_strikefiid = fi.t_fiid))
      -- Внебиржевые опционы и прочие ПФИ
      union all
      select '0000#SOFRXXX#' || to_char(fi2.t_fiid) || '#FIN' FINSTR_CODE,
             fi2.t_name FINSTR_NAME,
             substr(fi2.t_definition,1,50) FINSTR_NAME_S,
             case fi2.t_fi_kind
               when 1 then
                 '1'
               when 6 then
                 '1'
               when 2 then
                 '2'
               when 4 then
                 '2'
               when 3 then
                 '5'
               when 7 then
                 '4'
               when 8 then
                 '9'
             end TYPEFINSTR,
             qb_dwh_utils.DateToChar(
             case
               when decode(fi2.t_issued, to_date('0101001','ddmmyyyy'), to_date('31129999','ddmmyyyy'), fi2.t_issued) <
                    decode(avr.t_incirculationdate, to_date('0101001','ddmmyyyy'), to_date('31129999','ddmmyyyy'), avr.t_incirculationdate) and
                    decode(fi2.t_issued, to_date('0101001','ddmmyyyy'), to_date('31129999','ddmmyyyy'), fi2.t_issued) <
                    decode(avr.t_begplacementdate, to_date('0101001','ddmmyyyy'), to_date('31129999','ddmmyyyy'), avr.t_begplacementdate) then
                    decode(fi2.t_issued, to_date('0101001','ddmmyyyy'), to_date('01011980','ddmmyyyy'), fi2.t_issued)
               when decode(avr.t_incirculationdate, to_date('0101001','ddmmyyyy'), to_date('31129999','ddmmyyyy'), avr.t_incirculationdate) <
                    decode(fi2.t_issued, to_date('0101001','ddmmyyyy'), to_date('31129999','ddmmyyyy'), fi2.t_issued) and
                    decode(avr.t_incirculationdate, to_date('0101001','ddmmyyyy'), to_date('31129999','ddmmyyyy'), avr.t_incirculationdate) <
                    decode(avr.t_begplacementdate, to_date('0101001','ddmmyyyy'), to_date('31129999','ddmmyyyy'), avr.t_begplacementdate) then
                 decode(avr.t_incirculationdate, to_date('0101001','ddmmyyyy'), to_date('01011980','ddmmyyyy'), avr.t_incirculationdate)
               else
                 decode(avr.t_begplacementdate, to_date('0101001','ddmmyyyy'), to_date('01011980','ddmmyyyy'), avr.t_begplacementdate)
             end) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from dfininstr_dbt fi2
        inner join davoiriss_dbt avr
          on (fi2.t_fiid = avr.t_fiid)
        where fi2.t_fiid in (select nfi0.t_fiid
                               from ddvndeal_dbt dvn
                              inner join ddp_dep_dbt dp
                                 on (dvn.t_department = dp.t_code)
                              inner join ddvnfi_dbt nfi0
                                 on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                              inner join dfininstr_dbt fi
                                on (nfi0.t_fiid = fi.t_fiid)
                              where dvn.t_dvkind not in (3, 6, 4)
                                and dvn.t_date <= in_date
                                and fi.t_fi_kind not in (1, 6)
                              group by dvn.t_dvkind, nfi0.t_fiid
                              -- форварды из БОЦБ. BIC-8474 
                              union all
                              select leg0.t_pfi
                               from ddl_tick_dbt tic
                              inner join ddp_dep_dbt dp
                                 on (tic.t_department = dp.t_code)
                              inner join ddl_leg_dbt leg0
                                 on (tic.t_dealid = leg0.t_dealid and leg0.t_legkind = 0)
                              inner join dfininstr_dbt fi
                                on (leg0.t_pfi = fi.t_fiid)
                              where tic.t_bofficekind = 101 and tic.t_dealtype in (12183, 12193) and tic.t_ispfi = chr(88) and substr(tic.t_dealcode, 1, 2) = 'Д/'
                                and tic.t_dealdate <= in_date 
                              and BIQ_8474 = 1
                              group by leg0.t_pfi
                              )
      -- Внебиржевые опционы на форвард DEF-47223
       union all
       select 
         dvn.t_id ||'#DVN' FINSTR_CODE,
         'Базовый форвард опциона '|| dvn.t_code FINSTR_NAME,
         substr( 'Базовый форвард опциона '||dvn .t_code,1,50) FINSTR_NAME_S,
         '7' TYPEFINSTR,
         qb_dwh_utils.DateToChar(dvn.t_date ) DT,
         '0' REC_STATUS,
         to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
       from dfininstr_dbt fi2
       inner join davoiriss_dbt avr
          on (fi2.t_fiid = avr.t_fiid)
       inner join 
                 ( select nfi0.t_fiid, dvn.t_id, dvn.t_code,dvn.t_date
                     from ddvndeal_dbt dvn
                     inner join ddp_dep_dbt dp
                               on (dvn.t_department = dp.t_code)
                     inner join ddvnfi_dbt nfi0
                               on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 1 and dvn.t_dvkind = 2)
                     inner join dfininstr_dbt fi
                               on (nfi0.t_fiid = fi.t_fiid)
                    where dvn.t_dvkind not in (3, 6, 4)
                        and dvn.t_date <= in_date
                        and fi.t_fi_kind not in (1, 6)
                  group by nfi0.t_fiid, dvn.t_id, dvn.t_code,dvn.t_date                    
                      ) dvn
         on dvn.t_fiid = fi2.t_fiid                           
        
      -- Биржевые фьючерсы
      union all
      select '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN' FINSTR_CODE,
             fi.t_name FINSTR_NAME,
             substr(fi.t_definition,1,50) FINSTR_NAME_S,
             '2' TYPEFINSTR,
             qb_dwh_utils.DateToChar(decode(fi.t_issued, to_date('01010001','ddmmyyyy'), to_date('01011980','ddmmyyyy'), fi.t_issued)) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from dfininstr_dbt fi
        where fi.t_fi_kind = 4
          and fi.t_avoirkind = 1
          and (exists (select 1
                         from ddvdeal_dbt dv
                        where dv.t_date <= in_date
                          and dv.t_client = -1              -- исключаем клиентские сделки
                          and dv.t_istrust = chr(0)
                          and dv.t_fiid = fi.t_fiid)or
               exists (select 1
                         from dfideriv_dbt fdr
                        where fdr.t_incirculationdate <= in_date
                          and fdr.t_fiid = fi.t_fiid)or
               exists (select 1
                         from dfideriv_dbt fdr
                        where fdr.t_incirculationdate <= in_date
                          and fdr.t_strikefiid = fi.t_fiid))
      -- Валютные пары
      union all
      select distinct '0000#SOFRXXX#' || FINSTR_CODE,
                      'Валютная пара '||FINSTR_CODE FINSTR_NAME,
                      substr(FINSTR_CODE,1,50) FINSTR_NAME_S,
                      '3' TYPEFINSTR,
                      qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
                      '0' REC_STATUS,
                      to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE 
                      from (
                        select decode(fi1.t_fi_kind, 6, fi1.t_codeinaccount, fi1.t_iso_number) || '#' || decode(fi2.t_fi_kind, 6, fi2.t_codeinaccount, fi2.t_iso_number) FINSTR_CODE
                          from ddvndeal_dbt dvn
                         inner join ddvnfi_dbt nfi0
                            on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                         inner join dfininstr_dbt fi1
                            on (nfi0.t_fiid = fi1.t_fiid and fi1.t_fi_kind = 1)  --BIQ-8474, только валюты, без металлов
                         inner join dfininstr_dbt fi2
                            on (nfi0.t_pricefiid = fi2.t_fiid)
                         where dvn.t_dvkind in (3, 6)
                           and dvn.t_date <= in_date
                           and BIQ_8474 = 1 -- BIQ_8474 включен 
                         group by decode(fi1.t_fi_kind, 6, fi1.t_codeinaccount, fi1.t_iso_number) || '#' || decode(fi2.t_fi_kind, 6, fi2.t_codeinaccount, fi2.t_iso_number)
                        union all
                        select decode(fi1.t_fi_kind, 6, fi1.t_codeinaccount, fi1.t_iso_number) || '#' || decode(fi2.t_fi_kind, 6, fi2.t_codeinaccount, fi2.t_iso_number) FINSTR_CODE
                          from ddvndeal_dbt dvn
                         inner join ddvnfi_dbt nfi0
                            on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                         inner join dfininstr_dbt fi1
                            on (nfi0.t_fiid = fi1.t_fiid)
                         inner join dfininstr_dbt fi2
                            on (nfi0.t_pricefiid = fi2.t_fiid)
                         where dvn.t_dvkind in (3, 6)
                           and dvn.t_date <= in_date
                           and BIQ_8474 = 0 -- BIQ_8474 ВЫключен
                         group by decode(fi1.t_fi_kind, 6, fi1.t_codeinaccount, fi1.t_iso_number) || '#' || decode(fi2.t_fi_kind, 6, fi2.t_codeinaccount, fi2.t_iso_number)
                        union all
                        select case
                                 when f1.t_fi_kind = 1 then
                                   f1.t_iso_number
                                 when f1.t_fi_kind = 6 then
                                   f1.t_codeinaccount
                                 else
                                   to_char(f1.t_fiid) || '#FIN'
                               end || '#' ||
                               case
                                 when f2.t_fi_kind = 1 then
                                   f2.t_iso_number
                                 when f2.t_fi_kind = 6 then
                                   f2.t_codeinaccount
                                 else
                                   to_char(f2.t_fiid) || '#FIN'
                               end FINSTR_CODE
                          from DDVNFI_DBT fi1, DDVNFI_DBT fi2, ddvndeal_dbt dvn, dfininstr_dbt f1, dfininstr_dbt f2
                         where dvn.t_id = fi2.t_dealid
                           and dvn.t_id = fi1.t_dealid
                           and fi1.t_type = 0 and fi2.t_type = 2
                           and dvn.t_dvkind = 4
                           and fi1.t_fiid = f1.t_fiid
                           and fi2.t_fiid = f2.t_fiid
                           and f1.t_fi_kind in (1,6)
                           and f2.t_fi_kind in (1,6)
                           and dvn.t_date <= in_date
                          )
         --Драгметаллы BIQ-8474,
       union all 
       select distinct '0000#SOFRXXX#' || FINSTR_CODE,
                      'Пара с драгметаллом '||FINSTR_CODE FINSTR_NAME,
                      substr(FINSTR_CODE,1,50) FINSTR_NAME_S,
                      '10' TYPEFINSTR,
                      qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
                      '0' REC_STATUS,
                      to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE 
                      from (
      select decode(fi1.t_fi_kind, 6, fi1.t_codeinaccount, fi1.t_iso_number) || '#' || decode(fi2.t_fi_kind, 6, fi2.t_codeinaccount, fi2.t_iso_number) FINSTR_CODE
        from ddvndeal_dbt dvn
       inner join ddvnfi_dbt nfi0
          on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
       inner join dfininstr_dbt fi1
          on (nfi0.t_fiid = fi1.t_fiid and fi1.t_fi_kind=6) 
       inner join dfininstr_dbt fi2
          on (nfi0.t_pricefiid = fi2.t_fiid)
       where dvn.t_dvkind in (3, 6) 
              and dvn.t_date <= in_date
              and BIQ_8474 = 1 -- BIQ_8474 включен
       group by decode(fi1.t_fi_kind, 6, fi1.t_codeinaccount, fi1.t_iso_number) || '#' || decode(fi2.t_fi_kind, 6, fi2.t_codeinaccount, fi2.t_iso_number)
       ) 
       
       -- Плавающие ставки процентного свопа
       union all
       select distinct '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN' FINSTR_CODE,
             fi.t_name FINSTR_NAME,
             substr(fi.t_definition,1,50) FINSTR_NAME_S,
             case fi.t_fi_kind
               when 1 then
                 '1'
               when 6 then
                 '1'
               when 2 then
                 '2'
               when 4 then
                 '2'
               when 3 then
                 '5'
               when 7 then
                 '4'
               when 8 then
                 '9'
             end TYPEFINSTR,
             qb_dwh_utils.DateToChar(decode(fi.t_issued, to_date('01010001','ddmmyyyy'), to_date('01011980','ddmmyyyy'), fi.t_issued)) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from dfininstr_dbt fi
       where fi.t_fiid in (select nfi0.t_rateid
                              from ddvndeal_dbt dvn
                             inner join ddvnfi_dbt nfi0
                                on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                             inner join ddvnfi_dbt nfi2
                                on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                             where dvn.t_dvkind = 4
                               and (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0)
                               and dvn.t_date <= in_date
                               and nfi0.t_rateid  > -1
                             group by nfi0.t_rateid
                            union all
                            select nfi2.t_rateid
                              from ddvndeal_dbt dvn
                             inner join ddvnfi_dbt nfi0
                                on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                             inner join ddvnfi_dbt nfi2
                                on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                             where dvn.t_dvkind = 4
                               and (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0)
                               and dvn.t_date <= in_date
                               and nfi2.t_rateid  > -1
                             group by nfi2.t_rateid
                            ) and not (fi.t_fi_kind = 3 /*and fi.t_facevaluefi <> -1*/)
      -- Фиксированные ставки валютно-процентного свопа
      union all
      select distinct * from (
      select distinct '0000#SOFRXXX#' || to_char(nfi0.t_rate,'FM990.099999999')||'#'||nfi0.t_ratepoint||'#FIX' FINSTR_CODE,
             'Фиксированная ставка '||to_char(nfi0.t_rate,'FM990.099999999')||'%, точность '|| nfi0.t_ratepoint FINSTR_NAME,
             'Фиксированная ставка '||to_char(nfi0.t_rate,'FM990.099999999')||'#'||nfi0.t_ratepoint||'#FIX' FINSTR_NAME_S,
             '5' TYPEFINSTR,
             qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from ddvndeal_dbt dvn
       inner join ddvnfi_dbt nfi0
          on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
       inner join ddvnfi_dbt nfi2
          on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
       where dvn.t_dvkind = 4
         --and (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0)
         and dvn.t_date <= in_date
         and (nfi0.t_rateid  = -1 or nfi0.t_rate = 0)
       group by nfi0.t_rate, nfi0.t_ratepoint
      union all
      select distinct 
             '0000#SOFRXXX#' || 
             to_char(nfi2.t_rate,'FM990.099999999')||'#'||nfi2.t_ratepoint||'#FIX' FINSTR_CODE,
             'Фиксированная ставка '||to_char(nfi2.t_rate,'FM990.099999999')||'%, точность '|| nfi2.t_ratepoint FINSTR_NAME,
             'Фиксированная ставка '||to_char(nfi2.t_rate,'FM990.099999999')||'#'||nfi2.t_ratepoint||'#FIX' FINSTR_NAME_S,
             '5' TYPEFINSTR,
             qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from ddvndeal_dbt dvn
       inner join ddvnfi_dbt nfi0
          on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
       inner join ddvnfi_dbt nfi2
          on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
       where dvn.t_dvkind = 4
         --and (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0)
         and dvn.t_date <= in_date
         and (nfi2.t_rateid  = -1 or nfi0.t_rate = 0)
       group by nfi2.t_rate, nfi2.t_ratepoint)
      -- Пары ставок валютно-процентного свопа
      union all
      select distinct
            '0000#SOFRXXX#' || 
             decode(nfi0.t_rateid, -1, to_char(nfi0.t_rate,'FM990.099999999')||'#'||nfi0.t_ratepoint||'#FIX',  nfi0.t_rateid||'#FIN')||'#'||
             decode(nfi2.t_rateid, -1, to_char(nfi2.t_rate,'FM990.099999999')||'#'||nfi2.t_ratepoint||'#FIX',  nfi2.t_rateid||'#FIN') FINSTR_CODE,
             'Пара ставок '||decode(nfi0.t_rateid, -1, to_char(nfi0.t_rate,'FM990.099999999')||'#'||nfi0.t_ratepoint||'#FIX',  nfi0.t_rateid||'#FIN')||' и '||
             decode(nfi2.t_rateid, -1, to_char(nfi2.t_rate,'FM990.099999999')||'#'||nfi2.t_ratepoint||'#FIX',  nfi2.t_rateid||'#FIN') FINSTR_NAME,
             'Пара ставок '||decode(nfi0.t_rateid, -1, to_char(nfi0.t_rate,'FM990.099999999')||'#'||nfi0.t_ratepoint||'#FIX',  nfi0.t_rateid||'#FIN')||'#'||
                             decode(nfi2.t_rateid, -1, to_char(nfi2.t_rate,'FM990.099999999')||'#'||nfi2.t_ratepoint||'#FIX',  nfi2.t_rateid||'#FIN') FINSTR_NAME_S,
             --'5' TYPEFINSTR,
             decode (BIQ_8474, 0, '5', 1, '6') TYPEFINSTR, --BIQ-8474 п.5.1, таблица 5.
             qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from ddvndeal_dbt dvn
       inner join ddvnfi_dbt nfi0
          on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
       inner join ddvnfi_dbt nfi2
          on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
       where dvn.t_dvkind = 4
--         and (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0)
         and dvn.t_date <= in_date
      -- Индексы
      union all
      select distinct
             '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN' FINSTR_CODE,
             fi.t_name FINSTR_NAME,
             substr(fi.t_definition,1,50) FINSTR_NAME_S,
             '8' TYPEFINSTR,
             qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from dfininstr_dbt fi
        left join dfininstr_dbt fi1
          on (fi.t_facevaluefi = fi1.t_fiid)
       where fi.t_fi_kind = 3 --and fi.t_facevaluefi <> -1
       --Артикулы BIQ-8474
       union all
       select distinct
             '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN' FINSTR_CODE,
             fi.t_name FINSTR_NAME,
             substr(fi.t_definition,1,50) FINSTR_NAME_S,
             '4' TYPEFINSTR,
             qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from dfininstr_dbt fi  where fi.t_fi_kind = 7 
            and BIQ_8474 = 1
      );
commit;
      -->BIQ - 10007
      --выгрузка в DET_ACC_ASS_KIND       
      insert into ldr_infa_pfi.DET_ACC_ASS_KIND(acc_ass_kind_code, acc_ass_kind_name, dt, rec_status, sysmoment, ext_file)
      select '9999#SOFRXXX#CONN306' as acc_ass_kind_code,
             'Связь между сводным счетом и лицевым счетом СОФР' as acc_ass_kind_name,
             qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')) as dt,
             0 as rec_status,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') as sysmoment,
             dwhEXT_FILE as ext_file
      from dual;
      commit;
      
      INSERT /*+ APPEND */ INTO LDR_INFA_PFI.TMP_ACC306 (T_ACCOUNTID, t_account, t_nameaccount, t_legalform, t_open_date, t_close_date, t_client, t_department, t_name, 
                                                         t_code_currency, t_ccy, t_fi_code, t_notresident, t_nrcountry, T_USERFIELD4)
             select /*+use_hash(s p)*/ distinct
                    s.T_ACCOUNTID,
                    s.t_account,
                    s.t_nameaccount,
                    p.t_legalform,
                    s.t_open_date,
                    decode(s.t_close_date, emptdate, perpDWHDate, s.t_close_date),
                    s.t_client,
                    s.t_department,
                    d.t_name,
                    s.t_code_currency,
                    f.t_ccy,
                    f.t_fi_code,
                    p.t_notresident,
                    p.T_NRCOUNTRY,
                    s.T_USERFIELD4
       
             from Daccount_dbt s
             inner join dparty_dbt p on s.t_client = p.t_partyid
             inner join ddp_dep_dbt d on d.t_code = s.t_department
             inner join dfininstr_dbt f on f.t_fiid = s.t_code_currency and f.t_fi_kind = 1
             where (s.t_balance = '30601' or s.t_balance = '30606');
      commit;       
/*      DBMS_STATS.GATHER_TABLE_STATS ( OwnName        => 'LDR_INFA_PFI',
                                      TabName        => 'TMP_ACC306',
                                      Degree         => 4
                                    );             */
               
      insert /*+ APPEND*/  into LDR_INFA_PFI.TMP_ACC_SUBJ_306(t_accountid, t_account, t_partyid, SUBJECT_CODE)
          select distinct
               tmp.t_accountid,
               tmp.t_account,
               tmp.t_client,
               '9999' ||'#'|| 'IBSOXXX' ||'#'|| 
               t.t_code || case when 0 < ( nvl2(b1.t_code,1,0) + nvl2(b2.t_superior,1,0))
                                /*(select count(1)
                                        from Dobjcode_Dbt o
                                       where o.t_objecttype = 3
                                             and o.t_codekind in (3,6)
                                             and o.t_state = 0
                                             and o.t_objectid = t.t_partyid
                                         or 0 < (select count(1)
                                                   from dpartyown_dbt o
                                                  where o.t_partykind = 2
                                                        and o.t_partyid = t.t_partyid) */

                                      then '#BANKS'
                                     
                                when p.t_legalform = 1 then '#CUST_CORP'
                                when p.t_legalform = 2 then 
                                     case when pers.t_isemployer = chr(88) then '#CUST_CORP' 
                                          else '#PERSON'
                                     end
                                end as SUBJECT_CODE
               from dpartcode_dbt t
               inner join dparty_dbt p on p.t_partyid = t.t_partyid
               left join dpersn_dbt pers on p.t_partyid = pers.t_personid
               inner join ldr_infa_pfi.tmp_acc306 tmp on tmp.t_client = p.t_partyid
               
               left join Dobjcode_Dbt b1 on b1.t_objectid = t.t_partyid and b1.t_objecttype = 3 and b1.t_codekind in (3,6) and b1.t_state = 0
               left join dpartyown_dbt b2 on b2.t_partykind = 2 and b2.t_partyid = t.t_partyid
               
               where t.t_codekind = 101
                 and t.t_state = 0;
             commit;    
  /*  DBMS_STATS.GATHER_TABLE_STATS ( OwnName        => 'LDR_INFA_PFI',
                                    TabName        => 'TMP_ACC_SUBJ_306',
                                    Degree         => 4
                                  );*/
      --<BIQ - 10007      

      -- Выгрузка в DET_CONTRACT
      qb_bp_utils.SetError(EventID,
                           '',
                           to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка контрактов',
                           2,
                           null,
                           null);
      insert into ldr_infa_pfi.det_contract(code, finstr_code,basefinstr_finstr_code, price_curfinstr_finstr_code, contract_type, volume, lot, start_date, maturity_date, option_direction, option_style, option_kind, price_base, unit, dt, rec_status, sysmoment, ext_file)
      (select to_char(fdr.t_fiid) CODE,
              '0000#SOFRXXX#' || to_char(fdr.t_fiid) || '#FIN' FINSTR_CODE, --единообразие FINSTR_CODE
             case
               when fi2.t_fi_kind = 1 then
                 fi2.t_iso_number
               when fi2.t_fi_kind = 6 then
                 fi2.t_codeinaccount
               else
                 '0000#SOFRXXX#' || to_char(fi2.t_fiid) || '#FIN'
             end BASEFINSTR_FINSTR_CODE,
             case
               when fi1.t_fi_kind = 1 then
                 fi1.t_iso_number
               when fi1.t_fi_kind = 6 then
                 fi1.t_codeinaccount
               else
                  to_char(fi1.t_fiid) || '#FIN'
             end PRICE_CURFINSTR_FINSTR_CODE,
             case
               when fi.t_fi_kind = 4 and fi.t_avoirkind = 1 then
                 '1'
               when fi.t_fi_kind = 4 and fi.t_avoirkind = 2 then
                 '2'
               else
                 '0'
             end CONTRACT_TYPE,
             to_char(fi.t_facevalue, '999999999999999999999D999', 'nls_numeric_characters=''. ''') VOLUME, --fi
             null LOT,
             qb_dwh_utils.DateToChar(fdr.t_incirculationdate) START_DATE,
             qb_dwh_utils.DateToChar(fi.t_drawingdate) MATURITY_DATE,
             case
               when fi.t_fi_kind = 4 and fi.t_avoirkind = 2 then -- только для опционов
                 to_char(fdr.t_optiontype)
               else
                 null
             end OPTION_DIRECTION,
                    case
               when fi.t_fi_kind = 4 and fi.t_avoirkind = 2 then -- только для опционов
                 to_char(fdr.t_optionstyle)
               else
                 null
             end OPTION_STYLE,
             null OPTION_KIND,
             to_char(fdr.t_strike, '999999999999999999999D999', 'nls_numeric_characters=''. ''') PRICE_BASE,
             case
               when fi1.t_fi_kind = 1 then
                 fi1.t_iso_number
               when fi1.t_fi_kind = 6 then
                 fi1.t_codeinaccount
               else
                  to_char(fi1.t_fiid) || '#FIN'
             end UNIT,
             qb_dwh_utils.DateToChar(decode(fi.t_issued, to_date('01010001', 'ddmmyyyy'), to_date('01011980','ddmmyyyy'), fi.t_issued)) DT,
             '0' RECSTATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from dfideriv_dbt fdr
       inner join dfininstr_dbt fi
          on (fdr.t_fiid = fi.t_fiid)
       inner join dfininstr_dbt fi1
          on (fdr.t_strikefiid = fi1.t_fiid)
       inner join dfininstr_dbt fi2
          on (fi.t_facevaluefi = fi2.t_fiid)
       where fdr.t_incirculationdate <= in_date
      union all
      select dvn.t_id||'#DVN' CODE,
             case
               when fi1.t_fi_kind = 1 then
                 fi1.t_iso_number
               when fi1.t_fi_kind = 6 then
                 fi1.t_codeinaccount
               else
                 '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN'
             end FINSTR_CODE,
             case
               when fi1.t_fi_kind = 1 then
                 fi1.t_iso_number
               when fi1.t_fi_kind = 6 then
                 fi1.t_codeinaccount
               else
                 '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN'
             end BASEFINSTR_FINSTR_CODE,
             case
               when fi2.t_fi_kind = 1 then
                 fi2.t_iso_number
               when fi2.t_fi_kind = 6 then
                 fi2.t_codeinaccount
               else
                 '0000#SOFRXXX#' || to_char(fi2.t_fiid) || '#FIN'
             end PRICE_CURFINSTR_FINSTR_CODE,
             '2' CONTRACT_TYPE,
             to_char(nfi.t_amount, '999999999999999999999D999', 'nls_numeric_characters=''. ''') VOLUME,
             null LOT,
             qb_dwh_utils.DateToChar(dvn.t_date) START_DATE,
             qb_dwh_utils.DateToChar(nfi.t_execdate) MATURITY_DATE,
             to_char(dvn.t_optiontype) OPTION_DIRECTION,
             to_char(dvn.t_optionstyle) OPTION_STYLE,
             null OPTION_KIND,
             to_char(nfi.t_price, '999999999999999999999D999', 'nls_numeric_characters=''. ''') PRICE_BASE,
             case
               when fi2.t_fi_kind = 1 then
                 fi2.t_iso_number
               when fi2.t_fi_kind = 6 then
                 fi2.t_codeinaccount
               else
                 '0000#SOFRXXX#' || to_char(fi2.t_fiid) || '#FIN'
             end UNIT,
             qb_dwh_utils.DateToChar(dvn.t_date) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from ddvndeal_dbt dvn
      inner join ddvnfi_dbt nfi
          on (dvn.t_id = nfi.t_dealid and nfi.t_type = 0)
      inner join dfininstr_dbt fi1
          on (nfi.t_fiid = fi1.t_fiid)
      inner join dfininstr_dbt fi2
          on (nfi.t_pricefiid = fi2.t_fiid)
      where dvn.t_dvkind = 2
        and dvn.t_date <= in_date
        --DEF-47223 Из ранее реализованной выгрузки в DET_CONTRACT необходимо исключить записи, где ddvnfi_dbt.t_fiid=-1 и ddvnfi_dbt.t_stdfiid = -1
       and not (nfi.t_fiid=-1 and nfi.t_stdfiid = -1)

    -- Внебиржевые опционы на форвард DEF-47223
    union all
      select dvn.t_id||'#DVN' CODE,
             dvn.t_id||'#DVN'  FINSTR_CODE,
             case
               when fi1.t_fi_kind = 1 then
                 fi1.t_iso_number
               when fi1.t_fi_kind = 6 then
                 fi1.t_codeinaccount
               else
                 '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN'
             end BASEFINSTR_FINSTR_CODE,
             case
               when fi2.t_fi_kind = 1 then
                 fi2.t_iso_number
               when fi2.t_fi_kind = 6 then
                 fi2.t_codeinaccount
               else
                 '0000#SOFRXXX#' || to_char(fi2.t_fiid) || '#FIN'
             end PRICE_CURFINSTR_FINSTR_CODE,
             '2' CONTRACT_TYPE,
             to_char(nfi.t_amount, '999999999999999999999D999', 'nls_numeric_characters=''. ''') VOLUME,
             null LOT,
             qb_dwh_utils.DateToChar(dvn.t_date) START_DATE,
             qb_dwh_utils.DateToChar(nfi.t_execdate) MATURITY_DATE,
             to_char(dvn.t_optiontype) OPTION_DIRECTION,
             to_char(dvn.t_optionstyle) OPTION_STYLE,
             null OPTION_KIND,
             to_char(nfi.t_price, '999999999999999999999D999', 'nls_numeric_characters=''. ''') PRICE_BASE,
             case
               when fi2.t_fi_kind = 1 then
                 fi2.t_iso_number
               when fi2.t_fi_kind = 6 then
                 fi2.t_codeinaccount
               else
                 '0000#SOFRXXX#' || to_char(fi2.t_fiid) || '#FIN'
             end UNIT,
             qb_dwh_utils.DateToChar(dvn.t_date) DT,
             '0' REC_STATUS,
             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from ddvndeal_dbt dvn
      inner join ddvnfi_dbt nfi
          on (dvn.t_id = nfi.t_dealid and nfi.t_type = 1 and dvn.t_dvkind = 2)
      inner join dfininstr_dbt fi1
          on (nfi.t_fiid = fi1.t_fiid)
      inner join dfininstr_dbt fi2
          on (nfi.t_pricefiid = fi2.t_fiid)
      where dvn.t_dvkind = 2
        and dvn.t_date <= in_date
        );
commit;
      -- Выгрузка курсов ФИ
      qb_bp_utils.SetError(EventID,
                           '',
                           to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка курсов ФИ',
                           2,
                           null,
                           null);
      Insert into ldr_infa_pfi.fct_finstr_rate(finstr_numerator_finstr_code, finstr_denominator_finstr_code, finstr_rate, finstr_scale, type_finstr_rate_type_rate_cod, dt, rec_status, sysmoment, ext_file)
      (select * from ( select case
                         when fi.t_fi_kind = 1 then
                           fi.t_iso_number
                         when fi.t_fi_kind = 6 then
                           fi.t_codeinaccount
                         else
                           '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN'
                       end FINSTR_NUMERATOR_FINSTR_CODE,
                       case
                         when fir.t_fi_kind = 1 then
                           fir.t_iso_number
                         when fir.t_fi_kind = 6 then
                           fir.t_codeinaccount
                         else
                           '0000#SOFRXXX#' || to_char(fir.t_fiid) || '#FIN'
                       end FINSTR_DENUMERATOR_FINSTR_CODE,
                       qb_dwh_utils.NumberToChar(rh.t_rate/power(10, rh.t_point), rh.t_point) FINSTR_RATE,
                       to_char(rh.t_scale) FINSTR_SCALE,
                       rd.t_type TYPE_FINSTR_RATE_TYPE_RATE_COD,
                       qb_dwh_utils.DateToChar(decode(rh.t_sincedate, emptDate, firstDate, rh.t_sincedate)) dt,
                       '0' RECSTATUS,
                       to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                  from dfininstr_dbt fi
                 inner join dratedef_dbt rd
                    on (fi.t_fiid = rd.t_otherfi)
                 inner join dfininstr_dbt fir
                    on (fir.t_fiid = rd.t_fiid)
                 inner join dratehist_dbt rh
                   on (rd.t_rateid = rh.t_rateid)
                 where rd.t_type = 1
                   and exists (select 1
                                 from ldr_infa_pfi.det_finstr df
                                where df.finstr_code = '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN')
                   and rh.t_sincedate <= in_date
                   and fir.t_fiid <> -10
               union all
                select case
                         when fi.t_fi_kind = 1 then
                           fi.t_iso_number
                         when fi.t_fi_kind = 6 then
                           fi.t_codeinaccount
                         else
                           '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN'
                       end FINSTR_NUMERATOR_FINSTR_CODE,
                       case
                         when fir.t_fi_kind = 1 then
                           fir.t_iso_number
                         when fir.t_fi_kind = 6 then
                           fir.t_codeinaccount
                         else
                           '0000#SOFRXXX#' || to_char(fir.t_fiid) || '#FIN'
                       end FINSTR_DENUMERATOR_FINSTR_CODE,
                       qb_dwh_utils.NumberToChar(rd.t_rate/power(10, rd.t_point), rd.t_point) FINSTR_RATE,
                       to_char(rd.t_scale) FINSTR_SCALE,
                       rd.t_type TYPE_FINSTR_RATE_TYPE_RATE_COD,
                       qb_dwh_utils.DateToChar(decode(rd.t_sincedate, emptDate, firstDate, rd.t_sincedate)) dt,
                       '0' RECSTATUS,
                       to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                  from dfininstr_dbt fi
                 inner join dratedef_dbt rd
                    on (fi.t_fiid = rd.t_otherfi)
                 inner join dfininstr_dbt fir
                    on (fir.t_fiid = rd.t_fiid)
                 where rd.t_type = 1
                   and exists (select 1
                                 from ldr_infa_pfi.det_finstr df
                                where df.finstr_code = '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN')
                   and rd.t_sincedate <= in_date
                   and fir.t_fiid <> -10
                 ));
commit;
      -- Вставка в DET_EXCHANGE
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника торговых площадок',
                     2,
                     null,
                     null);

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
          insert into ldr_infa_pfi.det_exchange(code, name, dt, rec_status, sysmoment, ext_file)
                 values(rec.code, rec.name, qb_dwh_utils.DateToChar(firstDate), dwhRecStatus, to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS'), dwhEXT_FILE);                
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
commit;
      -- Вставка в DET_INDEX
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника индексов',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.det_index(code, finstr_code, basefinstr_finstr_code, dt, rec_status, sysmoment, ext_file)
      (select distinct
              to_char(fi.t_fiid) CODE,
              case
                when fi.t_fi_kind = 1 then
                  fi.t_iso_number
                when fi.t_fi_kind = 6 then
                  fi.t_codeinaccount
                else
                   to_char(fi.t_fiid) || '#FIN'
              end FINSTR_CODE,
              case
                when fi1.t_fi_kind = 1 then
                  fi1.t_iso_number
                when fi1.t_fi_kind = 6 then
                  fi1.t_codeinaccount
                else
                   to_char(fi1.t_fiid) || '#FIN'
              end BASEFINSTR_FINSTR_CODE,
              qb_dwh_utils.DateToChar(
              case
                when fi.t_issued = emptdate then
                  nvl ((select min(t_inputdate)
                         from dratedef_dbt rd
                        where rd.t_otherfi = fi.t_fiid), firstdate)
                else
                  nvl(fi.t_issued, firstdate)
              end) DT,
              '0' RECSTATUS,
              to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
        from dfininstr_dbt fi
        left join dfininstr_dbt fi1
          on (fi.t_facevaluefi = fi1.t_fiid)
       where fi.t_fi_kind = 3 /*and fi.t_facevaluefi <> -1*/);
commit;
      -- Вставка в DET_TYPE_RATE
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника типов курсов',
                     2,
                     null,
                     null);

      for rec in (select to_char(rt.t_type) type_rate_code,
                         rt.t_typename type_rate_name
                    from dratetype_dbt rt
                   where rt.t_type = 1)
      loop
        begin
          insert into ldr_infa_pfi.det_type_rate(type_rate_code, type_rate_name, dt, rec_status, sysmoment, ext_file)
                 values(rec.type_rate_code, rec.type_rate_name, qb_dwh_utils.DateToChar(firstDate), '0', to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS'), dwhEXT_FILE);
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
commit;
      -- Вставка в DET_PROCBASE --8474 добавлены фиксированные ставки
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника базы расчета процентов',
                     2,
                     null,
                     null);
      
      if (BIQ_8474 = 1) then

          insert into ldr_infa_pfi.det_procbase(code, name, days_year, days_month,sign_31, first_day, last_day, null_mainsum, dt, rec_status, sysmoment, ext_file)
           (select decode (to_char(fi.t_fiid), NULL, to_char(nfi.t_basis), to_char(fi.t_fiid) || '#' || to_char(nfi.t_basis)) CODE,
                   nvl(fi.t_name,'Фиксированная ставка') NAME,
                   case
                    when nfi.t_basis in (1, 2) then
                      '360'
                    when nfi.t_basis = 8 then
                      '365'
                    when nfi.t_basis = 4 then
                      '366'
                   end DAYS_YEAR,
                   case
                    when nfi.t_basis in (2, 4, 8) then
                      '31'
                    when nfi.t_basis = 1 then
                      '30'
                   end DAYS_MONTH,
                   case
                    when nfi.t_basis in (2, 4, 8) then
                      '1'
                    when nfi.t_basis = 0 then
                      '0'
                   end SIGN_31,
                   '1' FIRST_DAY,
                   '0' LAST_DAY,
                   '0' NULL_MAINSUM,
                   qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
                   '0' RECSTATUS,
                   to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
             from ddvnfi_dbt nfi
             left join dfininstr_dbt fi
                on (nfi.t_rateid = fi.t_fiid)
             where nfi.t_basis != 0 
             group by fi.t_fiid, fi.t_name, nfi.t_basis
                 );
      else 
             
          insert into ldr_infa_pfi.det_procbase(code, name, days_year, days_month,sign_31, first_day, last_day, null_mainsum, dt, rec_status, sysmoment, ext_file)
          (select to_char(fi.t_fiid) || '#' || to_char(nfi.t_basis) CODE,
               fi.t_name NAME,
               case
                when nfi.t_basis in (1, 2) then
                  '360'
                when nfi.t_basis = 8 then
                  '365'
                when nfi.t_basis = 4 then
                  '366'
               end DAYS_YEAR,
               case
                when nfi.t_basis in (2, 4, 8) then
                  '31'
                when nfi.t_basis = 1 then
                  '30'
               end DAYS_MONTH,
               case
                when nfi.t_basis in (2, 4, 8) then
                  '1'
                when nfi.t_basis = 0 then
                  '0'
               end SIGN_31,
               '1' FIRST_DAY,
               '0' LAST_DAY,
               '0' NULL_MAINSUM,
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
               '0' RECSTATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvnfi_dbt nfi
         inner join dfininstr_dbt fi
            on (nfi.t_rateid = fi.t_fiid)
         where fi.t_fi_kind = 3
         group by fi.t_fiid, fi.t_name, nfi.t_basis);

      end if;
commit;
      -- Вставка в DET_RATE
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника курсов',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.det_rate(code, dt, finstr_code, subkindprocrate_code, type_rate, rate_value, margin, rec_status, sysmoment, ext_file)
       (select distinct *
          from (-- Плавающие ставки
                select case
                         when fi.t_fi_kind = 1 then
                           fi.t_iso_number
                         when fi.t_fi_kind = 6 then
                           fi.t_codeinaccount
                         else
                            to_char(fi.t_fiid) || '#FIN'
                       end CODE,
                       qb_dwh_utils.DateToChar(rd.t_sincedate) DT,
                       case
                         when fi.t_fi_kind = 1 then
                           fi.t_iso_number
                         when fi.t_fi_kind = 6 then
                           fi.t_codeinaccount
                         else
                            '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN'
                       end FINSTR_CODE,
                       '-1' SUBKINDPROCRATE_CODE,
                       '2' TYPE_RATE,
                       to_char(rd.t_rate/power(10, rd.t_point), '999999999999999999999D999', 'nls_numeric_characters=''. ''') RATE_VALUE,
                       to_char(nvl((select t_spread
                                      from ddvnfi_dbt
                                     where t_rateid = rd.t_rateid
                                       and t_execdate = rd.t_sincedate), 0), '999999999999999999999D999', 'nls_numeric_characters=''. ''') MARGIN,
                       '0' REC_STATUS,
                       to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                  from dratedef_dbt rd
                  inner join dfininstr_dbt fi
                     on (rd.t_otherfi = fi.t_fiid)
                  left join ddvnfi_dbt nfi on (fi.t_fiid = nfi.t_fiid and fi.t_fi_kind = 3) --8474
                  left join ddvndeal_dbt dvn on (nfi.t_dealid = dvn.t_id and dvn.t_dvkind = 2)   
                 where t_otherfi in
                       (select t_fiid from dfininstr_dbt t where t.t_fi_kind = 3)
                   and nfi.t_id is null --8474 для опционов другой запрос   
                   and rd.t_sincedate <= in_date
                   and rd.t_type in (1, 7)
                union all
                select case
                         when fi.t_fi_kind = 1 then
                           fi.t_iso_number
                         when fi.t_fi_kind = 6 then
                           fi.t_codeinaccount
                         else
                            to_char(fi.t_fiid) || '#FIN'
                       end CODE,
                       qb_dwh_utils.DateToChar(rh.t_sincedate) DT,
                       case
                         when fi.t_fi_kind = 1 then
                           fi.t_iso_number
                         when fi.t_fi_kind = 6 then
                           fi.t_codeinaccount
                         else
                           '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN'
                       end FINSTR_CODE,
                       '-1' SUBKINDPROCRATE_CODE,
                       '2' TYPE_RATE,
                       to_char(rh.t_rate/power(10, rh.t_point), '999999999999999999999D999', 'nls_numeric_characters=''. ''') RATE_VALUE,
                       to_char(nvl((select t_spread
                                      from ddvnfi_dbt
                                     where t_rateid = rh.t_rateid
                                       and t_execdate = rh.t_sincedate), 0), '999999999999999999999D999', 'nls_numeric_characters=''. ''') MARGIN,
                       '0' REC_STATUS,
                       to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                  from dratedef_dbt rd
                 inner join dratehist_dbt rh
                    on (rd.t_rateid = rh.t_rateid)
                 inner join dfininstr_dbt fi
                     on (rd.t_otherfi = fi.t_fiid)
                 where t_otherfi in
                       (select t_fiid from dfininstr_dbt t where t.t_fi_kind = 3)
                   and rh.t_sincedate <= in_date
                   and rd.t_type in (1, 7)
                -- Фиксированные ставки
                union all
                select to_char(nfi0.t_rate,'FM990.099999999')||'#'||nfi0.t_ratepoint||'#FIX' CODE,
                       qb_dwh_utils.DateToChar(firstdate) DT,
                       '0000#SOFRXXX#' || to_char(nfi0.t_rate,'FM990.099999999')||'#'||nfi0.t_ratepoint||'#FIX' FINSTR_CODE,
                       '-1' SUBKINDPROCRATE_CODE,
                       '1' TYPE_RATE,
                       to_char(nfi0.t_rate, '999999999999999999999D999', 'nls_numeric_characters=''. ''') RATE_VALUE,
                       null MARGIN,
                       '0' REC_STATUS,
                       to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                  from ddvndeal_dbt dvn
                 inner join ddvnfi_dbt nfi0
                    on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                 inner join ddvnfi_dbt nfi2
                    on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                 where dvn.t_dvkind = 4
                   --and (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0)
                   and dvn.t_date <= in_date
                   and (nfi0.t_rateid  = -1 or nfi0.t_rate = 0)
                 group by nfi0.t_rate, nfi0.t_ratepoint
                union all
                select to_char(nfi2.t_rate,'FM990.099999999')||'#'||nfi2.t_ratepoint||'#FIX' CODE,
                       qb_dwh_utils.DateToChar(firstdate) DT,
                       '0000#SOFRXXX#' || to_char(nfi2.t_rate,'FM990.099999999')||'#'||nfi2.t_ratepoint||'#FIX' FINSTR_CODE,
                       '-1' SUBKINDPROCRATE_CODE,
                       '1' TYPE_RATE,
                       to_char(nfi2.t_rate, '999999999999999999999D999', 'nls_numeric_characters=''. ''') RATE_VALUE,
                       null MARGIN,
                       '0' REC_STATUS,
                       to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                  from ddvndeal_dbt dvn
                 inner join ddvnfi_dbt nfi0
                    on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                 inner join ddvnfi_dbt nfi2
                    on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                 where dvn.t_dvkind = 4
                   --and (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0)
                   and dvn.t_date <= in_date
                   and (nfi2.t_rateid  = -1 or nfi2.t_rate = 0)
                 group by nfi2.t_rate, nfi2.t_ratepoint
                 --BIQ-8474 п.3.2 пп.5 ставки для опционов
                 union all
                   select      to_char(rt.t_otherfi||'#'||'FIN') CODE,
                   qb_dwh_utils.DateToChar(rt.t_inputdate) DT,
                   '0000#SOFRXXX#' || to_char(fi.t_fiid) ||'#'||'FIN' FINSTR_CODE,
                   '-1' SUBKINDPROCRATE_CODE,
                   '2' TYPE_RATE,
                       to_char(rt.t_rate/power(10, rt.t_point), '999999999999999999999D999', 'nls_numeric_characters=''. ''') RATE_VALUE,
                       to_char(nvl((select t_spread
                                      from ddvnfi_dbt
                                     where t_rateid = rt.t_rateid
                                       and t_execdate = rt.t_sincedate), 0), '999999999999999999999D999', 'nls_numeric_characters=''. ''') MARGIN,
                       '0' REC_STATUS,
                       to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                       
                  from ddvndeal_dbt dvn
                  inner join ddvnfi_dbt nfi on nfi.t_dealid = dvn.t_id 
                  inner join dfininstr_dbt fi on (fi.t_fiid = nfi.t_fiid and fi.t_fi_kind = 3)
                  inner join dratedef_dbt rt on rt.t_otherfi = fi.t_fiid
                  where dvn.t_dvkind = 2 and dvn.t_date <= in_date 
                        and BIQ_8474 = 1 
                 ));
commit;
      -- Вставка в DET_RATE_PAIR
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника пар процентных ставок',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.det_rate_pair(code, dt, finstr_code, rate1_code, rate2_code, rec_status, sysmoment, ext_file)
       (select distinct
               decode(fi1.t_rateid, -1, to_char(fi1.t_rate,'FM990.099999999')||'#'||fi1.t_ratepoint||'#FIX',  fi1.t_rateid||'#FIN')||'#'||
               decode(fi2.t_rateid, -1, to_char(fi2.t_rate,'FM990.099999999')||'#'||fi2.t_ratepoint||'#FIX',  fi2.t_rateid||'#FIN') CODE,
               qb_dwh_utils.DateToChar(dts.t_sincedate) DT,
               decode(fi1.t_rateid, -1, to_char(fi1.t_rate,'FM990.099999999')||'#'||fi1.t_ratepoint||'#FIX',  fi1.t_rateid||'#FIN')||'#'||
               decode(fi2.t_rateid, -1, to_char(fi2.t_rate,'FM990.099999999')||'#'||fi2.t_ratepoint||'#FIX',  fi2.t_rateid||'#FIN') FINSTR_CODE,
               decode(fi1.t_rateid, -1, to_char(fi1.t_rate,'FM990.099999999')||'#'||fi1.t_ratepoint||'#FIX',  fi1.t_rateid||'#FIN') RATE1_CODE,
               decode(fi2.t_rateid, -1, to_char(fi2.t_rate,'FM990.099999999')||'#'||fi2.t_ratepoint||'#FIX',  fi2.t_rateid||'#FIN') RATE2_CODE,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from DDVNFI_DBT fi1, DDVNFI_DBT fi2, ddvndeal_dbt dvn,
               (select rd.t_sincedate, rd.t_otherfi from dratedef_dbt rd
                union
                select rh.t_sincedate, rd.t_otherfi from dratehist_dbt rh, dratedef_dbt rd where rh.t_rateid=rd.t_rateid
                union
                select to_date('01011980','ddmmyyyy'), -1 from dual)dts
         where dvn.t_id = fi2.t_dealid
           and dvn.t_id = fi1.t_dealid
           and fi1.t_type = 0 and fi2.t_type = 2
           and dvn.t_dvkind in (3, 6, 4)
           and dts.t_otherfi in (fi1.t_rateid,fi2.t_rateid)
           and (dts.t_otherfi != -1 or (fi1.t_rateid = -1 and fi2.t_rateid = -1))
           and dvn.t_date <= in_date
        );
commit;
      delete from ldr_infa_pfi.det_rate_pair where finstr_code = '0.0#0#FIX#0.0#0#FIX'; -- Такого ПФИ нет в det_finstr
commit;
      -- Выгрузка в DET_CURRENCY_PAIR
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника пар валют',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.det_currency_pair(code, dt, finstrpair_finstr_code, curfinstr1_finstr_code, curfinstr2_finstr_code, rec_status, sysmoment, ext_file)
      (select distinct * from(-- Валютные пары по валютным свопам
                              select decode(fi1.t_fi_kind, 6, fi1.t_codeinaccount, fi1.t_iso_number) || '#' || decode(fi2.t_fi_kind, 6, fi2.t_codeinaccount, fi2.t_iso_number) CODE,
                                     qb_dwh_utils.DateToChar(firstdate) DT,
                                     decode(fi1.t_fi_kind, 6, fi1.t_codeinaccount, fi1.t_iso_number) || '#' || decode(fi2.t_fi_kind, 6, fi2.t_codeinaccount, fi2.t_iso_number) FINSTR_CODE,
                                     decode(fi1.t_fi_kind, 6, fi1.t_codeinaccount, fi1.t_iso_number) CURFINSTR1_FINSTR_CODE,
                                     decode(fi2.t_fi_kind, 6, fi2.t_codeinaccount, fi2.t_iso_number) CURFINSTR2_FINSTR_CODE,
                                     '0' REC_STATUS,
                                     to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                                from ddvndeal_dbt dvn
                               inner join ddvnfi_dbt nfi0
                                  on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                               inner join dfininstr_dbt fi1
                                  on (nfi0.t_fiid = fi1.t_fiid)
                               inner join dfininstr_dbt fi2
                                  on (nfi0.t_pricefiid = fi2.t_fiid)
                               where dvn.t_dvkind in (3, 6)
                                 and dvn.t_date <= in_date
                               group by nfi0.t_fiid, fi1.t_name, fi1.t_definition, nfi0.t_pricefiid, fi2.t_name, fi2.t_definition, fi1.t_iso_number, fi2.t_iso_number, fi1.t_fi_kind, fi2.t_fi_kind, fi1.t_codeinaccount, fi2.t_codeinaccount
                              union all
                              select distinct
                                     case
                                       when f1.t_fi_kind = 1 then
                                         f1.t_iso_number
                                       when f1.t_fi_kind = 6 then
                                         f1.t_codeinaccount
                                       else
                                          to_char(f1.t_fiid) || '#FIN'
                                     end || '#' ||
                                     case
                                       when f2.t_fi_kind = 1 then
                                         f2.t_iso_number
                                       when f2.t_fi_kind = 6 then
                                         f2.t_codeinaccount
                                       else
                                          to_char(f2.t_fiid) || '#FIN'
                                     end CODE,
                                     qb_dwh_utils.DateToChar(firstdate) DT,
                                     case
                                       when f1.t_fi_kind = 1 then
                                         f1.t_iso_number
                                       when f1.t_fi_kind = 6 then
                                         f1.t_codeinaccount
                                       else
                                          to_char(f1.t_fiid) || '#FIN'
                                     end || '#' ||
                                     case
                                       when f2.t_fi_kind = 1 then
                                         f2.t_iso_number
                                       when f2.t_fi_kind = 6 then
                                         f2.t_codeinaccount
                                       else
                                          to_char(f2.t_fiid) || '#FIN'
                                     end FINSTR_CODE,
                                     case
                                       when f1.t_fi_kind = 1 then
                                         f1.t_iso_number
                                       when f1.t_fi_kind = 6 then
                                         f1.t_codeinaccount
                                       else
                                          to_char(f1.t_fiid) || '#FIN'
                                     end CURFINSTR1_FINSTR_CODE,
                                     case
                                       when f2.t_fi_kind = 1 then
                                         f2.t_iso_number
                                       when f2.t_fi_kind = 6 then
                                         f2.t_codeinaccount
                                       else
                                          to_char(f2.t_fiid) || '#FIN'
                                     end CURFINSTR2_FINSTR_CODE,
                                     '0' RECSTATUS,
                                     to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                                from DDVNFI_DBT fi1, DDVNFI_DBT fi2, ddvndeal_dbt dvn, dfininstr_dbt f1, dfininstr_dbt f2
                               where dvn.t_id = fi2.t_dealid
                                 and dvn.t_id = fi1.t_dealid
                                 and fi1.t_type = 0 and fi2.t_type = 2
                                 and dvn.t_dvkind = 4
                                 and fi1.t_fiid = f1.t_fiid
                                 and fi2.t_fiid = f2.t_fiid
                                 and dvn.t_date <= in_date));
commit;
      -- Вставка в ASS_ACCOUNTDEAL
      if (exp_mode <> 1) then
        qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка связей счета со сделкой (Шаг 1)',
                       2,
                       null,
                       null);
        insert /*+ parallel(4) enable_parallel_dml */ into ldr_infa_pfi.ass_accountdeal
          (account_code,
           deal_code,
           roleaccount_deal_code,
           dt,
           dt_end,
           rec_status,
           sysmoment, ext_file)
          select  /*+ parallel(4) */
           distinct '0000#IBSOXXX#' || uf4 account_code,
                          to_char(dealid) || '#DVN#' || case
                            when dvkind = 2 then
                             '95' -- Опцион
                            when dvkind in (1, 7) then
                             '90' -- Спот/Форвард
                            when dvkind in (3, 6) then
                             '91' -- Валютный своп
                            when dvkind = 8 then
                             '3' -- Банкнотные сделки
                            when dvkind = 4 then
                              case when nfiid0 <> 0 or nfiid2 <> 0 then
                                '96'
                              else
                                '93' -- Процентный своп
                              end
                            else
                             '-1-' || dvkind
                          end deal_code,
                          upper(catcode) roleaccount_deal_code,
                          qb_dwh_utils.datetochar(catdate) dt,
                          qb_dwh_utils.datetochar(decode(enddate, emptDate, null, enddate)) dt_end,
                          '0' rec_status,
                          to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE

            from (
                  -- Счета по сделке по дебету
                  select /*+ use_hash(dvn,acd)  use_nl(op) use_nl(od) use_nl(trn) INDEX(op DOPROPER_DBT_IDX1)*/
                         dvn.t_id            dealid,
                         dvn.t_dvkind        dvkind,
                         nfi0.t_fiid         nfiid0,
                         nfi2.t_fiid         nfiid2,
                         trn.t_account_payer acc,
                         trn.t_fiid_payer    fiid,
                         cat.t_code          catcode,
                         cat.t_name          catname,
                         acd.t_catid         catid,
                         acd.t_activatedate  catdate,
                         acd.t_disablingdate enddate,
                         --acnt.t_userfield4   uf4
                         case
                            when (acnt.t_userfield4 is null) or
                                (acnt.t_userfield4 = chr(0)) or
                                (acnt.t_userfield4 = chr(1)) or
                                (acnt.t_userfield4 like '0x%') 
                            then
                              acnt.t_account
                            else
                              acnt.t_userfield4
                            end uf4
                    from ddvndeal_dbt dvn
                    left join ddvnfi_dbt nfi0
                      on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                    left join ddvnfi_dbt nfi2
                      on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                   inner join doproper_dbt op
                      on (dvn.t_dockind = op.t_dockind and
                         lpad(to_char(dvn.t_id), 34, '0') = op.t_documentid)
                   inner join doprdocs_dbt od
                      on (op.t_id_operation = od.t_id_operation and od.t_dockind = 1)
                   inner join dacctrn_dbt trn
                      on (od.t_acctrnid = trn.t_acctrnid and trn.t_state = 1)
                   inner join dmcaccdoc_dbt acd
                      on (/*op.t_dockind*/dvn.t_dockind = acd.t_dockind and dvn.t_id = acd.t_docid and
                         trn.t_account_payer = acd.t_account and
                         acd.t_iscommon = chr(0) and
--> AS 2021-09-08 IM4250076 SD6227143 добавил 476, 477, 616, 617
                         acd.t_catid in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869))
                   inner join daccount_dbt acnt
                      on (trn.t_accountid_payer = acnt.t_accountid)
                   inner join dmccateg_dbt cat
                      on (acd.t_catid = cat.t_id)
                   where t_date <= in_date
                     and t_dvkind in (1, 2, 3, 4, 6, 7, 8)
                  --                        where dvn.t_id = 215579
                  union all
                  -- Счета по сделке по кредиту
                  select /*+ use_hash(dvn,acd)  use_nl(op) use_nl(od) use_nl(trn) INDEX(op DOPROPER_DBT_IDX1)*/
                         dvn.t_id            dealid,
                         dvn.t_dvkind        dvkind,
                         nfi0.t_fiid         nfiid0,
                         nfi2.t_fiid         nfiid2,
                         trn.t_account_receiver acc,
                         trn.t_fiid_receiver    fiid,
                         cat.t_code          catcode,
                         cat.t_name          catname,
                         acd.t_catid         catid,
                         acd.t_activatedate  catdate,
                         acd.t_disablingdate enddate,
                         --acnt.t_userfield4   uf4
                         case
                            when (acnt.t_userfield4 is null) or
                                (acnt.t_userfield4 = chr(0)) or
                                (acnt.t_userfield4 = chr(1)) or
                                (acnt.t_userfield4 like '0x%') 
                            then
                                acnt.t_account
                            else
                                acnt.t_userfield4
                            end uf4
                    from ddvndeal_dbt dvn
                    left join ddvnfi_dbt nfi0
                      on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                    left join ddvnfi_dbt nfi2
                      on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                   inner join doproper_dbt op
                      on (dvn.t_dockind = op.t_dockind and
                         lpad(to_char(dvn.t_id), 34, '0') = op.t_documentid)
                   inner join doprdocs_dbt od
                      on (op.t_id_operation = od.t_id_operation and od.t_dockind = 1)
                   inner join dacctrn_dbt trn
                      on (od.t_acctrnid = trn.t_acctrnid and trn.t_state = 1)
                   inner join dmcaccdoc_dbt acd
                      on (/*op.t_dockind*/dvn.t_dockind = acd.t_dockind and dvn.t_id = acd.t_docid and
                         trn.t_account_receiver = acd.t_account and
                         acd.t_iscommon = chr(0) and
--> AS 2021-09-08 IM4250076 SD6227143 нет связи счет сделка из СОФР в ЦХД
--                         acd.t_catid in (163, 164, 460, 461, 611, 612, 614, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869))
                         acd.t_catid in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869))
                   inner join daccount_dbt acnt
                      on (trn.t_accountid_receiver = acnt.t_accountid)
                   inner join dmccateg_dbt cat
                      on (acd.t_catid = cat.t_id)
                   where t_date <= in_date
                     and t_dvkind in (1, 2, 3, 4, 6, 7, 8)
                   );
                   commit;
        -- вторая часть insert into ldr_infa_pfi.ass_accountdeal
        qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка связей счета со сделкой (Шаг 2)',
                       2,
                       null,
                       null);
        insert /*+ parallel(4) enable_parallel_dml */ into ldr_infa_pfi.ass_accountdeal
          (account_code,
           deal_code,
           roleaccount_deal_code,
           dt,
           dt_end,
           rec_status,
           sysmoment,
           ext_file)
         with 
         sacc as -- Счета по сделке по дебету
           ( select /*+  materialize  parallel(4) */
           *
              from (select /*+ use_hash(DVN,OP,OD,TRN )*/
               op.t_dockind        opkind,
               dvn.t_id            dealid,
               dvn.t_dvkind        dvkind,
               nfi0.t_fiid         nfiid0,
               nfi2.t_fiid         nfiid2,
               trn.t_account_payer acc,
               trn.t_fiid_payer    fiid
        from ddvndeal_dbt dvn
        left join ddvnfi_dbt nfi0
          on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
        left join ddvnfi_dbt nfi2
          on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
        inner join doproper_dbt op
          on (dvn.t_dockind = op.t_dockind and
              lpad(to_char(dvn.t_id), 34, '0') = op.t_documentid)
        inner join doprdocs_dbt od
          on (op.t_id_operation = od.t_id_operation and od.t_dockind = 1)
        inner join dacctrn_dbt trn
          on (od.t_acctrnid = trn.t_acctrnid and trn.t_state = 1)
        where t_date <= in_date
          and t_dvkind in (1, 2, 3, 4, 6, 7, 8)
      union
        -- Счета по сделке по кредиту
        select /*+  use_hash(DVN,OP,OD,TRN )*/
               op.t_dockind           opkind,
               dvn.t_id               dealid,
               dvn.t_dvkind           dvkind,
               nfi0.t_fiid            nfiid0,
               nfi2.t_fiid            nfiid2,
               trn.t_account_receiver acc,
               trn.t_fiid_receiver    fiid
        from ddvndeal_dbt dvn
        left join ddvnfi_dbt nfi0
          on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
        left join ddvnfi_dbt nfi2
          on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
        inner join doproper_dbt op
          on (dvn.t_dockind = op.t_dockind and
              lpad(to_char(dvn.t_id), 34, '0') = op.t_documentid)
        inner join doprdocs_dbt od
          on (op.t_id_operation = od.t_id_operation and od.t_dockind = 1)
        inner join dacctrn_dbt trn
          on (od.t_acctrnid = trn.t_acctrnid and trn.t_state = 1)
        where t_date <= in_date
          and t_dvkind in (1, 2, 3, 4, 6, 7, 8)
      ))
    ,
  acc as 
    (
      Select /*+   materialize  parallel(4)*/ acdoc.t_id,
             acdoc.t_iscommon,
             acdoc.t_dockind,
             acdoc.t_docid,
             acdoc.t_catid,
             acdoc.t_chapter,
             acdoc.t_account,
             acdoc.t_currency,
             acdoc.t_activatedate,
             acdoc.t_disablingdate,
             min(decode(acdoc.t_iscommon,chr(88),acdoc.t_id)) over(partition by acdoc.t_account) min_t_id
      from dmcaccdoc_dbt acdoc
      where acdoc.t_catid in ( 163,164,460,461,476,477,611,612,614,616,617,766,767,781,782,874,875,184,185,453,454,605,606,608,609,768,769,779,780,789,790,794,795,868,869)
    )
    ,
  sacc_cnt as 
    (
      select /*+ parallel(4)*/ sacc.*,
             decode(acc.t_account,null,0,1) cnt_deal
      from sacc
      left join acc 
        on sacc.opkind = acc.t_dockind and
           sacc.dealid = acc.t_docid and
           sacc.acc = acc.t_account and
           acc.t_iscommon = chr(0)
      where acc.t_account is null
    )
    ,
  lsql as 
    (
      select --+ parallel(4)
             sacc_cnt.*,
             upper(cat.t_code) catcode,
             acc.t_activatedate catdate,
             acc.t_disablingdate enddate,
             --acnt.t_userfield4 uf4
             case
               when (acnt.t_accountid is null) then acc.t_account
               when (acnt.t_userfield4 is null) or
                    (acnt.t_userfield4 = chr(0)) or
                    (acnt.t_userfield4 = chr(1)) or
                    (acnt.t_userfield4 like '0x%') then acnt.t_account
               else acnt.t_userfield4
             end uf4
      from sacc_cnt
      inner join acc 
        on acc.t_account = sacc_cnt.acc and
           acc.t_id = acc.min_t_id
      left join daccount_dbt acnt
        on (acc.t_chapter = acnt.t_chapter and acc.t_account = acnt.t_account and acc.t_currency = acnt.t_code_currency)
      inner join dmccateg_dbt cat
        on (acc.t_catid = cat.t_id)
      where cnt_deal = 0
    )   
    select --+ parallel(4)
           distinct
           '0000#IBSOXXX#' || uf4 account_code,
           to_char(dealid) || '#DVN#' || case
                                           when dvkind = 2 then '95' -- Опцион
                                           when dvkind in (1, 7) then '90' -- Спот/Форвард
                                           when dvkind in (3, 6) then '91' -- Валютный своп
                                           when dvkind = 8 then '3' -- Банкнотные сделки
                                           when dvkind = 4 then
                                                             case
                                                               when nfiid0 <> 0 or nfiid2 <> 0 then '96'
                                                               else '93' -- Процентный своп
                                                             end
                                           else '-1-' || dvkind
                                         end deal_code,
           upper(catcode) roleaccount_deal_code,
           qb_dwh_utils.datetochar(catdate) dt,
           qb_dwh_utils.datetochar(decode(enddate, emptdate,null,enddate)) dt_end,
           '0' rec_status,
           to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') sysmoment,
           dwhEXT_FILE
           from lsql;
           
commit;
        -- Выгрузка в FCT_DEAL_RST
        qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка остатков по сделкам ',
                       2,
                       null,
                       null);
        -- Заполнение временной таблицы
        
 -- Дебетовые проводки по сделке       
insert /*+ parallel(4) enable_parallel_dml */ into ldr_infa_pfi.tmp_acctrn
         ( select /*+ parallel(4) */
 s.DEALID,
 s.DVKIND,
 s.NFIID0,
 s.NFIID2,
 s.ACC,
 s.T_FIID_RECEIVER,
 s.DEB,
 s.DEBNAT,
 s.CRD,
 s.CRDNAT,
 s.VDATE,
 s.UF4
  from (with DVN as (select /*+ full(dvn) */
                      dvn.t_id, dvn.t_dvkind, t_dockind
                       from ddvndeal_dbt dvn
                      where 1 = 1
                        and dvn.t_date <= in_date
                        and dvn.t_dvkind in (1, 2, 3, 4, 6, 7, 8))
         select /*+ full(TRN) full(OP) full(OD) use_hash(DVN,OP,OD,TRN,ACNT,NFI0,NFI2)*/ 
              distinct DVN.T_ID DEALID,
                DVN.T_DVKIND DVKIND,
                NFI0.T_FIID  NFIID0,
                NFI2.T_FIID  NFIID2, 
                TRN.T_ACCOUNT_RECEIVER ACC,
                TRN.T_FIID_RECEIVER,
                0 DEB,
                0 DEBNAT,
                TRN.T_SUM_RECEIVER CRD,
                TRN.T_SUM_NATCUR CRDNAT,
                TRN.T_DATE_CARRY VDATE,
                CASE
                  WHEN (ACNT.T_USERFIELD4 IS NULL) OR
                       (ACNT.T_USERFIELD4 = CHR(0)) OR
                       (ACNT.T_USERFIELD4 = CHR(1)) OR
                       (ACNT.T_USERFIELD4 LIKE '0x%') THEN
                   ACNT.T_ACCOUNT
                  ELSE
                   ACNT.T_USERFIELD4
                END as UF4
           from DVN
           LEFT JOIN DDVNFI_DBT NFI0
             ON (DVN.T_ID = NFI0.T_DEALID AND NFI0.T_TYPE = 0)
           LEFT JOIN DDVNFI_DBT NFI2
             ON (DVN.T_ID = NFI2.T_DEALID AND NFI2.T_TYPE = 2)
           INNER JOIN DOPROPER_DBT OP
             ON (DVN.T_DOCKIND = OP.T_DOCKIND AND
                LPAD(TO_CHAR(DVN.T_ID), 34, '0') = OP.T_DOCUMENTID)
          INNER JOIN DOPRDOCS_DBT OD --
             ON (OP.T_ID_OPERATION = OD.T_ID_OPERATION and OD.T_DOCKIND = 1)
          INNER JOIN DACCTRN_DBT TRN
             ON (OD.T_ACCTRNID = TRN.T_ACCTRNID AND TRN.T_STATE = 1)
          INNER JOIN DACCOUNT_DBT ACNT
             ON (TRN.T_ACCOUNTID_RECEIVER = ACNT.T_ACCOUNTID)
          /*INNER JOIN DMCACCDOC_DBT ACD
             ON (TRN.T_ACCOUNT_RECEIVER = ACD.T_ACCOUNT and
                ACD.T_CATID IN (163,
                                 164,
                                 460,
                                 461,
                                 476,
                                 477,
                                 611,
                                 612,
                                 614,
                                 616,
                                 617,
                                 766,
                                 767,
                                 781,
                                 782,
                                 874,
                                 875,
                                 184,
                                 185,
                                 453,
                                 454,
                                 605,
                                 606,
                                 608,
                                 609,
                                 768,
                                 769,
                                 779,
                                 780,
                                 789,
                                 790,
                                 794,
                                 795,
                                 868,
                                 869))*/
          where exists(select * from DMCACCDOC_DBT ACD
                   where TRN.T_ACCOUNT_RECEIVER = ACD.T_ACCOUNT and
                              ACD.T_CATID IN (163,
                                               164,
                                               460,
                                               461,
                                               476,
                                               477,
                                               611,
                                               612,
                                               614,
                                               616,
                                               617,
                                               766,
                                               767,
                                               781,
                                               782,
                                               874,
                                               875,
                                               184,
                                               185,
                                               453,
                                               454,
                                               605,
                                               606,
                                               608,
                                               609,
                                               768,
                                               769,
                                               779,
                                               780,
                                               789,
                                               790,
                                               794,
                                               795,
                                               868,
                                               869))) s
             );
             commit;

        qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Дебетовые проводки по сделке',
                       2,
                       null,
                       null);

        -- кредитовые проводки по сделке
        insert /*+ parallel(4) enable_parallel_dml */ into ldr_infa_pfi.tmp_acctrn
         ( select /*+ parallel(4) full(dvn) full(NFI0) full(NFI2) full(OP) full(OD) full(TRN) full(ACNT) full(ACD) use_hash(DVN,NFI2,NFI0,OP,OD,TRN,ACNT,ACD)*/
          distinct dvn.t_id               dealid,
                   dvn.t_dvkind           dvkind,
                   nfi0.t_fiid         nfiid0,
                   nfi2.t_fiid         nfiid2,
                   trn.t_account_receiver acc,
                   trn.t_fiid_receiver,
                   0                      deb,
                   0                      debnat,
                   trn.t_sum_receiver     crd,
                   trn.t_sum_natcur       crdnat,
                   trn.t_date_carry       vdate,
                   --acnt.t_userfield4      uf4
                   case
                            when (acnt.t_userfield4 is null) or
                                (acnt.t_userfield4 = chr(0)) or
                                (acnt.t_userfield4 = chr(1)) or
                                (acnt.t_userfield4 like '0x%') 
                            then
                              acnt.t_account
                            else
                              acnt.t_userfield4
                           end uf4
            from ddvndeal_dbt dvn
            left join ddvnfi_dbt nfi0
              on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
            left join ddvnfi_dbt nfi2
              on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
           inner join doproper_dbt op
              on (dvn.t_dockind = op.t_dockind and
                 lpad(to_char(dvn.t_id), 34, '0') = op.t_documentid)
           inner join doprdocs_dbt od
              on (op.t_id_operation = od.t_id_operation)
           inner join dacctrn_dbt trn
              on (od.t_acctrnid = trn.t_acctrnid)
           inner join daccount_dbt acnt
              on (trn.t_accountid_receiver = acnt.t_accountid)
           inner join dmcaccdoc_dbt acd
              on (trn.t_account_receiver = acd.t_account and
                 acd.t_catid in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869))
           where dvn.t_date <= in_date
             and t_dvkind in (1,2,3,4,6,7,8)
             and od.t_dockind = 1
             and trn.t_state = 1
             );
             commit;
        qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || ' Кредитовые проводки по сделке',
                       2,
                       null,
                       null);
             
        -- Суммы по платежам неттинга     
        insert /*+ parallel(4) enable_parallel_dml */ into ldr_infa_pfi.tmp_acctrn
         (select /*+ parallel(4) full(dvn) full(NFI0) full(NFI2) full(pm) use_hash(DVN,NFI2,NFI0,PM)*/
           distinct dvn.t_id dealid,
                    dvn.t_dvkind dvkind,
                    nfi0.t_fiid         nfiid0,
                    nfi2.t_fiid         nfiid2,
                    pm.t_receiveraccount acc,
                    pm.t_payfiid fiid,
                    0 deb,
                    0 debnat,
                    pm.t_amount crd,
                    rsb_fiinstr.convsum(pm.t_amount, pm.t_payfiid, 0, pm.t_valuedate) crdnat,
                    pm.t_valuedate vdate,
                    --acnt.t_userfield4 uf4
                    case
                      when (acnt.t_accountid is null) then
                        pm.t_receiveraccount
                      when (acnt.t_userfield4 is null) or
                          (acnt.t_userfield4 = chr(0)) or
                          (acnt.t_userfield4 = chr(1)) or
                          (acnt.t_userfield4 like '0x%') then
                        acnt.t_account
                      else
                        acnt.t_userfield4
                    end uf4
            from ddvndeal_dbt dvn
            left join ddvnfi_dbt nfi0
              on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
            left join ddvnfi_dbt nfi2
              on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
           inner join dpmpaym_dbt pm
              on (dvn.t_dockind = pm.t_dockind and dvn.t_id = pm.t_documentid and
                 pm.t_paymstatus = 150)
           left join daccount_dbt acnt
              on (pm.t_chapter = acnt.t_chapter and pm.t_receiveraccount = acnt.t_account and pm.t_payfiid = acnt.t_code_currency)
           where dvn.t_date <= in_date
              and dvn.t_dvkind in (1,2,3,4,6,7,8)
              and pm.t_receiverbankid = 1
              and exists (select 1
                            from dmcaccdoc_dbt acd
                           where acd.t_account = pm.t_receiveraccount --  
                             and acd.t_catid in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869))
             );
             commit;
        qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Суммы по платежам неттинга (Шаг 1)',
                       2,
                       null,
                       null);

        insert /*+ parallel(4) enable_parallel_dml */ into ldr_infa_pfi.tmp_acctrn
         (select /*+ parallel(4) full(dvn) full(NFI0) full(NFI2) full(pm) use_hash(DVN,NFI2,NFI0,PM)*/
          distinct dvn.t_id dealid,
                   dvn.t_dvkind dvkind,
                   nfi0.t_fiid         nfiid0,
                   nfi2.t_fiid         nfiid2,
                   pm.t_payeraccount acc,
                   pm.t_fiid fiid,
                   pm.t_amount deb,
                   rsb_fiinstr.convsum(pm.t_amount, pm.t_fiid, 0, pm.t_valuedate) debnat,
                   0 crd,
                   0 crdnat,
                   pm.t_valuedate vdate,
                   --acnt.t_userfield4 uf4
                   case
                      when (acnt.t_accountid is null) then
                        pm.t_payeraccount
                      when (acnt.t_userfield4 is null) or
                          (acnt.t_userfield4 = chr(0)) or
                          (acnt.t_userfield4 = chr(1)) or
                          (acnt.t_userfield4 like '0x%') then
                        acnt.t_account
                      else
                        acnt.t_userfield4
                   end uf4
            from ddvndeal_dbt dvn
            left join ddvnfi_dbt nfi0
              on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
            left join ddvnfi_dbt nfi2
              on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
           inner join dpmpaym_dbt pm
              on (dvn.t_dockind = pm.t_dockind and dvn.t_id = pm.t_documentid and
                 pm.t_paymstatus = 150)
           inner join daccount_dbt acnt
              on (pm.t_chapter = acnt.t_chapter and pm.t_payeraccount = acnt.t_account and pm.t_fiid = acnt.t_code_currency)
            where dvn.t_date <= in_date
              and dvn.t_dvkind in (1,2,3,4,6,7,8)
              and pm.t_payerbankid = 1
              and exists (select 1
                            from dmcaccdoc_dbt acd
                           where acd.t_account = pm.t_payeraccount
                             and acd.t_catid in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869))
              );
commit;
        qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || ' Суммы по платежам неттинга (Шаг 2)',
                       2,
                       null,
                       null);


        --Форварды на ценную бумагу из БОЦБ с признаком ПФИ, BIQ-8474
        if (BIQ_8474 = 1) then
            insert into ldr_infa_pfi.tmp_acctrn
            
            (select dealid, dvkind, nfiid0, nfiid2, acc, fiid, deb, debnat, crd, crdnat, vdate, uf4 from 
             (               
                -- Дебетовые проводки по сделке
             select /*+ INDEX(op DOPROPER_DBT_IDX1)*/
                       tic.t_dealid        dealid,
                       101                 dvkind,--это сделки из БОЦБ
                       leg0.t_pfi          nfiid0,
                       NULL                nfiid2,
                       trn.t_account_payer acc,
                       trn.t_fiid_payer    fiid,
                       trn.t_sum_payer     deb,
                       trn.t_sum_natcur    debnat,
                       0                   crd,
                       0                   crdnat,
                       trn.t_date_carry    vdate,
                       --acnt.t_userfield4   uf4
                       case
                                when (acnt.t_userfield4 is null) or
                                    (acnt.t_userfield4 = chr(0)) or
                                    (acnt.t_userfield4 = chr(1)) or
                                    (acnt.t_userfield4 like '0x%') 
                                then
                                  acnt.t_account
                                else
                                  acnt.t_userfield4
                               end uf4
             from ddl_tick_dbt tic
                left join ddl_leg_dbt leg0
                  on (tic.t_dealid = leg0.t_dealid and leg0.t_legkind = 0)
                inner join doproper_dbt op
                 on (tic.t_bofficekind = op.t_dockind and lpad(to_char(tic.t_dealid), 34, '0') = op.t_documentid)
               inner join doprdocs_dbt od
                  on (op.t_id_operation = od.t_id_operation)
               inner join dacctrn_dbt trn
                  on (od.t_acctrnid = trn.t_acctrnid)
               inner join dmcaccdoc_dbt acd
                  on (trn.t_account_payer = acd.t_account and
                     acd.t_catid in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869))
               inner join daccount_dbt acnt
                  on (trn.t_accountid_payer = acnt.t_accountid)
               where  tic.t_bofficekind = 101 and tic.t_dealtype in (12183, 12193) and tic.t_ispfi = chr(88) and substr(tic.t_dealcode, 1, 2) = 'Д/'
                 and od.t_dockind = 1
                 and trn.t_state = 1 
                 and tic.t_dealdate <= in_date
            union all
                -- кредитовые проводки по сделке
            select /*+ INDEX(op DOPROPER_DBT_IDX1)*/
                       tic.t_dealid           dealid,
                       101                    dvkind,--это сделки из БОЦБ
                       leg0.t_pfi             nfiid0,
                       NULL                   nfiid2,
                       trn.t_account_receiver acc,
                       trn.t_fiid_receiver    fiid,
                       0                      deb,
                       0                      debnat,
                       trn.t_sum_receiver     crd,
                       trn.t_sum_natcur       crdnat,
                       trn.t_date_carry       vdate,
                       --acnt.t_userfield4   uf4
                       case
                                when (acnt.t_userfield4 is null) or
                                    (acnt.t_userfield4 = chr(0)) or
                                    (acnt.t_userfield4 = chr(1)) or
                                    (acnt.t_userfield4 like '0x%') 
                                then
                                  acnt.t_account
                                else
                                  acnt.t_userfield4
                               end uf4
                from ddl_tick_dbt tic
                left join ddl_leg_dbt leg0
                  on (tic.t_dealid = leg0.t_dealid and leg0.t_legkind = 0)
                inner join doproper_dbt op
                 on (tic.t_bofficekind = op.t_dockind and lpad(to_char(tic.t_dealid), 34, '0') = op.t_documentid)
               inner join doprdocs_dbt od
                  on (op.t_id_operation = od.t_id_operation)
               inner join dacctrn_dbt trn
                  on (od.t_acctrnid = trn.t_acctrnid)
               inner join dmcaccdoc_dbt acd
                  on (trn.t_account_receiver = acd.t_account and
                     acd.t_catid in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869))
               inner join daccount_dbt acnt
                  on (trn.t_accountid_receiver = acnt.t_accountid)
               where  tic.t_bofficekind = 101 and tic.t_dealtype in (12183, 12193) and tic.t_ispfi = chr(88) and substr(tic.t_dealcode, 1, 2) = 'Д/'
                 and od.t_dockind = 1
                 and trn.t_state = 1 
                 and tic.t_dealdate <= in_date
                  )
                  group by dealid, dvkind, nfiid0, nfiid2, acc, fiid, deb, debnat, crd, crdnat, vdate, uf4
                  );
                qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Форварды на ценную бумагу из БОЦБ с признаком ПФИ',
                       2,
                       null,
                       null);
   
        end if;
commit;
        delete -- Удаление счетов открытых на неверных кодах ФИ металлов
          from ldr_infa_pfi.tmp_acctrn
         where (acc, fiid) in (select acc.t_account, acc.t_code_currency
                                 from daccount_dbt acc, dfininstr_dbt fn
                                where substr(acc.t_account, 6, 1) != 'A'
                                  and fn.t_fiid = acc.t_code_currency
                                  and fn.t_fi_kind = 6);
commit;
      qb_bp_utils.SetError(EventID,
                       '',
                       to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Удаление счетов открытых на неверных кодах ФИ металлов',
                       2,
                       null,
                       null);

        insert into ldr_infa_pfi.fct_deal_rst(deal_code, account_code, val_rst_acc_in, val_rst_cur_in, val_rst_nat_in, val_rst_rur_in, val_rst_amt_in, val_dbt_acc, val_dbt_cur, val_dbt_nat, val_dbt_rur, val_dbt_amt, val_crd_acc, val_crd_cur, val_crd_nat, val_crd_rur, val_crd_amt, val_rst_acc_out, val_rst_cur_out, val_rst_nat_out, val_rst_rur_out, val_rst_amt_out, dt, rec_status, sysmoment, ext_file)
        select * from (
         (with dob as
           (select dealid,
                   dvkind,
                   nfiid0,
                   nfiid2,
                   acc,
                   fiid,
                   vdate,
                   uf4,
                   round(sum(deb), 2) deb,
                   round(sum(debnat), 2) debnat,
                   round(sum(crd), 2) crd,
                   round(sum(crdnat), 2) crdnat,
                   round(sum(deb), 2) - round(sum(crd), 2) sum_ob,
                   round(sum(debnat), 2) - round(sum(crdnat), 2) sum_obnat
              from ldr_infa_pfi.tmp_acctrn
             group by dealid,
                      dvkind,
                      nfiid0,
                      nfiid2,
                      acc,
                      fiid,
                      vdate,
                      uf4),
          dob_rest as
           (select dob.*,
                   count(*) over(partition by dealid, acc) cnt,
                   sum(sum_ob) over(partition by dealid, acc order by vdate) - sum_ob rest_in,
                   sum(sum_ob) over(partition by dealid, acc order by vdate) rest_out,
                   sum(sum_obnat) over(partition by dealid, acc order by vdate) - sum_obnat rest_innat,
                   sum(sum_obnat) over(partition by dealid, acc order by vdate) rest_outnat
              from dob
             order by dealid,
                      acc,
                      vdate)
          select to_char(dealid) || case
                 when (dvkind = 101 and BIQ_8474 = 1)
                 then '#CB#DVN#90'
                 else
                   '#DVN#' || case
                   when dvkind = 2 then
                    '95' -- Опцион
                   when dvkind in (1, 7) then
                    '90' -- Валютный своп
                   when dvkind in (3, 6) then
                    '91' -- Валютный своп
                   when dvkind = 8 then
                    '3' -- Банкнотные сделки
                   when dvkind = 4 then
                    case
                      when nfiid0 <> 0 or nfiid2 <> 0 then
                       '96' -- Валютно-процентный своп
                      else
                       '93' -- Процентный своп
                    end
                   else
                    '-1-' || dvkind
                   end 
                 end deal_code,
                 '0000#IBSOXXX#' || uf4 account_code,
                 to_char(rest_in,
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_rst_acc_in,
                 null val_rst_cur_in,
                 to_char(rest_innat,
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_rst_nat_in,
                 to_char(rest_innat,
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_rst_rur_in,
                 null val_rst_amt_in,
                 to_char(deb,
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_dbt_acc,
                 null val_dbt_cur,
                 to_char(round(debnat, 2),
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_dbt_nat,
                 to_char(round(debnat, 2),
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_dbt_rur,
                 null val_dbt_amt,
                 to_char(crd,
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_crd_acc,
                 null val_crd_cur,
                 to_char(round(crdnat, 2),
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_crd_nat,
                 to_char(round(crdnat, 2),
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_crd_rur,
                 null val_crd_amt,
                 to_char(rest_out,
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_rst_acc_out,
                 null val_rst_cur_out,
                 to_char(rest_outnat,
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_rst_nat_out,
                 to_char(rest_outnat,
                         '999999999999999999999D999',
                         'nls_numeric_characters=''. ''') val_rst_rur_out,
                 null val_rst_amt_out,
                 qb_dwh_utils.datetochar(vdate) dt,
                 '0' rec_status,
                 to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') sysmoment,
                 dwhEXT_FILE
            from dob_rest));


      end if;
commit;
      -- Выгрузка в DET_ROLEACCOUNT_DEAL
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника ролей',
                     2,
                     null,
                     null);

      insert into ldr_infa_pfi.det_roleaccount_deal(code, name, orole_code, dt, rec_status, sysmoment, ext_file)
       (select '9999#SOFRXXX#' || Upper(cat.t_code) CODE,
               cat.t_name NAME,
               case when cat.t_code like '+Форвард%' then
                   '1'
                 when cat.t_code like '-Форвард%' then
                   '2'
                 else
                   '701'
               end OROLE_CODE,
               qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from dmccateg_dbt cat
--         where cat.t_code in ('-Форвард, расчеты0', '+Форвард, расчеты0', '-ПФИ0', '+ПФИ0'));
         where cat.t_id in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869));
commit;

      -- Выгрузка в DET_DEAL_CAT
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка категорий по сделкам',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.det_deal_cat(code_deal_cat, name_deal_cat, is_multivalued, dt, rec_status, sysmoment, ext_file)
       (select distinct to_char(ac.t_objecttype) || 'C' || ac.t_groupid CODE_DEAL_CAT,
               upper(gr.t_name) NAME_DEAL_CAT,
               case
                 when gr.t_type = chr(88) then
                   '0'
                 else
                   '1'
               end IS_MULTIVALUED,
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from dobjatcor_dbt ac
         inner join dobjgroup_dbt gr
            on (ac.t_objecttype = gr.t_objecttype and ac.t_groupid = gr.t_groupid)
         where ac.t_objecttype in (140, 145, 148)
           and ac.t_validfromdate <= in_date);
commit;           
      -- Выгрузка в DET_DEAL_CAT_VAL
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка допустимых значений категорий по сделкам',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.det_deal_cat_val(deal_cat_code, code_deal_cat_val, name_deal_cat_val, dt, rec_status, sysmoment, ext_file)
       (select distinct
               to_char(ac.t_objecttype) || 'C' || to_char(ac.t_groupid) DEAL_CAT_CODE,
               to_char(ac.t_objecttype) || 'C' || to_char(ac.t_groupid) || '#' || atr.t_attrid CODE_DEAL_CAT_VAL,
               to_char(ac.t_objecttype) || 'C' || to_char(ac.t_groupid) || '#' || case when trim(atr.t_fullname) is null or trim(atr.t_fullname) = chr(1) then atr.t_nameobject else atr.t_fullname end NAME_DEAL_CAT_VAL,
               qb_dwh_utils.datetochar(to_date('01011980','ddmmyyyy')) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from dobjatcor_dbt ac
         inner join dobjgroup_dbt gr
            on (ac.t_objecttype = gr.t_objecttype and ac.t_groupid = gr.t_groupid)
         inner join dobjattr_dbt atr
            on (gr.t_objecttype = atr.t_objecttype and gr.t_groupid = atr.t_groupid)
         where ac.t_objecttype in (140, 145, 148));
      -- Выгрузка в ASS_DEAL_CAT_VAL
commit;      
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка ассоциатора значений категорий по сделкам',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.ass_deal_cat_val(deal_code, deal_cat_val_code_deal_cat, deal_cat_val_code, dt, rec_status, sysmoment, ext_file)
       (select distinct
               to_char(to_number(ac.t_object)) || decode(ac.t_objecttype , 140, '#DV#', '#DVN#') ||
                 case
                   when ac.t_objecttype = 140 then
                    (select decode(fi.t_avoirkind, 2, '95', '94')
                       from dfininstr_dbt fi
                      where fi.t_fiid = (select dv.t_fiid
                                           from ddvdeal_dbt dv
                                          where dv.t_id = to_number(ac.t_object)
                                        )
                    )
                   else
                    (select case
                              when dvn.t_dvkind = 2 then
                               '95' -- Опцион
                              when dvn.t_dvkind in (1, 7) then
                               '90' -- SPOT (TOD, TOM, NEXT) и FORWARD
                              when dvn.t_dvkind in (3, 6) then
                               '91' -- Валютный своп
                              when dvn.t_dvkind = 8 then
                               '3' -- Банкнотные сделки
                              else
                               '-1-' || dvn.t_dvkind
                            end
                       from ddvndeal_dbt dvn
                       where dvn.t_id = to_number(ac.t_object))
                 end DEAL_CODE,
               to_char(ac.t_objecttype) || 'C' || to_char(ac.t_groupid) DEAL_CAT_VAL_CODE_DEAL_CAT,
               to_char(ac.t_objecttype) || 'C' || to_char(ac.t_groupid) || '#' || atr.t_attrid DEAL_CAT_VAL_CODE,
               qb_dwh_utils.datetochar(ac.t_validfromdate) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from dobjatcor_dbt ac
         inner join dobjgroup_dbt gr
            on (ac.t_objecttype = gr.t_objecttype and ac.t_groupid = gr.t_groupid)
         inner join dobjattr_dbt atr
            on (gr.t_objecttype = atr.t_objecttype and gr.t_groupid = atr.t_groupid)
         where (ac.t_objecttype = 140 and
                to_number(ac.t_object DEFAULT null ON CONVERSION ERROR ) in (select dvs.t_id  from ddvdeal_dbt dvs where dvs.t_date <= in_date  and dvs.t_client = -1))or
               (ac.t_objecttype in (145, 148) and
                to_number(ac.t_object DEFAULT null ON CONVERSION ERROR ) in (select dvns.t_id
                                             from ddvndeal_dbt dvns
                                            where dvns.t_dvkind in (1, 2, 3, 6, 7, 8)
                                              and dvns.t_date <= in_date )));
commit;                                              
    --> BIQ 10007                                          
    --Выгрузка аналитических счетов СОФР (DET_ACCOUNT_SOFR) 
    insert into ldr_infa_pfi.DET_ACCOUNT_SOFR(code, account_number, account_name, is_inconsolidate, dt_open_acc, dt_close_acc, system_code, subject_code, 
                                              finstr_code, department_code, currency_code_txt, chapter_code, dt, rec_status, sysmoment, ext_file)
         select '0000#SOFRXXX#' || acc.t_account as CODE,
                acc.t_account as ACCOUNT_NUMBER,
                acc.t_nameaccount as ACCOUNT_NAME,
                decode(acc.t_legalform,2,1,0) as IS_INCONSOLIDATE,
                qb_dwh_utils.DateToChar(acc.t_open_date) as DT_OPEN_ACC,
                qb_dwh_utils.DateToChar(acc.t_close_date) as DT_CLOSE_ACC,
                'SOFRXXX' as SYSTEM_CODE,
                acc_subj.subject_code as subject_code,
                acc.t_fi_code as FINSTR_CODE,                                      
                acc.t_name as DEPARTMENT_CODE,
                acc.t_ccy as CURRENCY_CODE_TXT,
                'А' as CHAPTER_CODE,
                nvl(qb_dwh_utils.DateToChar(acc.t_open_date),qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy'))) as DT,
                '0' as REC_STATUS,
                to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') as SYSMOMENT,
                dwhEXT_FILE as EXT_FILE
                                              
         from ldr_infa_pfi.tmp_acc306 acc
         inner join ldr_infa_pfi.tmp_acc_subj_306 acc_subj on acc.t_accountid = acc_subj.t_accountid;
commit;

    -- исключение для разовая выгрузка с 24 января 2023 года по 16 сентября 2023 года DEF-42824
    if (trunc(sysdate) <> trunc(DateForStartLoadFrom24012023)) then
      --Выгрузка Остатков и оборотов по лицевым счетам СОФР (FCT_ACCOUNT_SOFR) - BIQ 10007              
      insert /*+ parallel(8) enable_parallel_dml */ into ldr_infa_pfi.FCT_ACCOUNT_SOFR(account_sofr_code, rest, rest_nat, debet, debet_nat, credit, credit_nat, 
                                            dt, sysmoment, rec_status, ext_file)
       select '0000#SOFRXXX#' || a.t_account as ACCOUNT_SOFR_CODE,
              --nvl(v.t_rest, o.t_rest) as rest,
              --o.t_rest as rest_nat,
              --nvl(v_o.t_debet, o.t_debet) as debet,
              --o.t_debet as debet_nat,
              --nvl(v_o.t_credit, o.t_credit) as credit,
              --o.t_credit as credit_nat,
              to_char (rsi_rsb_account.restall(a.t_account, 1, a.t_code_currency, prev_in_date, a.t_code_currency)) as REST,
              to_char (round(rsi_rsb_account.restall(a.t_account, 1, a.t_code_currency, prev_in_date ,0),2)) as REST_NAT,
              to_char (rsi_rsb_account.debetac(a.t_account, 1, a.t_code_currency, prev_in_date , prev_in_date, a.t_code_currency)) as DEBET,
              to_char (rsi_rsb_account.debetac(a.t_account, 1, a.t_code_currency, prev_in_date , prev_in_date, 0)) as DEBET_NAT,
              to_char (rsi_rsb_account.kreditac(a.t_account, 1, a.t_code_currency, prev_in_date , prev_in_date , a.t_code_currency)) as CREDIT,
              to_char (rsi_rsb_account.kreditac(a.t_account, 1, a.t_code_currency, prev_in_date , prev_in_date, 0)) as CREDIT_NAT,
              qb_dwh_utils.DateToChar(prev_in_date) as DT,
              qb_dwh_utils.DateTimeToChar(sysdate) as SYSMOMENT,
              '0' as REC_STATUS,
              dwhEXT_FILE as EXT_FILE
       from ldr_infa_pfi.tmp_acc306 a
       --inner join Drestdate_dbt o on a.t_accountid = o.t_accountid and o.t_restdate = dd and o.t_restcurrency = 0
       --left join val_max_day d on d.t_accountid = a.t_accountid
       --left join Drestdate_dbt v on d.t_accountid = v.t_accountid and v.t_restdate = max_day and v.t_restcurrency <> 0
       --left join Drestdate_dbt v_o on d.t_accountid = v_o.t_accountid and v_o.t_restdate = dd and v_o.t_restcurrency <> 0
       union
       SELECT '0000#SOFRXXX#' || a.t_account as ACCOUNT_SOFR_CODE,
              to_char (rsi_rsb_account.restall(a.t_account, 1, a.t_code_currency, trn.t_date_carry, a.t_code_currency)) as REST,
              to_char (round(rsi_rsb_account.restall(a.t_account, 1, a.t_code_currency, trn.t_date_carry , 0),2)) as REST_NAT,
              to_char (rsi_rsb_account.debetac(a.t_account, 1, a.t_code_currency, trn.t_date_carry , trn.t_date_carry, a.t_code_currency)) as DEBET,
              to_char (rsi_rsb_account.debetac(a.t_account, 1, a.t_code_currency, trn.t_date_carry , trn.t_date_carry, 0)) as DEBET_NAT,
              to_char (rsi_rsb_account.kreditac(a.t_account, 1, a.t_code_currency, trn.t_date_carry , trn.t_date_carry , a.t_code_currency)) as CREDIT,
              to_char (rsi_rsb_account.kreditac(a.t_account, 1, a.t_code_currency, trn.t_date_carry , trn.t_date_carry, 0)) as CREDIT_NAT,
              qb_dwh_utils.DateToChar(trn.t_date_carry) as DT,
              qb_dwh_utils.DateTimeToChar(sysdate) as SYSMOMENT,
              '0' as REC_STATUS,
              dwhEXT_FILE as EXT_FILE
         FROM DACCTRN_DBT  trn
              INNER JOIN Daccount_dbt a
                  ON a.t_accountid =
               CASE
                   WHEN SUBSTR (trn.t_account_receiver, 1, 5) IN ('30606', '30601') 
                     THEN  trn.t_accountid_receiver
                   WHEN SUBSTR (trn.t_account_payer, 1, 5) IN ('30606', '30601')
                     THEN trn.t_accountid_payer
               END
        WHERE     trn.t_systemdate = prev_in_date
              AND (trn.t_systemdate - trn.t_date_carry) > 1 
       ;
     commit;
     delete from ldr_infa_pfi.FCT_ACCOUNT_SOFR f where f.debet = 0 and f.debet_nat = 0 and f.credit = 0 and f.credit_nat = 0;
     commit;
   end if;
    --< BIQ 10007                                              
    --end if;
                                              
      -- Выгрузка в DET_DEAL_TYPEATTR
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника доп. атрибутов',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.det_deal_typeattr(code, name, is_money_value, data_type, dt, rec_status, sysmoment, ext_file)
      (select distinct to_char(nt.t_objecttype) || 'T' || to_char(nt.t_notekind) code,
              upper(trim(nk.t_name)) name,
              case
                when nk.t_notetype = 25 then
                 '1'
                else
                 '0'
              end is_money_value,
              case
                when nk.t_notetype = 9 then -- Дата
                 '2'
                when nk.t_notetype = 7 then -- Дата
                 '3'
                else
                 '1'
              end data_type,
              qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
              '0' recstatus,
              to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
         from dnotetext_dbt nt
        inner join dnotekind_dbt nk
           on (nt.t_objecttype = nk.t_objecttype and nt.t_notekind = nk.t_notekind)
        where nt.t_objecttype in (140, 145, 148)
       union all
       select 'FIXING_DATE_RATE' code,
              'БЛИЖАЙШАЯ ДАТА ПЕРЕСМОТРА ПРОЦЕНТНОЙ СТАВКИ' name,
              '0' is_money_value,
              '2' data_type,
              qb_dwh_utils.datetochar(to_date('01011980', 'ddmmyyyy')) dt,
              '0' recstatus,
              to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
         from dual 
          union all
        select 'EXTERNAL_ID#SWP0' CODE,--BIQ-8474 п.3.2 пп.1, п.3.5.1
               'Параметры первой части ценовых условий' NAME,
               '0' is_money_value,
               '3' data_type,
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from dual where BIQ_8474 = 1
        union all
        select 'EXTERNAL_ID#SWP2' CODE,--BIQ-8474 п.3.2 пп.1, п.3.5.1
               'Параметры второй части ценовых условий' NAME,
               '0' is_money_value,
               '3' data_type,
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from dual where BIQ_8474 = 1
         );
         commit;
      -- Выгрузка в DET_TYPEATTR
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника значений доп. атрибутов субъекта',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.det_typeattr(code, name, multyvalue, dt, rec_status, sysmoment, ext_file)
       (select 'RATING_3453U' CODE,
               'Рейтинг контрагента в соответствии с  Указанием 3453-У' NAME,
               '0' MULTYVALUE,
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from dual
        union all
        select 'RATING_COMMON' CODE,
               'Рейтинг контрагента на отчетную дату' NAME,
               '0' MULTYVALUE,
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from dual
        union all
        select 'COUNTRY_ASSESSMENT' CODE,
               'Страновая оценка' NAME,
               '0' MULTYVALUE,
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from dual
        --> BIQ 10007
        union all
        select 'RESIDENCY' as CODE,
               'Резидентство' as NAME,
               '1' as MULTYVALUE,
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) as DT,
               '0' as REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') as SYSMOMENT, 
               dwhEXT_FILE as EXT_FILE
        from dual
        union all
        select 'NATIONALITY' as CODE,
               'Гражданство (страна регистрации)' as NAME,
               '1' as MULTYVALUE,
               qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) as DT,
               '0' as REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') as SYSMOMENT, 
               dwhEXT_FILE as EXT_FILE
        from dual
        --< BIQ 10007          
         );
         commit;
      -- Выгрузка в FCT_SUBJ_INDICATOR
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка списка значений доп. атрибутов субъекта',
                     2,
                     null,
                     null);
                   
      insert into ldr_infa_pfi.fct_subj_indicator(subject_code, department_code, system_code, typeattr_code, value, dt_reg, dt, rec_status, sysmoment, ext_file)
      (select * from (with subj as (
                      select * from(
                      select distinct t_contractor ptid, t_name dprt
                        from (select 1 t_contractor, dp.t_name
                                from ddvdeal_dbt dv
                               inner join ddp_dep_dbt dp
                                  on (dv.t_department = dp.t_code)
                               where dv.t_client = -1
                               group by dv.t_client, dp.t_name
                              union all
                              select decode(dvn.t_contractor,94606, 114511,dvn.t_contractor), dp.t_name
                                from ddvndeal_dbt dvn
                               inner join ddp_dep_dbt dp
                                  on (dvn.t_department = dp.t_code)
                                group by decode(dvn.t_contractor,94606, 114511,dvn.t_contractor), dp.t_name)
                        where t_contractor > 0))
                      select qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     decode(subj.ptid, -1, 1, subj.ptid)) SUBJECT_CODE,
                             subj.dprt DEPARTMENT_CODE,
                             'SOFRXXX' SYSTEM_CODE,
                             'RATING_3453U' TYPEATTR_CODE,
                             subjrate.rcode VALUE,
                             qb_dwh_utils.DateToChar(subjrate.rdate) DT_REG,
                             qb_dwh_utils.DateToChar(subjrate.rdate) DT,
                             '0' REC_STATUS,
                             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                       from subj
                      inner join (select to_number(ac.t_object) ptid, ac.t_validfromdate rdate, atr.t_name rcode
                                    from dobjatcor_dbt ac
                                   inner join dobjgroup_dbt gr
                                      on (ac.t_objecttype = gr.t_objecttype and ac.t_groupid = gr.t_groupid)
                                   inner join dobjattr_dbt atr
                                      on (gr.t_objecttype = atr.t_objecttype and gr.t_groupid = atr.t_groupid and ac.t_attrid = atr.t_attrid)
                                   where ac.t_objecttype = 3
                                     and ac.t_groupid = 19
                                     and ac.t_general = chr(88)
                                     and ac.t_validtodate <= in_date) subjrate
                              on (decode(subj.ptid, -1, 1, subj.ptid) = subjrate.ptid)    )
                      union all
                      select * from (
                      with subj as (
                      select * from(
                      select distinct t_contractor ptid, t_name dprt
                        from (select decode(dv.t_client, -1, 1,dv.t_client) t_contractor, dp.t_name
                                from ddvdeal_dbt dv
                               inner join ddp_dep_dbt dp
                                  on (dv.t_department = dp.t_code)
                               where dv.t_client = -1
                               group by decode(dv.t_client, -1, 1,dv.t_client), dp.t_name
                              union all
                              select decode(dvn.t_contractor, -1, 1, decode(dvn.t_contractor,94606, 114511,dvn.t_contractor)) t_contractor, dp.t_name
                                from ddvndeal_dbt dvn
                               inner join ddp_dep_dbt dp
                                  on (dvn.t_department = dp.t_code)
                                group by decode(dvn.t_contractor, -1, 1, decode(dvn.t_contractor,94606, 114511,dvn.t_contractor)), dp.t_name)
                        where t_contractor > 0))
                      select qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     decode(subj.ptid, -1, 1, subj.ptid)) SUBJECT_CODE,
                             subj.dprt DEPARTMENT_CODE,
                             'SOFRXXX' SYSTEM_CODE,
                             'RATING_COMMON' TYPEATTR_CODE,
                             subjrate.rcode VALUE,
                             qb_dwh_utils.DateToChar(subjrate.rdate) DT_REG,
                             qb_dwh_utils.DateToChar(subjrate.rdate) DT,
                             '0' REC_STATUS,
                             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                       from subj
                      inner join (select to_number(ac.t_object) ptid, ac.t_validfromdate rdate, atr.t_name rcode
                                    from dobjatcor_dbt ac
                                   inner join dobjgroup_dbt gr
                                      on (ac.t_objecttype = gr.t_objecttype and ac.t_groupid = gr.t_groupid)
                                   inner join dobjattr_dbt atr
                                      on (gr.t_objecttype = atr.t_objecttype and gr.t_groupid = atr.t_groupid and ac.t_attrid = atr.t_attrid)
                                   where ac.t_objecttype = 3
                                     and ac.t_groupid = 19
                                     and ac.t_general = chr(88)
                                     and in_date between ac.t_validfromdate and ac.t_validtodate) subjrate
                              on (decode(subj.ptid, -1, 1, subj.ptid) = subjrate.ptid)
                      )
                      union all
                      select * from (
                      with subj as (
                      select * from(
                      select distinct t_contractor ptid, t_name dprt
                        from (select decode(dv.t_client, -1, 1,dv.t_client) t_contractor,
                                     dp.t_name
                                from ddvdeal_dbt dv
                               inner join ddp_dep_dbt dp
                                  on (dv.t_department = dp.t_code)
                               where dv.t_client = -1
                               group by decode(dv.t_client, -1, 1,dv.t_client), dp.t_name
                              union all
                              select decode(dvn.t_contractor, -1, 1, decode(dvn.t_contractor, 94606, 114511,dvn.t_contractor)) t_contractor,
                                     dp.t_name
                                from ddvndeal_dbt dvn
                               inner join ddp_dep_dbt dp
                                  on (dvn.t_department = dp.t_code)
                                group by decode(dvn.t_contractor, -1, 1, decode(dvn.t_contractor, 94606, 114511,dvn.t_contractor)), dp.t_name)
                       where t_contractor > 0))
                      select qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     subj.ptid) SUBJECT_CODE,
                             subj.dprt DEPARTMENT_CODE,
                             'SOFRXXX' SYSTEM_CODE,
                             'COUNTRY_ASSESSMENT' TYPEATTR_CODE,
                             country.t_riskclass VALUE,
                             qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT_REG,
                             qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) DT,
                             '0' REC_STATUS,
                             to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                        from subj
                      inner join dparty_dbt pt
                        on (subj.ptid = pt.t_partyid)
                      inner join dcountry_dbt country
                        on (pt.t_nrcountry = country.t_codelat3)
                      where country.t_riskclass <> chr(0))
                      --> BIQ-10007
                      union all 
                      
                      select * 
                      from (
                      with acc_subj as 
                      (select distinct T_PARTYID, SUBJECT_CODE
                      from ldr_infa_pfi.tmp_acc_subj_306)
                       
                      select distinct ss.SUBJECT_CODE as SUBJECT_CODE,
                            acc.t_name as DEPARTMENT_CODE,
                            'SOFRXXX' as SYSTEM_CODE,
                            'RESIDENCY' as TYPEATTR_CODE,
                            decode(acc.t_notresident,'X','Нет','Да') as VALUE,
                            qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) as DT_REG,
                            qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) as DT,
                            '0' as REC_STATUS,
                            to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') as SYSMOMENT, 
                            dwhEXT_FILE as EXT_FILE  
                      from acc_subj ss
                      inner join ldr_infa_pfi.tmp_acc306 acc on ss.T_PARTYID = acc.t_client)
                      
                      union all 
                      
                      select * 
                      from (
                      with acc_subj as 
                      (select distinct T_PARTYID, SUBJECT_CODE
                      from ldr_infa_pfi.tmp_acc_subj_306)
                       
                      select distinct ss.SUBJECT_CODE as SUBJECT_CODE,
                            acc.t_name as DEPARTMENT_CODE,
                            'SOFRXXX' as SYSTEM_CODE,
                            'NATIONALITY' as TYPEATTR_CODE,
                            T_NRCOUNTRY as VALUE,
                            qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) as DT_REG,
                            qb_dwh_utils.DateToChar(to_date('01011980','ddmmyyyy')) as DT,
                            '0' as REC_STATUS,
                            to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') as SYSMOMENT, 
                            dwhEXT_FILE as EXT_FILE  
                      from acc_subj ss
                      inner join ldr_infa_pfi.tmp_acc306 acc on ss.T_PARTYID = acc.t_client)
                      --< BIQ-10007                      
                      );
                      commit;
                      --> BIQ-10007
                     insert /*+ parallel(4) enable_parallel_dml */ into ldr_infa_pfi.ASS_DET_ACCOUNT_SOFR(acc_ass_kind_code, account_par_code, account_sofr_chi_code, dt, sysmoment, rec_status, ext_file)
                             select 
                                  '9999#SOFRXXX#CONN306' as ACC_ASS_KIND_CODE,
                                  '0000#IBSOXXX#'||s.t_userfield4 as ACCOUNT_PAR_CODE,
                                  '0000#SOFRXXX#'||s.t_account as ACCOUNT_SOFR_CHI_CODE,
                                  qb_dwh_utils.DateToChar(nvl(s.t_open_date, to_date('01011980','ddmmyyyy'))) as DT,
                                  to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') as SYSMOMENT,
                                  '0' as REC_STATUS,
                                  dwhEXT_FILE as EXT_FILE
         
                             from ldr_infa_pfi.tmp_acc306 s
                             where t_legalform = 1
                             and s.t_userfield4 is not null and s.t_userfield4 <> '' --ЮЛ
                       union all     
                              select ACC_ASS_KIND_CODE, ACCOUNT_PAR_CODE, ACCOUNT_SOFR_CHI_CODE, DT, SYSMOMENT, REC_STATUS, EXT_FILE from (select 
                                   '9999#SOFRXXX#CONN306' as ACC_ASS_KIND_CODE, 
                                   '0000#IBSOXXX#'||nvl(decode(a.t_userfield4,chr(1),null,a.t_userfield4),br.t_account) as ACCOUNT_PAR_CODE,  
                                   '0000#SOFRXXX#'||dvn.t_Account ACCOUNT_SOFR_CHI_CODE, 
                                   qb_dwh_utils.DateToChar(nvl(s.t_open_date, to_date('01011980','ddmmyyyy'))) as DT,
                                   to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') as SYSMOMENT,
                                   '0' as REC_STATUS,
                                   dwhEXT_FILE as EXT_FILE
                               from dmcaccdoc_dbt dvn
                               inner join DSFCONTR_DBT sf on sf.t_id = dvn.t_clientcontrid and sf.t_partyid = dvn.t_owner and dvn.t_iscommon='X'
                               left join dobjatcor_dbt c
                                 on lpad(sf.T_ID, 10, '0')=c.t_object and c.t_objecttype = 659 and c.t_groupid = 102
                               left join DOBJATTR_DBT a1
                                 on a1.t_groupid = c.t_groupid and a1.t_attrid = c.t_attrid and a1.t_objecttype = c.t_objecttype
                               left join Dbrokacc_Dbt br on
                                 ((br.t_servkind = sf.t_servkind and br.t_servkindsub = sf.t_servkindsub and (a1.t_name is null or not a1.t_name='Да'))
                                 or ( a1.t_name='Да' and br.t_servkind = 0 and br.t_servkindsub = 0))
                                 and br.t_currency = dvn.t_currency and SUBSTR(br.t_Account, 1, 5) = SUBSTR(dvn.t_Account, 1, 5) 
                               inner join ldr_infa_pfi.tmp_acc306 s on s.t_account = dvn.t_Account and s.t_legalform = 2 --ФЛ
                               inner join Daccount_dbt s
                                 on s.t_account = dvn.t_Account
                               inner join dparty_dbt p
                                 on s.t_client = p.t_partyid
                               left join Daccount_dbt a
                                 on br.t_account = a.t_account and a.t_userfield4 is not null and a.t_userfield4 <> ' ')
                             group by ACC_ASS_KIND_CODE, ACCOUNT_PAR_CODE, ACCOUNT_SOFR_CHI_CODE, DT, SYSMOMENT, REC_STATUS, EXT_FILE ;  
                       commit;                              
                      --< BIQ-10007       

      -- Выгрузка в FCT_DEAL
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка списка сделок',
                     2,
                     null,
                     null);
      insert /*+ parallel(2) enable_parallel_dml */ into ldr_infa_pfi.fct_deal(code, department_code, subject_code, dealtype, docnum, is_interior, begindate, enddate, note, dt, rec_status, sysmoment, ext_file)
      (      select to_char(dv.t_id) || '#DV#' || decode(fi.t_avoirkind, 2, '95', '94') CODE,
               dp.t_name DEPARTMENT_CODE,
               qb_dwh_utils.ModifyCodeSubject('0000#IBSOXXX#' || pc.t_code || 
                                     case when 0 < (select count(1)
                                                      from Dobjcode_Dbt o
                                                     where o.t_objecttype = 3
                                                           and o.t_codekind in (3,6)
                                                           and o.t_state = 0
                                                           and o.t_objectid = pt.t_partyid
                                                       or 0 < (select count(1)
                                                                 from dpartyown_dbt o
                                                                where o.t_partykind = 2
                                                                      and o.t_partyid = pt.t_partyid)
                                                   ) then '#BANKS'
                                          when pt.t_legalform = 1 then '#CUST_CORP'
                                          when (pt.t_legalform = 2 and prs.t_isemployer=chr(88)) then '#CUST_CORP' -- ИП = юрлицо
                                          when pt.t_legalform = 2 then '#PERSON'
                                      end) SUBJECT_CODE,
              case
                when fi.t_avoirkind = 2 then
                  '95' -- Опцион
                when fi.t_avoirkind = 1 then
                  '94' -- Фьючерс
              end DEALTYPE,
              dv.t_code DOCNUM,
              '0' IS_INTERIOR,
              qb_dwh_utils.DateToChar(dv.t_date) BEGINDATE,
              qb_dwh_utils.DateToChar(nvl(fi.t_drawingdate,firstDate)) ENDDATE,
              null NOTE,
              qb_dwh_utils.DateToChar(dv.t_date) DT,
              '0' REC_STATUS,
              to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvdeal_dbt dv
         inner join dfininstr_dbt fi
            on (dv.t_fiid = fi.t_fiid)
         inner join ddp_dep_dbt dp
            on (dv.t_department = dp.t_code)
          left join dparty_dbt pt
            on (decode(dv.t_client, -1, 1, dv.t_client) = pt.t_partyid)
          left join dpersn_dbt prs --проверим признак "Предприниматель"
            on (prs.t_personid = pt.t_partyid)
          left join dpartcode_dbt pc
            on (pt.t_partyid = pc.t_partyid and pc.t_codekind = 101 and pc.t_state = 0)
          where dv.t_istrust = chr(0)
           and fi.t_avoirkind in (1, 2)
           and dv.t_client = -1
           and dv.t_date <= in_date
        union all
        -- Внебиржевые опционы и прочие ПФИ
        select to_char(dvn.t_id) || '#DVN#' || case
                                                when dvn.t_dvkind = 2 then
                                                  '95' -- Опцион
                                                when dvn.t_dvkind in (1, 7) then
                                                  '90' -- Валютный своп
                                                when dvn.t_dvkind in (3, 6) then
                                                  '91' -- Валютный своп
                                                when dvn.t_dvkind = 8 then
                                                  '3' -- Банкнотные сделки
                                                else
                                                  '-1-'|| dvn.t_dvkind
                                              end CODE,
               dp.t_name DEPARTMENT_CODE,
               qb_dwh_utils.ModifyCodeSubject('0000#IBSOXXX#' || pc.t_code ||
                                     case when 0 < (select count(1)
                                                      from Dobjcode_Dbt o
                                                     where o.t_objecttype = 3
                                                           and o.t_codekind in (3,6)
                                                           and o.t_state = 0
                                                           and o.t_objectid = pt.t_partyid
                                                       or 0 < (select count(1)
                                                                 from dpartyown_dbt o
                                                                where o.t_partykind = 2
                                                                      and o.t_partyid = pt.t_partyid)
                                                   ) then '#BANKS'
                                          when pt.t_legalform = 1 then '#CUST_CORP'
                                          when (pt.t_legalform = 2 and prs.t_isemployer=chr(88)) then '#CUST_CORP' -- ИП = юрлицо
                                          when pt.t_legalform = 2 then '#PERSON'
                                      end) SUBJECT_CODE,
              case
                when dvn.t_dvkind = 2 then
                  '95' -- Опцион
                when dvn.t_dvkind in (1, 7) then
                  '90' -- Валютный своп
                when dvn.t_dvkind in (3, 6) then
                  '91' -- Валютный своп
                when dvn.t_dvkind = 8 then
                  '3' -- Банкнотные сделки
                else
                  '-1-'|| dvn.t_dvkind
              end DEALTYPE,
              dvn.t_code DOCNUM,
              '0' IS_INTERIOR,
              qb_dwh_utils.DateToChar(dvn.t_date) BEGINDATE,
              qb_dwh_utils.DateToChar(nvl(nfi2.t_execdate, nfi0.t_execdate)) ENDDATE, -- KS 19.04.2022 В enddate должна попадать дата иполнения второй части (если она есть)
              null NOTE,
              qb_dwh_utils.DateToChar(dvn.t_date) DT,
              '0' REC_STATUS,
              to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvndeal_dbt dvn
         inner join ddp_dep_dbt dp
            on (dvn.t_department = dp.t_code)
          left join dparty_dbt pt
            on (decode(dvn.t_contractor, -1, 1, decode(dvn.t_contractor,94606, 114511,dvn.t_contractor)) = pt.t_partyid)
          left join dpersn_dbt prs --проверим признак "Предприниматель"
            on (prs.t_personid = pt.t_partyid)
          left join dpartcode_dbt pc
            on (pt.t_partyid = pc.t_partyid and pc.t_codekind = 101 and pc.t_state = 0)
          inner join ddvnfi_dbt nfi0
            on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
          left join ddvnfi_dbt nfi2 -- KS 19.04.2022 В enddate должна попадать дата иполнения второй части (если она есть)
            on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
         where dvn.t_dvkind in (1,2,3,6,7,8)
           and  dvn.t_date <= in_date
        union all
        --Форварды на ценную бумагу, BIQ-8474, п.3.2, пп4.
        --Валютно-процентные свопы
        select to_char(dvn.t_id) || '#DVN#' ||
                 case when (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0) then
                       '96'
                     else
                       '93'
                 end CODE,
               dp.t_name DEPARTMENT_CODE,
               qb_dwh_utils.ModifyCodeSubject('0000#IBSOXXX#' || pc.t_code ||
                                     case when 0 < (select count(1)
                                                      from Dobjcode_Dbt o
                                                     where o.t_objecttype = 3
                                                           and o.t_codekind in (3,6)
                                                           and o.t_state = 0
                                                           and o.t_objectid = pt.t_partyid
                                                       or 0 < (select count(1)
                                                                 from dpartyown_dbt o
                                                                where o.t_partykind = 2
                                                                      and o.t_partyid = pt.t_partyid)
                                                   ) then '#BANKS'
                                          when pt.t_legalform = 1 then '#CUST_CORP'
                                          when (pt.t_legalform = 2 and prs.t_isemployer=chr(88)) then '#CUST_CORP' -- ИП = юрлицо                                          
                                          when pt.t_legalform = 2 then '#PERSON'
                                      end) SUBJECT_CODE,

              case when (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0) then
                     '96'
                   else
                     '93'
              end DEALTYPE, -- Валютно-процентный своп
              dvn.t_code DOCNUM,
              '0' IS_INTERIOR,
              qb_dwh_utils.DateToChar(dvn.t_date) BEGINDATE,
              qb_dwh_utils.DateToChar(nvl(nvl(nfi2.t_execdate,nfi0.t_execdate),firstDate)) ENDDATE, -- KS 19.04.2022 В enddate должна попадать дата иполнения второй части (если она есть)
              null NOTE,
              qb_dwh_utils.DateToChar(dvn.t_date) DT,
              '0' REC_STATUS,
              to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvndeal_dbt dvn
         inner join ddp_dep_dbt dp
            on (dvn.t_department = dp.t_code)
         inner join ddvnfi_dbt nfi0
            on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
         inner join ddvnfi_dbt nfi2
            on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
          left join dparty_dbt pt
            on (decode(dvn.t_contractor, -1, 1, decode(dvn.t_contractor,94606, 114511,dvn.t_contractor)) = pt.t_partyid)
          left join dpersn_dbt prs --проверим признак "Предприниматель"
            on (prs.t_personid = pt.t_partyid)
          left join dpartcode_dbt pc
            on (pt.t_partyid = pc.t_partyid and pc.t_codekind = 101 and pc.t_state = 0)
          where dvn.t_dvkind = 4
           and  dvn.t_date <= in_date
         union all
        --Форварды на ценную бумагу из БОЦБ с признаком ПФИ, BIQ-8474
         select to_char(tic.t_dealid) || '#CB#DVN#90' CODE,
               dp.t_name DEPARTMENT_CODE,
               qb_dwh_utils.ModifyCodeSubject('0000#IBSOXXX#' || pc.t_code ||
                                     case when 0 < (select count(1)
                                                      from Dobjcode_Dbt o
                                                     where o.t_objecttype = 3
                                                           and o.t_codekind in (3,6)
                                                           and o.t_state = 0
                                                           and o.t_objectid = pt.t_partyid
                                                       or 0 < (select count(1)
                                                                 from dpartyown_dbt o
                                                                where o.t_partykind = 2
                                                                      and o.t_partyid = pt.t_partyid)
                                                   ) then '#BANKS'
                                          when pt.t_legalform = 1 then '#CUST_CORP'
                                          when (pt.t_legalform = 2 and prs.t_isemployer=chr(88)) then '#CUST_CORP' -- ИП = юрлицо                                          
                                          when pt.t_legalform = 2 then '#PERSON'
                                      end) SUBJECT_CODE,

              '90' DEALTYPE, -- Форвард
              tic.t_dealcode DOCNUM,
              '0' IS_INTERIOR,
              qb_dwh_utils.DateToChar(tic.t_dealdate) BEGINDATE,
              qb_dwh_utils.DateToChar(nvl(leg0.t_maturity,firstDate)) ENDDATE,
              null NOTE,
              qb_dwh_utils.DateToChar(tic.t_dealdate) DT,
              '0' REC_STATUS,
              to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE

         from ddl_tick_dbt tic
         inner join ddp_dep_dbt dp
            on (tic.t_department = dp.t_code)
         inner join ddl_leg_dbt leg0
            on (tic.t_dealid = leg0.t_dealid and leg0.t_legkind = 0)
          left join dparty_dbt pt
            on (decode(tic.t_partyid, -1, 1, decode(tic.t_partyid,94606, 114511,tic.t_partyid)) = pt.t_partyid)
          left join dpersn_dbt prs --проверим признак "Предприниматель"
            on (prs.t_personid = pt.t_partyid)
          left join dpartcode_dbt pc
            on (pt.t_partyid = pc.t_partyid and pc.t_codekind = 101 and pc.t_state = 0)
          where tic.t_bofficekind = 101 and tic.t_dealtype in (12183, 12193) and tic.t_ispfi = chr(88) and substr(tic.t_dealcode, 1, 2) = 'Д/'
           and  tic.t_dealdate <= in_date
           and BIQ_8474 = 1
           
          ) ;
commit;
      -- Выгрузка в FCT_DEAL_INDICATOR
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка списка значений доп. атрибутов',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.fct_deal_indicator(deal_code, deal_attr_code, currency_curr_code_txt, measurement_unit_code, number_value, date_value, string_value, dt, rec_status, sysmoment, ext_file)
      (select  /*+ parallel(4) enable_parallel_dml */ * from
       (select distinct
               deal_code deal_code,
               code deal_attr_code,
               '-1' currency_curr_code_txt,
               '-1' measurement_unit_code,
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
                 when type in (9, 10) then
                  noteval
                 else
                  null
               end string_value,
               qb_dwh_utils.datetochar(t_date) dt,
               '0' recstatus,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from (select to_char(to_number(nt.t_documentid)) ||
                       decode(nt.t_objecttype, 140, '#DV#', '#DVN#') || case
                         when nt.t_objecttype = 140 then
                          (select decode(fi.t_avoirkind, 2, '95', '94')
                             from dfininstr_dbt fi
                            where fi.t_fiid = dv.t_fiid)
                         else
                          case
                            when dvn.t_dvkind = 2 then
                             '95' -- Опцион
                            when dvn.t_dvkind in (1, 7) then
                             '90' -- Валютный своп
                            when dvn.t_dvkind in (3, 6) then
                             '91' -- Валютный своп
                            when dvn.t_dvkind = 8 then
                             '3' -- Банкнотные сделки
                            when dvn.t_dvkind = 4 then
                                 case when (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0) then
                                        '96'
                                      else
                                        '93'
                                 end
                            else
                             '-1-' || dvn.t_dvkind
                          end
                       end deal_code,
                       to_char(nt.t_objecttype) || 'T' || to_char(nt.t_notekind) code,
                       upper(trim(nk.t_name)) name,
                       nk.t_notetype type,
                       case nk.t_notetype
                         when 0 then
                          qb_dwh_utils.numbertochar(rsb_struct.getint(nt.t_text), 0)
                         when 1 then
                          qb_dwh_utils.numbertochar(rsb_struct.getlong(nt.t_text), 0)
                         when 2 then
                          qb_dwh_utils.numbertochar(rsb_struct.getdouble(nt.t_text))
                         when 3 then
                          qb_dwh_utils.numbertochar(rsb_struct.getdouble(nt.t_text))
                         when 4 then
                          qb_dwh_utils.numbertochar(rsb_struct.getdouble(nt.t_text))
                         when 7 then
                          rsb_struct.getstring(nt.t_text)
                         when 9 then
                          qb_dwh_utils.datetochar(rsb_struct.getdate(nt.t_text))
                         when 10 then
                          qb_dwh_utils.datetimetochar(rsb_struct.gettime(nt.t_text))
                         when 12 then
                          rsb_struct.getchar(nt.t_text)
                         when 25 then
                          qb_dwh_utils.numbertochar(rsb_struct.getmoney(nt.t_text), 2)
                         else
                          null
                       end noteval,
                       nt.t_date t_date,
                       nt.t_documentid
                  from dnotetext_dbt nt
                 inner join dnotekind_dbt nk
                    on (nt.t_objecttype = nk.t_objecttype and
                       nt.t_notekind = nk.t_notekind)
                  left join ddvdeal_dbt dv
                    on (nt.t_objecttype = 140 and to_number(nt.t_documentid) = dv.t_id)
                  left join ddvndeal_dbt dvn
                    on (nt.t_objecttype in (145, 148) and
                       to_number(nt.t_documentid) = dvn.t_id)
                  left join ddvnfi_dbt nfi0
                     on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                  left join ddvnfi_dbt nfi2
                     on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                 where nt.t_objecttype in (140, 145, 148)
                   and nt.t_date <= in_date)
        union all
        select distinct to_char(dv.t_id) || '#DV#' || case
                          when fi.t_avoirkind = 2 then
                           '95'
                          else
                           '94'
                        end deal_code,
                        'FIXING_DATE_RATE' deal_attr_code,
                        '-1' currency_curr_code_txt,
                        '-1' measurement_unit_code,
                        null number_value,
                        qb_dwh_utils.datetochar(mg.t_fixdate) date_value,
                        null string_value,
                        qb_dwh_utils.datetochar(mg.t_fixdate) dt,
                        '0' recstatus,
                        to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvdeal_dbt dv
         inner join ddvnpmgr_dbt mg
            on (dv.t_id = mg.t_dealid)
         inner join dfininstr_dbt fi
            on (dv.t_fiid = fi.t_fiid)
         where dv.t_date <= in_date
           and dv.t_client = -1
        union all
        select distinct to_char(dvn.t_id) || '#DVN#' || case
                          when dvn.t_dvkind = 2 then
                           '95' -- Опцион
                          when dvn.t_dvkind in (1, 7) then
                           '90' -- Валютный своп
                          when dvn.t_dvkind in (3, 6) then
                           '91' -- Валютный своп
                          when dvn.t_dvkind = 8 then
                           '3' -- Банкнотные сделки
                          when dvn.t_dvkind = 4 then
                               case when (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0) then
                                      '96'
                                    else
                                      '93'
                               end
                          else
                           '-1-' || dvn.t_dvkind
                        end deal_code,
                        'FIXING_DATE_RATE' deal_attr_code,
                        '-1' currency_curr_code_txt,
                        '-1' measurement_unit_code,
                        null number_value,
                        qb_dwh_utils.datetochar(mg.t_fixdate) date_value,
                        null string_value,
                        qb_dwh_utils.datetochar(mg.t_fixdate) dt,
                        '0' recstatus,
                        to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvndeal_dbt dvn
         inner join ddvnpmgr_dbt mg
            on (dvn.t_id = mg.t_dealid)
         left join ddvnfi_dbt nfi0
            on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
         left join ddvnfi_dbt nfi2
            on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
         where dvn.t_date <= in_date
           and dvn.t_dvkind in (1, 2, 3, 4, 6, 7, 8)
        union all --BIQ-8474 п.3.2 пп.1
        select to_char(dvn.t_id) || '#DVN#' || case
                          when dvn.t_dvkind in (3, 6) then
                           '91' -- Валютный своп
                          when dvn.t_dvkind = 4 then
                               case when (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0) then
                                      '96'
                                    else
                                      '93'
                               end
                          end deal_code,
                          decode (nfi.t_type, 0, 'EXTERNAL_ID#SWP0', 2, 'EXTERNAL_ID#SWP2') deal_attr_code,
                          '-1' currency_curr_code_txt,
                          '-1' measurement_unit_code,
                          null number_value,
                          null date_value,
                          to_char(nfi.t_id) string_value,
                          qb_dwh_utils.datetochar(dvn.t_date) dt,
                          '0' recstatus,
                          to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
         from ddvndeal_dbt dvn
         left join ddvnfi_dbt nfi0
            on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
         left join ddvnfi_dbt nfi2
            on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
         left join ddvnfi_dbt nfi
            on (dvn.t_id = nfi.t_dealid)   
         where dvn.t_date <= in_date
           and dvn.t_dvkind in (3, 4, 6)
           and BIQ_8474 = 1
         ) t
       where exists (select 1 from ldr_infa_pfi.fct_deal fd where fd.code = t.deal_code)
       );
commit;
      -- Выгрузка в ASS_CONTRACT_DEAL
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка связей контракта со сделкой',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.ass_contract_deal(deal_code, contract_code, dt, rec_status, sysmoment, ext_file)
       (select to_char(dv.t_id) || '#DV#' || decode(fi.t_avoirkind, 2, '95', '94') DEAL_CODE,
               to_char(dv.t_fiid) CONTRACT_CODE,
               qb_dwh_utils.DateToChar(dv.t_date) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvdeal_dbt dv
         inner join dfininstr_dbt fi
            on (dv.t_fiid = fi.t_fiid)
         where dv.t_IsTrust = chr(0)
           and fi.t_avoirkind in (1,2)
           and dv.t_date <= in_date
           and dv.t_client = -1
        union all
        select to_char(dvn.t_id) || '#DVN#95' DEAL_CODE,
               dvn.t_id||'#DVN' CONTRACT_CODE,
               qb_dwh_utils.DateToChar(dvn.t_date) DT,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvndeal_dbt dvn
         where dvn.t_dvkind = 2
           and dvn.t_date <= in_date);
commit;
      -- Выгрузка в FCT_OPTION
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с  опционами',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.fct_option(exec_form, refer_exchange, settlement_date, price, basis_type, agreement_type, direction, margin_threshold, is_derivative, dt_begin_asset, dt_end_asset, dt_fixing, prize_date, exec_date, is_executed, asset_price, is_registered_repos, is_margin, margin_freq, min_payment_sum, days_margin_recalc, term_of_pledge, prize_amount, quantity, exchange_code, deal_code, finstr_code, broker_code_subject, prize_curr_finstr_code, settl_curfinstr_finstr_code, price_curfinstr_finstr_code, rec_status, sysmoment, ext_file)
         (-- Биржевые опционы
        select case
                 when fi.t_settlement_code = 0 then
                   '1'
                 when fi.t_settlement_code = 1 then
                   '2'
                 else
                   '0'
               end  EXEC_FORM,
               '1' REFER_EXCHANGE,
               qb_dwh_utils.DateToChar(dv.t_date) SETTLEMENT_DATE,
               to_char(dv.t_price, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') PRICE,
               'N' BASIS_TYPE,
               null AGREEMENT_TYPE,
               case
                 when instr(ko.t_systypes, chr(66)) > 0 then -- покупка
                   '1'
                 when instr(ko.t_systypes, chr(83)) > 0 then -- продажа
                   '2'
                 else
                   '0'
               end DIRECTION,
               null MARGIN_THRESHOLD,
               '1' IS_DERIVATIVE,
               qb_dwh_utils.DateToChar(dv.t_date) DT_BEGIN_ASSET,
               qb_dwh_utils.DateToChar(fi.t_drawingdate) DT_END_ASSET,
               null DT_FIXING,
               null PRIZE_DATE,
               qb_dwh_utils.DateToChar(fi.t_drawingdate) EXEC_DATE,
               case when (select count(1)
                            from dspground_dbt ground, dspgrdoc_dbt grdoc
                           where GROUND.T_SPGROUNDID = grdoc.T_SPGROUNDID
                            and GROUND.T_KIND = 324
                            and grdoc.T_SOURCEDOCID = dv.t_id
                            and grdoc.T_SOURCEDOCKIND = 192
                          ) > 0  then
                      '0'
                    when dv.t_state = 2 then
                      '1'
                    else
                      '0'
               end IS_EXECUTED,
               to_char(dv.t_cost, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') ASSET_PRICE,
               '1' IS_REGISTERED_REPOS,
               '0' IS_MARGIN,
               null MARGIN_FREQ,
               null MIN_PAYMENT_SUM,
               null DAYS_MARGIN_RECALC,
               null TERM_OF_PLEDGE,
               to_char(round(dv.t_positionbonus, 2), '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') PRIZE_AMOUNT,
               to_char(dv.t_amount, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') QUANTITY,
               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                              1,
                                              fi.t_issuer) EXCHANGE_CODE,
               to_char(dv.t_id) || '#DV#95' DEAL_CODE,
               case
                 when fi2.t_fi_kind = 1 then
                   fi2.t_iso_number
                 when fi2.t_fi_kind = 6 then
                   fi2.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi2.t_fiid) || '#FIN'
               end /*|| '#FIN'*/ FINSTR_CODE,
               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             dv.t_broker) BROKER_CODE_SUBJECT,
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN'
               end PRIZE_CURR_FINSTR_CODE,
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN'
               end SETTL_CURFINSTR_FINSTR_CODE,
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN'
               end PRICE_CURFINSTR_FINSTR_CODE,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvdeal_dbt dv
         inner join dfininstr_dbt fi
            on (dv.t_fiid = fi.t_fiid)
         inner join doprkoper_dbt ko
            on (dv.t_kind = ko.t_kind_operation)
         inner join dfideriv_dbt dfi
            on (dv.t_fiid = dfi.t_fiid)
         inner join dfininstr_dbt fi1
            on (dfi.t_strikefiid = fi1.t_fiid)
         inner join dfininstr_dbt fi2
            on (fi.t_facevaluefi = fi2.t_fiid)
         where fi.t_Avoirkind = 2
           and dv.t_date <= in_date
           and dv.t_client = -1
        union all
        -- Внебиржевые опционы
        select case
                 when nfi0.t_exectype = 0 then
                   '1'
                 when nfi0.t_exectype = 1 then
                   '2'
                 else
                   '0'
               end  EXEC_FORM,

               '0' REFER_EXCHANGE,
               qb_dwh_utils.DateToChar(decode(nfi0.t_supldate, emptDate, dvn.t_date/*to_date('01013001','ddmmyyyy')*/, nfi0.t_supldate)) SETTLEMENT_DATE,
               to_char(nfi0.t_price, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') PRICE,
               'N' BASIS_TYPE,
               null AGREEMENT_TYPE,
               to_char(dvn.t_type) DIRECTION,
               null MARGIN_THRESHOLD,
               case
                 when dvn.t_ispfi = chr(0) then
                   '0'
                 else
                   '1'
               end IS_DERIVATIVE,
               qb_dwh_utils.DateToChar(dvn.t_date) DT_BEGIN_ASSET,
               qb_dwh_utils.DateToChar(greatest(nfi0.t_execdate, nfi0.t_supldate)) DT_END_ASSET,
               qb_dwh_utils.DateToChar(dvn.t_date - nfi0.t_fixdays) DT_FIXING,
               qb_dwh_utils.DateToChar(dvn.t_bonusdate) PRIZE_DATE,
               qb_dwh_utils.DateToChar(nfi0.t_execdate) EXEC_DATE,
               case when (select count(1)
                            from dspground_dbt ground, dspgrdoc_dbt grdoc
                           where GROUND.T_SPGROUNDID = grdoc.T_SPGROUNDID
                             and GROUND.T_KIND = 324
                             and grdoc.T_SOURCEDOCID = dvn.t_id
                             and grdoc.T_SOURCEDOCKIND = dvn.t_dockind

                          ) > 0  then
                      '0'
                    when dvn.t_state = 2 then
                      '1'
                    else
                      '0'
               end IS_EXECUTED,
               to_char(nfi0.t_cost, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') ASSET_PRICE,
               '1' IS_REGISTERED_REPOS,
               '0' IS_MARGIN,
               null MARGIN_FREQ,
               null MIN_PAYMENT_SUM,
               null DAYS_MARGIN_RECALC,
               null TERM_OF_PLEDGE,
               to_char(round(dvn.t_bonus, 2), '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') PRIZE_AMOUNT,
               to_char(nfi0.t_amount, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') QUANTITY,
               '-1' EXCHANGE_CODE,
               to_char(dvn.t_id) || '#DVN#95' DEAL_CODE,
               case
                 when nfi0.t_type=1 And nfi0.t_stdfiid = -1 then dvn.T_ID||'#DVN'
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN'
               end FINSTR_CODE,
               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             dvn.t_agent) BROKER_CODE_SUBJECT,
               case
                 when fi2.t_fi_kind = 1 then
                   fi2.t_iso_number
                 when fi2.t_fi_kind = 6 then
                   fi2.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi2.t_fiid) || '#FIN'
               end PRIZE_CURR_FINSTR_CODE,
               case
                 when nfi0.t_type=1 And nfi0.t_stdfiid = -1 then fi4.t_iso_number
                 when fi3.t_fi_kind = 1 then
                   fi3.t_iso_number
                 when fi3.t_fi_kind = 6 then
                   fi3.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi3.t_fiid) || '#FIN'
               end SETTL_CURFINSTR_FINSTR_CODE,
               case
                 when fi4.t_fi_kind = 1 then
                   fi4.t_iso_number
                 when fi4.t_fi_kind = 6 then
                   fi4.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi4.t_fiid) || '#FIN'
               end PRICE_CURFINSTR_FINSTR_CODE,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvndeal_dbt dvn
         inner join ddvnfi_dbt nfi0
            on (dvn.t_id = nfi0.t_dealid and ( nfi0.t_type = 0 or (nfi0.t_type=1 And nfi0.t_stdfiid = -1))) -- DEF-47223 внебиржевые опционы на форвард
         inner join dfininstr_dbt fi1
            on (nfi0.t_fiid = fi1.t_fiid)
         inner join dfininstr_dbt fi2
            on (dvn.t_bonusfiid = fi2.t_fiid)
         inner join dfininstr_dbt fi3
            on (nfi0.t_fiid = fi3.t_fiid)
         inner join dfininstr_dbt fi4
            on (nfi0.t_pricefiid = fi4.t_fiid)
         where dvn.t_dvkind = 2
           and dvn.t_date <= in_date);
commit;

      -- Выгрузка в FCT_SPOTFORWARD
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок SPOT и FORWART',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.fct_spotforward(exec_form, refer_exchange,delivery_date,settlement_date,type,price,volume,is_liq_netting,is_settl_netting,basis_type,agreement_type,margin_threshold,is_derivative,dt_begin_asset,dt_end_asset,is_sale,is_registered_repos,dt_fixing,is_margin,margin_freq,min_payment_sum,days_margin_recalc,term_of_pledge,exchange_code,deal_code,finstr_code,broker_code_subject,settl_curfinstr_finstr_code,price_curfinstr_finstr_code,sysmoment,ext_file,rec_status)
       (select case
                 when nfi.t_exectype = 0 then
                   '1'
                 when nfi.t_exectype = 1 then
                   '2'
               end EXEC_FORM,
               case
                 when dvn.t_sector = chr(0) then
                   '0'
                 when dvn.t_sector = chr(88) then
                   '1'
               end REFER_EXCHANGE,
               qb_dwh_utils.DateToChar(decode(nfi.t_supldate, emptdate, perpDWHDate, nfi.t_supldate)) DELIVERY_DATE,
               qb_dwh_utils.DateToChar(decode(nfi.t_paydate, emptdate, perpDWHDate, nfi.t_paydate)) SETTLEMENT_DATE,
               case
                 when dvn.t_dvkind = 1 and fi1.t_fi_kind = 2 and BIQ_8474 = 1 then --BIQ-8474 п.3.6.5 пп.5
                   '5'
                 when dvn.t_dvkind = 1 and fi1.t_fi_kind = 6 and BIQ_8474 = 1 then --BIQ-8474 п.3.6.5 
                   '6'
                 when dvn.t_dvkind = 1 then
                   '4'
                 when dvn.t_date = nfi.t_execdate then
                   '1'
                 when dvn.t_date + 1 = nfi.t_execdate  then
                   '2'
                 else
                   '3'
               end TYPE,
               to_char(nfi.t_price, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') PRICE,
               to_char(nfi.t_amount, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') VOLUME,
               --'0' IS_LIQ_NETTING,
               case 
                  when BIQ_8474 = 1 then
                    decode (agr.t_can_liquidnetting, chr(88), 1, 0) --BIQ-8474 п.3.6.5 
                  else
                    0
               end IS_LIQ_NETTING,
               case
                 when dvn.t_netting = chr(88) then
                   '1'
                 else
                   '0'
               end IS_SETTL_NETTING,
               'N' BASIS_TYPE,
               --null AGREEMENT_TYPE,
               case 
                  when BIQ_8474 = 1 then
                    to_char(typagr.t_code) 
                  else
                    null
               end AGREEMENT_TYPE, --BIQ-8474 п.3.6.5 
               null MARGIN_THRESHOLD,
               case
                 when dvn.t_ispfi = chr(0) then
                   '0'
                 else
                   '1'
               end IS_DERIVATIVE,
               qb_dwh_utils.DateToChar(dvn.t_date) DT_BEGIN_ASSET,
               qb_dwh_utils.DateToChar(nfi.t_execdate) DT_END_ASSET,
               case
                 when dvn.t_type = 1 then
                   '2'
                 when dvn.t_type = 2 then
                   '1'
               end IS_SALE,
               '1' IS_REGISTERED_REPOS,
               qb_dwh_utils.DateToChar(dvn.t_date - nfi.t_fixdays) DT_FIXING,
               '0' IS_MARGIN,
               null MARGIN_FREQ,
               null MIN_PAYMENT_SUM,
               null DAYS_MARGIN_RECALC,
               null TERM_OF_PLEDGE,
               '-1' EXCHANGE_CODE,
               to_char(dvn.t_id) || '#DVN#90' DEAL_CODE,
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN' --BIQ-8474, приводим к единообразию на стороне СОФР
               end FINSTR_CODE,
               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             dvn.t_agent) BROKER_CODE_SUBJECT,
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                  '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN' --BIQ-8474, приводим к единообразию на стороне СОФР
               end SETTL_CURFINSTR_FINSTR_CODE,
               case
                 when fi2.t_fi_kind = 1 then
                   fi2.t_iso_number
                 when fi2.t_fi_kind = 6 then
                   fi2.t_codeinaccount
                 else
                    to_char(fi2.t_fiid) || '#FIN'
               end PRICE_CURFINSTR_FINSTR_CODE,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE,
               '0' REC_STATUS
          from ddvndeal_dbt dvn
         inner join ddvnfi_dbt nfi
            on (dvn.t_id = nfi.t_dealid and nfi.t_type = 0)
         inner join dfininstr_dbt fi1
            on (nfi.t_fiid = fi1.t_fiid)
         inner join dfininstr_dbt fi2
            on (nfi.t_pricefiid = fi2.t_fiid)
         left join ddl_genagr_dbt agr
            on agr.t_genagrid = dvn.t_genagrid
         left join dir_typeagreement_dbt typagr
            on typagr.t_id = agr.t_type_gs
         where dvn.t_dvkind in (1, 7)
           and dvn.t_date <= in_date

         union all
         
       --Форварды на ценную бумагу из БОЦБ с признаком ПФИ, BIQ-8474
       select  '2' EXEC_FORM,   --сделки поставочные
               '0' REFER_EXCHANGE, --это внебиржевые сделки
               qb_dwh_utils.DateToChar(leg0.t_expiry) DELIVERY_DATE,
               qb_dwh_utils.DateToChar(leg0.t_maturity) SETTLEMENT_DATE,
               '5' TYPE, --форвард на ценную бумагу
               --для сделок с ценой в %, возьмём цену как результат деления стоимости на количество
               to_char(leg0.t_price, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') PRICE,
               to_char(leg0.t_principal, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') VOLUME,
               --'0' IS_LIQ_NETTING,
               decode (agr.t_can_liquidnetting, chr(88), 1, 0) IS_LIQ_NETTING, --BIQ-8474 п.3.6.5
               case
                 when tic.t_netting = 3 then
                   '1'
                 when tic.t_netting = 1 then
                   '0'
               end IS_SETTL_NETTING,
               'N' BASIS_TYPE,
               --null AGREEMENT_TYPE,
               to_char(nvl(typagr.t_code, chr(0))) AGREEMENT_TYPE, --BIQ-8474 п.3.6.5 
               null MARGIN_THRESHOLD,
               '1' IS_DERIVATIVE,  
               qb_dwh_utils.DateToChar(tic.t_dealdate) DT_BEGIN_ASSET,
               qb_dwh_utils.DateToChar(leg0.t_maturity) DT_END_ASSET,
               case
                 when tic.t_dealtype = 12183 then --покупка
                   '2'
                 when tic.t_dealtype = 12193 then --продажа
                   '1'
               end IS_SALE,
               '1' IS_REGISTERED_REPOS,
               qb_dwh_utils.DateToChar(tic.t_dealdate - 0) DT_FIXING, --nfi.t_fixdays у всех примеров = 0, либо найти аналог в leg0
               '0' IS_MARGIN,
               null MARGIN_FREQ,
               null MIN_PAYMENT_SUM,
               null DAYS_MARGIN_RECALC,
               null TERM_OF_PLEDGE,
               '-1' EXCHANGE_CODE,
               to_char(tic.t_dealid) || '#CB#DVN#90' DEAL_CODE,
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                   '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN' --BIQ-8474, приводим к единообразию на стороне СОФР
               end FINSTR_CODE,
               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             -1) BROKER_CODE_SUBJECT, --dvn.t_agent
                                             
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                  '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN' --BIQ-8474, приводим к единообразию на стороне СОФР
               end SETTL_CURFINSTR_FINSTR_CODE,
               case
                 when fi2.t_fi_kind = 1 then
                   fi2.t_iso_number
                 when fi2.t_fi_kind = 6 then
                   fi2.t_codeinaccount
                 else
                    to_char(fi2.t_fiid) || '#FIN'
               end PRICE_CURFINSTR_FINSTR_CODE,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE,
               '0' REC_STATUS

         from ddl_tick_dbt tic
         inner join ddp_dep_dbt dp
            on (tic.t_department = dp.t_code)
         inner join ddl_leg_dbt leg0
            on (tic.t_dealid = leg0.t_dealid and leg0.t_legkind = 0)
           inner join dfininstr_dbt fi1
            on (leg0.t_pfi = fi1.t_fiid)
         inner join dfininstr_dbt fi2
            on (leg0.t_cfi = fi2.t_fiid)
         left join ddl_genagr_dbt agr
            on agr.t_genagrid = tic.t_genagrid
         left join dir_typeagreement_dbt typagr
            on typagr.t_id = agr.t_type_gs
         where tic.t_bofficekind = 101 and tic.t_dealtype in (12183, 12193) and tic.t_ispfi = chr(88) and substr(tic.t_dealcode, 1, 2) = 'Д/'   
         and tic.t_dealdate <= in_date
         and BIQ_8474 = 1
        );
commit;
      -- Выгрузка в FCT_FUTURES
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с фьючерсами',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.fct_futures(exec_form,settlement_date,price,basis_type,direction,margin_threshold,is_derivative,dt_begin_asset,dt_end_asset,dt_fixing,is_margin,margin_freq,min_payment_sum,days_margin_recalc,term_of_pledge,quantity,exchange_code,deal_code,finstr_code,broker_code_subject,settl_curfinstr_finstr_code,price_curfinstr_finstr_code,rec_status,sysmoment, ext_file)
      (select case
                 when fi.t_settlement_code = 0 then
                   '1'
                 when fi.t_settlement_code = 1 then
                   '2'
               end EXEC_FORM,
               qb_dwh_utils.DateToChar(dv.t_date) SETTLEMENT_DATE,
               to_char(dv.t_price, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') PRICE,
               'N' BASIS_TYPE,
               case
                 when instr(ko.t_systypes, chr(66)) > 0 then -- покупка
                   '1'
                 when instr(ko.t_systypes, chr(83)) > 0 then -- продажа
                   '2'
                 else
                   '0'
               end DIRECTION,
               null MARGIN_THRESHOLD,
               '1' IS_DERIVATIVE,
               qb_dwh_utils.DateToChar(dv.t_date) DT_BEGIN_ASSET,
               qb_dwh_utils.DateToChar(fi.t_drawingdate) DT_END_ASSET,
               null DT_FIXING,
               '0'IS_MARGIN,
               null MARGIN_FREQ,
               null MIN_PAYMENT_SUM,
               null DAYS_MARGIN_RECALC,
               null TERM_OF_PLEDGE,
               to_char(dv.t_amount, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') QUANTITY,
               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             fi.t_issuer) EXCHANGE_CODE,
               to_char(dv.t_id) || '#DV#94' DEAL_CODE,
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                    '0000#SOFRXXX#' || to_char(fi1.t_fiid) || '#FIN'
               end FINSTR_CODE,
               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             dv.t_broker) BROKER_CODE_SUBJECT,
               case
                 when fi2.t_fi_kind = 1 then
                   fi2.t_iso_number
                 when fi2.t_fi_kind = 6 then
                   fi2.t_codeinaccount
                 else
                    to_char(fi2.t_fiid) || '#FIN'
               end SETTL_CURFINSTR_FINSTR_CODE,
               case
                 when fi2.t_fi_kind = 1 then
                   fi2.t_iso_number
                 when fi2.t_fi_kind = 6 then
                   fi2.t_codeinaccount
                 else
                    to_char(fi2.t_fiid) || '#FIN'
               end PRICE_CURFINSTR_FINSTR_CODE,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvdeal_dbt dv
         inner join dfininstr_dbt fi
            on (dv.t_fiid = fi.t_fiid)
         inner join doprkoper_dbt ko
            on (dv.t_kind = ko.t_kind_operation)
         inner join dfideriv_dbt dfi
            on (dv.t_fiid = dfi.t_fiid)
         inner join dfininstr_dbt fi1
            on (fi.t_facevaluefi = fi1.t_fiid)
         inner join dfininstr_dbt fi2
            on (dfi.t_strikefiid = fi2.t_fiid)
         where dv.t_IsTrust = chr(0)
           and fi.t_Avoirkind = 1
           and dv.t_date <= in_date
           and dv.t_client = -1
        );
commit;
      -- ФИ по сделкам с фьючерсами, которые не добавлены раннее
      Insert into ldr_infa_pfi.det_finstr(finstr_code, finstr_name, finstr_name_s, typefinstr, dt, rec_status, sysmoment, ext_file)
        (select case
                 when fi.t_fi_kind = 1 then
                   fi.t_iso_number
                 when fi.t_fi_kind = 6 then
                   fi.t_codeinaccount
                 else
                    '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN'
                end
              FINSTR_CODE,
              fi.t_name FINSTR_NAME,
              substr(fi.t_definition, 1, 50) FINSTR_NAME_S,
              case fi.t_fi_kind
                when 1 then
                  '1'
                when 6 then
                  '1'
                when 2 then
                  '2'
                when 4 then
                  '2'
                when 3 then
                  '5'
                when 7 then
                  '4'
                when 8 then
                  '9'
              end TYPEFINSTR,
              qb_dwh_utils.DateToChar(decode(fi.t_issued, to_date('01010001','ddmmyyyy'), to_date('01011980','ddmmyyyy'), fi.t_issued)) DT,
              '0' REC_STATUS,
              to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
         from dfininstr_dbt fi
        where  (case
                 when fi.t_fi_kind = 1 then
                   fi.t_iso_number
                 when fi.t_fi_kind = 6 then
                   fi.t_codeinaccount
                 else
                    '0000#SOFRXXX#' || to_char(fi.t_fiid) || '#FIN'
                end) in (select t.finstr_code
                             -- distinct to_number(replace(replace(t.finstr_code, '#FIN'), '0000#SOFRXXX#')) 
                             from ldr_infa_pfi.fct_futures t
                            where t.finstr_code not in
                                  (select finstr_code
                                     from ldr_infa_pfi.det_finstr)));
                                     commit;
      -- Удаление сделок с направлением отличным от покупка/продажа
      delete from ldr_infa_pfi.fct_option where direction = '0'; commit;
      delete from ldr_infa_pfi.fct_futures where direction = '0'; commit;

      -- Выгрузка в FCT_FXSWAP
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с валютными свопами',
                     2,
                     null,
                     null);
          if (BIQ_8474 = 1) then
              insert into ldr_infa_pfi.fct_fxswap(exec_form,refer_exchange,spot_delivery1_date,spot_delivery2_date,forw_delivery1_date,forw_delivery2_date,settlement_date,spot_rate,forward_rate,volume,direction,is_liq_netting,is_settl_netting,basis_type,agreement_type,margin_threshold,is_derivative,dt_begin_asset,dt_end_asset,is_registered_repos,is_margin,margin_freq,min_payment_sum,days_margin_recalc,term_of_pledge,exchange_code,deal_code,finstr_code,broker_code_subject,settl_curfinstr_finstr_code,exch_count,rec_status,sysmoment, ext_file, exec_date, exec2_date)
               (select case
                         when nfi0.t_exectype = 0 then
                           '1'
                         when nfi0.t_exectype = 1 then
                           '2'
                       end EXEC_FORM,
                       case
                         when dvn.t_sector = chr(0) then
                           '0'
                         when dvn.t_sector = chr(88) then
                           '1'
                       end REFER_EXCHANGE,
                       qb_dwh_utils.DateToChar(nfi0.t_supldate) SPOT_DELIVERY1_DATE,
                       qb_dwh_utils.DateToChar(nfi2.t_paydate) SPOT_DELIVERY2_DATE,
                       qb_dwh_utils.DateToChar(nfi0.t_supldate) FORW_DELIVERY1_DATE,
                       qb_dwh_utils.DateToChar(nfi2.t_paydate) FORW_DELIVERY2_DATE,
                       qb_dwh_utils.DateToChar(nfi0.t_supldate) SETTLEMENT_DATE,
                       to_char(nfi0.t_cost, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') SPOT_RATE,
                       to_char(nfi0.t_price, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') FORWARD_RATE,
                       to_char(nfi0.t_amount, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') VOLUME,
                       case
                         when dvn.t_type = 5 then
                           '1'
                         when dvn.t_type = 6 then
                           '2'
                         when dvn.t_type = 7 then
                           '2'
                         when dvn.t_type = 8 then
                           '1'
                         when dvn.t_type = 9 then
                           '2'
                         when dvn.t_type = 10 then
                           '1'
                       end DIRECTION ,
                       '0' IS_LIQ_NETTING,
                       case
                         when dvn.t_netting = chr(88) then
                           '1'
                         else
                           '0'
                       end IS_SETTL_NETTING,
                       'N' BASIS_TYPE,
                       null AGREEMENT_TYPE,
                       null MARGIN_THRESHOLD,
                       case
                         when dvn.t_ispfi = chr(0) then
                           '0'
                         else
                           '1'
                       end IS_DERIVATIVE,
                       qb_dwh_utils.DateToChar(dvn.t_date) DT_BEGIN_ASSET,
                       qb_dwh_utils.DateToChar(nfi0.t_execdate) DT_END_ASSET,
                       '1' IS_REGISTERED_REPOS,
                       '0' IS_MARGIN,
                       null MARGIN_FREQ,
                       null MIN_PAYMENT_SUM,
                       null DAYS_MARGIN_RECALC,
                       null TERM_OF_PLEDGE,
                       '-1' EXCHANGE_CODE,
                       to_char(dvn.t_id) || '#DVN#' || decode(dvn.t_dvkind, 4, '96', '91') DEAL_CODE,
                       '0000#SOFRXXX#' || case
                         when fi1.t_fi_kind = 1 then
                           fi1.t_iso_number
                         when fi1.t_fi_kind = 6 then
                           fi1.t_codeinaccount
                         else
                           to_char(fi1.t_fiid) || '#FIN'
                       end || '#' ||
                       case 
                         when dvn.t_dvkind in (3, 6) then
                           case
                             when fi2.t_fi_kind = 1 then
                               fi2.t_iso_number
                             when fi2.t_fi_kind = 6 then
                               fi2.t_codeinaccount
                             else
                                to_char(fi2.t_fiid) || '#FIN'
                           end
                         else
                           case
                             when fi3.t_fi_kind = 1 then
                               fi3.t_iso_number
                             when fi3.t_fi_kind = 6 then
                               fi3.t_codeinaccount
                             else
                               to_char(fi3.t_fiid) || '#FIN'
                           end
                       end 
                       FINSTR_CODE,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     dvn.t_agent) BROKER_CODE_SUBJECT,
                       case
                         when fi1.t_fi_kind = 1 then
                           fi1.t_iso_number
                         when fi1.t_fi_kind = 6 then
                           fi1.t_codeinaccount
                         else
                            to_char(fi1.t_fiid) || '#FIN'
                       end SETTL_CURFINSTR_FINSTR_CODE,
                       null EXCH_COUNT,
                       '0' REC_STATUS,
                       to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE,
                       qb_dwh_utils.DateToChar(nfi0.t_execdate) EXEC_DATE, --BIQ-8474 п.3.2 пп.7 
                       qb_dwh_utils.DateToChar(nfi2.t_execdate) EXEC2_DATE
                 from ddvndeal_dbt dvn
                 inner join ddvnfi_dbt nfi0
                    on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                 inner join ddvnfi_dbt nfi2
                    on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                 inner join dfininstr_dbt fi1
                    on (nfi0.t_fiid = fi1.t_fiid)
                 inner join dfininstr_dbt fi2
                    on (nfi0.t_pricefiid = fi2.t_fiid)
                 inner join dfininstr_dbt fi3
                    on (nfi2.t_fiid = fi3.t_fiid)
                 where (((dvn.t_dvkind = 4)
                   and (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0))
                    or (dvn.t_dvkind in (3, 6)))
                   and dvn.t_date <= in_date
                   );commit;
          else

                insert into ldr_infa_pfi.fct_fxswap(exec_form,refer_exchange,spot_delivery1_date,spot_delivery2_date,forw_delivery1_date,forw_delivery2_date,settlement_date,spot_rate,forward_rate,volume,direction,is_liq_netting,is_settl_netting,basis_type,agreement_type,margin_threshold,is_derivative,dt_begin_asset,dt_end_asset,is_registered_repos,is_margin,margin_freq,min_payment_sum,days_margin_recalc,term_of_pledge,exchange_code,deal_code,finstr_code,broker_code_subject,settl_curfinstr_finstr_code,exch_count,rec_status,sysmoment, ext_file)
               (select case
                         when nfi0.t_exectype = 0 then
                           '1'
                         when nfi0.t_exectype = 1 then
                           '2'
                       end EXEC_FORM,
                       case
                         when dvn.t_sector = chr(0) then
                           '0'
                         when dvn.t_sector = chr(88) then
                           '1'
                       end REFER_EXCHANGE,
                       qb_dwh_utils.DateToChar(nfi0.t_supldate) SPOT_DELIVERY1_DATE,
                       qb_dwh_utils.DateToChar(nfi2.t_paydate) SPOT_DELIVERY2_DATE,
                       qb_dwh_utils.DateToChar(nfi0.t_supldate) FORW_DELIVERY1_DATE,
                       qb_dwh_utils.DateToChar(nfi2.t_paydate) FORW_DELIVERY2_DATE,
                       qb_dwh_utils.DateToChar(nfi0.t_supldate) SETTLEMENT_DATE,
                       to_char(nfi0.t_cost, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') SPOT_RATE,
                       to_char(nfi0.t_price, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') FORWARD_RATE,
                       to_char(nfi0.t_amount, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') VOLUME,
                       case
                         when dvn.t_type = 5 then
                           '1'
                         when dvn.t_type = 6 then
                           '2'
                         when dvn.t_type = 7 then
                           '2'
                         when dvn.t_type = 9 then
                           '2'
                       end DIRECTION ,
                       '0' IS_LIQ_NETTING,
                       case
                         when dvn.t_netting = chr(88) then
                           '1'
                         else
                           '0'
                       end IS_SETTL_NETTING,
                       'N' BASIS_TYPE,
                       null AGREEMENT_TYPE,
                       null MARGIN_THRESHOLD,
                       case
                         when dvn.t_ispfi = chr(0) then
                           '0'
                         else
                           '1'
                       end IS_DERIVATIVE,
                       qb_dwh_utils.DateToChar(dvn.t_date) DT_BEGIN_ASSET,
                       qb_dwh_utils.DateToChar(nfi0.t_execdate) DT_END_ASSET,
                       '1' IS_REGISTERED_REPOS,
                       '0' IS_MARGIN,
                       null MARGIN_FREQ,
                       null MIN_PAYMENT_SUM,
                       null DAYS_MARGIN_RECALC,
                       null TERM_OF_PLEDGE,
                       '-1' EXCHANGE_CODE,
                       to_char(dvn.t_id) || '#DVN#' || decode(dvn.t_dvkind, 4, '96', '91') DEAL_CODE,
                       '0000#SOFRXXX#' || case
                         when fi1.t_fi_kind = 1 then
                           fi1.t_iso_number
                         when fi1.t_fi_kind = 6 then
                           fi1.t_codeinaccount
                         else
                           to_char(fi1.t_fiid) || '#FIN'
                       end || '#' ||
                       case 
                         when dvn.t_dvkind in (3, 6) then
                           case
                             when fi2.t_fi_kind = 1 then
                               fi2.t_iso_number
                             when fi2.t_fi_kind = 6 then
                               fi2.t_codeinaccount
                             else
                                to_char(fi2.t_fiid) || '#FIN'
                           end
                         else
                           case
                             when fi3.t_fi_kind = 1 then
                               fi3.t_iso_number
                             when fi3.t_fi_kind = 6 then
                               fi3.t_codeinaccount
                             else
                               to_char(fi3.t_fiid) || '#FIN'
                           end
                       end 
                       FINSTR_CODE,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     dvn.t_agent) BROKER_CODE_SUBJECT,
                       case
                         when fi1.t_fi_kind = 1 then
                           fi1.t_iso_number
                         when fi1.t_fi_kind = 6 then
                           fi1.t_codeinaccount
                         else
                            to_char(fi1.t_fiid) || '#FIN'
                       end SETTL_CURFINSTR_FINSTR_CODE,
                       null EXCH_COUNT,
                       '0' REC_STATUS,
                       to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
                 from ddvndeal_dbt dvn
                 inner join ddvnfi_dbt nfi0
                    on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                 inner join ddvnfi_dbt nfi2
                    on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                 inner join dfininstr_dbt fi1
                    on (nfi0.t_fiid = fi1.t_fiid)
                 inner join dfininstr_dbt fi2
                    on (nfi0.t_pricefiid = fi2.t_fiid)
                 inner join dfininstr_dbt fi3
                    on (nfi2.t_fiid = fi3.t_fiid)
                 where (((dvn.t_dvkind = 4)
                   and (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0))
                    or (dvn.t_dvkind in (3, 6)))
                   and dvn.t_date <= in_date
                   );commit;
          end if;

      -- Выгрузка в FCT_IRS
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка сделок с процентными свопами',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.fct_irs(exec_form,
                                       refer_exchange,
                                       settlement_date,
                                       type,
                                       price,
                                       is_liq_netting,
                                       is_settl_netting,
                                       basis_type,
                                       type_rate,
                                       agreement_type,
                                       direction,
                                       margin_threshold,
                                       is_derivative,
                                       dt_begin_asset,
                                       dt_end_asset,
                                       is_registered_repos,
                                       is_margin,
                                       margin_freq,
                                       min_payment_sum,
                                       days_margin_recalc,
                                       term_of_pledge,
                                       amount,
                                       exch_count,
                                       exchange_code,
                                       deal_code,
                                       finstr_code,
                                       broker_code_subject,
                                       procbase_code,
                                       settl_curfinstr_finstr_code,
                                       price_curfinstr_finstr_code,
                                       rec_status,
                                       sysmoment, ext_file)
       (select case
                 when nfi0.t_exectype = 0 then
                   '1'
                 when nfi0.t_exectype = 1 then
                   '2'
               end EXEC_FORM,
               case
                 when dvn.t_sector = chr(0) then
                   '0'
                 when dvn.t_sector = chr(88) then
                   '1'
               end REFER_EXCHANGE,
               qb_dwh_utils.DateToChar(nfi0.t_execdate) SETTLEMENT_DATE,
        --       qb_dwh_utils.NumberToChar(round(nfi0.t_cost, 13),13) SPOT_RATE,
        --       qb_dwh_utils.NumberToChar(round(nfi0.t_price, 13), 13) FORWARD_RATE,
        --       qb_dwh_utils.NumberToChar(round(nfi0.t_amount, 13), 13) VOLUME,
               case
                 when fi0.t_avoirkind = 2 then
                   '2'
                 else
                   '1'
               end TYPE ,
               to_char(nfi0.t_amount, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') PRICE,
               '0' IS_LIQ_NETTING,
               case
                 when dvn.t_netting = chr(88) then
                   '1'
                 else
                   '0'
               end IS_SETTL_NETTING,
               'N' BASIS_TYPE,
               case
                 when dvn.t_type = 7 then
                   '1'
                 when dvn.t_type = 8 then
                   '2'
                 when dvn.t_type = 9 then
                   '4'
                 when dvn.t_type = 10 then
                   '3'
                 else
                   '0'
               end TYPE_RATE,
               null AGREEMENT_TYPE,
               case
                 when dvn.t_type = 1 then
                   '1'
                 when dvn.t_type = 2 then
                   '2'
                 else
                   '2'
               end DIRECTION,
               null MARGIN_THRESHOLD,
               case
                 when dvn.t_ispfi = chr(0) then
                   '0'
                 else
                   '1'
               end IS_DERIVATIVE,
               qb_dwh_utils.DateToChar(dvn.t_date) DT_BEGIN_ASSET,
               qb_dwh_utils.DateToChar(nfi0.t_execdate) DT_END_ASSET,
               '1' IS_REGISTERED_REPOS,
               '0' IS_MARGIN,
               null MARGIN_FREQ,
               null MIN_PAYMENT_SUM,
               null DAYS_MARGIN_RECALC,
               null TERM_OF_PLEDGE,
               to_char(round(nfi0.t_amount, 2), '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''')  AMOUNT,
               null EXCH_COUNT,
               '-1' EXCHANGE_CODE,
               to_char(dvn.t_id) || '#DVN#' ||
                   case when (nfi0.t_fiid <> 0 or nfi2.t_fiid <> 0) then
                          '96'
                        else
                          '93'
                   end DEAL_CODE,
               '0000#SOFRXXX#' || 
               decode(nfi0.t_rateid, -1, to_char(nfi0.t_rate,'FM990.099999999')||'#'|| nfi0.t_ratepoint||'#FIX', nfi0.t_rateid||'#FIN')||'#'||
               decode(nfi2.t_rateid, -1, to_char(nfi2.t_rate,'FM990.099999999')||'#'|| nfi2.t_ratepoint||'#FIX', nfi2.t_rateid||'#FIN')
                FINSTR_CODE,
               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             dvn.t_agent) BROKER_CODE_SUBJECT,
               case
                 when nfi0.t_rateid > 0 then
                   to_char(nfi0.t_rateid) || '#' || to_char(nfi0.t_basis)
                 when nfi2.t_rateid > 2 then
                   to_char(nfi2.t_rateid) || '#' || to_char(nfi2.t_basis)
                 else
                   '-1'
               end PROCBASE,
               case
                 when fi0.t_fi_kind = 1 then
                   fi0.t_iso_number
                 when fi0.t_fi_kind = 6 then
                   fi0.t_codeinaccount
                 else
                    to_char(fi0.t_fiid) || '#FIN'
               end SETTL_CURFINSTR_FINSTR_CODE,
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                    to_char(fi1.t_fiid) || '#FIN'
               end PRICE_CURFINSTR_FINSTR_CODE,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvndeal_dbt dvn
         inner join ddvnfi_dbt nfi0
            on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
         inner join ddvnfi_dbt nfi2
            on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
         inner join dfininstr_dbt fi0
            on (nfi0.t_fiid = fi0.t_fiid)
         inner join dfininstr_dbt fi1
            on (nfi2.t_pricefiid = fi1.t_fiid)
         where dvn.t_dvkind = 4
           and dvn.t_date <= in_date
         );
commit;
      -- Выгрузка в FCT_BANKNOTE
      qb_bp_utils.SetError(EventID,
                     '',
                     to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка банкнотных сделок',
                     2,
                     null,
                     null);
      insert into ldr_infa_pfi.fct_banknote(exec_form, refer_exchange, volume, basis_type, agreement_type, margin_threshold, is_derivative, dt_begin_asset, dt_end_asset, is_sale, is_registered_repos, is_margin, margin_freq, min_payment_sum, term_of_pledge, exchange_code, deal_code, finstr_code, broker_code_subject, settl_curfinstr_finstr_code, price_curfinstr_finstr_code, rec_status, sysmoment, ext_file)
       (select case
                 when nfi0.t_exectype = 0 then
                   '1'
                 when nfi0.t_exectype = 1 then
                   '2'
               end EXEC_FORM,
               case
                 when dvn.t_sector = chr(0) then
                   '0'
                 when dvn.t_sector = chr(88) then
                   '1'
               end REFER_EXCHANGE,
               to_char(nfi0.t_amount, '999G999G999G999G999G999G999D999', 'nls_numeric_characters=''. ''') VOLUME,
               'N' BASIS_TYPE,
               null AGREEMENT_TYPE,
               null MARGIN_THRESHOLD,
               case
                 when dvn.t_ispfi = chr(0) then
                   '0'
                 else
                   '1'
               end IS_DERIVATIVE,
               qb_dwh_utils.DateToChar(dvn.t_date) DT_BEGIN_ASSET,
               qb_dwh_utils.DateToChar(nfi0.t_execdate) DT_END_ASSET,
               case
                 when dvn.t_type = 1 then
                   '2'
                 when dvn.t_type = 2 then
                   '1'
               end IS_SALE,
               '1' IS_REGISTERED_REPOS,
               '0' IS_MARGIN,
               null MARGIN_FREQ,
               null MIN_PAYMENT_SUM,
               null TERM_OF_PLEDGE,
               '-1' EXCHANGE_CODE,
               to_char(dvn.t_id) || '#DVN#3' DEAL_CODE,
               case
                 when fi0.t_fi_kind = 1 then
                   fi0.t_iso_number
                 when fi0.t_fi_kind = 6 then
                   fi0.t_codeinaccount
                 else
                    to_char(fi0.t_fiid) || '#FIN'
               end FINSTR_CODE,
               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                             qb_dwh_utils.System_IBSO,
                                             1,
                                             dvn.t_agent) BROKER_CODE_SUBJECT,
               case
                 when fi0.t_fi_kind = 1 then
                   fi0.t_iso_number
                 when fi0.t_fi_kind = 6 then
                   fi0.t_codeinaccount
                 else
                    to_char(fi0.t_fiid) || '#FIN'
               end SETTL_CURFINSTR_FINSTR_CODE,
               case
                 when fi1.t_fi_kind = 1 then
                   fi1.t_iso_number
                 when fi1.t_fi_kind = 6 then
                   fi1.t_codeinaccount
                 else
                    to_char(fi1.t_fiid) || '#FIN'
               end PRICE_CURFINSTR_FINSTR_CODE,
               '0' REC_STATUS,
               to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE
          from ddvndeal_dbt dvn
         inner join ddvnfi_dbt nfi0
            on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
         inner join dfininstr_dbt fi0
            on (nfi0.t_fiid = fi0.t_fiid)
         inner join dfininstr_dbt fi1
            on (nfi0.t_pricefiid = fi1.t_fiid)
         where dvn.t_dvkind = 8
           and dvn.t_date <= in_date
        );
commit;
      
      -- Выгрузка в DET_KINDPROCRATE. BIQ-8474 п.3.3.6
      if (BIQ_8474 = 1) then 
          qb_bp_utils.SetError(EventID,
                               '',
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника ФИ',
                               2,
                               null,
                               null);
          Insert into Ldr_Infa_pfi.DET_KINDPROCRATE (Code, Name, DT, Rec_Status, SysMoment, Ext_File)
                      ( select   
                        decode (nfi.t_type, 0 , '9999#SOFRXXX#IRS#0', 2, '9999#SOFRXXX#IRS#2') CODE ,
                        decode (nfi.t_type, 0 , 'IRS. Параметры ставки для обязательств', 2, 'IRS. Параметры ставки для требований') NAME ,
                        qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')) DT,
                        '0' REC_STATUS, 
                        to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE 
           from ddvndeal_dbt dvn
           inner join ddvnfi_dbt nfi
             on dvn.t_id = nfi.t_dealid
           where dvn.t_dvkind = 4 and dvn.t_date <= in_date
           group by nfi.t_type );
commit;                          
          -- Выгрузка в DET_SUBKINDPROCRATE. BIQ-8474 п.3.3.7, справочник индексов
           qb_bp_utils.SetError(EventID,
                               '',
                               to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника ФИ',
                               2,
                               null,
                               null);
            Insert into Ldr_Infa_pfi.DET_SUBKINDPROCRATE (Code, Name, Counts, Period, DT, Rec_Status, SysMoment, Ext_File)
             (select f.t_fi_code code, f.t_name name, f.t_duration counts, 
             decode (f.t_typeduration, 1 , 'D', 2, 'W', 3, 'M', 4, 'Y' ) period,
             qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')) DT,
                        '0' REC_STATUS, 
                        to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE 
             from dfininstr_dbt f
             --inner join ddvnfi_dbt nfi on nfi.t_rateid = f.t_fiid загрузка всего справочника
             where f.t_fi_kind = 3 
             group by f.t_fi_code, f.t_name, f.t_duration, f.t_typeduration );
commit;        
            -- Выгрузка в FCT_PROCRATE_DEAL. BIQ-8474
            qb_bp_utils.SetError(EventID,
                                 '',
                                 to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка справочника ФИ',
                                 2,
                                 null,
                                 null);
            Insert into Ldr_Infa_pfi.FCT_PROCRATE_DEAL (Deal_Code, KindProcRate_Code, SubKindProcRate_Code, ProcBase_Code, ProcRate, ProcSum,
                                                   DT_Next_OverValue, DT_Contract, Rec_Status, DT, SysMoment, Ext_File)
            -- Пара процентных ставок (TYPEFINSTR=6). п.3.2, пп.2
                        (  select   
                          fd.code, --  DEAL_CODE ,
                          decode (nfi.t_type, 0 , '9999#SOFRXXX#IRS#0', 2, '9999#SOFRXXX#IRS#2'),-- CODE записи из таблицы DET_KINDPROCRATE,
                          nvl(finstr.t_fi_code, nfi.t_rateid ), -- CODE записи из таблицы DET_SUBKINDPROCRATE
                          decode (to_char(finstr.t_fiid), NULL, to_char(nfi.t_basis), to_char(finstr.t_fiid) || '#' || to_char(nfi.t_basis)),--CODE записи из таблицы DET_PROCBASE
                          qb_dwh_utils.NumberToChar(nfi.t_rate,5),
                          nfi.t_cost,-- Сумма
                          --qb_dwh_utils.DateToChar(nfi.t_fixdays), -- Дата следующей переоценки
                          '',
                          qb_dwh_utils.DateToChar(nfi.t_execdate), -- Дата договора
                          '0', 
                          qb_dwh_utils.DateToChar(to_date('01011980', 'ddmmyyyy')),
                          to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS') SYSMOMENT, dwhEXT_FILE 
                          
             from ddvndeal_dbt dvn
             inner join ddvnfi_dbt nfi
               on dvn.t_id = nfi.t_dealid
             inner join ldr_infa_pfi.fct_deal fd 
               on fd.docnum = dvn.t_code
             left join dfininstr_dbt finstr
               on nfi.t_rateid = finstr.t_fiid 
             where dvn.t_dvkind = 4 
              and dvn.t_date <= in_date
               );
       commit;    
            -- Вставка в ASS_ACCOUNTDEAL связок для биржевых опционов и фьючерсов BIQ-8474 п.3.2 пп.3
            if (exp_mode <> 1) then 
             qb_bp_utils.SetError(EventID,
                             '',
                             to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка связей сделка-счет для биржевых опционов и фьючерсов',
                             2,
                             null,
                             null);
              vSysmmnt := to_char(sysdate, 'dd-mm-yyyy hh24:MI:SS');
            insert into ldr_infa_pfi.ass_accountdeal
                (account_code,
                 deal_code,
                 roleaccount_deal_code,
                 dt,
                 dt_end,
                 rec_status,
                 sysmoment, ext_file)
             select /*+ parallel(4)*/ account_code, deal_code, roleaccount_deal_code, dt, dt_end, rec_status, sysmoment, dwhEXT_FILE  from   
              (select account_code, deal_code, roleaccount_deal_code, dt, dt_end, rec_status, sysmoment, dwhEXT_FILE from
               (select '0000#IBSOXXX#'|| case
                                              when (acnt.t_accountid is null) then
                                                doc.t_account
                                              when (acnt.t_userfield4 is null) or
                                                  (acnt.t_userfield4 = chr(0)) or
                                                  (acnt.t_userfield4 = chr(1)) or
                                                  (acnt.t_userfield4 like '0x%') then
                                                acnt.t_account
                                              else
                                                acnt.t_userfield4
                                            end account_code,
                              to_char(dv.t_id) || '#DV#' || decode(fi.t_avoirkind, 2, '95', '94') deal_code, 
                              upper(cat.t_code) roleaccount_deal_code, 
                              qb_dwh_utils.DateToChar(doc.t_activatedate) dt, 
                              qb_dwh_utils.DateToChar(doc.t_disablingdate) dt_end, 
                              '0' rec_status,
                              vSysmmnt SYSMOMENT, dwhEXT_FILE  
              from dmcaccdoc_dbt doc
              inner join ddvdeal_dbt dv 
                    on (dv.t_id=doc.t_docid and dv.t_fiid=doc.t_fiid)
              inner join dfininstr_dbt fi
                  on (dv.t_fiid = fi.t_fiid)
              left join dmccateg_dbt cat 
                   on cat.t_id = doc.t_catid
              left join daccount_dbt acnt
                   on (doc.t_chapter = acnt.t_chapter 
                       and doc.t_account = acnt.t_account 
                       and doc.t_currency = acnt.t_code_currency)
              where doc.t_dockind = 192
                    and doc.t_catid in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869)
                    --and doc.t_disablingdate = to_date('01.01.0001','DD.MM.YYYY') только открытые
                    and dv.t_date <= in_date)
              group by account_code, deal_code, roleaccount_deal_code, dt, dt_end, rec_status, sysmoment, dwhEXT_FILE
              --Форварды на ценную бумагу из БОЦБ с признаком ПФИ, BIQ-8474
                union all
                select account_code, deal_code, roleaccount_deal_code, dt, dt_end, rec_status, sysmoment, dwhEXT_FILE from
                (select '0000#IBSOXXX#'|| case
                                              when (acnt.t_accountid is null) then
                                                doc.t_account
                                              when (acnt.t_userfield4 is null) or
                                                  (acnt.t_userfield4 = chr(0)) or
                                                  (acnt.t_userfield4 = chr(1)) or
                                                  (acnt.t_userfield4 like '0x%') then
                                                acnt.t_account
                                              else
                                                acnt.t_userfield4
                                            end account_code,
                              to_char(tic.t_dealid) || '#CB#DVN#90' deal_code, 
                              upper(cat.t_code) roleaccount_deal_code, 
                              qb_dwh_utils.DateToChar(doc.t_activatedate) dt,
                              qb_dwh_utils.DateToChar(doc.t_disablingdate) dt_end, 
                              '0' rec_status,
                              vSysmmnt SYSMOMENT, dwhEXT_FILE    
              from ddl_tick_dbt tic
              inner join ddl_leg_dbt leg0 
                   on (tic.t_dealid = leg0.t_dealid and leg0.t_legkind = 0)
              inner join dmcaccdoc_dbt doc
                   on (tic.t_dealid=doc.t_docid and doc.t_dockind=tic.t_bofficekind 
                       and doc.t_catid in (163, 164, 460, 461, 476, 477, 611, 612, 614, 616, 617, 766, 767, 781, 782, 874, 875, 184, 185, 453, 454, 605, 606, 608, 609, 768, 769, 779, 780, 789, 790, 794, 795, 868, 869))
              left join dmccateg_dbt cat 
                   on cat.t_id = doc.t_catid
              left join daccount_dbt acnt
                   on (doc.t_chapter = acnt.t_chapter 
                       and doc.t_account = acnt.t_account 
                       and doc.t_currency = acnt.t_code_currency)
              left join doproper_dbt op
                   on (tic.t_bofficekind = op.t_dockind and lpad(to_char(tic.t_dealid), 34, '0') = op.t_documentid)
              left join doprdocs_dbt od
                   on (op.t_id_operation = od.t_id_operation and od.t_dockind = 1)
              left join dacctrn_dbt trn
                   on (od.t_acctrnid = trn.t_acctrnid and trn.t_state = 1)
              where tic.t_bofficekind = 101 and tic.t_dealtype in (12183, 12193) and tic.t_ispfi = chr(88) and substr(tic.t_dealcode, 1, 2) = 'Д/'
                    and tic.t_dealdate <= in_date)
              group by account_code, deal_code, roleaccount_deal_code, dt, dt_end, rec_status, sysmoment, dwhEXT_FILE
              --счета из проводок для для форвардов на ЦБ (Д/...) из ФИССиКО, продублированные в БОЦБ
              union all
              select account_code, deal_code, roleaccount_deal_code, dt, dt_end, rec_status, sysmoment, dwhEXT_FILE from
              (with acct as 
                   (
                   Select acdoc.t_catid, acdoc.t_account, acdoc.t_activatedate, acdoc.t_disablingdate,
                   min(decode(acdoc.t_iscommon,chr(88),acdoc.t_id)) over(partition by acdoc.t_account) min_t_id
                   from dmcaccdoc_dbt acdoc
                   where acdoc.t_catid in ( 163,164,460,461,476,477,611,612,614,616,617,766,767,781,782,874,875,184,185,453,454,605,606,608,609,768,769,779,780,789,790,794,795,868,869)
                   )
               select --+ leading(dvn) parallel(4)
               distinct 
                 '0000#IBSOXXX#' || case
                     when (acc.t_accountid is null) then acc.t_account
                     when (acc.t_userfield4 is null) or
                          (acc.t_userfield4 = chr(0)) or
                          (acc.t_userfield4 = chr(1)) or
                          (acc.t_userfield4 like '0x%') then acc.t_account
                     else acc.t_userfield4
                   end account_code,
                 to_char(dvn.t_id) || '#DVN#90' deal_code,
                 upper(cat.t_code) roleaccount_deal_code,
                 qb_dwh_utils.datetochar(acct.t_activatedate) dt,
                 qb_dwh_utils.datetochar(decode(acct.t_disablingdate, emptdate, null, acct.t_disablingdate)) dt_end,
                 '0' rec_status,
                 vSysmmnt sysmoment, dwhEXT_FILE
                  from ddvndeal_dbt dvn
                       left join ddvnfi_dbt nfi0
                             on (dvn.t_id = nfi0.t_dealid and nfi0.t_type = 0)
                       left join ddvnfi_dbt nfi2
                             on (dvn.t_id = nfi2.t_dealid and nfi2.t_type = 2)
                       inner join doproper_dbt op
                             on (dvn.t_dockind = op.t_dockind and lpad(to_char(dvn.t_id), 34, '0') = op.t_documentid)
                       inner join doprdocs_dbt od
                             on (op.t_id_operation = od.t_id_operation and od.t_dockind = 1)
                       inner join dacctrn_dbt trn
                             on (od.t_acctrnid = trn.t_acctrnid and trn.t_state = 1)
                       inner join daccount_dbt acc
                             on (acc.t_account = trn.t_account_payer or acc.t_account=trn.t_account_receiver)
                       inner join acct
                             on acct.t_account = acc.t_account
                       inner join dmccateg_dbt cat
                             on (cat.t_id = acct.t_catid)  
                       where t_date <= in_date
                       and t_dvkind = 1
                       and dvn.t_code like 'Д/%' )     
                      ); commit;
            end if; 
      end if; --BIQ_8474

        delete from ldr_infa_pfi.ass_accountdeal where to_date(dt, 'dd-mm-yyyy') > in_date; commit;
        --очистим дубли, т.к. некоторые связки есть не только в БОЦБ. BIQ_8474, DEF-24723
        delete from ldr_infa_pfi.ass_accountdeal where (dt, account_code, roleaccount_deal_code, deal_code , rec_status )
            in 
            (select dt, account_code, roleaccount_deal_code, deal_code , rec_status from 
                    (
                    select   dt, account_code, roleaccount_deal_code, deal_code , rec_status, count(1) cnt
                    from ldr_infa_pfi.ASS_ACCOUNTDEAL
                    group by  dt, account_code, roleaccount_deal_code, deal_code, rec_status  
                    )
            where cnt > 1
            )
            and sysmoment != vSysmmnt;commit;
        delete from ldr_infa_pfi.det_roleaccount_deal
              where code not in (select distinct '9999#SOFRXXX#' || roleaccount_deal_code
                                   from ldr_infa_pfi.ass_accountdeal);commit;
        delete from ldr_infa_pfi.ASS_DEAL_CAT_VAL where to_date(dt, 'dd-mm-yyyy') > in_date;commit;
        delete from ldr_infa_pfi.DET_INDEX where to_date(dt, 'dd-mm-yyyy') > in_date;commit;
        delete from ldr_infa_pfi.DET_RATE_PAIR where to_date(dt, 'dd-mm-yyyy') > in_date;commit;
        delete from ldr_infa_pfi.FCT_DEAL_INDICATOR where to_date(dt, 'dd-mm-yyyy') > in_date;commit;
        delete from ldr_infa_pfi.FCT_DEAL_RST where to_date(dt, 'dd-mm-yyyy') > in_date;commit;
        delete from ldr_infa_pfi.fct_deal_indicator fdi where not exists (select 1
                                                                            from ldr_infa_pfi.fct_deal
                                                                           where code = fdi.deal_code);commit;
        delete from ldr_infa_pfi.ASS_DEAL_CAT_VAL adcv where not exists (select 1
                                                                            from ldr_infa_pfi.fct_deal
                                                                           where code = adcv.deal_code);commit;
        qb_bp_utils.EndEvent(EventID, null);
        for i in (select d.t_code from ddp_dep_dbt d where d.t_code = 1) loop
          vDateLastOD := qb_dwh_utils.GetLastClosedOD(i.t_code);
          vDateBeg   := sysdate;
          qb_dwh_utils.add_export_log_pfi ( nvl(EventID,0),
                                            -1,
                                            qb_dwh_utils.GetCODE_DEPARTMENT(i.t_code),
                                            vDateLastOD,
                                            vDateBeg,
                                            sysdate);
        end loop;
        if (hasEmptyTables) then
                     qb_bp_utils.SetError(EventID,
                              '',
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при заполнении данных по ПФИ: не все таблицы заполнены' ,
                              0,
                              null,
                              null);
        end if;
    exception
       when others then
         qb_bp_utils.SetError(EventID,
                              SQLCODE,
                              to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при заполнении данных по ПФИ: ' || SQLERRM,
                              0,
                              null,
                              null);
    end;
  end;

  procedure load_fct_account_sofr_trn_by_date(procid        in number, 
                                              in_date       in date) is
    pDepartment number :=1;
    EventID      number := 0;
    dwhRecStatus varchar2(1);
    dwhDT        varchar2(20);
    dwhSysMoment varchar2(19);
    dwhEXT_FILE  varchar2(300);
  begin
  
    startevent(cEvent_EXPORT_PFI, procid, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             QB_DWH_EXPORT.cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDepartment, pDepartment);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDT, in_date);

    qb_dwh_export.InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE, 2);
    qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 
                         'Разовая выгрузка данных по ПФИ (остатки и обороты по ЛС) ' || to_char(in_date, 'yyyy.mm.dd'),
                         2,
                         null,
                         null);

    insert into ldr_infa_pfi.FCT_ACCOUNT_SOFR(account_sofr_code, rest, rest_nat, debet, debet_nat, credit, credit_nat, 
                                              dt, sysmoment, rec_status, ext_file)
       select '0000#SOFRXXX#' || a.t_account as ACCOUNT_SOFR_CODE,
              --nvl(v.t_rest, o.t_rest) as rest,
              --o.t_rest as rest_nat,
              --nvl(v_o.t_debet, o.t_debet) as debet,
              --o.t_debet as debet_nat,
              --nvl(v_o.t_credit, o.t_credit) as credit,
              --o.t_credit as credit_nat,
              to_char (rsi_rsb_account.restall(a.t_account, 1, a.t_code_currency, in_date, a.t_code_currency)) as REST,
              to_char (rsi_rsb_account.restall(a.t_account, 1, a.t_code_currency, in_date ,0)) as REST_NAT,
              to_char (rsi_rsb_account.debetac(a.t_account, 1, a.t_code_currency, in_date , in_date, a.t_code_currency)) as DEBET,
              to_char (rsi_rsb_account.debetac(a.t_account, 1, a.t_code_currency, in_date , in_date, 0)) as DEBET_NAT,
              to_char (rsi_rsb_account.kreditac(a.t_account, 1, a.t_code_currency, in_date , in_date , a.t_code_currency)) as CREDIT,
              to_char (rsi_rsb_account.kreditac(a.t_account, 1, a.t_code_currency, in_date , in_date, 0)) as CREDIT_NAT,
              qb_dwh_utils.DateToChar(in_date) as DT,
              qb_dwh_utils.DateTimeToChar(sysdate) as SYSMOMENT,
              '0' as REC_STATUS,
              dwhEXT_FILE as EXT_FILE
       from ldr_infa_pfi.tmp_acc306 a --т.к. выполняется после export_PFI, данные уже должны быть
       union
       SELECT '0000#SOFRXXX#' || a.t_account as ACCOUNT_SOFR_CODE,
              to_char (rsi_rsb_account.restall(a.t_account, 1, a.t_code_currency, trn.t_date_carry, a.t_code_currency)) as REST,
              to_char (rsi_rsb_account.restall(a.t_account, 1, a.t_code_currency, trn.t_date_carry , 0)) as REST_NAT,
              to_char (rsi_rsb_account.debetac(a.t_account, 1, a.t_code_currency, trn.t_date_carry , trn.t_date_carry, a.t_code_currency)) as DEBET,
              to_char (rsi_rsb_account.debetac(a.t_account, 1, a.t_code_currency, trn.t_date_carry , trn.t_date_carry, 0)) as DEBET_NAT,
              to_char (rsi_rsb_account.kreditac(a.t_account, 1, a.t_code_currency, trn.t_date_carry , trn.t_date_carry , a.t_code_currency)) as CREDIT,
              to_char (rsi_rsb_account.kreditac(a.t_account, 1, a.t_code_currency, trn.t_date_carry , trn.t_date_carry, 0)) as CREDIT_NAT,
              qb_dwh_utils.DateToChar(trn.t_date_carry) as DT,
              qb_dwh_utils.DateTimeToChar(sysdate) as SYSMOMENT,
              '0' as REC_STATUS,
              dwhEXT_FILE as EXT_FILE
         FROM DACCTRN_DBT  trn
              INNER JOIN Daccount_dbt a
                  ON a.t_accountid =
               CASE
                   WHEN SUBSTR (trn.t_account_receiver, 1, 5) IN ('30606', '30601') 
                     THEN  trn.t_accountid_receiver
                   WHEN SUBSTR (trn.t_account_payer, 1, 5) IN ('30606', '30601')
                     THEN trn.t_accountid_payer
               END
        WHERE     trn.t_systemdate = in_date
              AND (trn.t_systemdate - trn.t_date_carry) > 1;
    commit;
  end;

  ------------------------------------------------------
  -- Выгрузка данных по ПФИ 
  ------------------------------------------------------
  procedure RunExport(in_Date date, procid number, export_mode number default 0) is
    vLdrClear varchar2(400);
  begin

    --обычная ежедневная выгрузка
    vLdrClear := nvl(RSB_Common.GetRegStrValue('РСХБ\ИНТЕГРАЦИЯ\ЦХД\TRUNCATE_LDRINFA'),'YES');
    if (export_mode in (0, 1) ) then
      if (vLdrClear = 'YES') then
        if (export_mode = 0) then
          clearPFIData(1);
        else
          clearPFIData(2);
        end if;
        --commit;
      end if;
      export_PFI(1, in_Date, procid, export_mode);
      --commit;
    end if;

    FOR OBJ IN (SELECT * FROM DDWHADDITIONPFIEXP_DBT)
    LOOP
      load_fct_account_sofr_trn_by_date(procid, OBJ.T_date);
    END LOOP;
    
    execute immediate 'truncate table DDWHADDITIONPFIEXP_DBT';

  end;

end QB_DWH_EXPORT_PFI;
/

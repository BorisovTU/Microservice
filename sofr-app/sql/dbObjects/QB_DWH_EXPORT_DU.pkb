create or replace package body QB_DWH_EXPORT_DU is

  firstDate          constant date := to_date('01011980', 'ddmmyyyy');
  emptDate           constant date := to_date('01010001', 'ddmmyyyy');
  type numberList_t is table of number(10) index by pls_integer;
  listCB numberList_t; -- выгружаемые ценные бумаги
  listCL numberList_t; --             клиенты
  listIS numberList_t; --             эмитенты
--  minDate            constant date := to_date('01012019', 'ddmmyyyy');
  maxDate            constant date := to_date('31129999', 'ddmmyyyy');
  cSECKIND_STOCK     constant varchar2(20) := 'SECKIND_STOCK';
  cSECKIND_BOND      constant varchar2(20) := 'SECKIND_BOND';
  cSECKIND_RECEIPT   constant varchar2(20) := 'SECKIND_RECEIPT';
  cSECKIND_BILL      constant varchar2(20) := 'SECKIND_BILL';
  cSECKIND_UIT       constant varchar2(20) := 'SECKIND_UIT';
  n110               constant pls_integer := 110;
  n210               constant pls_integer := 210;
  n310               constant pls_integer := 310;
  n436               constant pls_integer := 436;



  EventID      number := 0;
  dwhRecStatus varchar2(1);
  dwhDT        varchar2(20);
  dwhSysMoment varchar2(19);
  dwhEXT_FILE  varchar2(300);


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
  -- очистка данных по DU
  ------------------------------------------------------
  procedure clearDUData(in_Type number default 0) is
  begin
    execute immediate 'truncate table ldr_infa_du.det_finstr';
--    execute immediate 'truncate table ldr_infa_du.det_juridic_person';
    execute immediate 'truncate table ldr_infa_du.det_procbase';
    execute immediate 'truncate table ldr_infa_du.det_security';
    execute immediate 'truncate table ldr_infa_du.det_security_type_711';
--    execute immediate 'truncate table ldr_infa_du.det_subject';
    execute immediate 'truncate table ldr_infa_du.det_type_rate';
    execute immediate 'truncate table ldr_infa_du.fct_finstr_rate';
    execute immediate 'truncate table ldr_infa_du.fct_mgmt_position';
    execute immediate 'truncate table ldr_infa_du.fct_mgmt_sec';
    execute immediate 'truncate table ldr_infa_du.fct_mgmt_sec_shedule';
    /*delete from dqb_bp_event_error_dbt er
     where (select trunc(ev.t_timestamp)
              from dqb_bp_event_dbt ev
             where ev.t_id = er.t_idevent) < trunc(sysdate) - 6 * 30;*/
    /*if in_type = 0 then
      delete from DQB_BP_EVENT_ERROR_DBT; commit;  -- Ошибки произошедьшии при событии
      delete from DQB_BP_EVENT_ATTR_DBT;  commit;  -- Аттрибуты
      delete from DQB_BP_EVENT_DBT;       commit;  -- События
    end if;*/
  end;

  -----------------------------------------------------
  --Запись данных в в лог выгрузки с указанием филиала и последнего операционного дня (для выгрузки прочих ПФИ)
  -----------------------------------------------------
  procedure add_export_log ( in_id          in number,
                             in_id_pre      in number,
                             in_filcode     in varchar2,
                             in_datelastod  in date,
                             in_beg_date    in date,
                             in_end_date    in date) is
  v_table varchar2(4000);

  begin
    v_table := 'DET_FINSTR;DET_PROCBASE;DET_SECURITY;DET_SECURITY_TYPE_711;DET_TYPE_RATE;FCT_FINSTR_RATE;FCT_MGMT_POSITION;FCT_MGMT_SEC;FCT_MGMT_SEC_SHEDULE;';
    insert into ldr_infa_du.fct_department_od (id,
                                               id_pre,
                                               filcode,
                                               startlog,
                                               endlog,
                                               datelastod,
                                               corr,
                                               system_code,
                                               dt_begin,
                                               dt_end,
                                               table_load)
                                     values (in_id,
                                             decode(in_id_pre,
                                                    -1,(select nvl(max(id),0) from ldr_infa_du.fct_department_od),
                                                    in_id_pre),
                                             in_filcode,
                                             in_beg_date,
                                             in_end_date,
                                             in_datelastod,
                                             null,
                                             'SOFRXXX',
                                             trunc(in_beg_date),
                                             trunc(in_end_date),
                                             v_table
                                             );
commit;                                             
  end;
  /*
  procedure exportSUBJ (pt_id in number) is
  begin
    dbms_output.put_line('выгрузка субъекта' || pt_id);
    for rec in (select case
                         when po.t_partyid is not null then
                           '4'  -- Банк
                         when pt.t_legalform = 1 then
                           '2'  -- ЮЛ
                         when pt.t_legalform = 2 and pers.t_isemployer = chr(88) then
                           '3'  -- ИП
                         when pers.t_personid is not null then
                           '1'  -- ФЛ
                         else
                           '-1'  -- Не определен
                       end clientType,
                       qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                     qb_dwh_utils.System_IBSO,
                                                     1,
                                                     pt.t_partyid) code_subject,
                       nvl((select min(ob.t_bankdate)
                              from dobjcode_dbt ob
                             where ob.t_objecttype = 3
                               and ob.t_objectid = pt.t_partyid
                               and ob.t_bankdate <> to_date('01010001','ddmmyyyy')), to_date('01012019', 'ddmmyyyy')) dt,
                       ob_inn.t_code inn,
                       c.t_codenum3 country,
                       pt.t_shortname clname_short,
                       pt.t_name clname,
                       main_okved.okved_list,
                       ob_okato.t_code okato
                  from dparty_dbt pt
                  left join dpersn_dbt pers
                    on (pt.t_partyid = pers.t_personid)
                  left join dpartyown_dbt po
                    on (pt.t_partyid = po.t_partyid and po.t_partykind = 2)
                  left join dobjcode_dbt ob_inn
                    on (pt.t_partyid = ob_inn.t_objectid and ob_inn.t_objecttype = 3 and ob_inn.t_codekind = 16 and ob_inn.t_state = 0)
                  left join dcountry_dbt c
                    on (pt.t_nrcountry = c.t_codelat3)
                  left join (select to_number(cat64_2.t_object) ptid,
                                    listagg(atr64_2.t_nameobject, ',') within group(order by cat64_2.t_id) as okved_list
                               from dobjatcor_dbt cat64_2
                               left join dobjattr_dbt atr64_2
                                 on (cat64_2.t_objecttype = atr64_2.t_objecttype and
                                    cat64_2.t_groupid = atr64_2.t_groupid and
                                    cat64_2.t_attrid = atr64_2.t_attrid)
                              where cat64_2.t_objecttype = 3
                                and cat64_2.t_groupid = 64
                                and cat64_2.t_validtodate = to_date('31129999', 'ddmmyyyy')
                                and cat64_2.t_general = chr(88)
                              group by cat64_2.t_object) main_okved
                    on (pt.t_partyid = main_okved.ptid)
                  left join dobjcode_dbt ob_okato
                    on (pt.t_partyid = ob_okato.t_objectid and ob_okato.t_objecttype = 3 and ob_okato.t_codekind = 73 and ob_okato.t_State = 0)
                 where  pt.t_partyid =  pt_id and rownum = 1)
    loop
      insert into ldr_infa_du.det_subject(typesubject, code_subject, dt, sysmoment, dt_reg, inn, system_code, department_code, country_code_num, rec_status, ext_file)
             values (rec.clienttype, rec.code_subject, qb_dwh_utils.DateToChar(rec.dt), dwhSysMoment, qb_dwh_utils.DateToChar(rec.dt), rec.inn, 'SOFRXXX', '0000', rec.country, dwhRecStatus, dwhEXT_FILE);
      if (rec.clienttype = '2') then -- ЮЛ
        insert into ldr_infa_du.det_juridic_person(juridic_person_name_s, juridic_person_name, dt_registration, dt, sysmoment, note, subject_code, okved_code, okato_code, rec_status, ext_file)
               values(rec.clname_short, rec.clname, qb_dwh_utils.DateToChar(rec.dt), qb_dwh_utils.DateToChar(rec.dt), dwhSysMoment, null, rec.code_subject, rec.okved_list, rec.okato, dwhRecStatus, dwhEXT_FILE);
      end if;
    end loop;
  end;
  */
  procedure exportSPR is
  begin
    insert into ldr_infa_du.det_security_type_711(code_type, name_type, dt, sysmoment, rec_status, ext_file)
           values ('-1', 'Не определен', qb_dwh_utils.DateToChar(firstDate), dwhSysMoment, dwhRecStatus, dwhEXT_FILE);
commit;           

    insert into ldr_infa_du.det_procbase(code, name,days_year, days_month, sign_31, first_day, last_day, null_mainsum, type_prc_charge)
    select '360/0', 'Календарь/360', 360, 31, 1, 0, 1, 1, 0
      from dual
    union all
    select '366/0', 'Календарь/Календарь', 365, 31, 1, 0, 1, 1, 0
      from dual
    union all
    select '360#S', 'Календарь/360 (сложный %%)', 360, 31, 1, 0, 1, 0, 1
      from dual
    union all
    select '366#S', 'Календарь/Календарь (сложный %%)', 365, 31, 1, 0, 1, 0, 1
      from dual
    union all
    select '30/365', '30/365', 365, 30 , 0, 0, 1, 0, 0
      from dual
    union all
    select '-1', 'Не определено', 365, 1, 0, 0, 0, 0, 0
      from dual
    union all
    select '360_F', 'Календарь/360 c начислением за первый день', 360, 31, 1, 1, 0, 0, 0
      from dual
--    union all
--    select '1', 'Ежедневная процентная ставка', 1, 1, 1, 0, 1, 0, 0
--      from dual
    union all
    select '366', 'Календарь/Календарь', 366, 31, 1, 0, 1, 0, 0
      from dual
    union all
    select '365', 'Календарь/365', 365, 31, 1, 0, 1, 0, 0
      from dual
    union all
    select '360', 'Календарь/360', 360, 31, 1, 0, 1, 0, 0
      from dual
    union all
    select '30/360', '30/360', 360, 30, 0, 0, 1, 0, 0
      from dual
    union all
    select '31/360', '30/360 с учетом 31 числа, если кредит лежит неполный месяц', 360, 30, 1, 0, 1, 0, 0
      from dual
    union all
    select '30/366', '30/Календарь', 366, 30, 0, 0, 1, 0, 0
      from dual
    union all
    select '31/366', '30/Календарь с учетом 31 числа, если кредит лежит неполный месяц', 366, 30, 1, 0, 1, 0, 0
      from dual
    union all
    select 'Act/по_купонным_периодам', 'В году по куп. периодам, в мес. по кален', 0, 31, 1, 0, 1, 0, 0
      from dual
    union all
    select 'Act/365L', 'В году по оконч.куп.пер, в мес. по кален', 365, 31, 1, 0, 1, 0, 0
      from dual
    union all
    select 'Act/364', '364 дня в году, в месяце по календарю', 364, 31, 1, 0, 1, 0, 0
      from dual
    union all
    select '30E/360', '360 дней в году, 30 в месяце (Eurobond)', 360, 30, 0, 0, 1, 0, 0
      from dual
    union all
    select 'Act/Act_ICMA', 'Actual/Actual (ICMA)', 366, 31, 1, 0, 1, 0, 0
      from dual
    union all
    select '30/360_ISDA', 'Календарь/360', 360, 30, 0, 0, 1, 0, 0
      from dual;
commit;      
    update ldr_infa_du.det_procbase pb
       set pb.dt = qb_dwh_utils.DateToChar(firstDate),
           pb.sysmoment = dwhSysMoment,
           pb.rec_status = dwhRecStatus,
           pb.ext_file = dwhEXT_FILE;
commit;           

    for rec in (select to_char(rt.t_type) type_rate_code,
                       rt.t_typename type_rate_name
                  from dratetype_dbt rt)
    loop
      begin
        insert into ldr_infa_du.det_type_rate(type_rate_code, type_rate_name, dt, rec_status, sysmoment, ext_file)
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


  exception
     when others then
       qb_bp_utils.SetError(EventID,
                            SQLCODE,
                            to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при заполнении справочных данных по ДУ: ' || SQLERRM,
                            0,
                            null,
                            null);

  end;



  procedure exportCB(fiid in number, exp_date in date) is
    stype varchar2(2);
    pbase varchar2(50);
    vperiod varchar2(3);
    rate_sp varchar2(100);
    rate_moody varchar2(100);
    rate_fitch varchar2(100);
    rate_akra varchar2(100);
  begin
    for rec in (select to_char(fiid) || '#FIN' vcode,
                       substr(trim(fi.t_name), 1, 250) vname,
                       substr(trim(fi.t_definition), 1, 50) vnames,
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
                       qb_dwh_utils.NumberToChar(fi.t_facevalue) vnomin,
                       case when fi.t_fiid in (220, 1924, 1416, 1536, 2007, 1406, 1722, 1660, 2369, 1596, 1555, 2212, 2148, 5369, 5567, 9556, 7969, 16163, 11860, 11790, 14705) then        -- !!!!!!!! Для эмитентов по этим бумагам  нет выгружаемого кода БИСКВИТ
                              '-1'
                            else
                               qb_dwh_utils.GetComponentCode('DET_SUBJECT',
                                                           qb_dwh_utils.System_IBSO,
                                                           1,
                                                           fi.t_issuer)
                       end codeis,
                       fi.t_issuer issuer,
                       pfi.t_iso_number ficode,
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
                       end vdt,
                       av.t_lsin vregnum,
                       av.t_isin visin,
                       av.t_subordinated issub,
                       fi.t_avoirkind avoirkind,
                       av.t_nkdbase_kind nkdbase,
                       regexp_replace(replace(av.t_series, chr(1)) || '/' || replace(av.t_issue, chr(1)), '^/$') series,
                       decode(fi.t_drawingdate, to_date('01010001','ddmmyyyy'), to_date('01013001','ddmmyyyy'), fi.t_drawingdate) dt_repay,
                       trim(to_char(decode(fi.t_facevalue, 0, 0, rsb_fiinstr.FI_GetNominalOnDate(fi.t_fiid, trunc(sysdate))*100/ fi.t_facevalue), '999999999999999999999999990D99', 'nls_numeric_characters=''. ''')) nominal_perc,
                       trim(to_char(rsb_fiinstr.FI_GetNominalOnDate(fi.t_fiid, exp_date), '999999999999999999999999990D99', 'nls_numeric_characters=''. ''')) nominal
                  from dfininstr_dbt fi
                 inner join davoiriss_dbt av
                    on (fi.t_fiid = av.t_fiid)
                  left join dfininstr_dbt pfi
                    on (fi.t_facevaluefi = pfi.t_fiid)
                 where fi.t_fiid = fiid
                   and rownum = 1
          )
    loop
      listIS(rec.issuer) := null;
      insert into ldr_infa_du.det_finstr(typefinstr, finstr_code, finstr_name,finstr_name_s, dt, sysmoment, rec_status, ext_file)
             values (2, rec.vcode, rec.vname, rec.vnames, qb_dwh_utils.DateToChar(rec.vdt), dwhSysMoment, dwhRecStatus, dwhEXT_FILE);
commit;             
      stype := case when InConst(cSECKIND_STOCK, rec.avoirkind) then           -- акция
                  1
               when InConst(cSECKIND_BILL, rec.avoirkind)  then                -- вексель
                  3
               when InConst(cSECKIND_UIT, rec.avoirkind)  then                 -- ПИФ
                  5
               when InConst(cSECKIND_BOND, rec.avoirkind) then                 -- облигация
                  2
               when InConst(cSECKIND_RECEIPT, rec.avoirkind) then              -- депозитарная расписка
                  6
               else
                  null
               end;
      pbase := case rec.nkdbase
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
      insert into ldr_infa_du.det_security(typesecurity, code, date_issue, nominal, dt, sysmoment, regnum, finstrsecurity_finstr_code, security_type_711_code_type, finstrcurnom_finstr_code, underwriter_code_subject, issuer_code_subject, procbase_code, rec_status, ext_file)
             values (stype, rec.vcode, qb_dwh_utils.DateToChar(rec.date_is), rec.vnomin, qb_dwh_utils.DateToChar(rec.vdt), dwhSysMoment, rec.vregnum, rec.vcode, '-1', rec.ficode, '-1', rec.codeis, pbase, dwhRecStatus, dwhEXT_FILE);
commit;             
      for rec_s in ( select t.fiid,
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
                       t.proc_sum,
                       t.dt_pay
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
                                          rec.vdt,
                                          wr.t_drawingdate) + 1,
                                   1, rec.vdt) over(partition by wr.t_fiid, wr.t_ispartial order by wr.t_fiid, wr.t_ispartial, decode(wr.t_drawingdate, emptDate, rec.vdt, wr.t_drawingdate)) begindate_calc,
                               decode(wr.t_drawingdate, emptDate, rec.vdt, wr.t_drawingdate) enddate,

                               wr.t_incomerate proc_rate,
                               wr.t_incomevolume proc_sum,
                               (select t_drawingdate
                                  from (select fiw.t_drawingdate,
                                               row_number() over(order by fiw.t_drawingdate) rnk
                                          from dfiwarnts_dbt fiw
                                         where fiw.t_fiid = fiid
                                           and fiw.t_ispartial = chr(0)
                                           and fiw.t_drawingdate > exp_date)
                                 where rnk = 1) dt_pay
                          from dfiwarnts_dbt wr
                         inner join davoiriss_dbt av
                            on (wr.t_fiid = av.t_fiid)
                         where wr.t_fiid = fiid
                           and wr.t_drawingdate > emptDate
--                           and  (round(wr.t_incomerate, 3) <> 0 or round(wr.t_incomevolume, 3) <> 0)
                           ) t)
      loop
        if (rec_s.begindate <= rec_s.enddate) then
          insert into ldr_infa_du.fct_mgmt_sec_shedule(typeschedule, typerepaysec, begindate, enddate, proc_rate, amount, security_code, dt, dt_pay, rec_status, sysmoment, ext_file)
          values (rec_s.typeschedule, rec_s.typerepaysec, qb_dwh_utils.DateToChar(rec_s.begindate) , qb_dwh_utils.DateToChar(rec_s.enddate), qb_dwh_utils.NumberToChar(round(rec_s.proc_rate, 3), 3), qb_dwh_utils.NumberToChar(round(rec_s.proc_sum, 3), 3),
                  rec.vcode,  qb_dwh_utils.DateToChar(rec.date_is), qb_dwh_utils.DateToChar(rec_s.dt_pay), dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                  
        end if;
      end loop;
      begin
          /*--Алгоритм по первым двум выплатам
              select case cntdays
                       when 1 then
                        '12'
                       when 3 then
                        '4'
                       when 6 then
                        '2'
                       when 12 then
                        '1'
                       else
                        '0'
                     end
                into vperiod
                from (select fiw.t_fiid,
                             fiw.t_drawingdate,
                             lead(fiw.t_drawingdate, 1, null) over(partition by fiw.t_fiid order by fiw.t_drawingdate) next_date,
                             round((lead(fiw.t_drawingdate, 1, null)
                                    over(partition by fiw.t_fiid order by
                                         fiw.t_drawingdate) - fiw.t_drawingdate) / 30,
                                   0) cntdays,
                             row_number() over(partition by fiw.t_fiid order by fiw.t_drawingdate) rnk
                        from dfiwarnts_dbt fiw
                       where fiw.t_fiid = fiid
                         and fiw.t_ispartial = chr(0)
                       order by fiw.t_drawingdate)
               where rnk = 1;*/
        -- Среднее значение в год
        select case round(count(to_date(s.begindate, 'dd-mm-yyyy')) * (366/(max(to_date(s.enddate, 'dd-mm-yyyy'))- min(to_date(s.begindate, 'dd-mm-yyyy')))))
                 when 1 then
                   '1'
                 when 2 then
                   '2'
                 when 4 then
                   '4'
                 when 12 then
                   '12'
                 else
                   '0'
               end
          into vperiod
          from ldr_infa_du.fct_mgmt_sec_shedule s
         where  s.security_code = rec.vcode
           and s.typeschedule = '1'
           and s.typerepaysec = '1'
           and (to_date(s.enddate, 'dd-mm-yyyy') - to_date(s.begindate, 'dd-mm-yyyy')) > 25;
      exception
        when no_data_found then
          vperiod := null;
      end;
--      dbms_output.put_line('SEC '|| rec.vname || ' ' || period);
      begin
        select atr.t_name
          into rate_sp
          from dobjatcor_dbt ov
          inner join dfininstr_dbt fi
             on (to_number(ov.t_object) = fi.t_fiid)
          inner join dobjattr_dbt atr
            on (ov.t_objecttype = atr.t_objecttype and ov.t_groupid = atr.t_groupid and ov.t_attrid = atr.t_attrid)
         where ov.t_objecttype = 12 and ov.t_groupid = 53
           and ov.t_object = lpad(to_char(fiid), 10, '0')
           and exp_date between ov.t_validfromdate and ov.t_validtodate
           and atr.t_parentid = n110;
      exception
        when no_data_found then
          rate_sp := null;
      end;
      begin
        select atr.t_name
          into rate_moody
          from dobjatcor_dbt ov
          inner join dfininstr_dbt fi
             on (to_number(ov.t_object) = fi.t_fiid)
          inner join dobjattr_dbt atr
            on (ov.t_objecttype = atr.t_objecttype and ov.t_groupid = atr.t_groupid and ov.t_attrid = atr.t_attrid)
         where ov.t_objecttype = 12 and ov.t_groupid = 53
           and ov.t_object = lpad(to_char(fiid), 10, '0')
           and exp_date between ov.t_validfromdate and ov.t_validtodate
           and atr.t_parentid = n210;
      exception
        when no_data_found then
          rate_moody := null;
      end;
      begin
        select atr.t_name
          into rate_fitch
          from dobjatcor_dbt ov
          inner join dfininstr_dbt fi
             on (to_number(ov.t_object) = fi.t_fiid)
          inner join dobjattr_dbt atr
            on (ov.t_objecttype = atr.t_objecttype and ov.t_groupid = atr.t_groupid and ov.t_attrid = atr.t_attrid)
         where ov.t_objecttype = 12 and ov.t_groupid = 53
           and ov.t_object = lpad(to_char(fiid), 10, '0')
           and exp_date between ov.t_validfromdate and ov.t_validtodate
           and atr.t_parentid = n310;
      exception
        when no_data_found then
          rate_fitch := null;
      end;
      begin
        select atr.t_name
          into rate_akra
          from dobjatcor_dbt ov
          inner join dfininstr_dbt fi
             on (to_number(ov.t_object) = fi.t_fiid)
          inner join dobjattr_dbt atr
            on (ov.t_objecttype = atr.t_objecttype and ov.t_groupid = atr.t_groupid and ov.t_attrid = atr.t_attrid)
         where ov.t_objecttype = 12 and ov.t_groupid = 53
           and ov.t_object = lpad(to_char(fiid), 10, '0')
           and exp_date between ov.t_validfromdate and ov.t_validtodate
           and atr.t_parentid = n436;
      exception
        when no_data_found then
          rate_akra := null;
      end;

      insert into ldr_infa_du.fct_mgmt_sec(isin_code, dt, series, dt_repay, period_pay, rate_moodys_first, rate_fitch_first,rate_sp_first, rate_akra_first, sysmoment, security_code, rec_status, ext_file)
                  values (rec.visin, qb_dwh_utils.DateToChar(rec.vdt), rec.series, qb_dwh_utils.DateToChar(rec.dt_repay), vperiod, rate_moody, rate_fitch, rate_sp, rate_akra, dwhSysMoment, rec.vcode, dwhRecStatus, dwhEXT_FILE);
commit;                  

    end loop;

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
               where to_date(dt,'dd-mm-yyyy') <= exp_date)
    loop
          insert into ldr_infa_du.fct_finstr_rate(finstr_numerator_finstr_code, finstr_denumerator_finstr_code, finstr_rate, finstr_scale, type_finstr_rate_type_rate_cod, dt,rec_status,sysmoment, ext_file)
                 values (to_char(fiid) || '#FIN', rec.finstr_denumerator_finstr_code, rec.finstr_rate, rec.finstr_scale, rec.type_finstr_rate_type_rate_cod, rec.dt, dwhRecStatus, dwhSysMoment, dwhEXT_FILE);
commit;                 
    end loop;

  exception
     when others then
       qb_bp_utils.SetError(EventID,
                            SQLCODE,
                            to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при заполнении данных по ЦБ в ДУ: ' || SQLERRM,
                            0,
                            null,
                            fiid);

  end;



  procedure export_du(in_department in number,
                      in_date       in date,
                      procid        in number) is
    vDateLastOD date;
    vDateBeg    date;
    cnt pls_Integer;
    m pls_integer := 1;
    y pls_integer := 2020;
    d date;
    vrate_sp varchar2(100);
    vrate_moody varchar2(100);
    vrate_fitch varchar2(100);
    vrate_akra varchar2(100);
    vdate_offer date;
    v_CourceType NUMBER;
    br_cost number (32, 12);
    nominal_on_date number(32, 12);
    vrate   varchar2(50);
    vnkd  varchar2(40);
    vdrawdate date;
    contract_numb varchar2(100);
  begin

    startevent(cEvent_EXPORT_DU, procid, EventID);

    qb_bp_utils.SetAttrValue(EventID,
                             QB_DWH_EXPORT.cAttrRec_Status,
                             qb_dwh_utils.REC_ADD);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDepartment, in_department);
    qb_bp_utils.SetAttrValue(EventID, QB_DWH_EXPORT.cAttrDT, in_date);

    qb_dwh_export.InitExportData(EventID, dwhRecStatus, dwhDT, dwhSysMoment, dwhEXT_FILE, 2);
    qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка данных по ДУ',
                         2,
                         null,
                         null);
    v_CourceType := Rsb_Common.GetRegIntValue( 'SECUR\ВИД КУРСА СРЕДНЕВЗВЕШЕННАЯ ЦЕНА', 0 );
    -- Выгрузка справочных данных
    exportSPR;
    -- Цикл по месяцам

    loop
      d := to_date('01'||to_char(m, '09')||to_char(y), 'ddmmyyyy') - 1;
      exit when d > in_date;
      --dbms_output.put_line(d); -- выгружаем лоты за эти даты
      for rec in ( with
                    max_hist AS (
                        SELECT t_sumid,
                              Max(t_changedate) AS t_changedate
                        FROM   v_scwrthist
                        WHERE  t_changedate <= d -- дата изменения лота входит в выгружаемый период
                        GROUP BY t_sumid
                    ),
                    max_inst AS (
                        SELECT t_sumid,
                              t_changedate,
                              Max(t_instance) AS t_instance
                        FROM   v_scwrthistex
                        GROUP BY t_sumid, t_changedate
                    ),
                    scwrthistex as 
                       (select /*+ materialize */ * from v_scwrthistex t 
                        where t.T_AMOUNT > 0 and (t.t_contract,t.t_state) 
                           in (select c.value ,s.value
                                       from (select v.value from qb_dwh_const4exp c inner join qb_dwh_const4exp_val v on (c.id = v.id) where c.name = 'CONTRACTS_DU') c
                                       join (select v.value from qb_dwh_const4exp c inner join qb_dwh_const4exp_val v on (c.id = v.id) where c.name = 'LOT_STATE') s
                                         on (1 = 1)))
                    select distinct
                          t_sumid,
                          t_contract,
                          t_party,
                          t_changedate,
                          t_date,
                          t_fiid,
                          trim(to_char(t_nkdamount, '999999999999999999999999990D99', 'nls_numeric_characters=''. ''')) t_nkdamount,
                          trim(to_char(t_amount, '999999999999999999999999990D99', 'nls_numeric_characters=''. ''')) t_amount,
                          trim(to_char(t_cost, '999999999999999999999999990D99', 'nls_numeric_characters=''. ''')) t_cost,
                          t_sum,
                          t_dealid,
                          t_dealdate,
                          trim(to_char(deal_price, '999999999999999999999999990D99', 'nls_numeric_characters=''. ''')) deal_price,
                          trim(to_char(nvl(RSI_RSB_FIINSTR.ConvSum(deal_price, deal_cfi, 0, t_dealdate, 1), deal_price), '999999999999999999999999990D99', 'nls_numeric_characters=''. ''')) deal_price_rub
                     from (
                           select t.t_sumid,
                                  t.T_CONTRACT,
                                  t.t_changedate,
                                  t.t_instance,
                                  t.t_amount,
                                  t.t_party,
                                  t.T_DATE,
                                  t.T_FIID,
                                  t.T_SUM,
                                  t.T_NKDAMOUNT,
                                  t.T_DEALID,
                                  t.T_DEALCODE,
                                  t.T_DEALDATE,
                                  leg.t_price deal_price,
                                  leg.t_cfi deal_cfi,
                                  leg.t_cost,
                                  (select fi.t_facevaluefi
                                    from dfininstr_dbt fi
                                   where fi.t_fiid = t.T_FIID) facevalfi
                             from scwrthistex t -- v_scwrthistex t 
                             join max_hist mh
                               on mh.t_sumid = t.t_sumid and mh.t_changedate = t.t_changedate
                             join max_inst mi
                               on mi.t_sumid = t.t_sumid and mi.t_changedate = t.t_changedate and mi.t_instance = t.t_instance
                             left join ddl_leg_dbt leg
                               on (t.T_DEALID = leg.t_dealid and leg.t_legkind = 0 and leg.t_legid = 0)
                            where not exists (select 1
                                                From dpmwrtsum_dbt rtsum
                                                join ddl_tick_dbt tick
                                                  on tick.t_dealid = rtsum.t_dealid
                                               where tick.t_dealtype in (2122, 12122, 12123, 12132)
                                                 and rtsum.t_sumid = t.t_sumid) -- KS 05.04.2022 Isup 540285 Откинуть ОРЕПО
                      )

              )
      loop
        -- заполняем список ценных бумаг для выгрузки
        listCB(rec.t_fiid) := null; --
        --dbms_output.put_line(rec.t_fiid);
        -- заполняем список клиентов для выгрузки
        listCL(rec.t_party) := null;

        begin
          select atr.t_name
            into vrate_sp
            from dobjatcor_dbt ov
            inner join dfininstr_dbt fi
               on (to_number(ov.t_object) = fi.t_fiid)
            inner join dobjattr_dbt atr
              on (ov.t_objecttype = atr.t_objecttype and ov.t_groupid = atr.t_groupid and ov.t_attrid = atr.t_attrid)
           where ov.t_objecttype = 12 and ov.t_groupid = 53
             and ov.t_object = lpad(to_char(rec.t_fiid), 10, '0')
             and rec.t_dealdate between ov.t_validfromdate and ov.t_validtodate
             and atr.t_parentid = n110;
        exception
          when no_data_found then
            vrate_sp := null;
        end;
        begin
          select atr.t_name
            into vrate_moody
            from dobjatcor_dbt ov
            inner join dfininstr_dbt fi
               on (to_number(ov.t_object) = fi.t_fiid)
            inner join dobjattr_dbt atr
              on (ov.t_objecttype = atr.t_objecttype and ov.t_groupid = atr.t_groupid and ov.t_attrid = atr.t_attrid)
           where ov.t_objecttype = 12 and ov.t_groupid = 53
             and ov.t_object = lpad(to_char(rec.t_fiid), 10, '0')
             and rec.t_dealdate between ov.t_validfromdate and ov.t_validtodate
             and atr.t_parentid = n210;
        exception
          when no_data_found then
            vrate_moody := null;
        end;
        begin
          select atr.t_name
            into vrate_fitch
            from dobjatcor_dbt ov
            inner join dfininstr_dbt fi
               on (to_number(ov.t_object) = fi.t_fiid)
            inner join dobjattr_dbt atr
              on (ov.t_objecttype = atr.t_objecttype and ov.t_groupid = atr.t_groupid and ov.t_attrid = atr.t_attrid)
           where ov.t_objecttype = 12 and ov.t_groupid = 53
             and ov.t_object = lpad(to_char(rec.t_fiid), 10, '0')
             and rec.t_dealdate between ov.t_validfromdate and ov.t_validtodate
             and atr.t_parentid = n310;
        exception
          when no_data_found then
            vrate_fitch := null;
        end;
        begin
          select atr.t_name
            into vrate_akra
            from dobjatcor_dbt ov
            inner join dfininstr_dbt fi
               on (to_number(ov.t_object) = fi.t_fiid)
            inner join dobjattr_dbt atr
              on (ov.t_objecttype = atr.t_objecttype and ov.t_groupid = atr.t_groupid and ov.t_attrid = atr.t_attrid)
           where ov.t_objecttype = 12 and ov.t_groupid = 53
             and ov.t_object = lpad(to_char(rec.t_fiid), 10, '0')
             and rec.t_dealdate between ov.t_validfromdate and ov.t_validtodate
             and atr.t_parentid = n436;
        exception
          when no_data_found then
            vrate_akra := null;
        end;

        begin
          select t_dateredemption
            into vdate_offer
            from (select t.t_fiid,
                         t.t_dateredemption,
                         row_number() over(order by t.t_dateredemption) rnk
                    from doffers_dbt t
                   where (t.t_fiid = rec.t_fiid)
                     and t.t_dateredemption > in_date)
           where rnk = 1;
        exception
          when no_data_found then
            vdate_offer := null;
        end;

        begin

        select trim(to_char(nvl(finstr_rate, 0), '999999999999999999999999990D9999', 'nls_numeric_characters=''. '''))
          into br_cost
          from (
           select r.*, row_number() over (order by t_sincedate desc) rnk
              from (
                select qb_dwh_utils.NumberToChar(rh.t_rate/power(10, rh.t_point), rh.t_point) FINSTR_RATE,
                       rh.t_sincedate
                  from dfininstr_dbt fi
                 inner join dratedef_dbt rd
                    on (fi.t_fiid = rd.t_otherfi)
                 inner join dfininstr_dbt fir
                    on (fir.t_fiid = rd.t_fiid)
                 inner join dratehist_dbt rh
                   on (rd.t_rateid = rh.t_rateid)
                 where fi.t_fiid = rec.t_fiid
                   and rd.t_type = v_CourceType
               union all
                select qb_dwh_utils.NumberToChar(rd.t_rate/power(10, rd.t_point), rd.t_point) FINSTR_RATE,
                       rd.t_sincedate
                  from dfininstr_dbt fi
                 inner join dratedef_dbt rd
                    on (fi.t_fiid = rd.t_otherfi)
                 inner join dfininstr_dbt fir
                    on (fir.t_fiid = rd.t_fiid)
                 where fi.t_fiid = rec.t_fiid
                   and rd.t_type = v_CourceType
               ) r where r.t_sincedate <= d
               ) where rnk = 1;
        exception
          when no_data_found then
            br_cost := '0.0000';
        end;

        begin
          with t1 as
           (select nh.t_fiid fiid,
                   decode(nh.t_begdate, emptdate, firstdate, nh.t_begdate) chdate,
                   nh.t_facevalue nominal
              from dv_fi_facevalue_hist nh
             where nh.t_fiid = rec.t_fiid
            union
            select wr.t_fiid,
                   decode(wr.t_drawingdate,
                          emptdate,
                          firstdate,
                          wr.t_drawingdate),
                   rsb_fiinstr.fi_getnominalondate(wr.t_fiid,
                                                   wr.t_drawingdate)
              from dfiwarnts_dbt wr
             where t_ispartial = chr(88)
               and wr.t_fiid = rec.t_fiid),
          t2 as
           (select t1.*,
                   row_number() over(order by chdate desc) rnk
              from t1
             where chdate <= d)
          select nominal
            into nominal_on_date
            from t2
           where rnk = 1;

        exception
          when no_data_found then
            nominal_on_date := null;
        end;

        begin
          select trim(to_char(t_incomerate, '999999999999999999999999990D99', 'nls_numeric_characters=''. '''))
            into vrate
            from (select fiw.t_incomerate,
                         fiw.t_drawingdate,
                         row_number() over(order by fiw.t_drawingdate) rnk
                    from dfiwarnts_dbt fiw
                   where fiw.t_fiid = rec.t_fiid
                     and fiw.t_ispartial = chr(0)
                     and fiw.t_drawingdate <= d)
           where rnk = 1;
        exception
          when no_data_found then
            vrate := null;
        end;
        
        begin
          select t_drawingdate
            into vdrawdate
            from (select fiw.t_drawingdate,
                         row_number() over(order by fiw.t_drawingdate desc) rnk
                    from dfiwarnts_dbt fiw
                   where fiw.t_fiid = rec.t_fiid
                     and fiw.t_ispartial = chr(0)
                     and fiw.t_drawingdate <= d)
           where rnk = 1;
        exception
          when no_data_found then
            vdrawdate := firstDate;
        end;
        if (rec.t_dealdate <= vdrawdate) then
          vnkd := trim(to_char(0, '999999999999999999999999990D99', 'nls_numeric_characters=''. '''));
        else
          vnkd := rec.t_nkdamount;
        end if;
        begin
          select substr(regexp_substr(sfc.t_number, '^\d*'), 1, 10)
            into contract_numb
            from ddlcontr_dbt dlc
           inner join dsfcontr_dbt sfc
              on (dlc.t_sfcontrid = sfc.t_id)
           inner join ddlcontrmp_dbt lnk
              on (dlc.t_dlcontrid = lnk.t_dlcontrid)
           inner join dsfcontr_dbt sfc_sub
              on (lnk.t_sfcontrid = sfc_sub.t_id)
           where sfc_sub.t_id = rec.t_contract;
        exception
          when no_data_found then
            contract_numb := null;
        end;

        insert into ldr_infa_du.fct_mgmt_position(lotnum, contract_number, dt, dt_acquire, amount, unkd, deal_code, deal_amount, deal_price, broker_price, rate_moodys, rate_fitch, rate_sp, rate_akra, dt_offer, nominal_perc, rate, sysmoment, security_code, rec_status, ext_file)
               values(to_char(rec.t_sumid), contract_numb, qb_dwh_utils.DateToChar(d), qb_dwh_utils.DateToChar(rec.t_dealdate), rec.t_amount, vnkd, rec.t_dealid || '#TCK', rec.t_cost, rec.deal_price, qb_dwh_utils.NumberToChar(br_cost), vrate_moody, vrate_fitch, vrate_sp, vrate_akra, qb_dwh_utils.DateToChar(vdate_offer), qb_dwh_utils.NumberToChar(nominal_on_date), vrate, dwhSysMoment, rec.t_fiid || '#FIN', dwhRecStatus, dwhEXT_FILE);
        commit;               
      end loop;
      if (m = 12) then
        m := 1;
        y := y + 1;
      else
        m := m + 1;
      end if;
    end loop;

    --dbms_output.put_line('Количество ценных бумаг ' || listCB.count);
    cnt := listCB.first;
    while cnt is not null loop
        exportCB(cnt, in_date);
        --dbms_output.put_line(cnt);
        cnt := listCB.next(cnt);
    end loop;

    --dbms_output.put_line('Количество клиентов ' || listCL.count);
    /*cnt := listCL.first;
    while cnt is not null loop
        exportSUBJ(cnt);
        dbms_output.put_line(cnt);
        cnt := listCL.next(cnt);
    end loop;

    --dbms_output.put_line('Количество эмитентов ' || listIS.count);
    cnt := listIS.first;
    while cnt is not null loop
        exportSUBJ(cnt);
        dbms_output.put_line(cnt);
        cnt := listIS.next(cnt);
    end loop;
    */

    qb_bp_utils.EndEvent(EventID, null);
    qb_bp_utils.SetError(EventID,
                         '',
                         to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Выгрузка данных по ДУ завершена',
                         2,
                         null,
                         null);
    select count(*)
      into cnt
      from dqb_bp_event_error_dbt t
     where t.t_idevent = EventID
       and t.t_is_critical = 0;
    if (cnt = 0) then
      for i in (select d.t_code from ddp_dep_dbt d where d.t_code = 1) loop
        vDateLastOD := qb_dwh_utils.GetLastClosedOD(i.t_code);
        vDateBeg   := sysdate;
        add_export_log( nvl(EventID,0),
                        -1,
                        qb_dwh_utils.GetCODE_DEPARTMENT(i.t_code),
                        vDateLastOD,
                        vDateBeg,
                        sysdate);
      end loop;
    end if;
  exception
     when others then
       qb_bp_utils.SetError(EventID,
                            SQLCODE,
                            to_char(systimestamp, 'yyyy.mm.dd hh24:mi:ss.ff ') || 'Ошибка при заполнении данных по ДУ: ' || SQLERRM,
                            0,
                            null,
                            null);
  end;

  ------------------------------------------------------
  -- Выгрузка данных по ПФИ
  ------------------------------------------------------
  procedure RunExport(in_Date date, procid number) is
    vLdrClear varchar2(400);
  begin
    vLdrClear := nvl(RSB_Common.GetRegStrValue('РСХБ\ИНТЕГРАЦИЯ\ЦХД\TRUNCATE_LDRINFA'),'YES');
      if (vLdrClear = 'YES') then
        clearDUData();
      end if;
      export_DU(1, in_Date, procid);
  end;


end QB_DWH_EXPORT_DU;
/

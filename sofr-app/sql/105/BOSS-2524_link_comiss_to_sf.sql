begin
  execute immediate 'drop type t_tp_commiss_link_list';
exception
  when others then
    null;
end;
/

begin
  execute immediate 'drop type t_tp_commiss_link';
exception
  when others then
    null;
end;
/

create or replace type t_tp_commiss_link as object (tp_name varchar2(4000), commiss_name varchar2(4000));
/
create or replace type t_tp_commiss_link_list as table of t_tp_commiss_link;
/


declare  
  type t_string_list is table of varchar2(5);
  
  l_target_curr_list t_string_list := t_string_list('KZT', 'BYN', 'AED', 'HKD', 'TRY');
  --l_target_curr_list t_string_list := t_string_list('AED', 'HKD', 'TRY');
  
  g_source_curr_id number(3) := 7;
  g_begin_date     date      := to_date('01.01.2024', 'dd.mm.yyyy');
  
  l_curr_id        number(5);

  l_tp_commiss_link t_tp_commiss_link_list := t_tp_commiss_link_list(t_tp_commiss_link(tp_name => 'Базовый', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Профессиональный', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Профессиональный', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Профессиональный', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Профессиональный', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Трейдер', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Трейдер', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Трейдер', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Трейдер', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для финансовых институтов)', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для финансовых институтов)', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для финансовых институтов)', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для финансовых институтов)', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Инвестор', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Инвестор', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Инвестор', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Инвестор', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для малого, среднего и микробизнеса)', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для малого, среднего и микробизнеса)', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для малого, среднего и микробизнеса)', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для малого, среднего и микробизнеса)', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для крупного бизнеса)', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для крупного бизнеса)', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для крупного бизнеса)', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Базовый (для крупного бизнеса)', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Профессиональный (для финансовых институтов)', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Профессиональный (для финансовых институтов)', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Профессиональный (для финансовых институтов)', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Профессиональный (для финансовых институтов)', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Малый и средний бизнес', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Малый и средний бизнес', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Малый и средний бизнес', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Малый и средний бизнес', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Крупный бизнес', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Крупный бизнес', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Крупный бизнес', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Крупный бизнес', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Финансовые институты', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Финансовые институты', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Финансовые институты', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Финансовые институты', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'Трейдер - СВО (без фиксированной комиссии)', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Трейдер - СВО (без фиксированной комиссии)', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'Трейдер - СВО (без фиксированной комиссии)', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'Трейдер - СВО (без фиксированной комиссии)', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП'),
                                                                     
                                                                     t_tp_commiss_link(tp_name => 'ИТПФатеев', commiss_name => 'МСКБ_ВалБиржа_Вывод_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'ИТПФатеев', commiss_name => 'МСКБ_ВалБиржа_День'),
                                                                     t_tp_commiss_link(tp_name => 'ИТПФатеев', commiss_name => 'МСКБ_ВалБиржа_Зачисление_ДС'),
                                                                     t_tp_commiss_link(tp_name => 'ИТПФатеев', commiss_name => 'МСКБ_ВалБиржа_СпецСВОП')
                                                                    );   

  procedure log(p_text varchar2) is
  begin
--    dbms_output.put_line(p_text);
    it_log.log(p_msg      => 'limk_comiss_to_sf. ' || p_text,
               p_msg_type => it_log.C_MSG_TYPE__DEBUG);
  end log;

  function get_curr_id(p_curr_name dfininstr_dbt.t_ccy%type)
    return dfininstr_dbt.t_fiid%type is
    l_fiid dfininstr_dbt.t_fiid%type;
  begin
    select f.t_fiid
      into l_fiid
      from dfininstr_dbt f
     where f.t_ccy = p_curr_name;
    
    return l_fiid;
  end get_curr_id;
  
  function get_commiss (p_commis_id dsfcomiss_dbt.t_comissid%type)
    return dsfcomiss_dbt%rowtype is
    l_commiss_row dsfcomiss_dbt%rowtype;
  begin
    select *
      into l_commiss_row
      from dsfcomiss_dbt c
     where c.t_comissid = p_commis_id;
    
    return l_commiss_row;
  end get_commiss;
  
  function save_link_comiss_w_tp (
    p_tp_id           dsfconcom_dbt.t_objectid%type,
    p_feetype         dsfconcom_dbt.t_feetype%type,
    p_commnumber      dsfconcom_dbt.t_commnumber%type,
    p_calcperiodtype  dsfconcom_dbt.t_calcperiodtype%type,
    p_calcperiodnum   dsfconcom_dbt.t_calcperiodnum%type
  ) return dsfconcom_dbt.t_id%type is
    l_id dsfconcom_dbt.t_id%type;
  begin
    insert into dsfconcom_dbt(t_objectid,
                              t_feetype,
                              t_commnumber,
                              t_status,
                              t_calcperiodtype,
                              t_calcperiodnum,
                              t_date,
                              t_getsummin,
                              t_summin,
                              t_summax,
                              t_datebegin,
                              t_dateend,
                              t_objecttype,
                              t_id,
                              t_sfplanid,
                              t_isfreeperiod,
                              t_isindividual)
    values (p_tp_id,
            p_feetype,
            p_commnumber,
            case when p_calcperiodtype = 0 then 1 else 0 end,
            p_calcperiodtype,
            p_calcperiodnum,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            chr(0),
            0,
            0,
            g_begin_date,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            57,
            0,
            0,
            chr(0),
            chr(0))
    returning t_id into l_id;
    
    return l_id;
  end save_link_comiss_w_tp;
  
  function save_tarscl (
    p_feetype         dsftarscl_dbt.t_feetype%type,
    p_commnumber      dsftarscl_dbt.t_commnumber%type,
    p_link_id         dsftarscl_dbt.t_concomid%type
  ) return dsftarscl_dbt.t_id%type is
    l_id dsftarscl_dbt.t_id%type;
  begin
    insert into dsftarscl_dbt (t_feetype,
                               t_commnumber,
                               t_algkind,
                               t_algnumber,
                               t_begindate,
                               t_isblocked,
                               t_id,
                               t_enddate,
                               t_concomid)
    values (p_feetype,
            p_commnumber,
            8,
            1,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            chr(0),
            0,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            p_link_id)
    returning t_id into l_id;
    
    return l_id;
  end save_tarscl;
  
  procedure link_commiss_to_contracts (
    p_tp_id           dsfconcom_dbt.t_objectid%type,
    p_feetype         dsfconcom_dbt.t_feetype%type,
    p_commnumber      dsfconcom_dbt.t_commnumber%type
  ) is
  begin
    insert into dsfconcom_dbt (t_objectid,
                               t_objecttype,
                               t_feetype,
                               t_commnumber,
                               t_sfplanid,
                               t_status,
                               t_datebegin,
                               t_calcperiodtype,
                               t_calcperiodnum,
                               t_date,
                               t_isfreeperiod,
                               t_getsummin,
                               t_summin,
                               t_summax,
                               t_dateend,
                               t_isindividual,
                               t_isbankexpenses,
                               t_iscompensationcom)
    select p.t_sfcontrid,
           659,
           tp_link.t_feetype,
           tp_link.t_commnumber,
           p.t_sfplanid,
           tp_link.t_status,
           greatest(p.t_begin, tp_link.t_datebegin),
           tp_link.t_calcperiodtype,
           tp_link.t_calcperiodnum,
           tp_link.t_date,
           tp_link.t_isfreeperiod,
           tp_link.t_getsummin,
           tp_link.t_summin,
           tp_link.t_summax,
           tp_link.t_dateend,
           chr(0),
           tp_link.t_isbankexpenses,
           tp_link.t_iscompensationcom
      from dsfconcom_dbt tp_link
      join dsfcontrplan_dbt p on p.t_sfplanid = tp_link.t_objectid
     where tp_link.t_objectid = p_tp_id
       and tp_link.t_objecttype = 57
       and tp_link.t_feetype = p_feetype
       and tp_link.t_commnumber = p_commnumber
       and p.t_end = to_date('01.01.0001', 'dd.mm.yyyy')
       /*
       and not exists (select 1
                         from dsfconcom_dbt sf_link
                        where sf_link.t_feetype = tp_link.t_feetype
                          and sf_link.t_commnumber = tp_link.t_commnumber
                          and sf_link.t_objecttype = 659
                          and sf_link.t_sfplanid = p.t_sfplanid
                          and sf_link.t_objectid = p.t_sfcontrid)
                          */;
  end link_commiss_to_contracts;
  
  procedure link_tarscl_w_concom (
    p_tarscl_id     dsfcomtarscl_dbt.t_tarsclid%type,
    p_feetype       dsfconcom_dbt.t_feetype%type,
    p_commnumber    dsfconcom_dbt.t_commnumber%type,
    p_tp_id         dsfconcom_dbt.t_sfplanid%type
  ) is
  begin
    insert into dsfcomtarscl_dbt (t_concomid,
                                  t_tarsclid,
                                  t_level)
      select concom.t_id,
             p_tarscl_id,
             3
        from dsfconcom_dbt concom
       where concom.t_objecttype = 659
         and concom.t_feetype = p_feetype
         and concom.t_commnumber = p_commnumber
         and concom.t_sfplanid = p_tp_id
         /*
         and not exists (select 1
                          from dsfcomtarscl_dbt comtar
                         where comtar.t_concomid = concom.t_id
                           and comtar.t_tarsclid = p_tarscl_id
                           and comtar.t_level = 3)*/;
  end link_tarscl_w_concom;
  
  procedure link_commiss_to_tp (
    p_commiss_id dsfcomiss_dbt.t_comissid%type,
    p_tp_id      dsfplan_dbt.t_sfplanid%type
  ) is
    l_link_id     dsfconcom_dbt.t_id%type;
    l_commiss_row dsfcomiss_dbt%rowtype;
    l_tarscl_id   dsftarscl_dbt.t_id%type;
  begin
    l_commiss_row := get_commiss(p_commis_id => p_commiss_id);

    l_link_id := save_link_comiss_w_tp(p_tp_id          => p_tp_id,
                                       p_feetype        => l_commiss_row.t_feetype,
                                       p_commnumber     => l_commiss_row.t_number,
                                       p_calcperiodtype => l_commiss_row.t_calcperiodtype,
                                       p_calcperiodnum  => l_commiss_row.t_calcperiodnum);

    l_tarscl_id := save_tarscl(p_feetype    => l_commiss_row.t_feetype,
                               p_commnumber => l_commiss_row.t_number,
                               p_link_id    => l_link_id);

    link_commiss_to_contracts(p_tp_id      => p_tp_id,
                              p_feetype    => l_commiss_row.t_feetype,
                              p_commnumber => l_commiss_row.t_number);

    link_tarscl_w_concom(p_tarscl_id  => l_tarscl_id,
                         p_feetype    => l_commiss_row.t_feetype,
                         p_commnumber => l_commiss_row.t_number,
                         p_tp_id      => p_tp_id);
  end link_commiss_to_tp;
begin
  for i in 1..l_target_curr_list.count() loop
    l_curr_id := get_curr_id(p_curr_name => l_target_curr_list(i));
    for rec in (select p.t_sfplanid,
                       cc.t_comissid,
                       t.tp_name,
                       t.commiss_name
                  from table(l_tp_commiss_link) t
                  left join dsfplan_dbt p on p.t_name = t.tp_name
                  left join (     dsfcomiss_dbt cp
                             join dsfcomiss_dbt cc on cc.t_parentcomissid = cp.t_comissid
                            ) on cp.t_code = t.commiss_name and
                                 cc.t_fiid_comm = l_curr_id
               )
    loop
      if rec.t_sfplanid is null then
        log('Не найден тарифный план ' || rec.tp_name);
        continue;
      end if;

      if rec.t_comissid is null then
        log('Не найдена комиссия ' || rec.commiss_name);
        continue;
      end if;

      link_commiss_to_tp(p_commiss_id => rec.t_comissid, p_tp_id => rec.t_sfplanid);
      log('Обработано. Валюта ' || l_target_curr_list(i) || '; тарифный план ' || rec.tp_name || '; комиссия ' || rec.commiss_name);
      commit;
    end loop;
  end loop;

exception
  when others then
    log('Error: ' || sqlerrm);
    raise;
end;
/

begin
  execute immediate 'drop type t_tp_commiss_link_list';
exception
  when others then
    null;
end;
/

begin
  execute immediate 'drop type t_tp_commiss_link';
exception
  when others then
    null;
end;
/
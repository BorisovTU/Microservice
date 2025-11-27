declare
  g_date_begin_commiss date := to_date('01.01.2025', 'dd.mm.yyyy');
  g_commiss_id         dsfcomiss_dbt.t_comissid%type;
  g_log_object         varchar2(100) := 'BOSS-2303_CREATE_COMMISS';

  function insert_sfcomiss (
    p_feetype           dsfcomiss_dbt.t_feetype%type,
    p_number            dsfcomiss_dbt.t_number%type,
    p_code              dsfcomiss_dbt.t_code%type,
    p_name              dsfcomiss_dbt.t_name%type,
    p_calcperiodtype    dsfcomiss_dbt.t_calcperiodtype%type default 0,
    p_calcperiodnum     dsfcomiss_dbt.t_calcperiodnum%type default 0,
    p_date              dsfcomiss_dbt.t_date%type default to_date('01.01.0001', 'dd.mm.yyyy'),
    p_paynds            dsfcomiss_dbt.t_paynds%type,
    p_fiid_comm         dsfcomiss_dbt.t_fiid_comm%type,
    p_getsummin         dsfcomiss_dbt.t_getsummin%type default chr(1),
    p_summin            dsfcomiss_dbt.t_summin%type default 0,
    p_summax            dsfcomiss_dbt.t_summax%type default 0,
    p_ratetype          dsfcomiss_dbt.t_ratetype%type default 0,
    p_receiverid        dsfcomiss_dbt.t_receiverid%type,
    p_incfeetype        dsfcomiss_dbt.t_incfeetype%type default 0,
    p_inccommnumber     dsfcomiss_dbt.t_inccommnumber%type default 0,
    p_formalg           dsfcomiss_dbt.t_formalg%type,
    p_servicekind       dsfcomiss_dbt.t_servicekind%type,
    p_servicesubkind    dsfcomiss_dbt.t_servicesubkind%type,
    p_calccomisssumalg  dsfcomiss_dbt.t_calccomisssumalg%type,
    p_setaccsearchalg   dsfcomiss_dbt.t_setaccsearchalg%type,
    p_fiid_paysum       dsfcomiss_dbt.t_fiid_paysum%type default -1,
    p_datebegin         dsfcomiss_dbt.t_datebegin%type default to_date('01.01.0001', 'dd.mm.yyyy'),
    p_dateend           dsfcomiss_dbt.t_dateend%type default to_date('01.01.0001', 'dd.mm.yyyy'),
    p_instantpayment    dsfcomiss_dbt.t_instantpayment%type,
    p_productid         dsfcomiss_dbt.t_productid%type default 0,
    p_ndscateg          dsfcomiss_dbt.t_ndscateg%type default 0,
    p_isfreeperiod      dsfcomiss_dbt.t_isfreeperiod%type default chr(0),
    p_comment           dsfcomiss_dbt.t_comment%type default chr(0),
    p_parentcomissid    dsfcomiss_dbt.t_parentcomissid%type default 0,
    p_isbankexpenses    dsfcomiss_dbt.t_isbankexpenses%type default chr(0),
    p_iscompensationcom dsfcomiss_dbt.t_iscompensationcom%type default chr(0)
  ) return dsfcomiss_dbt.t_comissid%type is
    l_id dsfcomiss_dbt.t_comissid%type;
  begin
    insert into dsfcomiss_dbt (t_feetype,
                               t_number,
                               t_code,
                               t_name,
                               t_calcperiodtype,
                               t_calcperiodnum,
                               t_date,
                               t_paynds,
                               t_fiid_comm,
                               t_getsummin,
                               t_summin,
                               t_summax,
                               t_ratetype,
                               t_receiverid,
                               t_incfeetype,
                               t_inccommnumber,
                               t_formalg,
                               t_servicekind,
                               t_servicesubkind,
                               t_calccomisssumalg,
                               t_setaccsearchalg,
                               t_fiid_paysum,
                               t_datebegin,
                               t_dateend,
                               t_instantpayment,
                               t_productid,
                               t_ndscateg,
                               t_isfreeperiod,
                               t_comment,
                               t_parentcomissid,
                               t_isbankexpenses,
                               t_iscompensationcom)
    values (p_feetype,
            p_number,
            p_code,
            p_name,
            p_calcperiodtype,
            p_calcperiodnum,
            p_date,
            p_paynds,
            p_fiid_comm,
            p_getsummin,
            p_summin,
            p_summax,
            p_ratetype,
            p_receiverid,
            p_incfeetype,
            p_inccommnumber,
            p_formalg,
            p_servicekind,
            p_servicesubkind,
            p_calccomisssumalg,
            p_setaccsearchalg,
            p_fiid_paysum,
            p_datebegin,
            p_dateend,
            p_instantpayment,
            p_productid,
            p_ndscateg,
            p_isfreeperiod,
            p_comment,
            p_parentcomissid,
            p_isbankexpenses,
            p_iscompensationcom
           )
    returning t_comissid into l_id;
    
    return l_id;
  end insert_sfcomiss;
  
  function get_new_commiss_number return dsfcomiss_dbt.t_number%type is
    l_number dsfcomiss_dbt.t_number%type;
  begin
    select max(s.t_number) + 1
      into l_number
      from dsfcomiss_dbt s;

    return l_number;
  end get_new_commiss_number;
  
  /*
    Некий "шаблон" для соединения комиссии и тарифного плана.
    Для чего он нужен, не очень понимаю, но потом на его основе будут создаваться отдельные связи комиссия+тарифный план отдельно для каждого договора.
    Эти связи будут в этой же таблице dsfconcom_dbt
  */
  function save_commiss_tp_template (
    p_tp_id           dsfconcom_dbt.t_objectid%type,
    p_feetype         dsfconcom_dbt.t_feetype%type,
    p_commnumber      dsfconcom_dbt.t_commnumber%type,
    p_calcperiodtype  dsfconcom_dbt.t_calcperiodtype%type,
    p_calcperiodnum   dsfconcom_dbt.t_calcperiodnum%type,
    p_datebegin       dsfconcom_dbt.t_datebegin%type,
    p_isindividual    dsfconcom_dbt.t_isindividual%type
  ) return dsfconcom_dbt.t_id%type is
    l_id dsfconcom_dbt.t_id%type := 0;
    rec_count number := 0;
  begin
    select count(1) into rec_count from dsfconcom_dbt where t_objectid = p_tp_id and t_feetype = p_feetype and t_commnumber = p_commnumber and t_objecttype = 57;

    if rec_count = 0 then
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
              0,
              p_calcperiodtype,
              p_calcperiodnum,
              to_date('01.01.0001', 'dd.mm.yyyy'),
              chr(0),
              0,
              0,
              p_datebegin,
              to_date('01.01.0001', 'dd.mm.yyyy'),
              57,
              0,
              0,
              chr(0),
              p_isindividual
             )
      returning t_id into l_id;
    end if;
    
    return l_id;
  end save_commiss_tp_template;

  procedure link_commiss_to_contract (
    p_tp_id           dsfconcom_dbt.t_objectid%type,
    p_feetype         dsfconcom_dbt.t_feetype%type,
    p_commnumber      dsfconcom_dbt.t_commnumber%type,
    p_sfcontrid       dsfconcom_dbt.t_objectid%type,
    p_dbegin          dsfconcom_dbt.t_datebegin%type
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
    values(p_sfcontrid,
           659,
           p_feetype,
           p_commnumber,
           p_tp_id,
           0,
           p_dbegin,
           0,
           0,
           to_date('01.01.0001', 'dd.mm.yyyy'),
           chr(0),
           chr(0),
           0,
           0,
           to_date('01.01.0001', 'dd.mm.yyyy'),
           chr(0),
           chr(0),
           chr(0)
          );
  end link_commiss_to_contract;

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
       and p.t_end = to_date('01.01.0001', 'dd.mm.yyyy');
  end link_commiss_to_contracts;

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

  function create_one_time_commiss (
    p_code            dsfcomiss_dbt.t_code%type,
    p_name            dsfcomiss_dbt.t_name%type,
    p_instant_payment number,
    p_fiid            dsfcomiss_dbt.t_fiid_comm%type,
    p_paynds          dsfcomiss_dbt.t_paynds%type,
    p_receiverid      dsfcomiss_dbt.t_receiverid%type,
    p_formalg         dsfcomiss_dbt.t_formalg%type,
    p_calcsumalg      dsfcomiss_dbt.t_calccomisssumalg%type,
    p_spi_alg         dsfcomiss_dbt.t_setaccsearchalg%type,
    p_servicekind     dsfcomiss_dbt.t_servicekind%type,
    p_servicesubkind  dsfcomiss_dbt.t_servicesubkind%type
  ) return dsfcomiss_dbt.t_comissid%type is
    l_commiss_id   dsfcomiss_dbt.t_comissid%type;
  begin
    l_commiss_id := insert_sfcomiss(p_feetype          => 6,
                                    p_number           => get_new_commiss_number(),
                                    p_code             => p_code,
                                    p_name             => p_name,
                                    p_paynds           => p_paynds,
                                    p_fiid_comm        => p_fiid,
                                    p_receiverid       => p_receiverid,
                                    p_formalg          => p_formalg,
                                    p_servicekind      => p_servicekind,
                                    p_servicesubkind   => p_servicesubkind,
                                    p_calccomisssumalg => p_calcsumalg,
                                    p_setaccsearchalg  => p_spi_alg,
                                    p_instantpayment   => case when p_instant_payment = 1 then chr(88) else chr(0) end,
                                    p_comment          => p_name);
    return l_commiss_id;
  end create_one_time_commiss;
  
  procedure link_commiss_to_tp (
    p_commiss_id dsfcomiss_dbt.t_comissid%type,
    p_tp_id      dsfconcom_dbt.t_objectid%type  
  ) is
    l_commiss_row  dsfcomiss_dbt%rowtype;
    l_template_id  dsfconcom_dbt.t_id%type;
  begin
    l_commiss_row := get_commiss(p_commis_id => p_commiss_id);

    l_template_id := save_commiss_tp_template(p_tp_id          => p_tp_id,
                                              p_feetype        => l_commiss_row.t_feetype,
                                              p_commnumber     => l_commiss_row.t_number,
                                              p_calcperiodtype => l_commiss_row.t_calcperiodtype,
                                              p_calcperiodnum  => l_commiss_row.t_calcperiodnum,
                                              p_datebegin      => g_date_begin_commiss,
                                              p_isindividual   => chr(0));

    link_commiss_to_contracts(p_tp_id      => p_tp_id,
                              p_feetype    => l_commiss_row.t_feetype,
                              p_commnumber => l_commiss_row.t_number);
  end link_commiss_to_tp;
  
  function get_comm_number_by_code (
    p_code  dsfcomiss_dbt.t_code%type
  ) return dsfcomiss_dbt.t_number%type is
    l_comm_number dsfcomiss_dbt.t_number%type;
  begin
    select c.t_number
      into l_comm_number
      from dsfcomiss_dbt c
     where c.t_code = p_code;

    return l_comm_number;
  exception
    when others then
      return null;
  end get_comm_number_by_code;
begin
 
  g_commiss_id := create_one_time_commiss(p_code            => 'МскБиржКлирИспПИ',
                                          p_name            => 'Комиссия за клиринг при исполнении фьючерсных контрактов',
                                          p_instant_payment => 0,
                                          p_fiid            => 0, --rub
                                          p_paynds          => 1, --не облагается
                                          p_receiverid      => 2, --ММВБ
                                          p_formalg         => 1, --автоматически
                                          p_calcsumalg      => 1, --на дату расчёта
                                          p_spi_alg         => 2, --не искать
                                          p_servicekind     => 15, --срочные контракты
                                          p_servicesubkind  => 8); --биржевой рынок

  it_log.log_handle(p_object   => g_log_object,
                    p_msg      => 'comiss МскБиржКлирИспПИ created. id = ' || g_commiss_id,
                    p_msg_type => it_log.C_MSG_TYPE__DEBUG);

  link_commiss_to_contract (p_tp_id      => 0,
                            p_feetype    => 6,
                            p_commnumber => get_comm_number_by_code(p_code => 'МскБиржКлирИспПИ'),
                            p_sfcontrid  => 25094,
                            p_dbegin     => g_date_begin_commiss);

  g_commiss_id := create_one_time_commiss(p_code            => 'КлиентКлирИспПИ',
                                          p_name            => 'Комиссия за клиринг при исполнении фьючерсных контрактов',
                                          p_instant_payment => 0,
                                          p_fiid            => 0, --rub
                                          p_paynds          => 1, --не облагается
                                          p_receiverid      => 1, --РСХБ
                                          p_formalg         => 1, --автоматически
                                          p_calcsumalg      => 1, --на дату расчёта
                                          p_spi_alg         => 2, --не искать
                                          p_servicekind     => 15, --срочные контракты
                                          p_servicesubkind  => 8); --биржевой рынок

  it_log.log_handle(p_object   => g_log_object,
                    p_msg      => 'comiss КлиентКлирИспПИ created. id = ' || g_commiss_id,
                    p_msg_type => it_log.C_MSG_TYPE__DEBUG);

  for plans in (select p.t_sfplanid, p.t_name from dsfplan_dbt p)
  loop
    link_commiss_to_tp(p_commiss_id => g_commiss_id, p_tp_id => plans.t_sfplanid);
    it_log.log_handle(p_object   => g_log_object,
                      p_msg      => 'comiss КлиентКлирИспПИ linked to tp ' || plans.t_name || '(' || plans.t_sfplanid || ')',
                      p_msg_type => it_log.C_MSG_TYPE__DEBUG);
  end loop;
end;
/

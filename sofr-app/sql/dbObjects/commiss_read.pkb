create or replace package body commiss_read as

  g_calccommissalg_calc      constant number(1) := 1; --на день расчёта
  g_calccommissalg_pay       constant number(1) := 2;
  g_calccommissalg_beforepay constant number(1) := 3;
  
  g_oper_status_deffered     constant number(1) := 0; --статус комиссии "отложенная"
  
  g_once_time_com_objecttype constant number(3) := 664; --тип объекта "разовая комиссия"
  
  function calccommissalg_calc
  return number deterministic is
  begin
    return g_calccommissalg_calc;
  end calccommissalg_calc;
  
  function calccommissalg_pay
  return number deterministic is
  begin
    return g_calccommissalg_pay;
  end calccommissalg_pay;
  
  function calccommissalg_beforepay
  return number deterministic is
  begin
    return g_calccommissalg_beforepay;
  end calccommissalg_beforepay;
  
  function oper_status_deffered
  return number deterministic is
  begin
    return g_oper_status_deffered;
  end oper_status_deffered;
  
  function once_time_com_objecttype
  return number deterministic is
  begin
    return g_once_time_com_objecttype;
  end once_time_com_objecttype;

  function get_sfcontr_plan_id (
    p_sfcontr_id dsfcontrplan_dbt.t_sfcontrid%type,
    p_date       dsfcontrplan_dbt.t_begin%type
  ) return dsfcontrplan_dbt.t_sfplanid%type is
    l_plan_id dsfcontrplan_dbt.t_sfplanid%type;
  begin
    select p.t_sfplanid
      into l_plan_id
      from dsfcontrplan_dbt p
     where p.t_sfcontrid = p_sfcontr_id
       and p.t_begin <= p_date
       and (p.t_end > p_date or p.t_end = to_date('01.01.0001', 'dd.mm.yyyy'));

    return l_plan_id;
  exception
    when others then
      return null;
  end get_sfcontr_plan_id;

  function get_concom_id (
    p_sfcontr_id dsfconcom_dbt.t_objectid%type,
    p_feetype    dsfconcom_dbt.t_feetype%type,
    p_commnumber dsfconcom_dbt.t_commnumber%type,
    p_sfplan_id  dsfconcom_dbt.t_sfplanid%type,
    p_date       dsfconcom_dbt.t_datebegin%type
  ) return dsfconcom_dbt.t_id%type is
    l_concom_id dsfconcom_dbt.t_id%type;
  begin
    select c.t_id
      into l_concom_id
      from dsfconcom_dbt c
     where c.t_objecttype = sfcontr_read.subcontr_objecttype
       and c.t_objectid = p_sfcontr_id
       and c.t_feetype = p_feetype
       and c.t_commnumber = p_commnumber
       and c.t_sfplanid = p_sfplan_id
       and c.t_datebegin <= p_date
       and (c.t_dateend > p_date or c.t_dateend = to_date('01.01.0001', 'dd.mm.yyyy'));

    return l_concom_id;
  exception
    when others then
      return null;
  end get_concom_id;
  
  function get_receiver_id (
    p_feetype    dsfcomiss_dbt.t_feetype%type,
    p_commnumber dsfcomiss_dbt.t_number%type
  ) return dsfcomiss_dbt.t_receiverid%type is
    l_receiver_id dsfcomiss_dbt.t_receiverid%type;
  begin
    select c.t_receiverid
      into l_receiver_id
      from dsfcomiss_dbt c
     where c.t_feetype = p_feetype
       and c.t_number = p_commnumber;

    return l_receiver_id;
  exception
    when others then
      return null;
  end get_receiver_id;
  
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
  
  function get_service_type_by_contract (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number deterministic is
    l_service_type number(1);
  begin
    if sfcontr_read.is_separated_subcontr(p_sfcontr_id => p_sfcontr_id) = 1 then
      if party_read.is_legal_entity_clear(p_party_id => sfcontr_read.get_client_by_contract(p_sfcontr_id => p_sfcontr_id)) = 1 then
        l_service_type := 3;
      else
        l_service_type := 2;
      end if;
    else
      l_service_type := 1;
    end if;
    
    return l_service_type;
  end get_service_type_by_contract;

end commiss_read;
/

create or replace package commiss_read as
  
  function calccommissalg_calc
  return number deterministic;
  
  function calccommissalg_pay
  return number deterministic;
  
  function calccommissalg_beforepay
  return number deterministic;
  
  function oper_status_deffered
  return number deterministic;
  
  function once_time_com_objecttype
  return number deterministic;

  function get_sfcontr_plan_id (
    p_sfcontr_id dsfcontrplan_dbt.t_sfcontrid%type,
    p_date       dsfcontrplan_dbt.t_begin%type
  ) return dsfcontrplan_dbt.t_sfplanid%type;

  function get_concom_id (
    p_sfcontr_id dsfconcom_dbt.t_objectid%type,
    p_feetype    dsfconcom_dbt.t_feetype%type,
    p_commnumber dsfconcom_dbt.t_commnumber%type,
    p_sfplan_id  dsfconcom_dbt.t_sfplanid%type,
    p_date       dsfconcom_dbt.t_datebegin%type
  ) return dsfconcom_dbt.t_id%type;
  
  function get_receiver_id (
    p_feetype    dsfcomiss_dbt.t_feetype%type,
    p_commnumber dsfcomiss_dbt.t_number%type
  ) return dsfcomiss_dbt.t_receiverid%type;
  
  function get_comm_number_by_code (
    p_code  dsfcomiss_dbt.t_code%type
  ) return dsfcomiss_dbt.t_number%type;
  
  function get_service_type_by_contract (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number deterministic;

end commiss_read;
/

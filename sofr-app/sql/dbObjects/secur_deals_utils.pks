create or replace package secur_deals_utils as

  procedure save_client_request (
    pio_req_id in out ddl_req_dbt.t_id%type,
    p_code            ddl_req_dbt.t_code%type,
    p_codets          ddl_req_dbt.t_codets%type,
    p_date            ddl_req_dbt.t_date%type,
    p_time            ddl_req_dbt.t_time%type,
    p_party           ddl_req_dbt.t_party%type,
    p_fiid            ddl_req_dbt.t_fiid%type,
    p_amount          ddl_req_dbt.t_amount%type,
    p_price           ddl_req_dbt.t_price%type,
    p_sourceid        ddl_req_dbt.t_sourceid%type,
    p_contract        ddl_req_dbt.t_clientcontr%type,
    p_status          ddl_req_dbt.t_status%type,
    p_direction       ddl_req_dbt.t_direction%type
  );
  
  function is_substitution_deal (
    p_dealid ddl_tick_dbt.t_dealid%type
  ) return number;

  procedure math_otc_requests_w_deals (
    po_cnt_all            out number,
    po_cnt_too_many_deals out number,
    po_cnt_not_found_deal out number,
    po_cnt_success        out number
  );
  
  function is_buy_deal_type (
    p_dealtype ddl_tick_dbt.t_dealtype%type
  ) return number deterministic;
  
  function is_buy_deal (
    p_dealid ddl_tick_dbt.t_dealid%type
  ) return number deterministic;
  
  procedure update_deal_supply_schedule (
    p_dealid  ddl_tick_dbt.t_dealid%type,
    p_date    ddl_leg_dbt.t_expiry%type
  );
  
  procedure update_deal_pay_schedule (
    p_dealid  ddl_tick_dbt.t_dealid%type,
    p_date    ddl_leg_dbt.t_expiry%type
  );

end secur_deals_utils;
/

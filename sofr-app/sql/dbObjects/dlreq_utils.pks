create or replace package dlreq_utils as

  function dlreq_objecttype
    return number deterministic;

  procedure save_dlreq (
    pio_id  in out ddl_req_dbt.t_id%type,
    p_kind         ddl_req_dbt.t_kind%type,
    p_code         ddl_req_dbt.t_code%type,
    p_codets       ddl_req_dbt.t_codets%type,
    p_date         ddl_req_dbt.t_date%type,
    p_time         ddl_req_dbt.t_time%type,
    p_party        ddl_req_dbt.t_party%type,
    p_client       ddl_req_dbt.t_client%type,
    p_fiid         ddl_req_dbt.t_fiid%type,
    p_amount       ddl_req_dbt.t_amount%type,
    p_price        ddl_req_dbt.t_price%type,
    p_pricefiid    ddl_req_dbt.t_pricefiid%type,
    p_sourcekind   ddl_req_dbt.t_sourcekind%type,
    p_sourceid     ddl_req_dbt.t_sourceid%type,
    p_contract     ddl_req_dbt.t_clientcontr%type,
    p_status       ddl_req_dbt.t_status%type,
    p_direction    ddl_req_dbt.t_direction%type
  );

  function is_substitution_secur (
    p_id ddl_req_dbt.t_id%type
  ) return number;

end dlreq_utils;
/

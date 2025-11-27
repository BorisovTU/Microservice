create or replace package setacc_read as

  function get_setacc_row_by_id (
    p_setacc_id dsettacc_dbt.t_settaccid%type
  ) return dsettacc_dbt%rowtype;

  function get_setacc_row (
    p_partyid  dsettacc_dbt.t_partyid%type,
    p_fiid     dsettacc_dbt.t_fiid%type,
    p_chapter  dsettacc_dbt.t_chapter%type
  ) return dsettacc_dbt%rowtype;

end setacc_read;
/

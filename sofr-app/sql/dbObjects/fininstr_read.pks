create or replace package fininstr_read as

  function fi_kind_index
    return number deterministic;

  function fi_kind_derivative
    return number deterministic;

  function get_mmvb_code (
    p_fiid dfininstr_dbt.t_fiid%type
  ) return varchar2;

  function get_isin (
    p_fiid davoiriss_dbt.t_fiid%type
  ) return davoiriss_dbt.t_isin%type deterministic;

end fininstr_read;
/

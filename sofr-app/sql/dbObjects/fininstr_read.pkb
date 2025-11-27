create or replace package body fininstr_read as

  function fi_kind_index
    return number deterministic is
  begin
    return rsi_rsb_fiinstr.fikind_index;
  end fi_kind_index;

  function fi_kind_derivative
    return number deterministic is
  begin
    return rsi_rsb_fiinstr.fikind_derivative;
  end fi_kind_derivative;

  function get_mmvb_code (
    p_fiid dfininstr_dbt.t_fiid%type
  ) return varchar2 is
  begin
    return objcode_read.get_code(p_object_type => 9,
                                 p_code_kind   => 11,
                                 p_object_id   => p_fiid);
  end get_mmvb_code;

  function get_isin (
    p_fiid davoiriss_dbt.t_fiid%type
  ) return davoiriss_dbt.t_isin%type deterministic is
    l_isin davoiriss_dbt.t_isin%type;
  begin
    select t_isin
      into l_isin
      from davoiriss_dbt
     where t_fiid = p_fiid;
    
    return l_isin;
  exception
    when others then
      return null;
  end get_isin;

end fininstr_read;
/

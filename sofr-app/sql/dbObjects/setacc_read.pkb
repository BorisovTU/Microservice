create or replace package body setacc_read as

  function get_setacc_row_by_id (
    p_setacc_id dsettacc_dbt.t_settaccid%type
  ) return dsettacc_dbt%rowtype is
    l_setacc_row dsettacc_dbt%rowtype;
  begin
    select *
      into l_setacc_row
      from dsettacc_dbt s
     where s.t_settaccid = p_setacc_id;

    return l_setacc_row;
  exception
    when no_data_found then
      return null;
  end get_setacc_row_by_id;

  function get_setacc_row (
    p_partyid  dsettacc_dbt.t_partyid%type,
    p_fiid     dsettacc_dbt.t_fiid%type,
    p_chapter  dsettacc_dbt.t_chapter%type
  ) return dsettacc_dbt%rowtype is
    l_setacc_row dsettacc_dbt%rowtype;
  begin
    select *
      into l_setacc_row
      from dsettacc_dbt a
     where a.t_partyid = p_partyid
       and a.t_fiid = p_fiid
       and a.t_chapter = p_chapter
     order by a.t_order
     fetch first 1 rows only;

    return l_setacc_row;
  exception
    when no_data_found then
      return null;
  end get_setacc_row;

end setacc_read;
/

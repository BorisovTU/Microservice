create or replace package body objcode_read as

  function get_code_on_date (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type,
    p_date        dobjcode_dbt.t_bankdate%type
  ) return dobjcode_dbt.t_code%type is
    l_code dobjcode_dbt.t_code%type;
  begin
    select c.t_code
      into l_code
      from dobjcode_dbt c
     where c.t_objecttype = p_object_type
       and c.t_codekind = p_code_kind
       and c.t_objectid = p_object_id
       and c.t_bankdate <= p_date
       and (c.t_bankclosedate > p_date or
            c.t_bankclosedate = to_date('01.01.0001', 'dd.mm.yyyy'));

    return l_code;
  exception
    when others then
      return null;
  end get_code_on_date;

  function get_code (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type
  ) return dobjcode_dbt.t_code%type is
    l_code dobjcode_dbt.t_code%type;
  begin
    select c.t_code
      into l_code
      from dobjcode_dbt c
     where c.t_objecttype = p_object_type
       and c.t_codekind = p_code_kind
       and c.t_objectid = p_object_id
       and c.t_state = 0
       and c.t_bankclosedate = to_date('01.01.0001', 'dd.mm.yyyy');

    return l_code;
  exception
    when others then
      return null;
  end get_code;

  function get_code_row (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type
  ) return dobjcode_dbt%rowtype is
    l_objcode dobjcode_dbt%rowtype;
  begin
    select c.*
      into l_objcode
      from dobjcode_dbt c
     where c.t_objecttype = p_object_type
       and c.t_codekind = p_code_kind
       and c.t_objectid = p_object_id
       and c.t_state = 0
       and c.t_bankclosedate = to_date('01.01.0001', 'dd.mm.yyyy');

    return l_objcode;
  exception
    when others then
      return null;
  end get_code_row;

end objcode_read;
/

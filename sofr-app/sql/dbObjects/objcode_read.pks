create or replace package objcode_read as 

  function get_code_on_date (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type,
    p_date        dobjcode_dbt.t_bankdate%type
  ) return dobjcode_dbt.t_code%type;

  function get_code (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type
  ) return dobjcode_dbt.t_code%type;

  function get_code_row (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type
  ) return dobjcode_dbt%rowtype;

end objcode_read;
/

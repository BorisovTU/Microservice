create or replace package objcode_utils as 


  procedure insert_code (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type,
    p_code        dobjcode_dbt.t_code%type,
    p_date        dobjcode_dbt.t_bankdate%type
  );

  procedure save_code (
    p_object_type dobjcode_dbt.t_objecttype%type,
    p_code_kind   dobjcode_dbt.t_codekind%type,
    p_object_id   dobjcode_dbt.t_objectid%type,
    p_code        dobjcode_dbt.t_code%type,
    p_date        dobjcode_dbt.t_bankdate%type
  );

end objcode_utils;
/

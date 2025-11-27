create or replace package funcobj_utils as
  
  function state_ok
    return number deterministic;
  
  function state_repeate_error
    return number deterministic;
  
  function state_repeat_ok
    return number deterministic;

  function state_in_process
    return number deterministic;

  function get_func_id (
    p_code dllvalues_dbt.t_code%type
  ) return dllvalues_dbt.t_element%type;

  function get_priority_from_reserve (p_code dllvalues_dbt.t_code%type)
    return number;
  
  procedure create_func(
    p_code     dfunc_dbt.t_code%type,
    p_name     dfunc_dbt.t_name%type,
    p_file     dfunc_dbt.t_filename%type,
    p_func     dfunc_dbt.t_functionname%type,
    p_priority number default null
  );

  procedure save_func_obj (
    p_code      varchar2,
    p_name      varchar2,
    p_mac_file  varchar2,
    p_func_name varchar2
  );
  
  procedure save_task (
    p_objectid dfuncobj_dbt.t_objectid%type,
    p_funcid   dfuncobj_dbt.t_funcid%type,
    p_param    dfuncobj_dbt.t_param%type,
    p_priority dfuncobj_dbt.t_priority%type default 31
  );
  
  procedure delete_active_task (
    p_objectid dfuncobj_dbt.t_objectid%type,
    p_funcid   dfuncobj_dbt.t_funcid%type
  );
end funcobj_utils;
/

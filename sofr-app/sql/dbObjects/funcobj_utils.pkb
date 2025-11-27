create or replace package body funcobj_utils as

  g_state_ok            constant number(1) := 0;
  g_state_repeate_error constant number(1) := 1;
  g_state_fatal_error   constant number(1) := 2;
  g_state_max_repeat    constant number(1) := 3;
  g_state_repeat_ok     constant number(1) := 4;
  g_state_in_process    constant number(2) := 20;
  
  type t_string_number_map is table of number(10) index by varchar2(100);
  g_priority_cache  t_string_number_map;
  g_func_id_cache   t_string_number_map;
  
  function state_ok
    return number deterministic is
  begin
    return g_state_ok;
  end state_ok;
  
  function state_repeate_error
    return number deterministic is
  begin
    return g_state_repeate_error;
  end state_repeate_error;
  
  function state_repeat_ok
    return number deterministic is
  begin
    return g_state_repeat_ok;
  end state_repeat_ok;
  
  function state_in_process
    return number deterministic is
  begin
    return g_state_in_process;
  end state_in_process;

  function get_func_id (p_code dllvalues_dbt.t_code%type)
    return dllvalues_dbt.t_element%type is
    l_id dllvalues_dbt.t_element%type;
  begin
    if g_func_id_cache.exists(p_code)
    then
      return g_func_id_cache(p_code);
    end if;

    select v.t_element
      into l_id
      from dllvalues_dbt v
     where v.t_list = 5002
       and v.t_code = p_code;

    g_func_id_cache(p_code) := l_id;

    return l_id;
  exception
    when others then
      return null;
  end get_func_id;

  function get_priority_from_reserve (p_code dllvalues_dbt.t_code%type)
    return number is
    l_reserve number(10);
  begin
    if g_priority_cache.exists(p_code)
    then
      return g_priority_cache(p_code);
    end if;
      
    select to_number(v.t_reserve)
      into l_reserve
      from dllvalues_dbt v
     where v.t_list = 5002
       and v.t_code = p_code;
       
    g_priority_cache(p_code) := l_reserve;

    return l_reserve;
  exception
    when others then
      return 31; --default priority
  end get_priority_from_reserve;
  
  procedure create_func(
    p_code     dfunc_dbt.t_code%type,
    p_name     dfunc_dbt.t_name%type,
    p_file     dfunc_dbt.t_filename%type,
    p_func     dfunc_dbt.t_functionname%type,
    p_priority number default null
  ) is
    l_func_id dfunc_dbt.t_funcid%type;
  begin
    select max(t_funcid)+1 into l_func_id from dfunc_dbt;

    insert into dfunc_dbt (t_funcid,
                           t_code,
                           t_name,
                           t_type,
                           t_filename,
                           t_functionname,
                           t_interval,
                           t_version)
    values (l_func_id,
            p_code,
            p_name,
            1,
            p_file,
            p_func,
            0,
            0);

    insert into dllvalues_dbt (t_list,
                               t_element,
                               t_code,
                               t_name,
                               t_flag,
                               t_note,
                               t_reserve)
    values (5002,             
            l_func_id,
            p_code,
            p_name,
            l_func_id,
            p_name,
            nvl(to_char(p_priority), chr(1)));
  end create_func;
  
  procedure update_func(
    p_func_id dfunc_dbt.t_funcid%type,
    p_code    dfunc_dbt.t_code%type,
    p_name    dfunc_dbt.t_name%type,
    p_file    dfunc_dbt.t_filename%type,
    p_func    dfunc_dbt.t_functionname%type
  ) is
  begin
    update dfunc_dbt f
       set f.t_code         = p_code,
           f.t_name         = p_name,
           f.t_filename     = p_file,
           f.t_functionname = p_func
    where f.t_funcid = p_func_id;
  end update_func;

  procedure save_func_obj (
    p_code      varchar2,
    p_name      varchar2,
    p_mac_file  varchar2,
    p_func_name varchar2
  ) is
    l_func_id number(10);
  begin
    l_func_id := get_func_id(p_code => p_code);
    
    if l_func_id is null then
      create_func(p_code => p_code,
                  p_name => p_name,
                  p_file => p_mac_file,
                  p_func => p_func_name);

    else
      update_func(p_func_id => l_func_id,
                  p_code    => p_code,
                  p_name    => p_name,
                  p_file    => p_mac_file,
                  p_func    => p_func_name);
    end if;
  end save_func_obj;
  
  procedure save_task (
    p_objectid dfuncobj_dbt.t_objectid%type,
    p_funcid   dfuncobj_dbt.t_funcid%type,
    p_param    dfuncobj_dbt.t_param%type,
    p_priority dfuncobj_dbt.t_priority%type default 31
  ) is
  begin
    insert into dfuncobj_dbt (t_objecttype,
                              t_objectid,
                              t_funcid,
                              t_param,
                              t_priority,
                              t_version)
    values (0,
            p_objectid,
            p_funcid,
            p_param,
            p_priority,
            1);
  end save_task;
  
  procedure delete_active_task (
    p_objectid dfuncobj_dbt.t_objectid%type,
    p_funcid   dfuncobj_dbt.t_funcid%type
  ) is
  begin
    delete from dfuncobj_dbt f
    where f.t_objectid = p_objectid
      and f.t_funcid = p_funcid
      and f.t_state in (state_ok, state_repeate_error, state_repeat_ok);
  end delete_active_task;

end funcobj_utils;
/

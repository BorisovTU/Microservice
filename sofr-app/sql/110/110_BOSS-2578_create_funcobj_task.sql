declare
  l_code varchar2(100) := 'run_nptx_control';
  l_name varchar2(100) := 'Проведение контроля платежей операций вывода ДС';
  l_file varchar2(100) := 'nptx_money_control_funcobj.mac';
  l_func varchar2(100) := 'RunOperation';
  
  l_func_id number(10);
  
  function get_func_id (p_code dllvalues_dbt.t_code%type)
    return dllvalues_dbt.t_element%type is
    l_id dllvalues_dbt.t_element%type;
  begin
    select v.t_element
      into l_id
      from dllvalues_dbt v
     where v.t_list = 5002
       and v.t_code = p_code;

    return l_id;
  exception
    when no_data_found then
      return null;
  end get_func_id;
  
  procedure create_func(
    p_code    dfunc_dbt.t_code%type,
    p_name    dfunc_dbt.t_name%type,
    p_file    dfunc_dbt.t_filename%type,
    p_func    dfunc_dbt.t_functionname%type
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
            chr(1));
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
begin
  l_func_id := get_func_id(p_code => l_code);
  
  if l_func_id is null then
    create_func(p_code => l_code,
                p_name => l_name,
                p_file => l_file,
                p_func => l_func);

  else
    update_func(p_func_id => l_func_id,
                p_code    => l_code,
                p_name    => l_name,
                p_file    => l_file,
                p_func    => l_func);
  end if;

  --commit;
end;

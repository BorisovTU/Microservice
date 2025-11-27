declare
  procedure create_funcobj(
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
  end create_funcobj;
begin
  create_funcobj(p_code => 'WrtOffDebtCFT',
                 p_name => '‘¯¨á ­¨¥ ¡à®ª¥àáª®© § ¤®«¦¥­­®áâ¨ á â¥ªãé¨å áç¥â®¢ ¢ –”’',
                 p_file => 'WrtOffDebtSumFromCFT.mac',
                 p_func => 'WrtOffDebtSumFromCFT_Funcobj',
                 p_priority => 49);
end;
/
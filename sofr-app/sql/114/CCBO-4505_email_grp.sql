declare
  v_grp_name varchar2(15) := 'NKDEndMonth';
  v_group_id number(10) := 50;
  v_is_group_exists number(5) := 0;
begin

  select count(1) into v_is_group_exists from dllvalues_dbt l where l.t_list = 5009 and l.t_name = v_grp_name;
  
  if(v_is_group_exists <> 0) then
    return; 
  end if;

  insert into dllvalues_dbt (t_list,
                             t_element,
                             t_code,
                             t_name,
                             t_flag,
                             t_note)
  values (5009,
          v_group_id,
          v_group_id,
          v_grp_name,
          1,
          'Группа рассылки по загрузке данных НКД и номинала на конец месяца, приходящийся на выходной день');

  insert into usr_email_addr_dbt (t_group,
                                  t_email,
                                  t_place)
  values (v_group_id,
          'sofr-sup@rshb.ru',
          'R');

  insert into usr_email_addr_dbt (t_group,
                                  t_email,
                                  t_place)
  values (v_group_id,
          'bo_securities@rshb.ru',
          'R');
          
end;
/
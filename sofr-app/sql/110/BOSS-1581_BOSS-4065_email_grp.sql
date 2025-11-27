declare
  l_grp_name varchar2(100) := 'NonTradingOrderLoadingError';
  l_group_id number(10);
begin
  delete from usr_email_addr_dbt a where a.t_group in (select t_element from dllvalues_dbt l where l.t_list = 5009 and l.t_name = l_grp_name);
  delete from dllvalues_dbt l where l.t_list = 5009 and l.t_name = l_grp_name;

  select max(t_element) + 1
    into l_group_id
    from dllvalues_dbt where t_list = 5009;
  
  insert into dllvalues_dbt (t_list,
                             t_element,
                             t_code,
                             t_name,
                             t_flag,
                             t_note)
  values (5009,
          l_group_id,
          l_group_id,
          l_grp_name,
          1,
          'Уведомление об ошибках при загрузке неторговых поручений через kafka');

  insert into usr_email_addr_dbt (t_group,
                                  t_email,
                                  t_place)
  values (l_group_id,
          'broker@rshb.ru',
          'R');
          
end;
/
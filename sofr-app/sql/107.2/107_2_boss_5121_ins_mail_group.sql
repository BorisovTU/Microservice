declare
 v_proup number ;
begin
insert into dllvalues_dbt(t_list,t_element,t_code,t_name,t_flag,t_note) 
      values (5009,(select max(t_element)+1 from dllvalues_dbt where t_list = 5009),(select max(t_code)+1 from dllvalues_dbt where t_list = 5009),
                     'SUPPORT_QUIK',1,'Сопровождение ИТС QUIK') returning t_element into v_proup;
insert into usr_email_addr_dbt (t_group,t_email,t_place,t_comment) values (v_proup,'supportquik@rshb.ru','R', 'Сопровождение ИТС QUIK');                   
                     
end;

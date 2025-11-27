--Добавить с проверкой
declare
  v_count NUMBER(10);
begin
  select count(1) into v_count
    from itt_q_corrsystem
   where system_name = 'IPS_DFACTORY';
  
  if v_count = 0 then
    
    insert into itt_q_corrsystem 
           (system_name, system_caption, out_pack_message_proc, in_check_message_func)
    values ('IPS_DFACTORY', 'Фабрика документов (IPS)', 'it_ips_dfactory.out_pack_message', null);
  
  end if;
end;
/
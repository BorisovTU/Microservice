declare
  n integer;
begin
  update ITT_KAFKA_TOPIC t
     set t.system_name = 'DIASOFT_TAXES'
        ,t.msg_format  = 'XML'
   where t.msg_format = 'XDIASOFT_TAXES';
  
  select count(*) into n from itt_q_corrsystem where system_name = 'DIASOFT_TAXES';
  
  if n = 0
  then
    insert into itt_q_corrsystem
      (system_name
      ,system_caption
      ,out_pack_message_proc)
    values
      ('DIASOFT_TAXES'
      ,'?епозитарный учет (обмен сообшений через KAFKA)'
      ,'it_diasoft.out_pack_message_second');
  end if;
end;

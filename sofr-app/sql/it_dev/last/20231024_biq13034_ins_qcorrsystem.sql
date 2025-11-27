begin
  delete from itt_q_corrsystem s where s.system_name in ('DIASOFT');
  insert into itt_q_corrsystem
    (system_name
    ,system_caption
    ,out_pack_message_proc)
  values
    ('DIASOFT'
    ,'Депозитарный учет (обмен сообшений через KAFKA)'
    ,'it_diasoft.out_pack_message');
  commit;
end;

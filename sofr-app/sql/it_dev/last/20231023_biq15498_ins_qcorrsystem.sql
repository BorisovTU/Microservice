begin
  delete from itt_q_corrsystem s where s.system_name in ('QUIK');
  insert into itt_q_corrsystem
    (system_name
    ,system_caption
    ,out_pack_message_proc)
  values
    ('QUIK'
    ,'Фронт-офисная брокерская платформа (Обмен сообшений через KAFKA)'
    ,'it_quik.out_pack_message');
  commit;
end;

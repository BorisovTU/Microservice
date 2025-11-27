begin
  insert into usr_clear_log_params (t_table_name,
                                    t_query)
  values ('quik_sent_order_messages',
'begin
 delete from quik_sent_order_messages m
 where m.create_time < add_months(trunc(sysdate), -12);
 commit;
end;');
end;

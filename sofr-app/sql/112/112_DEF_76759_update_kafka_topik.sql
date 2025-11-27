update ITT_KAFKA_TOPIC t
   set t.system_name = 'DIASOFT'
      ,t.msg_format  = 'XDIASOFT_TAXES'
 where t.system_name = 'DIASOFT_TAXES'
/
delete from itt_q_corrsystem s
where s.system_name = 'DIASOFT_TAXES'
/

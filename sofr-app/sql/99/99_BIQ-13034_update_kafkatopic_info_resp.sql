begin
  update itt_kafka_topic t 
    set msg_param = 'http://www.rshb.ru/csm/diasoft/send_pko_info/202309/resp' 
    where upper(t.rootelement) = upper('SendPkoInfoResp');
  commit;
exception when others then
  null;
end;

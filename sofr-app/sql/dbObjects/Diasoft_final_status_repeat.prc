create or replace procedure Diasoft_final_status_repeat(
                              p_guid varchar2
                              ,o_MSGCode  out integer  -- 0 - нет ошибок, -1 - ошибка
                              ,o_MSGText  out varchar2
                              ) is
  SERVICE_NAME varchar2(100) := 'Diasoft.SendPkoStatusResult';
  v_rec_itt_q_message_log itt_q_message_log%rowtype;
  o_msgid     itt_q_message_log.msgid%type;
  o_messbody  clob;
begin
  o_MSGCode := 0;
  begin
    select * into v_rec_itt_q_message_log 
      from itt_q_message_log ilog
      where ilog.servicename = SERVICE_NAME
        and ilog.msgid = p_guid
        and ilog.queuetype = 'IN';
  exception
    when no_data_found then
      o_MSGText := 'Не найдено сообщение с указанным GUID';
      o_MSGCode := -1;
    when too_many_rows then
      o_MSGText := 'GUID дублируется';
      o_MSGCode := -1;
    when others then
      o_MSGText := 'Неизвестная ошибка';
      o_MSGCode := -1;
  end;
  if (o_MSGCode <> 0) then
    return;
  end if;
  Diasoft_FinalStatus(
    p_messbody => v_rec_itt_q_message_log.messbody, 
    o_msgid => o_msgid, 
    o_MSGCode => o_MSGCode, 
    o_MSGText => o_MSGText, 
    o_messbody => o_messbody);
end;
/

create or replace procedure Diasoft_send_info_repeat(
                              p_guid varchar2
                              ,o_MSGCode  out integer  -- 0 - нет ошибок, -1 - ошибка
                              ,o_MSGText  out varchar2
                              ) is
  SERVICE_NAME varchar2(100) := 'Diasoft.SendPkoInfo';
  v_rec_pko_writeoff PKO_WRITEOFF%rowtype;
  v_rec_itt_q_message_log itt_q_message_log%rowtype;
  cnt integer;
begin
  o_MSGCode := 0;
  begin
    select * into v_rec_itt_q_message_log
      from itt_q_message_log ilog
      where ilog.servicename =
        SERVICE_NAME
        and ilog.msgid = p_guid
        and ilog.queuetype = 'IN';
  exception
    when no_data_found then
      o_MSGText := 'Не найдено сообщение с указанным itt_q_message_log.msgid='||p_guid;
      o_MSGCode := -1;
    when others then
      o_MSGText := 'Неизвестная ошибка';
      o_MSGCode := -1;
  end;
  if (o_MSGCode <> 0) then
    return;
  end if;
  begin
    select * into v_rec_PKO_writeoff
      from pko_writeoff pko
      where pko.guid = p_guid;
  exception
    when no_data_found then
      null; -- игнорируем на случай несоздания записи
    when too_many_rows then
      o_MSGText := 'GUID дублируется';
      o_MSGCode := -1;
      return;
  end;

  -- проверим, существует ли в планировщике dfuncobj_dbt задание по данному guid 
  select count(*) into cnt from dfuncobj_dbt d, pko_writeoff p 
    where d.t_objecttype = 8208 and d.t_objectid = p.id
    and p.guid = p_guid
    and p.guid = d.t_param
    and d.t_state = 0;
  if cnt>0 then
    o_MSGText := 'Выполняется задание funcobj с данным guid, повторное создание невозможно';
    o_MSGCode := -1;
    return;
  end if;  
  if (v_rec_PKO_writeoff.Operationid>0) then
    o_MSGText := 'Операция уже создана, повторное создание невозможно';
    o_MSGCode := -1;
    return;
  else
    delete
      from pko_writeoff pko
      where pko.guid = p_guid;
  end if;
  Diasoft_SendPkoInfo(p_msgid => v_rec_itt_q_message_log.msgid,
                      xml_in => v_rec_itt_q_message_log.messbody
                      ,o_MSGCode => o_MSGCode 
                      ,o_MSGText => o_MSGText);
end;
/

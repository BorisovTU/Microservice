create or replace procedure rshb_Sofr_ServiceSend_Message(ServiceName in varchar2, Req_id out number, Message out clob, IsNext out number) IS
/* Процедура формирования запроса по параметрам из СОФР  */  
BEGIN
  IsNext := 0;
  -- отбор для примера
  SELECT client_id||';'||income_date_time_from||';'||income_date_time_by||';'||operation_id||';'||event_id||';'||return_canceled||';'||last_batch_id,
         floor(DBMS_RANDOM.VALUE*100)
    into Message, Req_id
  from (
    select t_partyid client_id, to_char(sysdate, 'dd.mm.yyyy hh24:mi') income_date_time_from, to_char(sysdate+1, 'dd.mm.yyyy hh24:mi') income_date_time_by, 
           DBMS_RANDOM.RANDOM operation_id,
           substr(replace(to_char(DBMS_RANDOM.VALUE),'.',''),1,15) event_id, 'true' return_canceled, 1 last_batch_id
      from dparty_dbt 
     where t_partyid = 112233
     );
     
EXCEPTION 
  when others then 
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg => 'Ошибка rshb_Sofr_ServiceSend_Message: '||sqlerrm);
    Req_id := 0;
    Message := 'ERROR: '||SQLERRM;
    IsNext := 0;
end;

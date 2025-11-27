/*
  @brief  Создание задания в funcobj для интеграции с Диасофт в части списаний 
  @param[in] typeMacros       in number,    - определить макрос 
  @param funcobjParameter in varchar2,  - параметр - GUID
  @param p_id             in number,    - id Картотеки pko_writeoff или dealid
  @param o_ErrorCode      out number,   - вернуть код ошибки
  @param o_ErrorDesc      out varchar2, - вернуть описание ошибки
  @param p_comment -      in varchar2   - комментарий (не используется)
*/

create or replace procedure diasoft_pko_funcobj_creator(typeMacros  in number,
                                                        funcobjParameter       in varchar2,
                                                        p_id in number,
                                                        o_ErrorCode out number,
                                                        o_ErrorDesc out varchar2,
                                                        p_comment   in varchar2 default null) is
  v_ObjectTypeCode varchar2(200);
  v_Priority       number(10) := 31; -- иначе будет задержка до 30 минут
begin
  if (typeMacros=1) then
    -- макрос diasoft_Pko_CancelExpiredOrders
    v_ObjectTypeCode := 8210;
  elsif (typeMacros=2) then
    -- макрос diasoft_Pko_blockSecurities_Open
    v_ObjectTypeCode := 8211;
  elsif (typeMacros=3) then
    -- макрос diasoft_Pko_NoSecurities
    v_ObjectTypeCode := 8212;
  elsif (typeMacros=4) then
    -- макрос создаваемый из Diasoft.FinalStatus_Close
    v_ObjectTypeCode := 8213;
  end if;
  o_errorCode := 0;

  funcobj_utils.save_task(
    p_objectid => p_id,
    p_funcid => funcobj_utils.get_func_id(p_code => v_ObjectTypeCode),
    p_param => funcobjParameter,
    p_priority => v_Priority
  );
  it_log.log('Вставка в funcobj');

exception
  when others then
    o_errorCode := abs(sqlcode);
    o_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
    it_error.put_error_in_stack;
    it_log.log(
      p_msg => 'Error#'||o_errorCode||':'||o_ErrorDesc,
      p_msg_type => it_log.C_MSG_TYPE__ERROR
      );
end;
/

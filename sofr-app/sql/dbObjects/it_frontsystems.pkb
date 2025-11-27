create or replace package body it_frontsystems is
 /**
   @file it_frontsystems.pkb
   @brief Пакет для интеграции 
   
    
   # tag
   - functional_block:Интеграция 
   - code_type:API 
        
   # changeLog
   |date       |author      |tasks           |note                                                        
   |-----------|------------|----------------|-------------------------------------------------------------
   |2024.09.26 |Зыков М.В.  |BOSS-1585       |Реализация в СОФР интеграции с ЕФР и ДБО ФЛ по отправке статусов исполненных поручений в рамках автоматизации поручений на вывод и перевод денежных средств по брокерским счетам                    
    
    
  */

  C_C_SYSTEM_NAME constant varchar2(128) := 'FRONTSYSTEMS';

  -- Упаковщик исходящх сообшений в JSON через KAFKA
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype) as
    v_rootElement    itt_kafka_topic.rootelement%type;
    v_rootElementIN  itt_kafka_topic.rootelement%type;
    v_SystemOriginIN varchar2(128);
    vj_in_messbody   clob;
    vj_out_messbody  clob;
    vz_GUID          itt_q_message_log.msgid%type;
    vz_GUIDReq       itt_q_message_log.corrmsgid%type;
    vz_ErrorCode     itt_q_message_log.msgcode%type;
    vz_ErrorDesc     itt_q_message_log.msgtext%type;
    v_select         varchar2(2000);
    vr_messageReq    itt_q_message_log%rowtype;
  begin
    o_messmeta    := p_message.MessMETA;
    --
    begin
      select t.rootelement
        into v_rootElement
        from itt_kafka_topic t
       where t.system_name = C_C_SYSTEM_NAME
         and t.servicename = p_message.ServiceName
         and t.queuetype = it_q_message.C_C_QUEUE_TYPE_OUT;
    exception
      when no_data_found then
        raise_application_error(-20000
                               ,'Сервис ' || p_message.ServiceName || ' не зарегистрирован как ' || it_q_message.C_C_QUEUE_TYPE_OUT || ' для KAFKA/' || C_C_SYSTEM_NAME);
    end;
    vj_in_messbody := p_message.MessBODY;
    if vj_in_messbody is not null
    then
      vj_out_messbody := vj_in_messbody;
      v_select        := 'select json_value(:1 ,''$."' || v_rootElement || '".GUID'') from dual';
      execute immediate v_select
        into vz_GUID
        using vj_out_messbody;
      if vz_GUID is null
      then
        raise_application_error(-20000
                               ,'Для сервиса ' || p_message.ServiceName || chr(10) || ' зарегистрированого как ' || it_q_message.C_C_QUEUE_TYPE_OUT || ' для KAFKA/' ||
                                C_C_SYSTEM_NAME || chr(10) || ' ожидался RootElement ' || v_rootElement || ' (ошибка формата JSON)');
      elsif p_message.msgid != vz_GUID
      then
        raise_application_error(-20001, 'Значение Msgid != GUID ! ');
      elsif p_message.message_type = it_q_message.C_C_MSG_TYPE_R
            and upper(v_rootElement) not like '%REQ'
      then
        raise_application_error(-20001
                               ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || chr(10) || ' должно быть ответом от сервиса ' ||
                                p_message.ServiceName || ' сформирован запрос на отправку! ');
      elsif p_message.message_type = it_q_message.C_C_MSG_TYPE_A
            and upper(v_rootElement) not like '%RESP'
      then
        raise_application_error(-20001
                               ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || chr(10) || ' должно быть запросом к сервису ' ||
                                p_message.ServiceName || ' сформирован  ответ на отправку! ');
      end if;
      if p_message.message_type = it_q_message.C_C_MSG_TYPE_A
      then
        v_select := 'select json_value(sJSON ,''$."' || v_rootElement || '".GUIDReq'' )
                 ,json_value(sJSON ,''$."' || v_rootElement || '".ErrorList.Error[0].ErrorCode'')
                 ,json_value(sJSON ,''$."' || v_rootElement || '".ErrorList.Error[0].ErrorDesc'')
          from (select :1 as sJSON from dual)';
        execute immediate v_select
          into vz_GUIDReq, vz_ErrorCode, vz_ErrorDesc
          using vj_out_messbody;
        if vz_GUIDReq is null
        then
          raise_application_error(-20001, 'Отсутствует обязательный елемент GUIDReq ! ');
        end if;
        if vz_ErrorCode is null
        then
          raise_application_error(-20001, 'Отсутствует обязательный елемент ErrorCode ! ');
        end if;
        if p_message.CORRmsgid != vz_GUIDReq
        then
          raise_application_error(-20001, 'Значение CORRmsgid != GUIDReq ! ');
        end if;
        if p_message.MSGCode != vz_ErrorCode
        then
          raise_application_error(-20001, 'Значение MSGCode != ErrorCode ! ');
        end if;
        if p_message.MSGCode != 0
           and (vz_ErrorDesc is null or vz_ErrorDesc != p_message.MSGText )
        then
          raise_application_error(-20001, 'Значение ErrorDesc должно быть "' || p_message.MSGText || '"! ');
        end if;
      end if;
    else
      if p_message.message_type = it_q_message.C_C_MSG_TYPE_A
      then
        begin
          select t.rootelement
            into v_rootElementIN
            from itt_kafka_topic t
           where t.system_name = C_C_SYSTEM_NAME
             and t.servicename = p_message.ServiceName
             and t.queuetype = it_q_message.C_C_QUEUE_TYPE_IN;
        exception
          when no_data_found then
            raise_application_error(-20000
                                   ,'Не найден TOPIC KAFKA для входщего сообщения сервиса ' || p_message.ServiceName || ' для KAFKA/' || C_C_SYSTEM_NAME);
        end;
        vr_messageReq := it_q_message.messlog_get(p_msgid => p_message.CORRmsgid);
        v_select      := 'select json_value(sJSON ,''$."' || v_rootElementIN || '".SystemOrigin'' )
          from (select :1 as sJSON from dual)';
        execute immediate v_select
          into v_SystemOriginIN
          using vr_messageReq.Messbody;
        select json_object(v_rootElement value json_object('GUID' value p_message.msgid
                                      ,'GUIDReq' value p_message.CORRmsgid
                                      ,'RequestTime' value p_message.RequestDT
                                      ,'SystemOrigin' value nvl(v_SystemOriginIN, 'UNKNOWNSYS')
                                      ,'ErrorList' value json_array(json_object('Error' value json_object('ErrorCode' value to_char(p_message.MSGCode)
                                                                         ,'ErrorDesc' value p_message.MSGText)))) FORMAT JSON)
          into vj_out_messbody
          from dual;
      else
        raise_application_error(-20001, 'Отправляемый запрос не должен быть пустым ! ');
      end if;
    end if;
    o_messbody := vj_out_messbody;
    o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
  end;

  function  check_required_params(  p_SystemOrigin    in varchar2
                                   ,p_ext_id          in nontrading_orders_buffer.external_id%type
                                   ,p_client_cft_id   in nontrading_orders_buffer.client_cft_id%type
                                   ,p_contract        in nontrading_orders_buffer.contract%type
                                   ,p_WithdrawalTradingPlatform in varchar2
                                   ,p_is_full_rest    in nontrading_orders_buffer.is_full_rest%type
                                   ,p_currency        in nontrading_orders_buffer.currency%type
                                   ,p_amount          in nontrading_orders_buffer.enroll_account%type
                                   ,p_req_date        in nontrading_orders_buffer.req_date%type
                                   ,p_check_result    out varchar2) 
    return number is 
    l_result number;
    l_check_result varchar2(2000);
    
    procedure PutComma (p_result in out number, 
                        p_text in out varchar2)
    is 
    begin
      if p_result = 12 then 
        p_text := p_text||',';
      else 
        p_result := 12;
      end if;
    end;
  begin
    l_result := 0;
    l_check_result := 'На заданы обязательные параметры:';
    if p_SystemOrigin is null then 
      l_result := 12;
      l_check_result := l_check_result||' SystemOrigin';
    end if;
    if p_ext_id is null then 
      PutComma (l_result, l_check_result);
      l_check_result := l_check_result||' OrderId';
    end if;
    if p_client_cft_id is null then 
      PutComma (l_result, l_check_result);
      l_check_result := l_check_result||' ClientId';
    end if;
    if p_contract is null then 
      PutComma (l_result, l_check_result);
      l_check_result := l_check_result||' ContractNumber';
    end if;
    if p_WithdrawalTradingPlatform is null then 
      PutComma (l_result, l_check_result);
      l_check_result := l_check_result||' WithdrawalTradingPlatform';
    end if;
    if p_is_full_rest is null then 
      PutComma (l_result, l_check_result);
      l_check_result := l_check_result||' IsEntireBalance';
    end if;
    if p_currency is null then 
      PutComma (l_result, l_check_result);
      l_check_result := l_check_result||' CurrencyCode';
    end if;
    if p_amount is null then 
      PutComma (l_result, l_check_result);
      l_check_result := l_check_result||' Amount';
    end if;
    if p_req_date is null then 
      PutComma (l_result, l_check_result);
      l_check_result := l_check_result||' OrderDate';
    end if;

    if l_result = 0 then 
      p_check_result := 'OK';
    else 
      p_check_result := l_check_result;
    end if;
    
    return l_result;
  end;

  -- BIQ-13121 Сервис загрузки неторгового поручения клиента на вывод/перевод дс'
  procedure save_req_from_json(p_worklogid integer
                              ,p_messbody  clob
                              ,p_messmeta  xmltype
                              ,o_msgid     out varchar2
                              ,o_MSGCode   out integer
                              ,o_MSGText   out varchar2
                              ,o_messbody  out clob
                              ,o_messmeta  out xmltype) is
    vr_message               itt_q_message_log%rowtype;
    v_SystemOrigin           varchar2(2000);
    l_ext_id                 nontrading_orders_buffer.external_id%type;
    l_client_cft_id          nontrading_orders_buffer.client_cft_id%type;
    l_contract               nontrading_orders_buffer.contract%type;
    l_marketplace_withdrawal nontrading_orders_buffer.marketplace_withdrawal%type;
    l_marketplace_enroll     nontrading_orders_buffer.marketplace_enroll%type;
    l_is_full_rest           nontrading_orders_buffer.is_full_rest%type;
    l_currency               nontrading_orders_buffer.currency%type;
    l_amount                 nontrading_orders_buffer.amount%type;
    l_req_date               nontrading_orders_buffer.req_date%type;
    l_req_time               nontrading_orders_buffer.req_time%type;
    l_iis                    nontrading_orders_buffer.iis%type;
    l_account                nontrading_orders_buffer.enroll_account%type;
    l_department             nontrading_orders_buffer.department%type;
    l_orderdate_str          varchar2(50);
    l_orderdate_tz           timestamp with time zone;
    
    v_WithdrawalTradingPlatform varchar2(2000);
    v_TransferTradingPlatform varchar2(2000);
    
    function Get_RespJSON(p_MSGCode integer
                         ,p_MSGText varchar2) return clob as
      vj_out_messbody clob;
    begin
      select json_object('SendNonTradingOrderResp' value
                         json_object('GUID' value o_msgid
                                    ,'GUIDReq' value vr_message.msgid
                                    ,'RequestTime' value systimestamp
                                    ,'SystemOrigin' value v_SystemOrigin
                                    ,'ErrorList' value json_array(json_object('Error' value json_object('ErrorCode' value to_char(p_MSGCode), 'ErrorDesc' value p_MSGText))))
                         FORMAT JSON)
        into vj_out_messbody
        from dual;
      return vj_out_messbody;
    end;
  
  begin
    o_msgid    := it_q_message.get_sys_guid;
    o_MSGCode  := 0;
    o_MSGText  := 'OK';
    vr_message := it_q_message.messlog_get(p_logid => p_worklogid);
    begin
      select jt.SystemOrigin
            ,jt.ContractNumber
            ,jt.WithdrawalTradingPlatform
            ,jt.TransferTradingPlatform
            ,case
               when lower(jt.IsEntireBalance) = 'true' then
                1
               else
                0
             end as is_full_rest
            ,jt.Amount
            ,jt.CurrencyCode
            ,jt.OrderDate
            ,jt.ClientId
            ,jt.OrderID
            ,case
               when lower(jt.IsIISContract) = 'true' then
                1
               else
                0
             end as is_iis
            ,jt.TransferAccount
            ,jt.BranchId
        into v_SystemOrigin
            ,l_contract
            ,v_WithdrawalTradingPlatform
            ,v_TransferTradingPlatform
            ,l_is_full_rest
            ,l_amount
            ,l_currency
            ,l_orderdate_str
            ,l_client_cft_id
            ,l_ext_id
            ,l_iis
            ,l_account
            ,l_department
        from json_table(p_messbody
                       ,'$.SendNonTradingOrderReq' columns SystemOrigin varchar2(35) path '$.SystemOrigin'
                       ,ContractNumber varchar2(50) path '$.ContractNumber'
                       ,WithdrawalTradingPlatform varchar2(100) path '$.WithdrawalTradingPlatform'
                       ,TransferTradingPlatform varchar2(100) path '$.TransferTradingPlatform'
                       ,IsEntireBalance varchar2(50) path '$.IsEntireBalance'
                       ,IsIISContract varchar2(50) path '$.IsIISContract'
                       ,TransferAccount varchar2(20) path '$.TransferAccount'
                       ,Amount number path '$.Amount'
                       ,OrderDate varchar2(50) path '$.OrderDate'
                       ,CurrencyCode varchar2(3) path '$.CurrencyCode'
                       ,ClientId varchar2(30) path '$.ClientId.ObjectId'
                       ,OrderId varchar2(30) path '$.OrderId.ObjectId'
                       ,BranchId varchar2(30) path '$.BranchId.ObjectId') jt;

      l_orderdate_str := upper(l_orderdate_str);
      l_orderdate_str := replace(l_orderdate_str, 'Z', ' UTC');
      l_orderdate_tz := to_timestamp_tz(l_orderdate_str, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZR');

      l_req_time := cast(l_orderdate_tz at local as date);
      l_req_date := trunc(l_req_time);
      
    exception
      when others then
        o_MSGCode := 10;
        o_MSGText := 'Ошибка разбора входящего сообщения';
        nontrading_orders_utils.send_error_notification(p_proc => 'SAVE_REQ_FROM_JSON', p_errcode => o_MSGCode, p_errtxt => o_MSGText || ' GUID:' || vr_message.msgid, p_nosupport => 0);
        return;
    end;
    if o_MSGCode = 0
    then
      o_MSGCode := check_required_params(p_SystemOrigin              => v_SystemOrigin
                                        ,p_ext_id                    => l_ext_id
                                        ,p_client_cft_id             => l_client_cft_id
                                        ,p_contract                  => l_contract
                                        ,p_WithdrawalTradingPlatform => v_WithdrawalTradingPlatform
                                        ,p_is_full_rest              => l_is_full_rest
                                        ,p_currency                  => l_currency
                                        ,p_amount                    => l_amount
                                        ,p_req_date                  => l_req_date
                                        ,p_check_result              => o_MSGText);
    end if;
    if o_MSGCode = 0
    then
      if nontrading_orders_read.is_allowed_system_kafka(p_system_name => nontrading_orders_read.get_buf_by_incoming_src_name(p_incoming_name => v_SystemOrigin)) = 0
      then
        o_MSGCode := 11;
        o_MSGText := 'Загрузка сообщений из системы ' || v_SystemOrigin || ' недоступна ';
      end if;
    end if;
    if o_MSGCode = 0
    then
      l_marketplace_withdrawal := nontrading_orders_read.get_exchange(v_WithdrawalTradingPlatform); 
      l_marketplace_enroll := nontrading_orders_read.get_exchange(v_TransferTradingPlatform);

      o_MSGCode := nontrading_orders_utils.process_req(p_src                => v_SystemOrigin,
                                                       p_ext_id             => l_ext_id,
                                                       p_client_cft_id      => l_client_cft_id,
                                                       p_iis                => l_iis,
                                                       p_contract           => l_contract,
                                                       p_marketplace        => l_marketplace_withdrawal,
                                                       p_is_full_rest       => l_is_full_rest,
                                                       p_currency           => l_currency,
                                                       p_amount             => l_amount,
                                                       p_marketplace_enroll => l_marketplace_enroll,
                                                       p_account            => l_account,
                                                       p_department         => l_department,
                                                       p_req_date           => l_req_date,
                                                       p_req_time           => l_req_time,
                                                       p_file_name          => 'json');

      if o_MSGCode != 0
      then
        o_MSGText := 'Не удалось загрузить неторговое поручение в СОФР';
        nontrading_orders_utils.send_error_notification(p_proc => 'SAVE_REQ_FROM_JSON', p_errcode => o_MSGCode, p_errtxt => o_MSGText || ' GUID:' || vr_message.msgid, p_nosupport => 1);
      end if;
    end if;
    o_messbody := Get_RespJSON(o_MSGCode, o_MSGText);
  exception
    when others then
      declare
        v_SQLCODE integer;
      begin
        v_SQLCODE := abs(sqlcode);
        it_error.put_error_in_stack;
        it_log.log(p_msg => 'SAVE_REQ_FROM_JSON ' || vr_message.msgid, p_msg_type => it_log.C_MSG_TYPE__ERROR);
        it_error.clear_error_stack;
        nontrading_orders_utils.send_error_notification(p_proc => 'SAVE_REQ_FROM_JSON'
                                                       ,p_errcode => v_SQLCODE
                                                       ,p_errtxt => 'Не удалось загрузить неторговое поручение в СОФР' || ' GUID:' || vr_message.msgid
                                                       ,p_nosupport => 0);
      end;
      o_MSGCode  := it_q_manager.C_N_ERROR_OTHERS_MSGCODE;
      o_MSGText  := it_q_manager.C_C_ERROR_OTHERS_MSGTEXT;
      o_messbody := Get_RespJSON(o_MSGCode, o_MSGText);
  end save_req_from_json;

  -- получение json сообщения  о статусе и отправка в траспорт 
  procedure form_status_message_json(p_system_origin  varchar2 --  "ДБО ФЛ" или "ЕФР" в зависимости от источника
                                    ,p_status         varchar2 --  Текстовая расшифровка числового значения из send_order_status()
                                    ,p_decline_reason varchar2 -- Причина отклонения. Определяется по примечанию вида 103 "Отметка об отказе в исполнении"
                                    ,p_clientid       varchar2 -- ЦФТ-id клиента из поручения. Определяется по коду вида 101 для клиента из DNPTXOP.T_CLIENT
                                    ,p_orderid        varchar2 -- Идентификатор поручения из внешней системы.
                                    ,p_operid         DNPTXOP_DBT.T_ID%type -- Идентификатор T_ID из DNPTXOP_DBT.
                                    ,o_ErrorCode      out integer
                                    ,o_ErrorDesc      out varchar2) as
    v_msgid         itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    vj_out_messbody clob;
    vx_MessMETA     xmltype;
  begin
    o_ErrorCode := 0 ;
    select xmlelement("XML", xmlattributes(p_operid as "OperID")) into vx_MessMETA from dual;
    if p_decline_reason is null
    then
      select json_object('SendNonTradingOrderStatusReq' value --
                         json_object('GUID' value v_msgid
                                    ,'RequestTime' value systimestamp
                                    ,'SystemOrigin' value p_system_origin
                                    ,'Status' value lower(p_status)
                                    ,'ClientId' value --
                                     json_object('ObjectId' is p_clientid)
                                    ,'OrderId' value --
                                     json_object('ObjectId' is p_orderid)) FORMAT JSON returning clob)
        into vj_out_messbody
        from dual;
    else
      select json_object('SendNonTradingOrderStatusReq' value --
                         json_object('GUID' value v_msgid
                                    ,'RequestTime' value systimestamp
                                    ,'SystemOrigin' value p_system_origin
                                    ,'Status' value lower(p_status)
                                    ,'DeclineReason' value p_decline_reason
                                    ,'ClientId' value --
                                     json_object('ObjectId' is p_clientid)
                                    ,'OrderId' value --
                                     json_object('ObjectId' is p_orderid)) FORMAT JSON returning clob)
        into vj_out_messbody
        from dual;
    end if;
    it_kafka.load_msg(io_msgid => v_msgID
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                     ,p_ServiceName => 'FrontSystems.SendNonTradingOrderStatus'
                     ,p_Receiver => C_C_SYSTEM_NAME
                     ,p_MESSBODY => vj_out_messbody
                     ,p_MessMETA => vx_MessMETA
                     ,o_ErrorCode => o_ErrorCode
                     ,o_ErrorDesc => o_ErrorDesc
                      -- ,p_CORRmsgid =>
                      -- ,p_MSGCode =>
                      -- ,p_MSGText =>
                      -- ,p_comment =>
                      );              
  end;

  --для заглушек.
  procedure mock_answer(p_worklogid integer
                       ,p_messbody  clob
                       ,p_messmeta  xmltype
                       ,o_msgid     out varchar2
                       ,o_MSGCode   out integer
                       ,o_MSGText   out varchar2
                       ,o_messbody  out clob
                       ,o_messmeta  out xmltype)
  is
  begin
    null;
  end;

end it_frontsystems;
/

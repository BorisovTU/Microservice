create or replace package body it_sinv is

  /**
  * Упаковщик исходящих сообшений в Свои Инвестиции через KAFKA
  * @since RSHB 110
  * @qtest NO
  */
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype) as
    v_rootElement    itt_kafka_topic.rootelement%type;
    v_msgFormat      itt_kafka_topic.msg_format %type;
    vj_in_messbody   clob;
  begin
    begin
      select t.rootelement, t.msg_format
        into v_rootElement, v_msgFormat
        from itt_kafka_topic t
       where t.system_name = C_C_SYSTEM_NAME
         and t.servicename = p_message.ServiceName
         and t.queuetype = it_q_message.C_C_QUEUE_TYPE_OUT;
    exception
      when no_data_found then
        raise_application_error(-20000
                               ,'Сервис ' || p_message.ServiceName || ' не зарегистрирован как ' || it_q_message.C_C_QUEUE_TYPE_OUT || ' для KAFKA/' ||
                                C_C_SYSTEM_NAME);
    end;
    
    if v_msgFormat = 'DIRECT' then
      o_messbody := p_message.MessBODY;
      o_messmeta := p_message.MessMETA;
      o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
      return;
    end if;
 -- Для других форматов 
    o_messbody := p_message.MessBODY;
    o_messmeta := p_message.MessMETA;
    o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
  end out_pack_message;

  --корпоративные действия. Погашения бумаг. Неторговые поручения. Запрос на блокировку лимитов
  procedure ca_sec_nontrade_ord_lim_listener(
    p_worklogid integer
   ,p_messbody  clob
   ,p_messmeta  xmltype
   ,o_msgid     out varchar2
   ,o_MSGCode   out integer
   ,o_MSGText   out varchar2
   ,o_messbody  out clob
   ,o_messmeta  out xmltype
  ) as
    l_q_message_log itt_q_message_log%rowtype;

    l_isin            varchar2(25);
    l_contract_number varchar2(100);
    l_depo_account    varchar2(30);
    l_reg_number      varchar2(40);

    l_status          pko_writeoff.pkostatus%type;
    l_quantity        pko_writeoff.qnty%type;
    l_order_id        pko_writeoff.custodyorderid%type;
    l_guid            pko_writeoff.guid%type;
    l_operation_time  pko_writeoff.operationtime%type;
    l_oper_type       pko_writeoff.opertype%type;
    l_market          pko_writeoff.market%type;
    l_client_type     pko_writeoff.clienttype%type;
    l_clientcode      pko_writeoff.clientcode%type;
    l_expiration_date timestamp;
  begin
    o_MSGCode := 0;
    l_q_message_log := it_q_message.messlog_get(p_logid => p_worklogid);

    begin
      select status,
             isin,
             clientcode,
             quantity,
             order_id,
             guid,
             contract_number,
             market,
             depo_account,
             reg_number,
             operation_time,
             expiration_date,
             oper_type,
             client_type
        into l_status,
             l_isin,
             l_clientcode,
             l_quantity,
             l_order_id,
             l_guid,
             l_contract_number,
             l_market,
             l_depo_account,
             l_reg_number,
             l_operation_time,
             l_expiration_date,
             l_oper_type,
             l_client_type
        from json_table(p_messbody,
                        '$.SecuritiesBlockingReq' columns
                        status          integer        path '$.PkoStatus',
                        isin            varchar2(25)   path '$.ISIN',
                        clientcode      varchar2(35)   path '$.ClientCode',
                        quantity        number(32, 12) path '$.QuantitySecurities',
                        order_id        varchar2(30)   path '$.CustodyOrderId.ObjectId',
                        guid            varchar2(128)  path '$.GUID',
                        contract_number varchar2(100)  path '$.BrokerContractNumber',
                        market          varchar2(30)   path '$.Market',
                        depo_account    varchar2(30)   path '$.AccountDepoNumber',
                        reg_number      varchar2(40)   path '$.RegNumber',
                        operation_time  varchar2(30)   path '$.OperationTime',
                        expiration_date timestamp      path '$.ExpirationDate',
                        oper_type       integer        path '$.OperType',
                        client_type     integer        path '$.ClientType'
                        );
    exception
      when others then
        raise_application_error(-20000, 'Ошибка данных в сообщении');      
    end;

    Diasoft_SendPkoInfo(
      p_msgid => l_q_message_log.msgid,
      p_Pkostatus => l_status,
      p_isin => l_isin,
      p_ClientCode => l_clientcode,
      p_qnty => l_quantity,
      p_CustodyOrderId => l_order_id,
      p_guid => l_guid,
      p_contract_number => l_contract_number,
      p_market => l_market,
      p_depo_account => l_depo_account,
      p_reg_number => l_reg_number,
      p_OperationTime => l_operation_time,
      p_expiration_date => l_expiration_date,
      p_oper_type => l_oper_type,
      p_client_type => l_client_type,
      p_source_clob => p_messbody,
      o_MSGCode  => o_MSGCode,
      o_MSGText => o_MSGText
    );
  end ca_sec_nontrade_ord_lim_listener;

  procedure send_nontrade_limit_state (
    p_deal_id integer,
    p_limit_status integer
  ) is
    l_pko_row pko_writeoff%rowtype;

    l_msg_id itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    l_service_name varchar2(50) := 'SINV.CAVoluntaryRedemptionLimit';
    l_limit_comment varchar2(50);
    l_json clob;
    l_error_code integer;
    l_error_descr varchar2(2000);
  begin
    l_pko_row := nontrading_secur_orders_utils.get_pko_row_by_dealid(p_deal_id => p_deal_id);
    l_limit_comment := case when p_limit_status = 1 then 'Лимит на списание есть' else 'Лимитов на списание нет' end;

    select json_object('SecuritiesBlockingResp' is
                json_object('GUID' is l_msg_id,
                            'GUIDReq' is l_pko_row.guid,
                            'RequestTime' is systimestamp,
                            'LimitCheckStatus' is p_limit_status,
                            'LimitCheckStatusComment' is l_limit_comment,
                            'CustodyOrderId' is json_object('ObjectId' is l_pko_row.custodyorderid),
                            'SofrOperationId' is json_object('ObjectId' is p_deal_id)
                )
            )
      into l_json
      from dual;

    it_kafka.load_msg(io_msgid => l_msg_id
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                     ,p_ServiceName => l_service_name
                     ,p_Receiver => C_C_SYSTEM_NAME
                     ,p_MESSBODY => l_json
                     ,p_CORRmsgid => l_pko_row.guid 
                     ,o_ErrorCode => l_error_code
                     ,o_ErrorDesc => l_error_descr);
    if l_error_code != 0
    then
      raise_application_error(-20000, 'Ошибка отправки информации по блокировке лимитов в ' 
                                      || C_C_SYSTEM_NAME || ': ' || l_error_descr);
    end if;
  end send_nontrade_limit_state;

  --корпоративные действия. Погашения бумаг. Неторговые поручения. Исполнение поручения
  procedure ca_sec_nontrade_ord_fin_listener(
    p_worklogid integer
   ,p_messbody  clob
   ,p_messmeta  xmltype
   ,o_msgid     out varchar2
   ,o_MSGCode   out integer
   ,o_MSGText   out varchar2
   ,o_messbody  out clob
   ,o_messmeta  out xmltype
  ) is
    l_pko_row pko_writeoff%rowtype;
    l_guid varchar2(128);
    l_execution_date date;
    l_exec_status integer;
    l_deal_id integer;
    l_custody_order_id varchar2(100);
    l_request_time timestamp;

    l_error_code integer;
    l_error_descr varchar2(2000);
  begin
    o_MSGCode := 0;

    begin
      select guid,
             request_time,
             execution_date,
             exec_status,
             deal_id,
             custody_order_id
        into l_guid,
             l_request_time,
             l_execution_date,
             l_exec_status,
             l_deal_id,
             l_custody_order_id
        from json_table(p_messbody,
                        '$.SecuritiesWithdrawalReq' columns
                        guid             varchar2(128) path '$.GUID',
                        request_time     timestamp     path '$.RequestTime',
                        execution_date   date          path '$.FinalStatusDate',
                        exec_status      integer       path '$.OperationExecutionStatus',
                        deal_id          integer       path '$.SofrOperationId.ObjectId',
                        custody_order_id varchar2(100) path '$.CustodyOrderId.ObjectId'
                        );
    exception
      when others then
        raise_application_error(-20000, 'Ошибка данных в сообщении');
    end;

    --process
    l_pko_row := nontrading_secur_orders_utils.get_pko_row_by_dealid(p_deal_id => l_deal_id);
    if l_pko_row.id is null
    then
      raise_application_error(-20000, 'pko with dealid = ' || l_deal_id || ' not found');
    end if;

    if l_exec_status = 2
    then
      --success
      it_diasoft.Set_DealDate(
        p_deailid => l_deal_id,
        p_finalstatusdate => l_execution_date,
        o_ErrorCode => l_error_code,
        o_ErrorDesc => l_error_descr
      );
      if l_error_descr is not null
      then
        raise_application_error(-20000, l_error_descr);
      end if;
      
      update pko_writeoff
         set iscompleted         = chr(88)
            ,completiontimestamp = nvl(completiontimestamp, l_request_time)
            ,step1_waitstatus    = 1
            ,step2_reject        = 0
            ,step3_writeoff      = 1
       where dealid = l_deal_id;

      nontrading_secur_orders_utils.set_is_cancelled_categ(
        p_deal_id => l_deal_id,
        p_is_cancelled => 0
      );
    else
      --cancel
      update pko_writeoff
         set iscanceled           = chr(88)
            ,cancelationtimestamp = nvl(cancelationtimestamp, l_request_time)
            ,step1_waitstatus     = 1
            ,step2_reject         = 1
            ,step3_writeoff       = 0
       where dealid = l_deal_id;

      nontrading_secur_orders_utils.set_is_cancelled_categ(
        p_deal_id => l_deal_id,
        p_is_cancelled => 1
      );
    end if;

    nontrading_secur_orders_utils.push_to_execute_deal(p_deal_id => l_pko_row.dealid);
  end ca_sec_nontrade_ord_fin_listener;

  procedure send_ca_securities_final_state (
    p_id pko_writeoff.id%type
  ) is
    l_pko_row pko_writeoff%rowtype;

    l_msg_id itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    l_service_name varchar2(50) := 'SINV.CAVoluntaryRedemptionFinal';
    l_json clob;
    l_error_code integer;
    l_error_descr varchar2(2000);
  begin
    l_pko_row := nontrading_secur_orders_utils.get_pko_row_by_id(p_id => p_id);

    select json_object('SecuritiesWithdrawalResp' is
                json_object('GUID' is l_msg_id,
                            'GUIDReq' is l_pko_row.guid,
                            'RequestTime' is systimestamp,
                            'CustodyOrderId' is json_object('ObjectId' is l_pko_row.custodyorderid),
                            'SofrOperationId' is json_object('ObjectId' is l_pko_row.dealid)
                )
            )
      into l_json
      from dual;

    it_kafka.load_msg(io_msgid => l_msg_id
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                     ,p_ServiceName => l_service_name
                     ,p_Receiver => C_C_SYSTEM_NAME
                     ,p_MESSBODY => l_json
                     ,p_CORRmsgid => l_pko_row.guid 
                     ,o_ErrorCode => l_error_code
                     ,o_ErrorDesc => l_error_descr);
    if l_error_code != 0
    then
      raise_application_error(-20000, 'Ошибка отправки информации по исполнению КД в ' 
                                      || C_C_SYSTEM_NAME || ': ' || l_error_descr);
    end if;
  end send_ca_securities_final_state;

end it_sinv;
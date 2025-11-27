create or replace package body it_diasoft is

  /*********************************************************************************************************************************************************\
    Пакет для обмена сообщениями СОФР Диасофт через KAFKA
   *********************************************************************************************************************************************************
    Изменения:
   ---------------------------------------------------------------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------------------------------------------------------------
   24.10.2023  Зыков М.В.       BOSS-1231                     BIQ-13034 BOSS-770 Доработка QManager для получения сообщений из Кафку и запуска процедур СОФР
  \*********************************************************************************************************************************************************/
  -- Упаковщик исходящх сообшений в DIASIFT через KAFKA
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype) as
    v_rootElement   itt_kafka_topic.rootelement%type;
    v_msg_param     itt_kafka_topic.msg_param%type;
    v_msg_format    itt_kafka_topic.msg_format%type;
    vx_in_messbody  xmltype;
    vx_out_messbody xmltype;
    v_node          varchar2(100);
    vz_GUID         itt_q_message_log.msgid%type;
    vz_GUIDReq      itt_q_message_log.corrmsgid%type;
    vz_ErrorCode    itt_q_message_log.msgcode%type;
    vz_ErrorDesc    itt_q_message_log.msgtext%type;
    v_dom_doc       dbms_xmldom.domdocument;
    v_dom_eliment   dbms_xmldom.domelement;
  begin
    o_messmeta    := p_message.MessMETA;
    --
    begin
      select t.rootelement
            ,t.msg_param
            ,t.msg_format
        into v_rootElement
            ,v_msg_param
            ,v_msg_format
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
    if v_msg_format =it_kafka.C_C_MSG_FORMAT_XDIASOFT_TAXES then  -- DEF-76759
      out_pack_message_second(p_message      => p_message
                              ,p_expire      => p_expire
                              ,o_correlation => o_correlation
                              ,o_messbody    => o_messbody
                              ,o_messmeta    => o_messmeta) ;

      return;
    end if ;
    vx_in_messbody := it_xml.Clob_to_xml(p_message.MessBODY);
    if vx_in_messbody is not null
    then
      vx_out_messbody := vx_in_messbody;
      if v_rootElement != vx_out_messbody.getRootElement
      then
        raise_application_error(-20001
                               ,'Для сервиса ' || p_message.ServiceName || chr(10) || ' зарегистрированого как ' || it_q_message.C_C_QUEUE_TYPE_OUT ||
                                ' для KAFKA/' || C_C_SYSTEM_NAME || chr(10) || ' ожидался RootElement ' || v_rootElement || ' получен ' ||
                                vx_out_messbody.getRootElement);
      elsif p_message.message_type = it_q_message.C_C_MSG_TYPE_R
            and upper(v_rootElement) not like '%REQ'
      then
        raise_application_error(-20001
                               ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || chr(10) ||
                                ' должно быть ответом от сервиса ' || p_message.ServiceName || ' сформирован запрос на отправку! ');
      elsif p_message.message_type = it_q_message.C_C_MSG_TYPE_A
            and upper(v_rootElement) not like '%RESP'
      then
        raise_application_error(-20001
                               ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || chr(10) ||
                                ' должно быть запросом к сервису ' || p_message.ServiceName || ' сформирован  ответ на отправку! ');
      end if;
    else
      select xmlelement(evalname v_rootElement
                       ,xmlelement("GUID", p_message.msgid)
                        ,case
                          when p_message.CORRmsgid is not null then
                           xmlelement("GUIDReq", p_message.CORRmsgid)
                        end
                        ,xmlelement("RequestTime", it_xml.date_to_char_iso8601(p_message.RequestDT)))
        into vx_out_messbody
        from dual;
    end if;
    with d_out as
     (select vx_out_messbody as x from dual)
    select EXTRACTVALUE(d_out.x, '*/GUID')
          ,EXTRACTVALUE(d_out.x, '*/GUIDReq')
          ,to_number(EXTRACTVALUE(d_out.x, '*/ErrorList/Error/ErrorCode') /*default null on CONVERSION ERROR*/)
          ,EXTRACTVALUE(d_out.x, '*/ErrorList/Error/ErrorDesc')
      into vz_GUID
          ,vz_GUIDReq
          ,vz_ErrorCode
          ,vz_ErrorDesc
      from d_out;
    if vx_out_messbody.existsNode('*/GUID') = 1
    then
      if vz_GUID is null
         or vz_GUID != p_message.msgid
      then
        raise_application_error(-20001
                               ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || chr(10) ||
                                ' должно иметь атрибут GUID = ' || p_message.msgid || ' получен ' || vz_GUID);
      end if;
    else
      select insertchildxml(vx_out_messbody, '*', 'GUID', xmlelement("GUID", p_message.msgid)) into vx_out_messbody from dual;
    end if;
    if vx_out_messbody.existsNode('*/RequestTime') != 1
    then
      select insertchildxml(vx_out_messbody, '*', 'RequestTime', xmlelement("RequestTime", it_xml.date_to_char_iso8601(p_message.RequestDT)))
        into vx_out_messbody
        from dual;
    end if;
    if vx_out_messbody.existsNode('*/GUIDReq') = 1
    then
      if vz_GUIDReq is null
         or vz_GUIDReq != nvl(p_message.CORRmsgid, vz_GUIDReq || 'X')
      then
        raise_application_error(-20001
                               ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME || ' с   RootElement ' || v_rootElement || chr(10) ||
                                ' должно иметь атрибут GUIDReq = ' || p_message.CORRmsgid || ' получен ' || vz_GUID);
      end if;
    /*elsif p_message.CORRmsgid is not null
    then
      select insertchildxml(vx_out_messbody, '*', 'GUIDReq', xmlelement("GUIDReq", p_message.CORRmsgid)) into vx_out_messbody from dual;*/
    end if;
    if vx_out_messbody.existsNode('*/RequestTime') != 1
    then
      v_node := case
                  when p_message.message_type = it_q_message.C_C_MSG_TYPE_R then
                   'GUID'
                  else
                   'GUIDReq'
                end;
      select INSERTCHILDXMLAFTER(vx_out_messbody, '*', v_node, xmlelement("RequestTime", it_xml.timestamp_to_char_iso8601(p_message.RequestDT)))
        into vx_out_messbody
        from dual;
    end if;
    if p_message.message_type = it_q_message.C_C_MSG_TYPE_A
    then
      if vx_out_messbody.existsNode('*/ErrorList/Error/ErrorCode') = 1
      then
        if vz_ErrorCode is null
           or vz_ErrorCode != p_message.MSGCode
        then
          select UPDATEXML(vx_out_messbody, '*/ErrorList/Error/ErrorCode/text()', p_message.MSGCode) into vx_out_messbody from dual;
        end if;
      else
        select insertchildxml(vx_out_messbody
                             ,'*'
                             ,'ErrorList'
                             ,xmlelement("ErrorList"
                                         ,xmlelement("Error", xmlelement("ErrorCode", p_message.MSGCode), xmlelement("ErrorDesc", p_message.MSGText))))
          into vx_out_messbody
          from dual;
      end if;
      if vx_out_messbody.existsNode('*/ErrorList/Error/ErrorDesc') = 1
      then
        if vz_ErrorDesc is null
           or vz_ErrorDesc != p_message.MSGText
        then
          select UPDATEXML(vx_out_messbody, '*/ErrorList/Error/ErrorDesc/text()', p_message.MSGText) into vx_out_messbody from dual;
        end if;
      end if;
    end if;
    --
    if v_msg_param is not null
    then
      v_dom_doc     := dbms_xmldom.newdomdocument(vx_out_messbody.getClobVal);
      v_dom_eliment := dbms_xmldom.getdocumentelement(v_dom_doc);
      dbms_xmldom.setAttribute(v_dom_eliment, 'xmlns', v_msg_param);
      vx_out_messbody := dbms_xmldom.getxmltype(v_dom_doc);
    end if;
    o_messbody := vx_out_messbody.getClobVal;
    o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
  end;

  -- Упаковщик исходящх сообшений в DIASIFT через KAFKA ДЛЯ ТВИС 16598 - https://sdlc.go.rshbank.ru/confluence/pages/viewpage.action?pageId=268501101
  procedure out_pack_message_second(p_message     it_q_message_t
                                    ,p_expire      date
                                    ,o_correlation out varchar2
                                    ,o_messbody    out clob
                                    ,o_messmeta    out xmltype) as
    v_rootElement   itt_kafka_topic.rootelement%type;
    v_msg_param     itt_kafka_topic.msg_param%type;
    vx_in_messbody  xmltype;
    vx_out_messbody xmltype;
    v_node          varchar2(100);
    vz_GUID         itt_q_message_log.msgid%type;
    vz_GUIDReq      itt_q_message_log.corrmsgid%type;
    vz_GUIDResp    itt_q_message_log.msgid%type;
    vz_ErrorCode    itt_q_message_log.msgcode%type;
    vz_ErrorDesc    itt_q_message_log.msgtext%type;
    v_dom_doc       dbms_xmldom.domdocument;
    v_dom_eliment   dbms_xmldom.domelement;
  begin
    o_messmeta    := p_message.MessMETA;
    --
    begin
      select t.rootelement
            ,t.msg_param
        into v_rootElement
            ,v_msg_param
        from itt_kafka_topic t
       where t.system_name = C_C_SYSTEM_NAME_TAXES
         and t.servicename = p_message.ServiceName
         and t.queuetype = it_q_message.C_C_QUEUE_TYPE_OUT;
    exception
      when no_data_found then
        raise_application_error(-20000
                               ,'Сервис ' || p_message.ServiceName || ' не зарегистрирован как ' || it_q_message.C_C_QUEUE_TYPE_OUT || ' для KAFKA/' ||
                                C_C_SYSTEM_NAME_TAXES);
    end;
    vx_in_messbody := it_xml.Clob_to_xml(p_message.MessBODY);
    if vx_in_messbody is not null
    then
      vx_out_messbody := vx_in_messbody;
      if v_rootElement != vx_out_messbody.getRootElement
      then
        raise_application_error(-20001
                               ,'Для сервиса ' || p_message.ServiceName || chr(10) || ' зарегистрированого как ' || it_q_message.C_C_QUEUE_TYPE_OUT ||
                                ' для KAFKA/' || C_C_SYSTEM_NAME_TAXES || chr(10) || ' ожидался RootElement ' || v_rootElement || ' получен ' ||
                                vx_out_messbody.getRootElement);
      elsif p_message.message_type = it_q_message.C_C_MSG_TYPE_R
            and upper(v_rootElement) not like '%REQ'
      then
        raise_application_error(-20001
                               ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME_TAXES || ' с   RootElement ' || v_rootElement || chr(10) ||
                                ' должно быть ответом от сервиса ' || p_message.ServiceName || ' сформирован запрос на отправку! ');
      elsif p_message.message_type = it_q_message.C_C_MSG_TYPE_A
            and upper(v_rootElement) not like '%RESP'
      then
        raise_application_error(-20001
                               ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME_TAXES || ' с   RootElement ' || v_rootElement || chr(10) ||
                                ' должно быть запросом к сервису ' || p_message.ServiceName || ' сформирован  ответ на отправку! ');
      end if;
    else
      select xmlelement(evalname v_rootElement
                       ,xmlelement("GUID", p_message.msgid)
                        ,case
                          when p_message.CORRmsgid is not null then
                           xmlelement("GUIDReq", p_message.CORRmsgid)
                        end
                        ,xmlelement("RequestTime", it_xml.date_to_char_iso8601(p_message.RequestDT)))
        into vx_out_messbody
        from dual;
    end if;
    with d_out as
     (select vx_out_messbody as x from dual)
    select EXTRACTVALUE(d_out.x, '*/GUID')
          ,EXTRACTVALUE(d_out.x, '*/GUIDReq')
          ,EXTRACTVALUE(d_out.x, '*/GUIDResp')
          ,to_number(EXTRACTVALUE(d_out.x, '*/ErrorList/Error/ErrorCode') /*default null on CONVERSION ERROR*/)
          ,EXTRACTVALUE(d_out.x, '*/ErrorList/Error/ErrorDesc')
      into vz_GUID
          ,vz_GUIDReq
          ,vz_GUIDResp
          ,vz_ErrorCode
          ,vz_ErrorDesc
      from d_out;
    if vx_out_messbody.existsNode('*/GUID') = 1
    then
      if vz_GUID is null
         or vz_GUID != p_message.msgid
      then
        if (vx_out_messbody.existsNode('*/GUIDResp') = 1) then
            if vz_GUIDResp != p_message.msgid then 
                raise_application_error(-20001
                                       ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME_TAXES || ' с   RootElement ' || v_rootElement || chr(10) ||
                                        ' должно иметь атрибут GUID = ' || p_message.msgid || ' получен ' || vz_GUIDResp);
            end if;
        else 
                        raise_application_error(-20001
                                       ,'Сообщение для KAFKA/' || C_C_SYSTEM_NAME_TAXES || ' с   RootElement ' || v_rootElement || chr(10) ||
                                        ' должно иметь атрибут GUID = ' || p_message.msgid || ' получен ' || vz_GUID);
        end if;
      end if;
    else
      select insertchildxml(vx_out_messbody, '*', 'GUID', xmlelement("GUID", p_message.msgid)) into vx_out_messbody from dual;
    end if;
    if vx_out_messbody.existsNode('*/RequestTime') != 1
    then
      v_node := case
                  when p_message.message_type = it_q_message.C_C_MSG_TYPE_R then
                   'GUID'
                  else
                   'GUIDResp'
                end;
      select INSERTCHILDXMLAFTER(vx_out_messbody, '*', v_node, xmlelement("RequestTime", it_xml.timestamp_to_char_iso8601(p_message.RequestDT)))
        into vx_out_messbody
        from dual;
    end if;
    if p_message.message_type = it_q_message.C_C_MSG_TYPE_A
    then
      if vx_out_messbody.existsNode('*/ErrorList/Error/ErrorCode') = 1
      then
        if vz_ErrorCode is null
           or vz_ErrorCode != p_message.MSGCode
        then
          select UPDATEXML(vx_out_messbody, '*/ErrorList/Error/ErrorCode/text()', p_message.MSGCode) into vx_out_messbody from dual;
        end if;
      else
        select insertchildxml(vx_out_messbody
                             ,'*'
                             ,'ErrorList'
                             ,xmlelement("ErrorList"
                                         ,xmlelement("Error", xmlelement("ErrorCode", p_message.MSGCode), xmlelement("ErrorDesc", p_message.MSGText))))
          into vx_out_messbody
          from dual;
      end if;
      if vx_out_messbody.existsNode('*/ErrorList/Error/ErrorDesc') = 1
      then
        if vz_ErrorDesc is null
           or vz_ErrorDesc != p_message.MSGText
        then
          select UPDATEXML(vx_out_messbody, '*/ErrorList/Error/ErrorDesc/text()', p_message.MSGText) into vx_out_messbody from dual;
        end if;
      end if;
    end if;
    --
    if v_msg_param is not null
    then
      v_dom_doc     := dbms_xmldom.newdomdocument(vx_out_messbody.getClobVal);
      v_dom_eliment := dbms_xmldom.getdocumentelement(v_dom_doc);
      dbms_xmldom.setAttribute(v_dom_eliment, 'xmlns', v_msg_param);
      vx_out_messbody := dbms_xmldom.getxmltype(v_dom_doc);
    end if;
    o_messbody := vx_out_messbody.getClobVal;
    o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));

  end;

  --  Обработчик SendPkoInfoReq BIQ-1304 
  procedure SendPkoInfo(p_worklogid integer
                       ,p_messbody  clob
                       ,p_messmeta  xmltype
                       ,o_msgid     out varchar2
                       ,o_MSGCode   out integer
                       ,o_MSGText   out varchar2
                       ,o_messbody  out clob
                       ,o_messmeta  out xmltype) is
    vr_message itt_q_message_log%rowtype;
    v_xml_in         xmltype;

    ISIN                 varchar2(25);
    BrokerContractNumber varchar2(100);
    AccountDepoNumber    varchar2(30);
    regNumber            varchar2(40);
    ExpirationDateStr    varchar2(30);

    -- данные для картотеки PKO_WriteOff
    Pkostatus           PKO_WriteOff.Pkostatus%type;
    quantitysecurities  PKO_WriteOff.qnty%type;
    CustodyOrderId      PKO_WriteOff.CustodyOrderId%type;
    v_GUID              PKO_WriteOff.guid%type;
    OperationTime       PKO_WriteOff.OperationTime%type;
    OperType            PKO_WriteOff.OperType%type;
    Market              PKO_WriteOff.Market%type;
    ClientType          PKO_WriteOff.ClientType%type;
    ClientCode          PKO_WriteOff.ClientCode%type;
    expiration_date     timestamp;

    v_namespace      varchar2(128) := it_kafka.get_namespace(p_system_name => it_diasoft.C_C_SYSTEM_NAME, p_rootelement => 'SendPkoInfoReq');
  begin
    vr_message := it_q_message.messlog_get(p_logid => p_worklogid);

    v_xml_in := it_xml.Clob_to_xml(p_messbody);
    with oper as
    (select v_xml_in xml from dual)
    select extractValue(t.xml, 'SendPkoInfoReq/PkoStatus', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/ISIN', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/ClientCode', v_namespace)
          ,it_xml.char_to_number(extractValue(t.xml, 'SendPkoInfoReq/QuantitySecurities', v_namespace))
          ,extractValue(t.xml, 'SendPkoInfoReq/CustodyOrderId/ObjectId', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/GUID', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/BrokerContractNumber', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/Market', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/AccountDepoNumber', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/RegNumber', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/OperationTime', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/ExpirationDate', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/OperType', v_namespace)
          ,extractValue(t.xml, 'SendPkoInfoReq/ClientType', v_namespace)
      into Pkostatus
          ,ISIN
          ,ClientCode
          ,QuantitySecurities
          ,CustodyOrderId
          ,v_GUID
          ,BrokerContractNumber
          ,Market
          ,AccountDepoNumber
          ,regNumber
          ,OperationTime
          ,ExpirationDateStr
          ,OperType
          ,ClientType
      from oper t;

    expiration_date := to_timestamp(ExpirationDateStr, 'yyyy-mm-dd"T"hh24:mi:ss');
  
    Diasoft_SendPkoInfo(
      p_msgid => vr_message.msgid,
      p_Pkostatus => Pkostatus,
      p_isin => ISIN,
      p_ClientCode => ClientCode,
      p_qnty => quantitysecurities,
      p_CustodyOrderId => CustodyOrderId,
      p_guid => v_GUID,
      p_contract_number => BrokerContractNumber,
      p_market => Market,
      p_depo_account => AccountDepoNumber,
      p_reg_number => regNumber,
      p_OperationTime => OperationTime,
      p_expiration_date => expiration_date,
      p_oper_type => OperType,
      p_client_type => ClientType,
      p_source_clob => p_messbody,
      o_MSGCode  => o_MSGCode,
      o_MSGText => o_MSGText
    );
  end SendPkoInfo;

  -- Обработчик SendPkoStatusResult 
  -- Dylgerov BOSS-1473 (BOSS-1479) BIQ-13034 
  procedure SendPkoStatusResult(p_worklogid integer
                               ,p_messbody  clob
                               ,p_messmeta  xmltype
                               ,o_msgid     out varchar2
                               ,o_MSGCode   out integer
                               ,o_MSGText   out varchar2
                               ,o_messbody  out clob
                               ,o_messmeta  out xmltype) is
  begin
    if it_rs_interface.get_parm_varchar_path(p_parm_path => 'РСХБ\ИНТЕГРАЦИЯ\CHECK_WRITEOFF') != chr(88) then
       Diasoft_FinalStatus(p_messbody => p_messbody, o_msgid => o_msgid, o_MSGCode => o_MSGCode, o_MSGText => o_MSGText, o_messbody => o_messbody);
    else
       Diasoft_UpperCustody(p_messbody => p_messbody, o_msgid => o_msgid, o_MSGCode => o_MSGCode, o_MSGText => o_MSGText, o_messbody => o_messbody);
    end if;
  end;


  -- Формирование сообщения при нажатии ctrl-z и выбора "достаточно"/"недостаточно" лимитов из операции списания ЦБ
  -- Dylgerov BOSS-1473 (BOSS-1479) BIQ-13034
  procedure SendLimitMessage(p_SofrOperationId         in number -- Id операции в СОФР (ddl_tick_dbt.t_dealid)
                            ,p_DiasoftId               in varchar2 default null -- Id операции в Диасофт 
                            ,p_LimitCheckStatus        number -- статус проверки лимитов в СОФР 
                            ,p_LimitCheckStatusComment varchar2 -- комментарий статуса проверки лимитов в СОФР 
                            ,o_ErrorCode               out number -- != 0 ошибка создания сообщения  o_ErrorDesc
                            ,o_ErrorDesc               out varchar2
                            ,p_MSGCode                 in number default 0 -- Код ошибки обработки 
                            ,p_MSGText                 in varchar2 default null
                            ,p_comment                 in varchar2 default null) is
    v_msgID itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    v_ServiceName constant itt_q_message_log.servicename%type := 'Diasoft.SendPkoStatus';
    vx_MESSBODY xmltype;
    v_DiasoftId pko_writeoff.custodyorderid%type := p_DiasoftId;
    v_CORRmsgid pko_writeoff.guid%type;
  begin
    if p_SofrOperationId is not null
       and p_DiasoftId is null
    then
      begin
        select w.custodyorderid,w.guid into v_DiasoftId,v_CORRmsgid from pko_writeoff w where w.dealid = p_SofrOperationId;
      exception
        when no_data_found then
          v_DiasoftId := null;
      end;
      if v_DiasoftId is null
      then
        o_ErrorCode := it_q_manager.C_N_ERROR_OTHERS_MSGCODE;
        o_ErrorDesc := 'Не указано обязательное значение CustodyOrderId ';
        return;
      end if;
    end if;
    select xmlelement("SendPkoStatusReq"
                      ,xmlelement("GUID", v_msgID)
                      ,xmlelement("RequestTime", it_xml.date_to_char_iso8601(sysdate))
                      ,xmlelement("LimitCheckStatus", p_LimitCheckStatus)
                      ,xmlelement("LimitCheckStatusComment", p_LimitCheckStatusComment)
                      ,xmlelement("CustodyOrderId", xmlelement("ObjectId", v_DiasoftId))
                      ,xmlelement("SofrOperationId", xmlelement("ObjectId", p_SofrOperationId)))
      into vx_MESSBODY
      from dual;
    it_kafka.load_msg(io_msgid => v_msgID
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                     ,p_ServiceName => v_ServiceName
                     ,p_Receiver => C_C_SYSTEM_NAME
                     ,p_MESSBODY => vx_MESSBODY.getClobVal
                     ,p_CORRmsgid => v_CORRmsgid 
                     ,o_ErrorCode => o_ErrorCode
                     ,o_ErrorDesc => o_ErrorDesc
                     ,p_MSGCode => nvl(p_MSGCode, 0)
                     ,p_MSGText => p_MSGText
                     ,p_comment => p_comment);
  end;

  --  Формирование ответа на  SendPkoInfoReq BIQ-13034 
  procedure SendPkoInfoResp(p_GUIDReq         in varchar2 -- GUID из входящего сообщения  SendPkoInfoReq,
                           ,p_CustodyOrderId  in varchar2 -- Id поручения в Диасофт, из CustodyOrderId во входящем xml
                           ,p_SofrOperationId in varchar2 -- Id свежесозданной операции в СОФР (ddl_tick_dbt.t_dealid). Не заполняется, если операцию не удалось создать.
                           ,o_ErrorCode       out number -- != 0 ошибка создания сообщения  o_ErrorDesc
                           ,o_ErrorDesc       out varchar2
                           ,p_MSGCode         in number default 0 -- Код ошибки обработки SendPkoInfoReq
                           ,p_MSGText         in varchar2 default null
                           ,p_comment         in varchar2 default null) is
    v_msgID itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
    v_ServiceName constant itt_q_message_log.servicename%type := 'Diasoft.SendPkoInfo';
    vx_MESSBODY xmltype;
    v_errList   xmlType;
  begin
    -- TODO - недоделано - не хватает ErrorDesc
    -- также появляется xmlns
    /*with 
    a as (SELECT regexp_substr( p_MSGText , '[^,]+', 1, level) id
            FROM dual
          CONNECT BY NVL(regexp_instr(p_MSGText, '[^,]+', 1, level), 0) <> 0),
    b as (select xmlelement("ErrorCode", nvl(id,0) ) res, 1 id1  from a)
    select xmlelement("ErrorList", xmlelement("Error",xmlagg(b.res order by b.id1 ) ) ) 
      into v_errList 
      from b group by id1;*/
    select xmlelement("SendPkoInfoResp"
                      ,xmlelement("GUID", v_msgID)
                      ,xmlelement("GUIDReq", p_GUIDReq)
                      ,xmlelement("RequestTime", it_xml.date_to_char_iso8601(sysdate))
                      ,xmlelement("CustodyOrderId", xmlelement("ObjectId", p_CustodyOrderId))
                      ,xmlelement("SofrOperationId", xmlelement("ObjectId", nvl(p_SofrOperationId, 0)))
                      ,xmlelement("ErrorList", xmlelement("Error", xmlelement("ErrorCode", nvl(p_MSGCode, 0)), xmlelement("ErrorDesc", p_MSGText)))
                      /*v_errList*/)
      into vx_MESSBODY
      from dual;
    
    it_kafka.load_msg(io_msgid => v_msgID
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_A
                     ,p_ServiceName => v_ServiceName
                     ,p_Receiver => C_C_SYSTEM_NAME
                     ,p_MESSBODY => vx_MESSBODY.getClobVal
                     ,o_ErrorCode => o_ErrorCode
                     ,o_ErrorDesc => o_ErrorDesc
                     ,p_CORRmsgid => p_GUIDReq
                     ,p_MSGCode => nvl(p_MSGCode, 0)
                     ,p_MSGText => p_MSGText
                     ,p_comment => p_comment);
  end;

   /**
  * BIQ-16598 Ответ на запрос УНКД из Диасофта
  * @since RSHB 107
  * @qtest NO
  */
  procedure GetAllocatedCouponInfoResp(p_GUIDReq in varchar2  -- GUID из входящего сообщения GetAllocatedCouponInfoReq
                            ,p_GUIDResp in varchar2           -- GUID ответного сообщения 
                            ,p_MSGCode in number default 0            -- глобальный код ошибки
                            ,p_MSGText in varchar2 default 'Успешно'  -- глобальный текст ошибки
                            ,o_messbody out clob       -- сформированный ответ
                            ,o_ErrorCode out number     -- != 0 ошибка создания сообщения 
                            ,o_ErrorDesc out varchar2   -- описание ошибки
                            ) is
    v_msgID                 itt_q_message_log.msgid%type; 
    v_ServiceName constant  itt_q_message_log.servicename%type := 'Diasoft.GetAllocatedCouponInfo';
    vx_messbody             xmltype;
    v_errList               xmlType;
    v_GUIDResp              DNPTXNKDREQDIAS_DBT.t_guidresp %type;
    
  begin
    o_ErrorCode := 0;
    o_ErrorDesc := 'OK';
    /*DEF-69598  в itt_q_,essage_log в msgid записываем p_GUIDResp, при этом внутри XML GUID = входящему p_GUIDReq, GUID_Resp = ответному p_GUIDResp*/
    --v_msgID := p_GUIDReq;
     v_msgID := p_GUIDResp;  
  
    v_GUIDResp := p_GUIDResp;
    IF length(nvl(v_GUIDResp,'-')) > 1 THEN  -- такого быть не должно
      update DNPTXNKDREQDIAS_DBT
         set T_GUIDRESP = v_GUIDResp
       where t_guid = p_GUIDReq
         and length(nvl(t_guidresp,'-')) <= 1;
    END IF;
    
    select xmlelement("GetAllocatedCouponInfoResp", 
                        xmlelement("GUID", p_GUIDReq),
                        xmlelement("GUIDResp", v_GUIDResp),
                        xmlelement("PaymentList", 
                                    xmlagg(xmlelement("Payment", 
                                                        xmlforest(  req.t_paymentid as "PaymentId",
                                                                    req.t_factsum as "AllocatedCouponAmount",
                                                                    'RUB' as "AllocatedCouponCurrency",
                                                                    xmlelement("BusinessError", 
                                                                               xmlforest(req.t_errorcode as "BusinessErrorCode", 
                                                                                         replace(req.t_error,chr(1)) as "BusinessErrorDesc")
                                                                               ) as "BusinessErrorList"
                                                                   )
                                                      )
                                           )
                                   ),
                       xmlelement("ErrorList", 
                                    xmlelement("Error", 
                                                xmlforest(p_MSGCode as "ErrorCode", 
                                                          nvl(p_MSGText,'Успешно') as "ErrorDesc")
                                               )                                           
                                   )
                      )
      into vx_messbody
      from DNPTXNKDREQDIAS_DBT req
     where t_guid = p_GUIDReq
    group by req.t_guid;

    o_messbody := vx_messbody.getClobVal();
    
    it_kafka.load_msg(io_msgid => v_msgID
                    ,p_message_type => it_q_message.C_C_MSG_TYPE_A
                    ,p_ServiceName => v_ServiceName
                    ,p_Receiver => C_C_SYSTEM_NAME_TAXES
                    ,p_MESSBODY => o_messbody
                    ,o_ErrorCode => o_ErrorCode
                    ,o_ErrorDesc => o_ErrorDesc
                    ,p_CORRmsgid => p_GUIDReq
                    ,p_MSGCode => nvl(p_MSGCode, 0)
                    ,p_MSGText => p_MSGText
                    ,p_comment => '');
                    
     if o_ErrorCode > 0 then
        it_log.log(o_ErrorDesc,it_log.C_MSG_TYPE__ERROR, o_messbody);
     end if;
  end;

  /**
  * Получение идентификатора клиента по виду кода "Код ЦФТ"  
  * @since RSHB 107
  * @qtest NO
  * @param p_PartyCode Код клиента
  * @return идентификатор клиента
  */
  FUNCTION GetPartyIDByCFT(p_PartyCode IN VARCHAR2) 
    RETURN NUMBER
  IS
    v_PartyID dparty_dbt.t_PartyID%TYPE;
  BEGIN
    SELECT t_ObjectID INTO v_PartyID
      FROM dobjcode_dbt 
     WHERE t_ObjectType = PM_COMMON.OBJTYPE_PARTY
       AND t_CodeKind = PTCK_CFT
       AND t_Code = p_PartyCode 
       AND t_State = 0;
    RETURN v_PartyID;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
  END GetPartyIDByCFT;
  
  /**
  * Получение внутреннего идентификатора ценной бумаги в СОФР по коду ISIN/LSIN 
  * @since RSHB 107
  * @qtest NO
  * @param p_ISINLSIN Идентификатор выпуска ISIN/LSIN
  * @return идентификатор ц/б
  */
  FUNCTION GetAvoirFIID(p_ISINLSIN IN VARCHAR2) 
    RETURN NUMBER
  IS
    v_FIID davoiriss_dbt.t_FIID%TYPE;
  BEGIN
    BEGIN
      SELECT t_FIID INTO v_FIID
        FROM davoiriss_dbt 
       WHERE t_ISIN = p_ISINLSIN;
    EXCEPTION WHEN NO_DATA_FOUND THEN
      BEGIN
        SELECT t_FIID INTO v_FIID 
          FROM davoiriss_dbt
         WHERE t_LSIN = p_ISINLSIN;
      EXCEPTION WHEN NO_DATA_FOUND THEN
        v_FIID := -1;
      END;
    END;
    RETURN v_FIID;
  END GetAvoirFIID;
  
 /**
  * Проверка наличия договора БО для переданного клиента и номера договора
  * @since RSHB 107
  * @qtest NO
  * @param p_PartyID   Идентификатор клиента
  * @param p_Number    Номер договора
  * @param p_StartDate Дата начала периода
  * @param p_EndDate   Дата окончания периода
  * @return dsfconrt_dbt.t_id, если есть ДБО; -1, если нет ДБО
  */
  FUNCTION CheckDBO(p_PartyID IN NUMBER, p_Number in varchar2, p_StartDate IN DATE, p_EndDate IN DATE) 
    RETURN NUMBER
  IS
    v_ret dsfcontr_dbt.t_id%type;
  BEGIN
    SELECT t_id INTO v_ret
      FROM dsfcontr_dbt 
     WHERE t_ServKind = 0
       AND t_PartyID = p_PartyID
       AND t_number = p_Number
       AND t_DateBegin <= p_EndDate 
       AND (t_DateClose = TO_DATE('01.01.0001', 'DD.MM.YYYY') OR t_DateClose >= p_StartDate)
       AND ROWNUM = 1;
    RETURN v_ret;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN RETURN -1;
  END CheckDBO;

  PROCEDURE InsDfltIntoNPTXNKDREQDIAS( p_reqdias IN OUT DNPTXNKDREQDIAS_DBT%ROWTYPE )
  IS
  BEGIN
    p_reqdias.T_GUID                 := NVL(p_reqdias.T_GUID                , CHR(1));
    p_reqdias.T_GUIDRESP             := NVL(p_reqdias.T_GUIDRESP            , CHR(1));
    p_reqdias.T_REQUESTDATE          := NVL(p_reqdias.T_REQUESTDATE         , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_reqdias.T_REQUESTTIME          := NVL(p_reqdias.T_REQUESTTIME         , TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'));
    p_reqdias.T_PAYMENTID            := NVL(p_reqdias.T_PAYMENTID           , CHR(1));
    p_reqdias.T_PAYMENTACTION        := NVL(p_reqdias.T_PAYMENTACTION       , 0);
    p_reqdias.T_FIXINGDATE           := NVL(p_reqdias.T_FIXINGDATE          , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_reqdias.T_CLIENTID             := NVL(p_reqdias.T_CLIENTID            , CHR(1));
    p_reqdias.T_AGREEMENTNUMBER      := NVL(p_reqdias.T_AGREEMENTNUMBER     , CHR(1));
    p_reqdias.T_ISIIS                := NVL(p_reqdias.T_ISIIS               , CHR(0));
    p_reqdias.T_MARKETPLACE          := NVL(p_reqdias.T_MARKETPLACE         , CHR(1));
    p_reqdias.T_ISINREGNUMBER        := NVL(p_reqdias.T_ISINREGNUMBER       , CHR(1));
    p_reqdias.T_COUPONNUMBER         := NVL(p_reqdias.T_COUPONNUMBER        , CHR(1));
    p_reqdias.T_COUPONSTARTDATE      := NVL(p_reqdias.T_COUPONSTARTDATE     , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_reqdias.T_COUPONENDDATE        := NVL(p_reqdias.T_COUPONENDDATE       , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_reqdias.T_CREATEDATE           := NVL(p_reqdias.T_CREATEDATE          , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_reqdias.T_CREATETIME           := NVL(p_reqdias.T_CREATETIME          , TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'));
    p_reqdias.T_CHANGEDATE           := NVL(p_reqdias.T_CHANGEDATE          , TO_DATE('01.01.0001','DD.MM.YYYY'));
    p_reqdias.T_CHANGETIME           := NVL(p_reqdias.T_CHANGETIME          , TO_DATE('01.01.0001 00:00:00','DD.MM.YYYY HH24:MI:SS'));
    p_reqdias.T_ERRORCODE            := NVL(p_reqdias.T_ERRORCODE           , 0);
    p_reqdias.T_ERROR                := NVL(p_reqdias.T_ERROR               , CHR(1));
    p_reqdias.T_SUM                  := NVL(p_reqdias.T_SUM                 , 0);
    p_reqdias.T_PARTYID              := NVL(p_reqdias.T_PARTYID             , -1);
    p_reqdias.T_CONTRACTID           := NVL(p_reqdias.T_CONTRACTID          , 0);
    p_reqdias.T_FIID                 := NVL(p_reqdias.T_FIID                , -1);
    p_reqdias.T_RECEIVEDCOUPONAMOUNT := NVL(p_reqdias.T_RECEIVEDCOUPONAMOUNT, 0);
    p_reqdias.T_FACTSUM              := NVL(p_reqdias.T_FACTSUM             , 0);
    p_reqdias.T_GUIDRESULT           := NVL(p_reqdias.T_GUIDRESULT          , CHR(1));
    p_reqdias.T_STATUS               := NVL(p_reqdias.T_STATUS              , IT_DIASOFT.NPTXNKDREQDIAS_STATUS_NEW);
  END;


  /**
  * BIQ-16598 Запрос УНКД из Диасофта
  * @since RSHB 107
  * @qtest NO
  */
  procedure GetAllocatedCouponInfoReq(p_worklogid integer
                               ,p_messbody  clob
                               ,p_messmeta  xmltype
                               ,o_msgid     out varchar2
                               ,o_MSGCode   out integer
                               ,o_MSGText   out varchar2
                               ,o_messbody  out clob
                               ,o_messmeta  out xmltype) is 
    v_xml           XMLTYPE;
    v_namespace     VARCHAR2(128) := it_kafka.get_namespace(it_diasoft.C_C_SYSTEM_NAME_TAXES, 'GetAllocatedCouponInfoReq');
    v_namespace_send    VARCHAR2(128) := 'xmlns="http://www.rshb.ru/csm/sofr/send_allocated_coupon_result/202404/req"';

    vx_messbody     XMLTYPE;

    v_cnt_guid  number;
    v_GUID      DNPTXNKDREQDIAS_DBT.t_guid%type;

    TYPE payment_t IS TABLE OF DNPTXNKDREQDIAS_DBT%rowtype
    INDEX BY PLS_INTEGER;
    v_payments payment_t;
    
    v_reqdias DNPTXNKDREQDIAS_DBT%rowtype;

    v_txObj DNPTXOBJ_DBT%ROWTYPE;
    v_Cur   NUMBER := 0;
    
    -- локальные константы
    C_NAME_REGVAL  varchar2(104) := 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ПЕРЕДАЧА УНКД В ДИАСОФТ';
    vTurnOn char(1);

    v_Market_Code VARCHAR2(2000) := CHR(1);
    v_MarketID NUMBER := -1;
    v_ContractID NUMBER := 0;
  BEGIN
    o_MSGCode := 0;
    o_MSGText := 'Успешно';
    
    vTurnOn :=  RSB_Common.GetRegFlagValue(C_NAME_REGVAL);
    if vTurnOn <> 'X' then
        o_MSGText := 'Настройка '||C_NAME_REGVAL||' отключена';
        return;
    end if;
    
    v_xml := it_xml.Clob_to_xml(p_messbody);
    
    select extractvalue(v_xml, '/GetAllocatedCouponInfoReq/GUID',v_namespace) 
      into v_GUID
      from dual;
        
    v_cnt_guid := 0;
    IF v_GUID IS NULL THEN
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не удалось разобрать xml по причине: отсутствует тег GUID';
      return;
    ELSE 
      select count(*) into v_cnt_guid
       from DNPTXNKDREQDIAS_DBT 
      where t_guid = v_GUID;
    END IF;
      
    IF v_cnt_guid = 0 THEN 
      BEGIN
        select  0 as T_ID,
                extractvalue(v_xml, '/GetAllocatedCouponInfoReq/GUID',v_namespace)          as T_GUID,
                CHR(1)                                                                      as T_GUIDRESP,
                to_date(to_char(it_xml.char_to_timestamp(extractvalue(v_xml, '/GetAllocatedCouponInfoReq/RequestTime',v_namespace)),'dd.mm.yyyy'),'dd.mm.yyyy') as T_REQUESTDATE,
                to_date(to_char(it_xml.char_to_timestamp(extractvalue(v_xml, '/GetAllocatedCouponInfoReq/RequestTime',v_namespace)),'"01.01.0001" hh24:mi:ss'),'dd.mm.yyyy hh24:mi:ss') as T_REQUESTTIME,
                extractvalue( value(t), 'Payment/PaymentId',v_namespace)                    as T_PAYMENTID,
                TO_NUMBER(extractvalue( value(t), 'Payment/PaymentAction',v_namespace))     as T_PAYMENTACTION,
                it_xml.char_to_date(extractvalue( value(t), 'Payment/FixingDate',v_namespace))  as T_FIXINGDATE,
                extractvalue( value(t), 'Payment/ClientId/ObjectId',v_namespace)            as T_CLIENTID,
                extractvalue( value(t), 'Payment/AgreementNumber',v_namespace)              as T_AGREEMENTNUMBER,
                extractvalue( value(t), 'Payment/IsIIS',v_namespace)                        as T_ISIIS,
                upper(extractvalue( value(t), 'Payment/MarketPlace',v_namespace))           as T_MARKETPLACE,
                extractvalue( value(t), 'Payment/ISINRegistrationNumber',v_namespace)       as T_ISINREGNUMBER,
                extractvalue( value(t), 'Payment/CouponNumber',v_namespace)                 as T_COUPONNUMBER,
                it_xml.char_to_date(extractvalue( value(t), 'Payment/CouponStartDate',v_namespace)) as T_COUPONSTARTDATE,
                it_xml.char_to_date(extractvalue( value(t), 'Payment/CouponEndDate',v_namespace))   as T_COUPONENDDATE,
                to_date(to_char(sysdate,'dd.mm.yyyy'),'dd.mm.yyyy')                         as T_CREATEDATE,
                to_date(to_char(sysdate,'"01.01.0001" hh24:mi:ss'),'dd.mm.yyyy hh24:mi:ss') as T_CREATETIME,
                to_date(to_char(sysdate,'dd.mm.yyyy'),'dd.mm.yyyy')                         as T_CHANGEDATE,
                to_date(to_char(sysdate,'"01.01.0001" hh24:mi:ss'),'dd.mm.yyyy hh24:mi:ss') as T_CHANGETIME,
                0                                                                         as T_ERRORCODE,
                CHR(1)                                                                    as T_ERROR,
                0                                                                         as T_SUM,
                -1                                                                        as T_PARTYID,
                -1                                                                        as T_CONTRACTID,
                -1                                                                        as T_FIID,
                it_xml.char_to_number(NVL(extractvalue( value(t), 'Payment/ReceivedCouponAmount',v_namespace), 0)) as T_RECEIVEDCOUPONAMOUNT,
                0                                                                         as T_FACTSUM,
                CHR(1)                                                                    as T_GUIDRESULT,
                IT_DIASOFT.NPTXNKDREQDIAS_STATUS_NEW                                      as T_STATUS
         bulk collect into v_payments
         from table(
            xmlsequence(
              extract( v_xml, '/GetAllocatedCouponInfoReq/PaymentList/Payment', v_namespace )
              )
            ) t;
      EXCEPTION
        when others then 
          o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
          o_MSGText := 'Не удалось разобрать xml по причине: неверный формат '||SQLERRM;
      END;

      FOR i IN v_payments.FIRST .. v_payments.LAST LOOP
        -- Проверка необходимых параметров
        IF v_payments(i).T_CLIENTID IS NULL THEN
          v_payments(i).T_ERRORCODE := ERROR_UNEXPECTED_GET_DATA;
          v_payments(i).T_ERROR := 'Не удалось разобрать xml по причине: отсутствует тег ClientId/ObjectId';
          v_payments(i).T_STATUS := IT_DIASOFT.NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR;
        ELSIF v_payments(i).T_AGREEMENTNUMBER IS NULL THEN
          v_payments(i).T_ERRORCODE := ERROR_UNEXPECTED_GET_DATA;
          v_payments(i).T_ERROR := 'Не удалось разобрать xml по причине: отсутствует тег AgreementNumber';
          v_payments(i).T_STATUS := IT_DIASOFT.NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR;
        ELSIF v_payments(i).T_ISINREGNUMBER IS NULL THEN
          v_payments(i).T_ERRORCODE := ERROR_UNEXPECTED_GET_DATA;
          v_payments(i).T_ERROR := 'Не удалось разобрать xml по причине: отсутствует тег ISINRegistrationNumber';
          v_payments(i).T_STATUS := IT_DIASOFT.NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR;
        ELSIF v_payments(i).T_FIXINGDATE IS NULL THEN
          v_payments(i).T_ERRORCODE := ERROR_UNEXPECTED_GET_DATA;
          v_payments(i).T_ERROR := 'Не удалось разобрать xml по причине: отсутствует тег FixingDate';
          v_payments(i).T_STATUS := IT_DIASOFT.NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR;
        END IF;
        
        IF v_payments(i).T_MARKETPLACE IS NULL THEN
          v_payments(i).T_ERRORCODE := ERROR_UNEXPECTED_GET_DATA;
          v_payments(i).T_ERROR:= 'Не удалось разобрать xml по причине: отсутствует тег MarketPlace';
          v_payments(i).T_STATUS := IT_DIASOFT.NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR;
        ELSE 
          IF v_payments(i).T_MARKETPLACE not in (C_MARKETPLACE_NRD, C_MARKETPLACE_SPB) THEN
            v_payments(i).T_ERRORCODE := ERROR_UNEXPECTED_GET_DATA;
            v_payments(i).T_ERROR := 'Не удалось разобрать xml по причине: неверный формат MarketPlace, ожидается: '||C_MARKETPLACE_NRD||','||C_MARKETPLACE_SPB||' получено '||v_payments(i).T_MARKETPLACE;
            v_payments(i).T_STATUS := IT_DIASOFT.NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR;
          END IF;
        END IF;
        
        IF v_payments(i).T_ERRORCODE = 0 THEN 
          -- Поиск клиента по переданному коду
          v_payments(i).T_PARTYID := GetPartyIDByCFT(v_payments(i).T_CLIENTID);
          IF v_payments(i).T_PARTYID = -1 THEN
            v_payments(i).T_ERRORCODE := ERROR_CLIENT_NOTFOUND;
            v_payments(i).T_ERROR := 'Не найден клиент по ЦФТ Id '||v_payments(i).T_CLIENTID;
            v_payments(i).T_STATUS := IT_DIASOFT.NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR;
          ELSE
            --Проверка наличия договора у клиента в СОФР
            v_payments(i).T_CONTRACTID := CheckDBO(v_payments(i).T_PARTYID, v_payments(i).T_AGREEMENTNUMBER, v_payments(i).T_FIXINGDATE, v_payments(i).T_FIXINGDATE);
            IF v_payments(i).T_CONTRACTID = -1 THEN
              v_payments(i).T_ERRORCODE := ERROR_CONTRACT_NOTFOUND;
              v_payments(i).T_ERROR := 'Не найден договор с номером '||v_payments(i).T_AGREEMENTNUMBER||' для клиента '||v_payments(i).T_CLIENTID;
              v_payments(i).T_STATUS := IT_DIASOFT.NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR;
            END IF;
          END IF;
        END IF;

        IF v_payments(i).T_RECEIVEDCOUPONAMOUNT IS NULL THEN
          v_payments(i).T_RECEIVEDCOUPONAMOUNT := 0;
        END IF;
        
        -- Получение идентификатора ЦБ по ISIN/LSIN
        IF v_payments(i).T_ERRORCODE = 0 THEN 
          v_payments(i).T_FIID := GetAvoirFIID(v_payments(i).T_ISINREGNUMBER); 
          IF v_payments(i).T_FIID = -1 THEN
            v_payments(i).T_ERRORCODE := ERROR_AVOIR_NOTFOUND;
            v_payments(i).T_ERROR := 'Не найдена ценная бумага ' || v_payments(i).T_ISINREGNUMBER;
            v_payments(i).T_STATUS := IT_DIASOFT.NPTXNKDREQDIAS_STATUS_VALIDATION_ERROR;
          END IF;
        END IF;

        InsDfltIntoNPTXNKDREQDIAS(v_payments(i));
        
        insert into DNPTXNKDREQDIAS_DBT values v_payments(i) returning t_id into v_payments(i).t_id;
        
      END LOOP;
    ELSE 
      it_log.log('Запрос уже существует в таблице DNPTXNKDREQDIAS_DBT. Не обрабатывается ',it_log.C_MSG_TYPE__MSG, p_messbody);
    END IF;
  
  end;

  /**
  * BIQ-16598 Ответ на ответ на запрос УНКД из Диасофта
  * @since RSHB 107
  * @qtest NO
  */
  procedure SendAllocatedCouponResultReq(p_worklogid integer
                               ,p_messbody  clob
                               ,p_messmeta  xmltype
                               ,o_msgid     out varchar2
                               ,o_MSGCode   out integer
                               ,o_MSGText   out varchar2
                               ,o_messbody  out clob
                               ,o_messmeta  out xmltype) is 
    v_xml           XMLTYPE;
    v_namespace     VARCHAR2(128) := it_kafka.get_namespace(it_diasoft.C_C_SYSTEM_NAME_TAXES, 'SendAllocatedCouponResultReq');

    vx_messbody     XMLTYPE;

    v_cnt_guid number;
    v_GUIDResult DNPTXNKDREQDIAS_DBT.t_guid%type;
    v_GUIDResp   DNPTXNKDREQDIAS_DBT.t_guidresp%type;

    TYPE payment_t IS TABLE OF DNPTXNKDREQDIAS_DBT%rowtype
    INDEX BY PLS_INTEGER;
    v_payments payment_t;
    
    v_reqdias DNPTXNKDREQDIAS_DBT%rowtype;

    v_txObj DNPTXOBJ_DBT%ROWTYPE;
    v_Cur   NUMBER := 0;
    
    -- локальные константы
    C_NAME_REGVAL  varchar2(104) := 'РСХБ\БРОКЕРСКОЕ ОБСЛУЖИВАНИЕ\ПЕРЕДАЧА УНКД В ДИАСОФТ';
    vTurnOn char(1);

    v_Market_Code VARCHAR2(2000) := CHR(1);
    v_MarketID NUMBER := -1;
    v_ContractID NUMBER := 0;
  BEGIN
    o_MSGCode := 0;
    o_MSGText := 'Успешно';
    
    vTurnOn :=  RSB_Common.GetRegFlagValue(C_NAME_REGVAL);
    if vTurnOn <> 'X' then
        o_MSGText := 'Настройка '||C_NAME_REGVAL||' отключена';
        return;
    end if;
    
    v_xml := it_xml.Clob_to_xml(p_messbody);
    
    select extractvalue(v_xml, '/SendAllocatedCouponResultReq/GUIDResp',v_namespace) 
      into v_GUIDResp
      from dual;

    select extractvalue(v_xml, '/SendAllocatedCouponResultReq/GUID',v_namespace) 
      into v_GUIDResult
      from dual;

    v_cnt_guid := 0;
    IF v_GUIDResp IS NULL THEN
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не удалось разобрать xml по причине: отсутствует тег GUIDResp';
      return;
    ELSIF v_GUIDResult IS NULL THEN
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText:= 'Не удалось разобрать xml по причине: отсутствует тег GUID';
      return;
    ELSE 
      begin
        select * into v_reqdias
         from DNPTXNKDREQDIAS_DBT 
        where t_guidresp = v_GUIDResp
          and rownum = 1;
        v_cnt_guid := 1;
      exception
        when no_data_found then  -- запрос, который является ответом не понятно, на что, нужно ли возвращать ошибку? Ответ мы не отправляем
              o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
              o_MSGText:= 'Не найден запрос GetAllocatedCouponInfoReq по его GUIDResp';
          v_cnt_guid := 0;
        when others then raise;
      end;
    END IF;
      
    IF v_cnt_guid = 1 THEN 
      BEGIN

        IF UPPER(TRIM(v_reqDias.t_MarketPlace)) = 'NRD' THEN
            v_Market_Code := RSB_Common.GetRegStrValue('SECUR\MICEX_CODE');
        ELSE
            v_Market_Code := RSB_Common.GetRegStrValue('SECUR\SPBEX_CODE');
        END IF;

        select t_objectid into v_MarketID from dobjcode_dbt where t_objecttype = 3 and t_codekind = 1 and t_state = 0 and t_code = v_Market_Code;

        SELECT sf_mp.T_ID INTO v_ContractID 
          FROM ddlcontr_dbt dlc, ddlcontrmp_dbt mp, dsfcontr_dbt sf_mp 
         WHERE dlc.t_SfContrID = v_reqdias.T_CONTRACTID 
           AND mp.t_DlContrID = dlc.t_DlContrID 
           AND sf_mp.t_ID = mp.t_SfContrID 
           AND sf_mp.t_ServKind = 1
           AND mp.t_MarketID = v_MarketID
           AND (sf_mp.t_DateClose = TO_DATE('01.01.0001','DD.MM.YYYY') or sf_mp.t_DateClose >= v_reqdias.T_FIXINGDATE) 
           AND ROWNUM = 1;

        for errorlist in (
          select  to_number(extractvalue( value(t), 'Error/ErrorCode',v_namespace))     as ErrorCode,
                  extractvalue( value(t), 'Error/ErrorDesc',v_namespace)                as ErrorDesc
           from table(
              xmlsequence(
                extract( v_xml, '/SendAllocatedCouponResultReq/ErrorList/Error', v_namespace )
                )
              ) t ) loop
          /* запуск обработки */
          IF errorlist.ErrorCode <> 0 THEN
            DELETE FROM DNPTXLNKDIASPAY_DBT
             WHERE T_PAYMENTID = v_reqdias.T_PAYMENTID
               AND T_NKDBTSOBJID = 0;


            UPDATE DNPTXNKDREQDIAS_DBT
               SET T_STATUS = IT_DIASOFT.NPTXNKDREQDIAS_STATUS_NOTCONFIRMED,
                   T_ERRORCODE = errorlist.ErrorCode,
                   T_ERROR = errorlist.ErrorDesc,
                   T_GUIDRESULT = v_GUIDResult
             WHERE T_ID = v_reqdias.T_ID;

          ELSE

            FOR one_pay IN (SELECT DP.T_ID, DP.T_RECEIVEDCOUPONAMOUNT, TK.T_BOFFICEKIND, TK.T_DEALID
                              FROM DNPTXLNKDIASPAY_DBT DP, DDL_TICK_DBT TK
                             WHERE DP.T_PAYMENTID = v_reqdias.T_PAYMENTID
                               AND DP.T_NKDBTSOBJID = 0
                               AND TK.T_DEALID = DP.T_BUYID
                           )
            LOOP
              v_txObj.T_OBJID := 0;

              v_txObj.T_OUTSYSTCODE := 'DEPO';
              v_txObj.T_OUTOBJID    := v_reqdias.T_PAYMENTID;
              v_txObj.T_SOURCEOBJID := 0;
    
              v_txObj.T_ANALITICKIND1 := (CASE WHEN one_pay.T_BOFFICEKIND = RSB_SECUR.DL_SECURITYDOC THEN RSI_NPTXC.TXOBJ_KIND1010 WHEN one_pay.T_BOFFICEKIND = RSB_SECUR.DL_AVRWRT THEN RSI_NPTXC.TXOBJ_KIND1070 ELSE 0 END);
              v_txObj.T_ANALITIC1     := one_pay.T_DEALID;
              v_txObj.T_ANALITICKIND2 := RSI_NPTXC.TXOBJ_KIND2030;
              v_txObj.T_ANALITIC2     := v_reqdias.T_COUPONNUMBER;
              v_txObj.T_ANALITICKIND3 := RSI_NPTXC.TXOBJ_KIND3010;
              v_txObj.T_ANALITIC3     := v_reqdias.T_FIID;
              v_txObj.T_ANALITICKIND4 := RSI_NPTXC.TXOBJ_KIND4010;
              v_txObj.T_ANALITIC4     := RSI_NPTO.Market2dates(v_reqdias.T_FIID, v_reqdias.T_FIXINGDATE, NULL);
              v_txObj.T_ANALITICKIND5 := RSI_NPTXC.TXOBJ_KIND5010;
              v_txObj.T_ANALITIC5     := npto.GetPaperTaxGroupNPTX(v_reqdias.T_FIID);
              v_txObj.T_ANALITICKIND6 := RSI_NPTXC.TXOBJ_KIND6020;
              v_txObj.T_ANALITIC6     := v_ContractID;
              v_txObj.T_DATE          := v_reqdias.T_FIXINGDATE;
              v_txObj.T_CLIENT        := v_reqdias.T_PARTYID;
              v_txObj.T_LEVEL         := 2;
              v_txObj.T_USER          := CHR(0);
              v_txObj.T_TECHNICAL     := CHR(0);
              v_txObj.T_KIND          := RSI_NPTXC.TXOBJ_NKDB_TS;
              v_txObj.T_DIRECTION     := RSI_NPTXC.TXOBJ_DIR_OUT;
              v_txObj.T_FROMOUTSYST   := CHR(0);
              v_txObj.T_SUM           := one_pay.T_RECEIVEDCOUPONAMOUNT;
              v_txObj.T_CUR           := RSI_RSB_FIInstr.NATCUR;
              v_txObj.T_SUM0          := one_pay.T_RECEIVEDCOUPONAMOUNT;

              INSERT INTO DNPTXOBJ_DBT VALUES v_txObj RETURNING T_OBJID INTO v_txObj.T_OBJID;

              UPDATE DNPTXLNKDIASPAY_DBT 
                 SET T_NKDBTSOBJID = v_txObj.T_OBJID,
                     T_CONFIRMDATE = TRUNC(SYSDATE) 
               WHERE T_ID = one_pay.T_ID;

            END LOOP;

            UPDATE DNPTXNKDREQDIAS_DBT
               SET T_STATUS = IT_DIASOFT.NPTXNKDREQDIAS_STATUS_PROCESSED,
                   T_ERRORCODE = 0,
                   T_ERROR = CHR(1),
                   T_GUIDRESULT = v_GUIDResult
             WHERE T_ID = v_reqdias.T_ID;

          END IF;

        end loop;
      EXCEPTION
        when others then 
          o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
          o_MSGText := 'Не удалось разобрать xml по причине: неверный формат '||SQLERRM;
          it_log.log(o_MSGText,it_log.C_MSG_TYPE__ERROR, p_messbody);
      END;
    ELSE 
      it_log.log('Не найдены данные в таблице DNPTXNKDREQDIAS_DBT по GUIDResp. Не обрабатывается ',it_log.C_MSG_TYPE__MSG, p_messbody);
    END IF;
   
  end;

  -- BOSS-1984 СОФР. Мониторинг ошибок по BIQ-13034 Отправка события .
  procedure SendErrorEvent(p_ErrorCode  in integer
                          ,p_Head       in varchar2
                          ,p_Text       in clob
                          ,p_monitoring in boolean default false) as
  pragma autonomous_transaction;
  begin
      if nvl(p_ErrorCode, 0) != 0
       and (p_monitoring or p_ErrorCode in (10, 13, 20000))
    then
      it_event.RegisterError(p_SystemId => 'СОФР.BIQ-13034', p_ServiceName => p_Head, p_ErrorCode => p_ErrorCode, p_ErrorDesc => p_Text, p_LevelInfo => 2);
      if p_monitoring
      then
         rsb_payments_api.InsertEmailNotify(76, p_Head, p_Text);
      end if;
    commit;
    end if;
  end;

  /*BIQ-1304  
  Отбирает записи PKO_WriteOff, где ExpirationDate < Текущей календарной даты и не IsLimitCorrected и не IsCanceled и не IsCompleted,
  проставляет признак IsCanceled и CancelationTimestamp = текущая дата/время
  Добавляет записи с id операции в очередь для вызова макроса diasoft_Pko_CancelExpiredOrders */
  procedure MarkExpiredOrders(p_CalcDate date default trunc(sysdate)) as
    pragma autonomous_transaction;
    v_ErrorCode number := 0;
    v_ErrorDesc varchar2(2000);
    v_ntmp      number;
  begin
    for cur in (select t.t_dealid
                      ,t.t_dealtype
                      ,w.*
                  from PKO_WriteOff w
                  join ddl_tick_dbt t
                    on t.t_dealid = w.dealid
                 where w.expirationdate < p_CalcDate
                   and w.opertype = 2
                   and w.pkostatus != 7
                   and nvl(w.islimitcorrected, chr(0)) != 'X'
                   and nvl(w.iscanceled, chr(0)) != 'X'
                   and nvl(w.iscompleted, chr(0)) != 'X'
                   and w.operationid is not null
                   and t.t_dealtype != 2010)
    loop
      begin
        select w.id
          into v_ntmp
          from PKO_WriteOff w
         where w.id = cur.id
           and nvl(w.iscanceled, chr(0)) != 'X'
           for update nowait;
      exception
        when others then
          it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.id || ' занят другим процессом');
          continue;
      end;
      Set_DealDate(p_deailid => cur.dealid, p_finalstatusdate => trunc(cur.operationtimeora), o_ErrorCode => v_ErrorCode, o_ErrorDesc => v_ErrorDesc);
      if v_ErrorCode = 0
      then
        it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.id || ' (diasoft_Pko_CancelExpiredOrders)');
        nontrading_secur_orders_utils.push_to_cancel(p_pko_id => cur.id);

        update PKO_WriteOff w
           set w.iscanceled           = 'X'
              ,w.cancelationtimestamp = systimestamp
         where w.id = cur.id;
        Rsb_Secur.SetDealAttrID(cur.dealid, sysdate, 1 /*Да*/, 213);
        commit;
      else
        rollback;
        it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.id || ' Error#' || v_errorCode || ':' || v_ErrorDesc, p_msg_type => it_log.C_MSG_TYPE__ERROR);
      end if;
    end loop;
  exception
    when others then
      rollback;
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
  end;

  procedure start_Pko_NoSecurities(p_WriteOffid  number
                                  ,p_operationid number
                                  ,p_Dealid      number
                                  ,o_ErrorCode   out number
                                  ,o_ErrorDesc   out varchar2
                                  ,p_send_notify number default 0 -- 0- не отправлять 
                                  ,p_Head       varchar2 default null
                                  ,p_Text       clob default null
                                   ) as
  begin
   it_log.log(p_msg => 'PKO_WriteOff.id =' || p_WriteOffid );
   Rsb_Secur.SetDealAttrID(p_Dealid, sysdate, 2 /*Нет*/, 215);
    --  diasoft_Pko_NoSecurities
    diasoft_pko_funcobj_creator(typeMacros => 3
                               ,funcobjParameter => p_operationid
                               ,p_id => p_WriteOffid
                               ,o_ErrorCode => o_ErrorCode
                               ,o_ErrorDesc => o_ErrorDesc);
     if p_send_notify != 0 and o_ErrorCode = 0 then  
        rsb_payments_api.InsertEmailNotify(76, p_Head,p_Text);
     end if ;

  end;

  procedure start_Pko_blockSecurities_Open(p_WriteOffid  number
                                          ,p_operationid number
                                          ,p_Dealid      number
                                          ,o_ErrorCode   out number
                                          ,o_ErrorDesc   out varchar2) as
    v_ntmp number;
  begin
   it_log.log(p_msg => 'PKO_WriteOff.id =' || p_WriteOffid );
    begin
    select w.id into v_ntmp  from PKO_WriteOff w
       where w.id = p_WriteOffid
       and nvl(w.islimitcorrected, chr(0)) != 'X'
       for update nowait;
    exception
      when others then
        it_log.log(p_msg => 'PKO_WriteOff.id =' || p_WriteOffid||' занят другим процессом' );
        return ;
    end;
    Rsb_Secur.SetDealAttrID(p_Dealid, sysdate, 1 /*Да*/, 215);

    update PKO_WriteOff w
       set w.islimitcorrected         = 'X'
          ,w.limitcorrectiontimestamp = systimestamp
     where w.id = p_WriteOffid
       and nvl(w.islimitcorrected, chr(0)) != 'X';
    if sql%rowcount > 0
    then
      diasoft_pko_funcobj_creator(typeMacros => 2
                                 ,funcobjParameter => p_operationid
                                 ,p_id => p_WriteOffid
                                 ,o_ErrorCode => o_ErrorCode
                                 ,o_ErrorDesc => o_ErrorDesc);
    end if;
  end;


  function Get_PKO_ISIN(p_xml_from_diasoft pko_writeoff.xml_from_diasoft%type) return varchar2 as
   v_namespace       varchar2(128) := it_kafka.get_namespace(p_system_name => it_diasoft.C_C_SYSTEM_NAME, p_rootelement => 'SendPkoInfoReq');
   v_ISIN            davoiriss_dbt.t_isin%type;
  begin
     with oper as
        (select XMLType(p_xml_from_diasoft) xml from dual)
       select ExtractValue(t.xml,'SendPkoInfoReq/ISIN', v_namespace) into v_ISIN from oper t;
    return v_ISIN;
  end; 
  
  function get_DealRecv(p_deailid       number
                       ,o_dealtype      out number 
                       ,o_clientid      out number
                       ,o_clientcontrid out number
                       ,o_pfi           out number
                       ,o_PKO_opertype  out number 
                       ,o_principal     out number
                       ,o_id_operation  out number
                       ,o_marketid      out number
                       ,o_ClientCode    out varchar2
                       ,o_DlContrID     out number) return number as
    v_ErrorCode number;
    v_ErrorDesc varchar2(2000);
  begin
    select tick.t_clientid --идентификатор клиента
          ,tick.t_clientcontrid ---Идентфикатор субдоговора
          ,tick.t_dealtype 
          ,leg.t_pfi -- Идентификатор ценной бумаги
          ,leg.t_principal -- Количество ценных бумаг
          ,op.t_id_operation -- Идентификатор операции
          ,cmp.t_marketid -- 
          ,cmp.t_mpcode
          ,cmp.t_DlContrID
          ,pko.opertype
      into o_clientid
          ,o_clientcontrid
          ,o_dealtype
          ,o_pfi
          ,o_principal
          ,o_id_operation
          ,o_marketid
          ,o_ClientCode
          ,o_DlContrID
          ,o_PKO_opertype
      from ddl_tick_dbt tick
     inner join ddl_leg_dbt leg
        on leg.t_dealid = tick.t_dealid
     inner join pko_writeoff pko on pko.dealid = tick.t_dealId
      left join ddlcontrmp_dbt cmp
        on cmp.t_SfContrID = tick.t_clientcontrid
      left join doproper_dbt op
        on op.t_dockind = 127
       and op.t_documentid = LPAD(tick.t_dealid, 34, 0)
     where tick.t_dealId = p_deailid; --Идентификатор сделки 
    return 1;
  exception
    when no_data_found then
      return 0;
    when others then
      v_errorCode := abs(sqlcode);
      v_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error#' || v_errorCode || ':' || v_ErrorDesc, p_msg_type => it_log.C_MSG_TYPE__ERROR);
  end;

   function get_RestFI(p_pfi number, p_clientid number, p_clientcontrid number, p_dayCalc date) return number as
     v_SumQuantity number;
   begin
     select NVL(sum(lot.t_Amount), 0)
       into v_SumQuantity
       from v_scwrthistex lot
      where lot.t_Amount > 0
        and lot.t_DocKind in (29, 135)
        and lot.t_DocID > 0
        and lot.t_state = 1
        and lot.t_contract = p_clientcontrid --%IdСубдоговора
        and lot.t_Party = p_clientid -- %IdКлиента --- 
        and lot.t_fiid = p_pfi -- %IdЦеннойБумаги
        and lot.t_portfolio = 0
        and lot.t_Buy_Sale in (RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY, RSB_PMWRTOFF.PM_WRITEOFF_SUM_BUY_BO)
        and lot.t_ChangeDate < trunc(p_dayCalc) --:ДатаРасчета 
        and decode(lot.t_Instance
                  ,(select max(bc.t_Instance)
                     from v_scwrthistex bc
                    where bc.t_SumID = lot.t_SumID
                      and bc.t_ChangeDate < trunc(p_dayCalc) --:ДатаРасчета
                   )
                  ,1
                  ,0) = 1;
    return v_SumQuantity;
   end;
   
  procedure Get_forPLAN(p_pfi number
                       ,p_clientid      number
                       ,p_clientcontrid number
                       ,p_dayCalc       date
                       ,o_for_plan1     out number
                       ,o_for_plan2     out number) as
  begin
    with q as
     (select *
        from ddl_tick_dbt tk
       where tk.t_ClientID = p_clientid --%IdКлиента
         and tk.t_ClientContrID = p_clientcontrid --%IdСубдоговора
      )
    select NVL(sum(case
                     when rq.t_Kind = 0 then
                      rq.t_Amount
                     else
                      -rq.t_Amount
                   end)
              ,0) for_plan1 ---t_Kind: 0 =требование, 1=обязательство 
          ,NVL(sum(case
                     when  rq.t_FactDate !=  TO_DATE('01.01.0001', 'DD.MM.YYYY') and rq.t_FactDate <= p_dayCalc -- :ДатаРасчета  
                      then
                      case
                        when rq.t_Kind = 0 then
                         rq.t_Amount
                        else
                         -rq.t_Amount
                      end
                     else
                      0
                   end)
              ,0) for_plan2
      into o_for_plan1
          ,o_for_plan2
      from ddlrq_dbt rq
          ,q
     where rq.t_DocKind = q.t_BOfficeKind
       and rq.t_DocID = q.t_DealID
       and rq.t_Type in (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY)
       and rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
       and rq.t_FIID = p_pfi --:IdЦеннойБумаги
       and rq.t_state not in ( -1,7)  --bpv технический статус чтобы отрубить старые неисполненные ТО и в статусе "отказ"
       and (rq.t_FactDate = TO_DATE('01.01.0001', 'DD.MM.YYYY') or rq.t_FactDate >= p_dayCalc );
    return;
  end;
  
  function Get_PKO_limitcorrected(p_pfi           number
                                 ,p_clientid      number
                                 ,p_clientcontrid number
                                 ,p_dayCalc       date) return number as
    v_open_writeoff number;
  begin
    
    select nvl(sum(pw.qnty), 0)
      into v_open_writeoff
      from pko_writeoff pw
     inner join ddl_tick_dbt tick
        on tick.t_dealid = pw.dealid
     where pw.clientid = p_clientid --:IdКлиента
       and pw.securityID = p_pfi -- :IdЦеннойБумаги
       and tick.t_clientcontrid = p_clientcontrid --:IdСубдоговора
       and pw.opertype = 2
       and pw.islimitcorrected = 'X'
       and nvl(pw.iscanceled, chr(0)) != 'X'
       and nvl(pw.iscompleted, chr(0)) != 'X'
       and not exists(select 1
              from ddlrq_dbt rq
             where rq.t_DocKind = tick.t_BOfficeKind
               and rq.t_DocID = tick.t_DealID
               and rq.t_Type in (RSI_DLRQ.DLRQ_TYPE_DELIVERY, RSI_DLRQ.DLRQ_TYPE_COMPDELIVERY)
               and rq.t_SubKind = RSI_DLRQ.DLRQ_SUBKIND_AVOIRISS
               and rq.t_FIID = p_pfi --:IdЦеннойБумаги
               and rq.t_state <> -1
               and rq.t_FactDate !=  TO_DATE('01.01.0001', 'DD.MM.YYYY') and rq.t_FactDate <= p_dayCalc);
    return v_open_writeoff;
  end;

  
  
  procedure Set_GRSOBUDate(p_deailid     number
                           ,p_GRSOBUDate date) as
  type tt_tmp is table of number;
  vt_tmp tt_tmp;
  begin                         
     --
    select gr.t_id
      bulk collect
      into vt_tmp
      from ddlrq_dbt gr
     where gr.t_docid = p_deailid
       and gr.t_dockind = 127
       for update wait 3;
    if vt_tmp.count > 0
    then
      update ddlrq_dbt gr
         set gr.t_plandate = p_GRSOBUDate,
            gr.t_changedate = p_GRSOBUDate
       where gr.t_docid = p_deailid
         and gr.t_dockind = 127;
    end if;

    --
    select gr.t_id
      bulk collect
      into vt_tmp
      from ddl_leg_dbt gr
    where gr.t_dealid =  p_deailid
       for update wait 3;
    if vt_tmp.count > 0
    then
      update ddl_leg_dbt gr
         set gr.t_maturity = p_GRSOBUDate,
            gr.t_expiry = p_GRSOBUDate
       where gr.t_dealid =  p_deailid;
    end if;
  end;
  
  
  procedure Set_DealDate(p_deailid      number
                        ,p_finalstatusdate date
                        ,o_ErrorCode       out number
                        ,o_ErrorDesc       out varchar2) as
    pragma autonomous_transaction;
    v_clientid      ddl_tick_dbt.t_clientid%type; --идентификатор клиента
    v_clientcontrid ddl_tick_dbt.t_clientcontrid%type; ---Идентфикатор субдоговора
    v_pfi           ddl_leg_dbt.t_pfi %type; -- Идентификатор ценной бумаги
    v_principal     ddl_leg_dbt.t_principal %type; -- Количество ценных бумаг
    v_id_operation  doproper_dbt.t_id_operation %type; -- Идентификатор операции  
    v_marketid      ddlcontrmp_dbt.t_marketid%type;
    v_ClientCode    ddlcontrmp_dbt.t_mpcode%type;
    v_dlcontrid     ddlcontrmp_dbt.t_dlcontrid%type;
    v_PKO_opertype  pko_writeoff.opertype%type;
    type tt_tmp is table of number;
    vt_tmp tt_tmp;
    v_dealtype       ddl_tick_dbt.t_dealtype%type; 
    v_WriteOffId  pko_writeoff.id%type;
  begin
    if get_DealRecv(p_deailid => p_deailid
                   ,o_dealtype => v_dealtype
                   ,o_clientid => v_clientid
                   ,o_clientcontrid => v_clientcontrid
                   ,o_pfi => v_pfi
                   ,o_PKO_opertype => v_PKO_opertype
                   ,o_principal => v_principal
                   ,o_id_operation => v_id_operation
                   ,o_marketid => v_marketid
                   ,o_ClientCode => v_ClientCode
                   ,o_dlcontrid => v_dlcontrid) = 0
    then
      o_ErrorCode := 20;
      o_ErrorDesc := 'Сделка dealId = ' || p_deailid || ' не найдена ';
      rollback;
      return;
    end if;
    o_ErrorCode := 0;
    --
    select s.t_id_operation
      bulk collect
      into vt_tmp
      from DOPRSTEP_DBT s
     where s.t_id_operation = v_id_operation
       and s.t_id_step in (2, 3)
       for update wait 3;
    if vt_tmp.count > 0
    then
      update DOPRSTEP_DBT s
         set s.t_plan_date   = p_finalstatusdate
            ,s.t_accountdate = p_finalstatusdate
       where s.t_id_operation = v_id_operation
         and s.t_id_step in (2, 3);
    end if;
    --
    select t.t_dealid bulk collect into vt_tmp from ddl_tick_dbt t where t.t_dealid = p_deailid for update wait 3;
    if vt_tmp.count > 0
    then
      update ddl_tick_dbt t set t.t_dealdate = p_finalstatusdate where t.t_dealid = p_deailid;
    end if;
    --
    select do.t_id_operation
      bulk collect
      into vt_tmp
      from doprdates_dbt do
     where do.t_id_operation = v_id_operation
       and do.t_datekindid = 12700000
       for update wait 3;
    if vt_tmp.count > 0
    then
      update doprdates_dbt do
         set do.t_date = p_finalstatusdate
       where do.t_id_operation = v_id_operation
         and do.t_datekindid = 12700000;
    end if;
    --
    select gr.t_id
      bulk collect
      into vt_tmp
      from ddlgrdeal_dbt gr
     where gr.t_docid = p_deailid
       and gr.t_dockind = 127
       for update wait 3;
    if vt_tmp.count > 0
    then
      update ddlgrdeal_dbt gr
         set gr.t_plandate = p_finalstatusdate
       where gr.t_docid = p_deailid
         and gr.t_dockind = 127;
    end if;
    
    Set_GRSOBUDate(p_deailid,p_finalstatusdate);
    
    select max(pko.id) into v_WriteOffId from pko_writeoff pko where pko.dealid = p_deailid ;

    it_log.log(p_msg => 'PKO_WriteOff.id =' ||v_WriteOffId||' Для dealid '||p_deailid||' дата сделки '||to_char(p_finalstatusdate,'dd.mm.yyyy') );    
    
    commit;
  exception
    when others then
      rollback;
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg_clob => sys.dbms_utility.format_error_backtrace);
      it_error.clear_error_stack;
      o_ErrorCode := 20000;
      o_ErrorDesc := 'Ощибка установки даты исполнения операции';
  end;

  /*BIQ-1304 Рассчитывается плановый остаток = фактический остаток + плановые требования - плановые обязательства.
  Добавляет записи с id операции в очередь для вызова макроса diasoft_Pko_blockSecurities_Open   или diasoft_Pko_NoSecurities по условию*/
  procedure CheckSecuritiesOTC(p_WriteOffid number
                              ,o_ErrorCode  out number
                              ,o_ErrorDesc  out varchar2
                              ,p_send_notify number default 0 -- 0- не отправлять 1- отправка при недостаточности ЦБ  
                              ,p_Head       varchar2 default null
                              ,p_Text       clob default null
                               ) as
    v_SumQuantity   number;
    v_for_plan1     number;
    v_for_plan2     number;
    v_open_writeoff number;
    v_clientid      ddl_tick_dbt.t_clientid%type; --идентификатор клиента
    v_clientcontrid ddl_tick_dbt.t_clientcontrid%type; ---Идентфикатор субдоговора
    v_pfi           ddl_leg_dbt.t_pfi %type; -- Идентификатор ценной бумаги
    v_principal     ddl_leg_dbt.t_principal %type; -- Количество ценных бумаг
    v_id_operation  doproper_dbt.t_id_operation %type; -- Идентификатор операции  
    v_marketid      ddlcontrmp_dbt.t_marketid%type;
    v_ClientCode    ddlcontrmp_dbt.t_mpcode%type;
    v_dlcontrid     ddlcontrmp_dbt.t_dlcontrid%type;
    v_PKO_opertype  PKO_WriteOff.Opertype%type;
    v_dayCalc       date := trunc(sysdate); --ДатаРасчета
    r_WriteOff      PKO_WriteOff%rowtype;
    v_dealtype       ddl_tick_dbt.t_dealtype%type;
    v_is_enough_limits boolean;
  begin
    o_ErrorCode := 0;
    begin
      select * into r_WriteOff from PKO_WriteOff w where w.id = p_WriteOffid;
    exception
      when others then
        o_ErrorCode := 10;
        o_ErrorDesc := 'Ошибка поиска записи ID= ' || p_WriteOffid || ' в картотеке (PKO_WriteOff)';
        return;
    end;
    if get_DealRecv(p_deailid => r_WriteOff.Dealid
                   ,o_dealtype => v_dealtype
                   ,o_clientid => v_clientid
                   ,o_clientcontrid => v_clientcontrid
                   ,o_pfi => v_pfi
                   ,o_PKO_opertype => v_PKO_opertype
                   ,o_principal => v_principal
                   ,o_id_operation => v_id_operation
                   ,o_marketid => v_marketid
                   ,o_ClientCode => v_ClientCode
                   ,o_dlcontrid => v_dlcontrid) = 0
       or v_PKO_opertype not in (2, 102)
    then
      o_ErrorCode := 20;
      o_ErrorDesc := 'Сделка dealId = ' || r_WriteOff.Dealid || ' не найдена (it_diasoft.CheckSecuritiesOTC)';
      return;
    end if;
    if v_dealtype != 2010 then
      v_SumQuantity := get_RestFI(p_pfi => v_pfi, p_clientid => v_clientid, p_clientcontrid => v_clientcontrid, p_dayCalc => v_dayCalc);
      Get_forPLAN(p_pfi => v_pfi
                ,p_clientid => v_clientid
                ,p_clientcontrid => v_clientcontrid
                ,p_dayCalc => v_dayCalc
                ,o_for_plan1 => v_for_plan1
                ,o_for_plan2 => v_for_plan2);
      v_open_writeoff := Get_PKO_limitcorrected(p_pfi => v_pfi, p_clientid => v_clientid, p_clientcontrid => v_clientcontrid, p_dayCalc => v_dayCalc);

      v_is_enough_limits := v_SumQuantity + v_for_plan1 >= 0 and v_SumQuantity + V_for_plan2 - v_open_writeoff - r_WriteOff.Qnty >= 0;
      if nontrading_secur_orders_utils.is_voluntary_redemption_by_row(p_pko_row => r_WriteOff) = 1
      then
        if v_is_enough_limits
        then
          nontrading_secur_orders_utils.set_is_enough_quantity(p_deal_id => r_WriteOff.dealid, p_is_enough_quantity => 1);
          nontrading_secur_orders_utils.set_is_limit_corrected(p_pko_id => r_WriteOff.id);
          nontrading_secur_orders_utils.set_wait_status(p_pko_id => r_WriteOff.id, p_is_wait => 1);
          it_sinv.send_nontrade_limit_state(p_deal_id => r_WriteOff.dealid, p_limit_status => 1);
          nontrading_secur_orders_utils.push_to_execute_deal(p_deal_id => r_WriteOff.dealid);
        else
          nontrading_secur_orders_utils.set_is_enough_quantity(p_deal_id => r_WriteOff.dealid, p_is_enough_quantity => 0);
          --тут должна быть отправка сообщения в it_sinv о недостаточности лимитов. Но сообщение будет отправлено из макроса diasoft_Pko_CancelExpiredOrders.mac
          nontrading_secur_orders_utils.push_to_cancel(p_pko_id => r_WriteOff.id);
        end if;
      else
        if v_is_enough_limits
        then
          start_Pko_blockSecurities_Open(p_WriteOffid => r_WriteOff.Id
                                        ,p_operationid => r_WriteOff.Operationid
                                        ,p_Dealid => r_WriteOff.Dealid
                                        ,o_ErrorCode => o_ErrorCode
                                        ,o_ErrorDesc => o_ErrorDesc);
        else
          start_Pko_NoSecurities(p_WriteOffid => r_WriteOff.Id
                                ,p_operationid => r_WriteOff.Operationid
                                ,p_Dealid => r_WriteOff.Dealid
                                ,o_ErrorCode => o_ErrorCode
                                ,o_ErrorDesc => o_ErrorDesc
                                ,p_send_notify => case when p_send_notify = 1 then 1 else 0 end  
                                ,p_Head => p_Head  
                                ,p_Text => p_Text);
    
        end if;
      end if;
    else
       it_log.log(p_msg => 'PKO_WriteOff.id =' ||r_WriteOff.Id ||' dealtype = 2010 контроль достаточности бумаг не проводится ' );
    end if;  
  exception
    when others then
      o_errorCode := abs(sqlcode);
      o_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'Error#' || o_errorCode || ':' || o_ErrorDesc, p_msg_type => it_log.C_MSG_TYPE__ERROR);
  end;

   -- BIQ-1304 Вызывается в конце процедуры расчета лимитов
   procedure PKO_CheckAndCorrectSecuritiesLimits(p_MarketID       number
                                                ,p_CalcDate       date
                                                ,p_UseListClients number) as
     pragma autonomous_transaction;
     v_ErrorCode       number;
     v_ErrorDesc       varchar2(2000);
     v_clientid        ddl_tick_dbt.t_clientid%type; --идентификатор клиента
     v_clientcontrid   ddl_tick_dbt.t_clientcontrid%type; ---Идентфикатор субдоговора
     v_pfi             ddl_leg_dbt.t_pfi %type; -- Идентификатор ценной бумаги
     v_principal       ddl_leg_dbt.t_principal %type; -- Количество ценных бумаг
     v_id_operation    doproper_dbt.t_id_operation %type; -- Идентификатор операции  
     v_marketid        ddlcontrmp_dbt.t_marketid%type;
     v_ClientCode      ddlcontrmp_dbt.t_mpcode%type;
     v_dlcontrid       ddlcontrmp_dbt.t_dlcontrid%type;
     v_PKO_opertype    PKO_WriteOff.Opertype%type;
     v_calc_panelcontr DDL_PANELCONTR_DBT.T_CALC_SID%type := RSHB_RSI_SCLIMIT.g_calc_panelcontr;
     v_tmp             number;
     v_dealtype        ddl_tick_dbt.t_dealtype%type;
     v_notify_Head     DEMAIL_NOTIFY_DBT.T_HEAD%type;
     v_notify_Text     DEMAIL_NOTIFY_DBT.T_TEXT%type;
     v_notify_Send     number;
     v_CalcDateLast    date := RSHB_RSI_SCLIMIT.GetCheckDateByParams(p_Kind => 1, p_Date => p_CalcDate, p_MarketID => -1, p_IsEDP => 1) - 1 ;
   begin
     if p_CalcDate < trunc(sysdate)
     then
       return;
     end if;
     if p_MarketID = RSHB_RSI_SCLIMIT.GetSpbexID
     then
       MarkExpiredOrders(p_CalcDate);
     end if;
     for cur in (select *
                   from PKO_WriteOff w
                   join ddl_tick_dbt t on t.t_dealid = w.dealid 
                  where w.opertype = 2
                    and nvl(w.islimitcorrected, chr(0)) = 'X'
                    and nvl(w.iscanceled, chr(0)) != 'X'
                    and nvl(w.iscompleted, chr(0)) != 'X'
                    and t.t_dealtype != 2010)
     loop
       if get_DealRecv(p_deailid => cur.dealid
                      ,o_dealtype => v_dealtype
                      ,o_clientid => v_clientid
                      ,o_clientcontrid => v_clientcontrid
                      ,o_pfi => v_pfi
                      ,o_PKO_opertype => v_PKO_opertype
                      ,o_principal => v_principal
                      ,o_id_operation => v_id_operation
                      ,o_marketid => v_marketid
                      ,o_ClientCode => v_ClientCode
                      ,o_dlcontrid => v_dlcontrid) = 0
       then
         it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.Id || '(islimitcorrected) параметры сделки не определены ');
         continue;
       elsif v_marketid != p_MarketID
       then
         continue;
       elsif p_UseListClients = 1
       then
         select count(*)
           into v_tmp
           from DDL_PANELCONTR_DBT p
          where t_calc_sid = v_calc_panelcontr
            and p.t_dlcontrid = v_dlcontrid
            and p.t_setflag = chr(88);
         if v_tmp = 0
         then
           it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.Id || '(islimitcorrected) dlcontrid =' || v_dlcontrid || ' не в списке расчета ' || v_calc_panelcontr);
           continue;
         end if;
       end if;
       update DDL_LIMITSECURITES_DBT lim
          set lim.t_open_balance    = lim.t_open_balance - cur.qnty
             ,lim.t_plan_minus_deal = lim.t_plan_minus_deal + cur.qnty
        where lim.t_date = p_CalcDate
          and lim.t_market = v_marketid
          and lim.t_client_code = v_ClientCode
          and lim.t_security = v_pfi
          and lim.t_limit_kind in (0, 1, 2, 365);
       if sql%rowcount = 0
       then
         it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.Id || '(islimitcorrected) лимиты не найдены ');
       end if;
     end loop;
     commit;
     -- PKO_WriteOff, где StartWriteOffDate<=Текущей календарной даты и не IsLimitCorrected и не IsCanceled и не IsCompleted 
     for cur in (select *
                   from PKO_WriteOff w
                   join ddl_tick_dbt t on t.t_dealid = w.dealid 
                  where w.opertype = 2
                    and w.pkostatus != 7
                    and w.expirationdate >= p_CalcDate
                    and w.StartWriteOffDate <= p_CalcDate
                    and nvl(w.islimitcorrected, chr(0)) != 'X'
                    and nvl(w.iscanceled, chr(0)) != 'X'
                    and nvl(w.iscompleted, chr(0)) != 'X'
                    and t.t_dealtype != 2010
                  order by w.id)
     loop
       v_ErrorCode := 0;
       v_ErrorDesc := null;
       begin
         if get_DealRecv(p_deailid => cur.dealid
                        ,o_dealtype => v_dealtype
                        ,o_clientid => v_clientid
                        ,o_clientcontrid => v_clientcontrid
                        ,o_pfi => v_pfi
                        ,o_PKO_opertype => v_PKO_opertype
                        ,o_principal => v_principal
                        ,o_id_operation => v_id_operation
                        ,o_marketid => v_marketid
                        ,o_ClientCode => v_ClientCode
                        ,o_dlcontrid => v_dlcontrid) = 0
         then
           it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.Id || ' параметры сделки не определены ');
           continue;
         end if;
         v_notify_Head := 'Недостаточно ц/б в ВУ для списания по клиенту ' || cur.clientcode || ' по ц/б ' || Get_PKO_ISIN(cur.xml_from_diasoft);
         v_notify_Text := 'Просьба проверить входящие остатки клиента в ВУ и ДУ на предмет расхождений. При необходимости провести корректирующие операции';
         v_notify_Send := case
                            when cur.expirationdate between p_CalcDate and v_CalcDateLast then
                             0
                            else
                             1
                          end;
         if v_marketid = -1
            and v_dealtype != 2010
            and p_MarketID = RSHB_RSI_SCLIMIT.GetSpbexID
         then
           if p_UseListClients = 1
           then
             select count(*)
               into v_tmp
               from DDL_PANELCONTR_DBT p
              where t_calc_sid = v_calc_panelcontr
                and p.t_dlcontrid = v_dlcontrid
                and p.t_setflag = chr(88);
             if v_tmp = 0
             then
               it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.Id || ' dlcontrid =' || v_dlcontrid || ' не в списке расчета ' || v_calc_panelcontr);
               continue;
             end if;
           end if;
           CheckSecuritiesOTC(p_WriteOffid => cur.id
                             ,o_ErrorCode => v_ErrorCode
                             ,o_ErrorDesc => v_ErrorDesc
                             ,p_send_notify => v_notify_Send
                             ,p_Head => v_notify_Head
                             ,p_Text => v_notify_Text);
           if v_ErrorCode = 0
           then
             commit;
           else
             rollback;
             it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.Id || ' (CheckSecuritiesOTC) Error#' || v_ErrorCode, p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg_clob => v_ErrorDesc);
           end if;
           continue;
         elsif v_marketid != p_MarketID
         then
           continue;
         elsif p_UseListClients = 1
         then
           select count(*)
             into v_tmp
             from DDL_PANELCONTR_DBT p
            where t_calc_sid = v_calc_panelcontr
              and p.t_dlcontrid = v_dlcontrid
              and p.t_setflag = chr(88);
           if v_tmp = 0
           then
             it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.Id || ' dlcontrid =' || v_dlcontrid || ' не в списке расчета ' || v_calc_panelcontr);
             continue;
           end if;
         end if;
         select count(*)
           into v_tmp
           from (select lim.t_limit_kind
                       ,sum(lim.t_open_balance) as t_open_balance
                   from DDL_LIMITSECURITES_DBT lim
                  where lim.t_date = p_CalcDate
                    and lim.t_market = v_marketid
                    and lim.t_client_code = v_ClientCode
                    and lim.t_security = v_pfi
                    and lim.t_limit_kind in (0, 1, 2, 365)
                  group by lim.t_limit_kind)
          where t_open_balance >= cur.qnty;
         if v_tmp = 4 --  по всем 4м видам лимитов t_openbalance>=Qnty,
         then
           /*уменьшаются значения полей ddl_limitsecurities_dbt.t_openbalance и увеличиваются значение поля ddl_limitsecurities_dbt .t_plan_minus_deal
           в PKO_WriteOff проставляется LimitCorrectionTimeStamp и IsLimitCorrected*/
           update DDL_LIMITSECURITES_DBT lim
              set lim.t_open_balance    = lim.t_open_balance - cur.qnty
                 ,lim.t_plan_minus_deal = lim.t_plan_minus_deal + cur.qnty
            where lim.t_date = p_CalcDate
              and lim.t_market = v_marketid
              and lim.t_client_code = v_ClientCode
              and lim.t_security = v_pfi
              and lim.t_limit_kind in (0, 1, 2, 365);
           start_Pko_blockSecurities_Open(p_WriteOffid => cur.Id, p_operationid => cur.operationid, p_Dealid => cur.Dealid, o_ErrorCode => v_ErrorCode, o_ErrorDesc => v_ErrorDesc);
         else
           /*обавляет записи с id операции в очередь для вызова макроса diasoft_Pko_blockSecurities_Open 
           (если IsEnough= истина) или diasoft_Pko_NoSecurities - в противном случае*/
           start_Pko_NoSecurities(p_WriteOffid => cur.Id
                                 ,p_operationid => cur.operationid
                                 ,p_Dealid => cur.Dealid
                                 ,o_ErrorCode => v_ErrorCode
                                 ,o_ErrorDesc => v_ErrorDesc
                                 ,p_send_notify => v_notify_Send
                                 ,p_Head => v_notify_Head
                                 ,p_Text => v_notify_Text);
         end if;
       exception
         when others then
           v_ErrorCode := abs(sqlcode);
           v_ErrorDesc := substr(it_q_message.get_errtxt(p_sqlerrm => sqlerrm) || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace, 1, 2000);
       end;
       if v_ErrorCode = 0
       then
         commit;
       else
         rollback;
         it_log.log(p_msg => 'PKO_WriteOff.id =' || cur.Id || ' Error#' || v_ErrorCode, p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg_clob => v_ErrorDesc);
       end if;
     end loop;
     -- Отправка уведомдений об окончании срока ПКО 
     if p_MarketID = RSHB_RSI_SCLIMIT.GetSpbexID
     then
       for cur in (select d.t_shortname
                         ,w.*
                     from PKO_WriteOff w
                     join ddl_tick_dbt t
                       on t.t_dealid = w.dealid
                     left join dparty_dbt d
                       on d.t_partyid = w.Clientid
                    where w.expirationdate between p_CalcDate and v_CalcDateLast
                      and w.opertype = 2
                      and w.pkostatus != 7
                      and nvl(w.islimitcorrected, chr(0)) != 'X'
                      and nvl(w.iscanceled, chr(0)) != 'X'
                      and nvl(w.iscompleted, chr(0)) != 'X'
                      and w.operationid is not null
                      and t.t_dealtype != 2010)
       loop
         rsb_payments_api.InsertEmailNotify(76
                                           ,'Наступил последний день срока поручения на списание ц/б ' || cur.Dealcode || ' , клиент ' ||
                                            nvl(cur.t_shortname, '(ОШИБКА:НЕ ОПРЕДЕЛЕН)') || ' (' || cur.Clientcode || '). Ц/Б ' || Get_PKO_ISIN(cur.xml_from_diasoft)
                                           ,'Просьба в течении дня проверить достаточность текущих лимитов по клиенту в QUIK ' ||
                                            'и по сочетанию клавиш Ctrl-Z выбрать соответствующий пункт: "Отметить как заблокированные в лимитах" ' ||
                                            'при достаточном количестве ц/б или "Передать сообщение о недостаточности ц/б" при недостаточном');
       end loop;
     end if;
   end;
  /**
  @brief Создать событие в utableprocessevent_dbt
  @param[in] p_objecttype тип объекта (dfunc_dbt)
  @param[in] p_objectid id объекта 
  */
  procedure PutInProcess(p_objecttype number, p_objectid number)
  is
    recid uTableProcessEvent_dbt.t_recid%type := 0;
    Contr_number dsfcontr_dbt.t_number%type;
  begin
    BEGIN
      SELECT t_recid
        INTO recid
        FROM uTableProcessEvent_dbt a
       WHERE t_objecttype = p_objecttype AND t_objectid = p_objectid
         AND t_recid =
              (SELECT MAX (b.t_recid)
                 FROM uTableProcessEvent_dbt B
                WHERE b.t_objecttype = a.t_objecttype
                  AND b.t_objectid = a.t_objectid
                  AND b.t_status <> 4);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN recid := 0;
    END;
                        
    if recid = 0 then
      INSERT INTO uTableProcessEvent_dbt( t_timestamp, t_objecttype, t_objectid, t_type, t_status, t_note ) 
      VALUES( sysdate, p_objecttype, p_objectid, 2, 1, ''); 
    end if;

  exception 
    WHEN others THEN  
      select sf.t_number into Contr_number from dsfcontr_dbt sf where sf.t_id in (select dd.t_sfcontrid from ddlcontr_dbt dd where dd.t_dlcontrid = p_objectid);
      it_event.RegisterError(p_SystemId => 'Diasoft',p_ServiceName => 'GetBrokerContractDepo',p_ErrorCode => -1,p_ErrorDesc => 'Сервис ChangeDepositoryContractReq: Не удалось создать событие в utableprocessevent_dbt для договора '||Contr_number, p_LevelInfo => 8);      
  end;

  /**
  @brief Сервис для закрытия и обновления договора в ЦФТ (BIQ-7089)
  @param[in] p_objectid id объекта
  @param[in] p_typeaction вид действия: 1 - обновление, 3 - закрытие, 2 - вызов при смене ТП у договора   
  */
  procedure InsEvent_UpdateContract(p_objectid in number, p_typeaction in number)
  is
    Contr_number dsfcontr_dbt.t_number%type;
    CV_TYPE NUMBER := 5100; /* Тип Запуск выгрузки закрытия */
  begin
    INSERT INTO uTableProcessEvent_dbt( t_objecttype, t_objectid, t_type, t_status, t_timestamp )
               ( SELECT CV_TYPE, p_objectid, p_typeaction, 1, SYSDATE 
                   FROM DUAL
                  WHERE NOT EXISTS(SELECT t_recid 
                                     FROM utableprocessevent_dbt 
                                    WHERE t_objecttype = CV_TYPE
                                      AND t_objectid = p_objectid
                                      AND t_type = p_typeaction
                                      AND TRUNC(t_timestamp) = TRUNC(SYSDATE)
                                      AND t_status IN (1, 2) /* статус 4 исключаем, так как обновлений может быть несколько. Наверное. */
                                  )
                    AND ROWNUM = 1 ); /* На всякий случай */
  exception 
    WHEN others THEN  
      select sf.t_number into Contr_number from dsfcontr_dbt sf where sf.t_id in (select dd.t_sfcontrid from ddlcontr_dbt dd where dd.t_dlcontrid = p_objectid);
      it_event.RegisterError(p_SystemId => 'Diasoft',p_ServiceName => 'GetBrokerContractDepo',p_ErrorCode => -1,p_ErrorDesc => 'Сервис ChangeDepositoryContractReq: Не удалось вставить задание на обновление статуса в ЦФТ для договора '||Contr_number, p_LevelInfo => 8);      
  end InsEvent_UpdateContract;
 
  /**
  @brief Устанавливает примечание 
  @param[in] p_Dlcontrid тип объекта (dfunc_dbt)
  @param[in] p_Number id объекта 
  @param[in] p_objecttype тип объекта (dfunc_dbt)
  @param[in] p_NoteType id объекта 
  */
  procedure SetNoteText(p_Dlcontrid IN number,p_Number varchar2 default null, p_depodate date default null, p_objecttype number, p_NoteType number) 
  IS
    id_note integer := 0;
    Contr_number dsfcontr_dbt.t_number%type;
  begin
    begin
      select t_id into id_note 
        from dnotetext_dbt
       where t_objecttype = p_objecttype and t_notekind = p_NoteType 
         and t_documentid = LPAD(p_Dlcontrid, 34, 0) 
         and t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy');
    exception
      when no_data_found then
        null;
    end;

    RSB_Struct.readStruct('dnotetext_dbt');
       
    if id_note > 0 and p_Number is not null then
      update dnotetext_dbt set t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy'), t_text = rpad(utl_raw.cast_to_raw(c => p_Number), 3000, 0), t_date = trunc(sysdate) , t_time = to_date('01010001'||to_char(sysdate,'hh24miss'),'DDMMYYYYhh24miss')  where t_id = id_note;
    elsif id_note > 0 and p_depodate is not null then  
      update dnotetext_dbt set t_validtodate = to_date('31.12.9999', 'dd.mm.yyyy'), t_text =  RSB_Struct.PutDate('t_text', rpad('0',3000, '0'), p_depodate, (-1)*53) , t_date =  trunc(sysdate) , t_time =  to_date('01010001'||to_char(sysdate,'hh24miss'),'DDMMYYYYhh24miss') where t_id = id_note;
    elsif id_note = 0 and p_Number is not null then
      insert into dnotetext_dbt (t_objecttype,
                                 t_documentid,
                                 t_notekind,
                                 t_oper,
                                 t_date,
                                 t_time,
                                 t_text,
                                 t_validtodate,
                                 t_branch,
                                 t_numsession)
      VALUES (p_objecttype,
              LPAD(p_Dlcontrid, 34, 0),
              p_NoteType, 
              INTEGRATION_OPER,
              trunc(sysdate),
              to_date('01010001'||to_char(sysdate,'hh24miss'),'DDMMYYYYhh24miss'),
              rpad(utl_raw.cast_to_raw(c => p_Number), 3000, 0),
              to_date('31129999','ddmmyyyy'),
              1,
              0);                
                          
    elsif id_note = 0 and p_depodate is not null then 
      insert into dnotetext_dbt (t_objecttype,
                                 t_documentid,
                                 t_notekind,
                                 t_oper,
                                 t_date,
                                 t_time,
                                 t_text,
                                 t_validtodate,
                                 t_branch,
                                 t_numsession)
      VALUES (p_objecttype,
              LPAD(p_Dlcontrid, 34, 0),
              p_NoteType, 
              INTEGRATION_OPER,
              trunc(sysdate) ,
              to_date('01010001'||to_char(sysdate,'hh24miss'),'DDMMYYYYhh24miss'),
              RSB_Struct.PutDate('t_text', rpad('0',3000, '0'), p_depodate, (-1)*53), 
              to_date('31129999','ddmmyyyy'),
              1,
              0);
    end if;
    commit;

  exception 
    WHEN others THEN  
      select sf.t_number into Contr_number from dsfcontr_dbt sf where sf.t_id in (select dd.t_sfcontrid from ddlcontr_dbt dd where dd.t_dlcontrid = p_Dlcontrid);
      it_event.RegisterError(p_SystemId => 'Diasoft',p_ServiceName => 'GetBrokerContractDepo',p_ErrorCode => -1,p_ErrorDesc => 'Сервис ChangeDepositoryContractReq: Не удалось установить примечание '||p_NoteType||' для договора '||Contr_number,p_LevelInfo => 8);      
  end;

  /**
  @brief Устанавливает категорию объекта 
  @param[in] p_Dlcontrid тип объекта (dfunc_dbt)
  @param[in] p_Number id объекта 
  @param[in] p_objecttype тип объекта (dfunc_dbt)
  @param[in] p_groupid id объекта 
  */
  procedure SetObjAttr(p_Dlcontrid IN number, p_new_status number, p_objecttype number, p_groupid number) 
  IS
    id_attr integer := 0;
    Contr_number dsfcontr_dbt.t_number%type;
  begin
    begin
      select  RSB_SECUR.GetMainObjAttr (p_objecttype, LPAD (p_Dlcontrid, 34, '0'), NOTEKIND_DEPOACCOUNT_K, TRUNC (SYSDATE)) into id_attr from dual; 
    exception
      when no_data_found then
        null;
    end;
   
    if (id_attr > 0) then
      update  dobjatcor_dbt set t_attrid = p_new_status where t_objecttype =p_objecttype and t_groupid = p_groupid and t_object = LPAD (p_Dlcontrid, 34, '0');
    else
      INSERT INTO dobjatcor_dbt( T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_ISAUTO )
      VALUES   (p_objecttype,
                p_groupid,
                p_new_status,
                LPAD (p_Dlcontrid, 34, '0'), 
                'X',
                trunc(sysdate),
                INTEGRATION_OPER, 
                to_date('31129999','ddmmyyyy'),
                'X'
               );   
    end if;
   
    commit;   

  exception 
    WHEN others THEN  
      select sf.t_number into Contr_number from dsfcontr_dbt sf where sf.t_id in (select dd.t_sfcontrid from ddlcontr_dbt dd where dd.t_dlcontrid = p_Dlcontrid);
      it_event.RegisterError(p_SystemId => 'Diasoft',p_ServiceName => 'GetBrokerContractDepo',p_ErrorCode => -1,p_ErrorDesc => 'Сервис ChangeDepositoryContractReq: Не удалось установить категорию '||p_groupid||' для договора '||Contr_number,p_LevelInfo => 8);        
  end;

/**
@brief Устанавливает категорию объекта 
@param[in] p_ObjType тип объекта
@param[in] p_GroupId номер категории
@param[in] p_Dlcontrid id договора
@param[in] p_AttrId id значения категории
@param[in] p_Date дата начала действия

Так как нам нужно логировать ошибку с номером договора, то решение не универсальное, v_Objectid генерируется по образцу объекта 207 (договор)
*/
  PROCEDURE ConnectAttr( p_ObjType IN NUMBER, p_GroupId IN NUMBER, p_Dlcontrid IN number, p_AttrId IN NUMBER, p_Date IN DATE)
  IS
    v_IsAttrPresense    boolean;
    v_Objectid          varchar2(34);
    Contr_number        dsfcontr_dbt.t_number%type;
    C_EVERLASTING_DATE  date := to_date('31.12.9999','dd.mm.yyyy');
  BEGIN
    v_IsAttrPresense := false;
    v_Objectid := lpad(p_Dlcontrid, 34,'0');
    
    -- уже установленные действующие значения, если есть
    FOR objatcor_rec IN(
      SELECT t_id, t_attrid, t_validfromdate, t_validtodate
        FROM dobjatcor_dbt
       WHERE t_objecttype = p_ObjType
         AND t_groupid = p_GroupId
         AND t_object = v_Objectid
         AND t_validtodate = C_EVERLASTING_DATE
         ) LOOP
      if objatcor_rec.t_attrid = p_AttrId then
        v_IsAttrPresense := true;
      else 
        /* за дату может быть только 1 значение категории */
        if objatcor_rec.t_validfromdate >= p_Date then 
          DELETE FROM dobjatcor_dbt
           WHERE t_id = objatcor_rec.t_id;
        else 
          UPDATE dobjatcor_dbt
             SET t_validtodate = p_Date - 1
           WHERE t_id = objatcor_rec.t_id;
        end if;
      end if;
    END LOOP;

    -- новое значение 
    if not v_IsAttrPresense then
      INSERT INTO dobjatcor_dbt( T_OBJECTTYPE, T_GROUPID, T_ATTRID, T_OBJECT, T_GENERAL, T_VALIDFROMDATE, T_OPER, T_VALIDTODATE, T_SYSDATE, T_SYSTIME, T_ISAUTO )
      VALUES( p_ObjType, p_GroupId, p_AttrId, v_Objectid, CHR(88), p_Date, INTEGRATION_OPER, C_EVERLASTING_DATE, trunc(sysdate), to_date('01010001 '||to_char(sysdate,'hh24:mi:ss'),'ddmmyyyy hh24:mi:ss'), chr(88));
    end if;

    commit;

  EXCEPTION 
    WHEN others THEN  
      select sf.t_number into Contr_number from dsfcontr_dbt sf where sf.t_id in (select dd.t_sfcontrid from ddlcontr_dbt dd where dd.t_dlcontrid = p_Dlcontrid);
      it_event.RegisterError( p_SystemId => 'Diasoft',
                              p_ServiceName => 'GetBrokerContractDepo',
                              p_ErrorCode => -1,
                              p_ErrorDesc => 'Сервис ChangeDepositoryContractReq: Не удалось установить категорию '||p_groupid||' для договора '||Contr_number,
                              p_LevelInfo => 8);
  END;

  /**
  @brief Установить примечание 
  @param[in] p_DBOID id договора ДБО
  */
  procedure SendBrokerContractDepo(p_DBOID number) is
     v_GUID varchar(32);
     ErrorDesc varchar2(3000);
     ErrorCode  integer;
     MSGCode integer;
     MSGText varchar2(1000);
     sfnumber varchar(20);
     sfname varchar(320);
     sfdatebegin date;
     sfdateclose date;
     ClientMOEXCode varchar(64);
     ClientSPBEXCode varchar(64);
     ClientRegistrationCodeSPBEX varchar(64);
     QualifiedInvestor dscqinv_dbt.t_state%type;
     QualifiedInvestorStartDate dscqinv_dbt.t_regdate%type;
     RecognitionMethod varchar(20);
     QualifiedInvestorEndDate dscqinv_dbt.t_changedate%type;
     ChangeDate dscqinv_dbt.t_changedate%type;     
     RegistrationNumberInRegister  dscqinv_dbt.t_code%type;
     clientid  dobjcode_dbt.t_code%type;
     FilialId dobjcode_dbt.t_code%type;
     ClientType number(2);
     sfcontrid dsfcontr_dbt.t_id%type;
     MESSBODY xmltype;
     vx_MESSBODY xmltype;
   begin
     QualifiedInvestorStartDate := null;
     RecognitionMethod := null; 
     RegistrationNumberInRegister  := null;
     
     SELECT cntr.t_number,
            cntr.t_name,
            cntr.t_datebegin,
            cntr.t_dateclose,
            (SELECT d.t_code
               FROM ddlobjcode_dbt d
              WHERE d.t_objecttype = OBJECTTYPE_CONTRACT
                AND d.t_codekind = 1
                AND d.t_bankclosedate = TO_DATE ('01010001', 'ddmmyyyy')
                AND d.t_objectid = dl.t_dlcontrid)
              AS ClientMOEXCode,                     
            (SELECT mp.t_mpcode
               FROM ddlcontrmp_dbt mp
              WHERE mp.t_dlcontrid = p_DBOID
                AND mp.t_marketid IN
                  (SELECT obj.t_objectid
                    FROM dobjcode_dbt obj
                   WHERE t_code IN
                     (SELECT TRIM(CHR(0) FROM TO_CHAR(t_fmtblobdata_xxxx))
                        FROM DREGVAL_DBT
                       WHERE t_keyid IN
                         (SELECT t_KeyId
                            FROM DREGPARM_DBT
                           WHERE t_ParentId = (SELECT T_KEYID
                                                 FROM DREGPARM_DBT
                                                WHERE T_PARENTID = 0
                                                  AND LOWER (T_NAME) = LOWER ('SECUR')
                                              )
                             AND LOWER (T_NAME) = LOWER ('SPBEX_CODE')
                         )
                     )
                     AND t_codekind = CODEKIND_SPB)
            ) as ClientSPBEXCode, 
            (SELECT dd.t_code
               FROM ddlobjcode_dbt dd
              WHERE dd.t_objecttype = OBJECTTYPE_CONTRACT
                AND dd.t_codekind = CODEKIND_REGISTER_SPB
                AND dd.t_bankclosedate = TO_DATE ('01010001', 'ddmmyyyy')
                AND dd.t_objectid = dl.t_dlcontrid)
              AS ClientRegistrationCodeSPBEX,        
            (SELECT CASE
                  WHEN (SELECT 1
                          FROM dscqinv_dbt sc
                         WHERE sc.t_partyid = cntr.t_partyid)
                          IS NULL THEN 0
                  WHEN (SELECT sc.t_state
                          FROM dscqinv_dbt sc
                         WHERE sc.t_partyid = cntr.t_partyid) = 0 THEN 2
                  WHEN (SELECT sc.t_state
                          FROM dscqinv_dbt sc
                         WHERE sc.t_partyid = cntr.t_partyid) = 1 THEN 1
                    END
               FROM DUAL)
               AS QualifiedInvestor,
            (SELECT sc.t_regdate
               FROM dscqinv_dbt sc
              WHERE sc.t_partyid = cntr.t_partyid)
               AS QualifiedInvestorStartDate,            
                      (SELECT case when sc.t_kind = 1 then 'По закону'
                                             when sc.t_kind = 2 then 'По заявлению' end
               FROM dscqinv_dbt sc
              WHERE sc.t_partyid = cntr.t_partyid)
               AS RecognitionMethod,       
                   (SELECT sc.t_changedate 
               FROM dscqinv_dbt sc
              WHERE sc.t_partyid = cntr.t_partyid and sc.t_state = 0)
              AS  QualifiedInvestorEndDate,   
                    (SELECT sc.t_code 
               FROM dscqinv_dbt sc
              WHERE sc.t_partyid = cntr.t_partyid)
               AS RegistrationNumberInRegister,            
                   (SELECT sc.t_changedate 
               FROM dscqinv_dbt sc
              WHERE sc.t_partyid = cntr.t_partyid and sc.t_state = 0)     
               as ChangeDate,        
            (SELECT db.t_code
               FROM dobjcode_dbt db
              WHERE     db.t_objecttype = OBJECTTYPE_CLIENT
                    AND db.t_codekind = NOTEKIND_DEPOACCOUNT_K
                    AND db.t_state = 0
                    AND db.t_objectid = cntr.t_partyid)
               AS clientid,
            '0000' AS FilialId,
            (SELECT  case dp.t_legalform
                        when 2 then
                           case (select t_isemployer from dpersn_dbt pers 
                                  where pers.t_personid = dp.t_partyid)
                              when 'X' then 4
                              else 1
                           end
                        when 1 then
                           case nvl((select 1 from dpartyown_dbt ow 
                                      where ow.t_partykind = 65 and ow.t_partyid = dp.t_partyid),-1)
                              when 1 then 3
                              else 2
                           end
                     end
              FROM dparty_dbt dp
             WHERE dp.t_partyid = cntr.t_partyid ) AS ClientType
      INTO sfnumber,
           sfname,
           sfdatebegin,
           sfdateclose,
           ClientMOEXCode,
           ClientSPBEXCode,
           ClientRegistrationCodeSPBEX,
           QualifiedInvestor,
           QualifiedInvestorStartDate,
           RecognitionMethod,
           QualifiedInvestorEndDate,
           RegistrationNumberInRegister,
           ChangeDate,
           clientid,
           FilialId,
           ClientType
      FROM dsfcontr_dbt cntr, ddlcontr_dbt dl
     WHERE cntr.t_id = dl.t_sfcontrid
       AND dl.t_dlcontrid = p_DBOID
       AND cntr.t_dateclose = TO_DATE ('01.01.0001', 'dd.mm.yyyy'); 
 
    /*Получение уникального текстового GUID*/                               
    SELECT CAST(SYS_GUID() AS VARCHAR2(32)) into v_GUID FROM dual;

    SELECT XMLELEMENT (
          "TransferBrokerContractReq",
          XMLELEMENT ("GUID", v_GUID),
          XMLELEMENT ("RequestTime", (SELECT IT_XML.TIMESTAMP_TO_CHAR_ISO8601(SYSDATE) FROM DUAL)),
          XMLELEMENT ("ContractParameters",
             XMLELEMENT ("ContractNumber", sfnumber),
             XMLELEMENT ("ContractName", sfname),
             XMLELEMENT ("ContractOpenDate", sfdatebegin),
             XMLELEMENT ("QualifiedInvestor", QualifiedInvestor),
            case when QualifiedInvestorStartDate is not null then XMLELEMENT ("QualifiedInvestorStartDate", QualifiedInvestorStartDate) else null end, 
             case when RegistrationNumberInRegister = chr(0) then null when RegistrationNumberInRegister = chr(1) then null else XMLELEMENT ("RegistrationNumberInRegister",RegistrationNumberInRegister) end, 
             case when ChangeDate <> to_date('01.01.0001','dd.mm.yyyy') then XMLELEMENT ("ChangeDate",ChangeDate) else null end, 
             case when RecognitionMethod is not null then   XMLELEMENT ("RecognitionMethod",RecognitionMethod) else null end,      
             case when QualifiedInvestorEndDate is not null then   XMLELEMENT ("QualifiedInvestorEndDate",QualifiedInvestorEndDate) else null end,      
             XMLELEMENT ("ClientRegistrationCodeSPBEX", ClientRegistrationCodeSPBEX),
             XMLELEMENT ( "ClientMOEXCode", xmlelement("RecordCode",ClientMOEXCode)),
             XMLELEMENT ("ClientSPBEXCode", xmlelement("RecordCode",ClientSPBEXCode)),
             XMLELEMENT ("ClientId",  xmlelement("ObjectId",ClientId)),
             XMLELEMENT ("FilialId", xmlelement("ObjectId",FilialId)),
             XMLELEMENT ("ClientType", xmlelement("RecordCode",ClientType)),
             XMLELEMENT ("ContractId", xmlelement("ObjectId",p_DBOID)),
             (SELECT XMLELEMENT ( "CurrencyCodeList",
                        XMLAGG (
                           XMLELEMENT ( "CurrencyCode",
                              XMLELEMENT ("DigitalCurrencyCode", xmlelement("RecordCode",d.t_iso_number)),
                              XMLELEMENT ("AlphabeticCurrencyCode", xmlelement("RecordCode",d.t_ccy) ))))
                FROM (  SELECT fin.t_iso_number, fin.t_ccy
                          FROM dsfcontr_dbt cntr,
                               ddlcontrmp_dbt mp,
                               dmcaccdoc_dbt accdoc,
                               dfininstr_dbt fin
                         WHERE cntr.t_id = mp.t_sfcontrid
                               AND cntr.t_dateclose =
                                      TO_DATE ('01.01.0001', 'dd.mm.yyyy')
                               AND cntr.t_servkind = 1
                               AND accdoc.t_clientcontrid = cntr.t_id
                               AND accdoc.t_catid = 70
                               AND accdoc.t_iscommon = 'X'
                               AND accdoc.t_disablingdate =
                                      TO_DATE ('01010001', 'ddmmyyyy')
                               AND accdoc.t_currency = fin.t_fiid
                               AND fin.t_fi_kind = 1
                               AND mp.t_dlcontrid = p_DBOID
                      GROUP BY fin.t_ccy, fin.t_iso_number) d),
             (SELECT XMLELEMENT ("TradingMarketList",
                        XMLAGG (
                           XMLELEMENT ( "TradingMarket",
                              XMLELEMENT ( "TradingMarketType", 
                              ( xmlelement("RecordCode", 
                                 (CASE
                                     WHEN cntr.t_servkind = 1
                                          AND cntr.t_servkindsub = 9
                                     THEN
                                        2
                                     WHEN     cntr.t_servkind = 1
                                          AND cntr.t_servkindsub = 8
                                          AND mp.t_marketid = 2
                                     THEN
                                        1
                                     WHEN     cntr.t_servkind = 1
                                          AND cntr.t_servkindsub = 8
                                          AND mp.t_marketid = 151337
                                     THEN
                                        3
                                  END)) )),
                              (SELECT XMLELEMENT ( "ClientAccountList",
                                         XMLAGG (
                                            XMLELEMENT ("ClientAccount",
                                               XMLELEMENT ("ClientAccountNumber", accdoc.t_account),
                                               XMLELEMENT ("AccountOpenDate",ac.t_open_date),
                                               XMLELEMENT ("AccountType",   xmlelement("RecordCode",'1')))))
                                 FROM dmcaccdoc_dbt accdoc, daccount_dbt ac
                                WHERE     accdoc.t_catid = 70
                                      AND accdoc.t_iscommon = 'X'
                                      AND accdoc.t_clientcontrid = cntr.t_id
                                      AND accdoc.t_disablingdate =
                                             TO_DATE ('01010001', 'ddmmyyyy')
                                      AND ac.t_account = accdoc.t_account
                                      AND ac.t_chapter = accdoc.t_chapter
                                      AND ac.t_code_currency =
                                             accdoc.t_currency))))
                FROM dsfcontr_dbt cntr, ddlcontrmp_dbt mp
               WHERE cntr.t_id = mp.t_sfcontrid AND mp.t_dlcontrid = p_DBOID
                     AND cntr.t_dateclose =
                            TO_DATE ('01.01.0001', 'dd.mm.yyyy')
                     AND cntr.t_servkind = 1)))
      INTO vx_MESSBODY
      FROM DUAL;
   
    BEGIN 
      it_kafka.load_msg(v_GUID,'R','Diasoft.TransferBrokerContract','DIASOFT', vx_MESSBODY.getClobVal, ErrorCode, ErrorDesc,NULL,MSGCode, MSGText);   
    END;   

    commit;

  EXCEPTION
    when others then 
      it_log.log(p_msg => 'Не удалось собрать xml по причине:  непредвиденная ошибка '||SQLERRM||': '||ERROR_UNEXPECTED_GET_DATA);
  end;  
  
  /**
  @brief  вызывается при получении сообщения из топика Кафки
  @param[in] p_messbody Тело сообщения (сюда приходит xml)
  */
  procedure GetBrokerContractDepo(p_worklogid integer
                       ,p_messbody  clob
                       ,p_messmeta  xmltype
                       ,o_msgid     out varchar2
                       ,o_MSGCode   out integer
                       ,o_MSGText   out varchar2
                       ,o_messbody  out clob
                       ,o_messmeta  out xmltype
                       ) is
    MESSBODY clob;
    v_xml_in xmltype;
    v_namespace varchar2(128) := it_kafka.get_namespace(p_system_name => it_diasoft.C_C_SYSTEM_NAME, p_rootelement => 'ChangeDepositoryContractReq');
    GUID varchar2(1000);
    DepoNumber varchar2(100);
    DepoStartDate varchar2(100);
    ServiceContractId  varchar2(100);
    ContractBrokerNumber dsfcontr_dbt.t_number%type;
    Dlcontrid dsfcontr_dbt.t_id%type;
    Contr_Dateclose dsfcontr_dbt.t_dateclose%type;
    id_note integer := 0;
    DepoAccountList varchar2(100);
    DepoAccountLetter varchar2(2);
    DepoAccountLetter_UK varchar2(4);
    DepoAccountNumber varchar(100);
    AttrCode number := 0;
    new_status number;
    sendmesstate varchar2(100);
    respdata varchar2(100);
    duplicate number := 0;
    send_mes boolean;
    DLCONTR_NO_FOUND EXCEPTION;
    DLCONTR_CLOSE EXCEPTION;
    IT_IS_NOT_XML EXCEPTION;
    PRAGMA EXCEPTION_INIT(IT_IS_NOT_XML, -31011); --ORA-31011: XML parsing failed
  begin
    v_xml_in := it_xml.Clob_to_xml(p_MESSBODY);
  
    with oper as
       (select v_xml_in xml from dual)
    select extractValue(t.xml, '*/ContractParameters/DepoNumber', v_namespace)
          ,extractValue(t.xml, '*/ContractParameters/DepoStartDate', v_namespace)
          ,extractValue(t.xml, '*/ContractParameters/ServiceContractId/ObjectId', v_namespace)
      into DepoNumber
          ,DepoStartDate
          ,ServiceContractId
      from oper t;  
  
    for ContractBrokerList in (
      select  extractvalue( value(t), 'ContractBroker/ContractBrokerNumber',v_namespace) as ContractBrokerNumber
             ,extractvalue( value(t), 'ContractBroker/IsSeparationBasicStandart',v_namespace) as IsSeparationBasicStandart
        from table(
               xmlsequence(
                 extract( v_xml_in, '*/ContractParameters/ContractBrokerList/ContractBroker', v_namespace )
                          )
                  ) t
    ) loop                
      ContractBrokerNumber := ContractBrokerList.ContractBrokerNumber;  

      begin     
        select dl.t_dlcontrid, sf.t_dateclose into Dlcontrid , Contr_Dateclose from ddlcontr_dbt dl, dsfcontr_dbt sf where sf.t_number = ContractBrokerNumber  and dl.t_sfcontrid = sf.t_id;-- and sf.t_dateclose = to_date('01.01.0001','dd.mm.yyyy') ;     
        IF Contr_Dateclose != to_date('01.01.0001','dd.mm.yyyy')  THEN
          RAISE DLCONTR_CLOSE;
        END IF;     
      exception WHEN NO_DATA_FOUND THEN  
        RAISE DLCONTR_NO_FOUND;
      end;

      --устанавливаем примечания
      SetNoteText(LPAD (Dlcontrid, 34, '0'),DepoNumber,null,OBJECTTYPE_CONTRACT,NOTEKIND_DEPONUMBER_K); 
      SetNoteText(LPAD (Dlcontrid, 34, '0'),null,to_date(DepoStartDate, 'yyyy-mm-dd'), OBJECTTYPE_CONTRACT,NOTEKIND_DEPOSTARTDATE); 
    
      for DepoAccountList in (
        select  extractvalue( value(t), '/DepoAccount/DepoAccountNumber',v_namespace) as DepoAccountNumber
          from table(
                 xmlsequence(
                   extract( v_xml_in, '*/ContractParameters/ContractBrokerList/ContractBroker/DepoAccountList/DepoAccount', v_namespace )
                            )
                    ) t 
      ) loop  
        DepoAccountNumber := DepoAccountList.DepoAccountNumber;
          
        select substr(DepoAccountNumber,1,1) into DepoAccountLetter from dual;
   
        select substr(DepoAccountNumber,1,3) into DepoAccountLetter_UK from dual;
           
        if DepoAccountLetter = 'K' or (DepoAccountLetter = 'D'  and DepoAccountLetter_UK <> 'D-T' ) then
          SetNoteText(LPAD (Dlcontrid, 34, '0'),DepoAccountNumber,null,OBJECTTYPE_CONTRACT,NOTEKIND_DEPOACCOUNT_K); 
        elsif DepoAccountLetter = 'T'  or  DepoAccountLetter_UK= 'D-T' then
          SetNoteText(LPAD (Dlcontrid, 34, '0'),DepoAccountNumber,null,OBJECTTYPE_CONTRACT,NOTEKIND_DEPONUMBER_T);
        end if;  
  
        FOR DepoAccountPartitionList IN (
          SELECT EXTRACTVALUE (VALUE (t),
                               '/DepoAccountPartition/DepoTradingAccountNumber',
                               v_namespace)
                   AS DepoTradingAccountNumber,
                 EXTRACTVALUE (VALUE (t),
                               '/DepoAccountPartition/DepoPartitionType/RecordCode',
                               v_namespace)
                  AS DepoPartitionType
            FROM TABLE (
                   XMLSEQUENCE (
                     EXTRACT (
                       v_xml_in,
                       '*/ContractParameters/ContractBrokerList/ContractBroker/DepoAccountList/DepoAccount/DepoAccountPartitionList/DepoAccountPartition',
                       v_namespace))) t
        ) LOOP

          IF DepoAccountPartitionList.DepoPartitionType = 2 OR DepoAccountPartitionList.DepoPartitionType = 0 THEN
            SetNoteText(LPAD (Dlcontrid, 34, '0'),DepoAccountPartitionList.DepoTradingAccountNumber,null,OBJECTTYPE_CONTRACT,105);   
          ELSIF DepoAccountPartitionList.DepoPartitionType = 1 THEN
            SetNoteText(LPAD (Dlcontrid, 34, '0'),DepoAccountPartitionList.DepoTradingAccountNumber,null,OBJECTTYPE_CONTRACT,NOTEKIND_DEPO_TRADING_ACCOUNT); 
          ELSIF DepoAccountPartitionList.DepoPartitionType = 3 THEN
            SetNoteText(LPAD (Dlcontrid, 34, '0'),DepoAccountPartitionList.DepoTradingAccountNumber,null,OBJECTTYPE_CONTRACT,115); 
          END IF;

        END LOOP;
      END LOOP;

      --логика из func_lib.mac UpdateStatusContractFromDepo
      new_status := CATEGORY_STATUS_DEPO_ACCEPT;
      AttrCode := RSB_SECUR.GetMainObjAttr ( OBJECTTYPE_CONTRACT, LPAD (Dlcontrid, 34, '0'), CONTRACT_CATEGORY_STATUS, trunc(sysdate));

      begin
        if AttrCode not in (CATEGORY_STATUS_MOEX_ACCEPT, CATEGORY_STATUS_FINISHED) then
        -- 5 если статус = Проведена регистрация на ММВБ, то доп.проверка не нужна
        -- 3  -- если обработка завершена, нечего проверять

          SELECT m.t_sendmesstate,
                 CASE
                    WHEN LENGTH (m.t_respdata) > 1
                    THEN
                       LOWER (
                          EXTRACTVALUE (
                             XMLType (m.t_respdata),
                             '/MICEX_ERROR',
                             'xmlns=""http://www.moex.com/application/output""'))
                    ELSE ''
                 END t_respdata
            INTO sendmesstate, respdata
            FROM ddlcontrmsg_dbt m, ddlcontr_dbt d, dsfcontr_dbt s
           WHERE m.t_dlcontrid = d.t_dlcontrid
             AND s.t_id = d.t_sfcontrid
             AND m.t_dlcontrid = Dlcontrid
             AND t_kind IN (1, 2)
             AND t_isonlinesendmes = CHR (88)
             AND ROWNUM = 1
           ORDER BY t_senddate DESC;
 
          select instr(respdata,'duplicate') into duplicate from dual;
 
          if sendmesstate != 210 then -- последняя не успешная заявка
            if sendmesstate = 102 or sendmesstate = 103 and duplicate > 0 then  
              send_mes := true; 
            else 
              send_mes := false; 
            end if;
      
            if send_mes then 
              AttrCode := CATEGORY_STATUS_MOEX_ACCEPT; 
            end if; -- или успешно отправлено, или не онлайн       
          end if;
        end if;
  
      exception WHEN NO_DATA_FOUND or IT_IS_NOT_XML THEN --Ошибка, когда в t_respdata вместо xml находится json, актуальная только на тестовых стендах
        null; -- значит не было ошибки дубликации или регистрации еще не было
      end;
  
      if AttrCode in (CATEGORY_STATUS_UNDEFINED, CATEGORY_STATUS_NEW_CONTRACT) then -- категория пустая или статус "новый"
        new_status := CATEGORY_STATUS_DEPO_ACCEPT;     -- Депозитарий подтверждено
      elsif AttrCode = CATEGORY_STATUS_DEPO_ACCEPT then -- получено подтверждение депозитария (второй раз пришел запрос?)
        new_status := CATEGORY_STATUS_DEPO_ACCEPT;     -- Депозитарий подтверждено
      elsif AttrCode = CATEGORY_STATUS_ASOA_ACCEPT then
        new_status := CATEGORY_STATUS_DEPO_ACCEPT;
      elsif AttrCode = CATEGORY_STATUS_FINISHED then -- обработка завершена (второй раз пришел запрос?)
        new_status := CATEGORY_STATUS_FINISHED;     -- обработка завершена
      elsif AttrCode = CATEGORY_STATUS_MOEX_ACCEPT  then-- ММВБ успешно
        new_status := CATEGORY_STATUS_FINISHED;     -- обработка завершена
      end if;


      if AttrCode != new_status then
        ConnectAttr(OBJECTTYPE_CONTRACT, CONTRACT_CATEGORY_STATUS, Dlcontrid, new_status, trunc(sysdate));
      end if;

      if new_status = CATEGORY_STATUS_FINISHED and AttrCode != new_status then 
        PutInProcess(OBJECTTYPE_NOTIFICATION, dlcontrid); -- для отправки уведомления
        PutInProcess(OBJECTTYPE_UPLOADDOC, dlcontrid);  -- для UploadDoc
        InsEvent_UpdateContract(dlcontrid, 1); -- перевод в состояние "действующий"
      end if;  
    END LOOP;

  EXCEPTION
    WHEN DLCONTR_NO_FOUND THEN  it_event.RegisterError(p_SystemId => 'Diasoft',p_ServiceName => 'GetBrokerContractDepo',p_ErrorCode => ERROR_CLIENT_NOTFOUND,p_ErrorDesc => 'Сервис ChangeDepositoryContractReq: В СОФР не найден  ДБО с номером '||ContractBrokerNumber,p_LevelInfo => 8);
    WHEN DLCONTR_CLOSE THEN  it_event.RegisterError(p_SystemId => 'Diasoft',p_ServiceName => 'GetBrokerContractDepo',p_ErrorCode => ERROR_CLIENT_NOTFOUND,p_ErrorDesc =>  'Сервис ChangeDepositoryContractReq: В СОФР закрыт  ДБО с номером '||ContractBrokerNumber,p_LevelInfo => 8);
    WHEN OTHERS THEN 
      o_MSGCode := ERROR_UNEXPECTED_GET_DATA;
      o_MSGText := 'Не удалось разобрать xml по причине: неверный формат '||SQLERRM;
  end;
  
  procedure process_corp_action_redempt (
      p_worklogid     integer
     ,p_messbody      clob
     ,p_messmeta      xmltype
     ,o_msgid     out varchar2
     ,o_MSGCode   out integer
     ,o_MSGText   out varchar2
     ,o_messbody  out clob
     ,o_messmeta  out xmltype
  ) is
    l_message_type    varchar2(35);
    l_fix_date        date;
    l_complete_date   date;
    l_source_number   varchar2(200);
    l_source_id       number(20);
    l_action_type     varchar2(200);
    l_owner_date      date;
    l_depositary_name varchar2(200);
    l_isin            varchar2(50);
    l_reg_number      varchar2(50);
    l_nrd_code        varchar2(50);
    l_quantity        number(12, 6);
  begin
    o_MSGCode := 0;

    begin    
      select message_type,
            fix_date,
            complete_date,
            source_number,
            source_id,
            action_type,
            owner_date,
            depositary_name,
            isin,
            reg_number,
            nrd_code,
            quantity
        into l_message_type,
            l_fix_date,
            l_complete_date,
            l_source_number,
            l_source_id,
            l_action_type,
            l_owner_date,
            l_depositary_name,
            l_isin,
            l_reg_number,
            l_nrd_code,
            l_quantity
        from json_table(p_messbody,
                        '$.SendDepoCorpActionsRedInfoReq.RedemptionData' columns
                          message_type    varchar2(35)  path '$.RedemptionMessageType',
                          fix_date        date          path '$.RedemptionFixationDate',
                          complete_date   date          path '$.RedemptionCompletionDate',
                          source_number   varchar2(200) path '$.RedemptionNumber',
                          source_id       number(20)    path '$.RedemptionID',
                          action_type     varchar2(200) path '$.RedemptionActionType',
                          owner_date      date          path '$.RedemptionOwnerDate',
                          depositary_name varchar2(200) path '$.RedemptionInstitutionID',
                          isin            varchar2(50)  path '$.RedemptionISIN',
                          reg_number      varchar2(50)  path '$.RedemptionRegNumber',
                          nrd_code        varchar2(50)  path '$.RedemptionNRDCode',
                          quantity        number(12, 6) path '$.RedemptionQuantity'
                        );
    exception
      when others then
          raise_application_error(-20000
                               ,'Ошибка данных в сообщении SendDepoKDREDInfoReq ');      
    end;
    validation_utils.not_null(l_message_type, 'message_type');
    validation_utils.not_null(l_fix_date, 'fix_date');
    validation_utils.not_null(l_source_number, 'source_number');
    validation_utils.not_null(l_source_id, 'source_id');
    validation_utils.not_null(l_action_type, 'action_type');
    validation_utils.not_null(l_depositary_name, 'depositary_name');
    validation_utils.not_null(l_isin, 'isin');
    validation_utils.not_null(l_reg_number, 'reg_number');
    validation_utils.not_null(l_nrd_code, 'nrd_code');
    
    if l_message_type = secur_redemption_utils.message_type_completion then
      validation_utils.not_null(l_complete_date, 'complete_date');
      validation_utils.not_null(l_owner_date, 'owner_date');
      validation_utils.not_null(l_quantity, 'quantity');
    end if;
    
    secur_redemption_utils.save_redemption(p_message_type    => l_message_type,
                                           p_fix_date        => l_fix_date,
                                           p_complete_date   => l_complete_date,
                                           p_source_number   => l_source_number,
                                           p_source_id       => l_source_id,
                                           p_action_type     => l_action_type,
                                           p_owner_date      => l_owner_date,
                                           p_depositary_name => l_depositary_name,
                                           p_isin            => l_isin,
                                           p_reg_number      => l_reg_number,
                                           p_nrd_code        => l_nrd_code,
                                           p_quantity        => l_quantity);
  end process_corp_action_redempt;
end it_diasoft;
/

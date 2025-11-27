create or replace package body it_efr is

  /**
  * Упаковщик исходящих сообшений в ЕФР через KAFKA
  * BIQ-18375 Автоматизация отчетной формы "Справка 5798-У" (справка для гос. служащего)
  * @since RSHB 105
  * @qtest NO
  */
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype) as
    v_rootElement    itt_kafka_topic.rootelement%type;
    v_msg_param      itt_kafka_topic.msg_param%type;
    vx_in_messbody   xmltype;
    vx_out_messbody  xmltype;
    v_node           varchar2(100);
    vz_GUID          itt_q_message_log.msgid%type;
    vz_GUIDReq       itt_q_message_log.corrmsgid%type;
    vz_ErrorCode     itt_q_message_log.msgcode%type;
    vz_ErrorDesc     itt_q_message_log.msgtext%type;
    vz_RequestNumber varchar2(100);
    v_dom_doc        dbms_xmldom.domdocument;
    v_dom_eliment    dbms_xmldom.domelement;

    vr_message_lin   itt_q_message_log%rowtype;
    vj_in_headers    json_object_t;
    v_in_header      clob;
    v_out_header     clob;
    v_request_id     varchar2(100);
  begin
    begin
      select t.rootelement
            ,t.msg_param
        into v_rootElement
            ,v_msg_param
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
          ,to_number(EXTRACTVALUE(d_out.x, '*/ErrorList/Error/ErrorCode') default null on CONVERSION ERROR)
          ,EXTRACTVALUE(d_out.x, '*/ErrorList/Error/ErrorDesc')
          ,EXTRACTVALUE(d_out.x, '*/RequestNumber')
      into vz_GUID
          ,vz_GUIDReq
          ,vz_ErrorCode
          ,vz_ErrorDesc
          ,vz_RequestNumber
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
      select insertchildxmlbefore(vx_out_messbody, '*', 'RequestNumber', xmlelement("GUID", p_message.msgid)) into vx_out_messbody from dual;
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
    elsif p_message.CORRmsgid is not null
    then
      select insertchildxmlafter(vx_out_messbody, '*', 'GUID', xmlelement("GUIDReq", p_message.CORRmsgid)) into vx_out_messbody from dual;
    end if;
    if vx_out_messbody.existsNode('*/RequestTime') != 1
    then
      v_node := case
                  when p_message.message_type = it_q_message.C_C_MSG_TYPE_R then
                   'GUID'
                  else
                   'GUIDReq'
                end;
      select insertchildxmlafter(vx_out_messbody, '*', v_node, xmlelement("RequestTime", it_xml.timestamp_to_char_iso8601(p_message.RequestDT)))
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
    end if;
    /*RequestNumber всегда существует для установки GUID в нужное место, но после того пустой он не нужен*/
    if vx_out_messbody.existsNode('*/RequestNumber') = 1
    then
      if vz_RequestNumber is null
      then
        select DELETEXML(vx_out_messbody, '*/RequestNumber') into vx_out_messbody from dual;
      end if;
    end if;   

    if v_msg_param is not null
    then
      v_dom_doc     := dbms_xmldom.newdomdocument(vx_out_messbody.getClobVal);
      v_dom_eliment := dbms_xmldom.getdocumentelement(v_dom_doc);
      dbms_xmldom.setAttribute(v_dom_eliment, 'xmlns', v_msg_param);
      vx_out_messbody := dbms_xmldom.getxmltype(v_dom_doc);
    end if;
    o_messbody := vx_out_messbody.getClobVal;
    o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));

    --ФОРМИРУЕМ HEADER
    --Получим входящее сообщение
    vr_message_lin := it_q_message.messlog_get(p_msgid => p_message.CORRmsgid, p_queuetype => it_q_message.C_C_QUEUE_TYPE_IN); 
    --Получим Header входящего сообщения из XML
    SELECT EXTRACTVALUE(xmltype(vr_message_lin.messmeta), '/KAFKA/Header') 
      INTO v_in_header
      FROM dual;
    --Преобразуем полученный Header в JSON
    begin 
      vj_in_headers := JSON_OBJECT_T.parse(v_in_header); 
    exception 
      when others then
         vj_in_headers := JSON_OBJECT_T.parse(convert(v_in_header, 'UTF8')); 
    end;
    --Достанем те входящие мета-данные, которые пригодятся
    v_request_id  := vj_in_headers.get_String('x-request-id');

    --Формируем JSON
    SELECT JSON_OBJECTAGG (key t_Key value t_Value) 
      INTO v_out_header
      FROM (SELECT t_Name AS t_Key, t_Note AS t_Value
              FROM dllvalues_dbt
             WHERE t_List = OBJTYPE_EFR_HEADERS
             UNION ALL
            SELECT 'x-request-id' AS t_Key, v_request_id AS t_Value FROM dual
           )
     WHERE t_Value IS NOT NULL; 

    --Завернем JSON в XML
    o_messmeta := it_kafka.add_Header_Xmessmeta(p_Header => v_out_header , px_messmeta => p_message.MessMETA) ;

      
    IF p_message.MSGCode IN (ERROR_IN_THE_SERVICE, ERROR_UNEXPECTED_GET_DATA) THEN
      --Зарегистрируем события для отправки на почту и в телеграмм сопровождения
      it_event.RegisterError(p_SystemId => 'С5798-Y'
                            ,p_ServiceName => 'GetReferenceFromEFR'
                            ,p_ErrorCode => p_message.MSGCode
                            ,p_ErrorDesc => p_message.MSGText||(case when p_message.MSGCode = ERROR_IN_THE_SERVICE then ': '||it_q_message.get_errtxt(p_sqlerrm => vr_message_lin.commenttxt) else '' end)
                            ,p_LevelInfo => 8);
    END IF;

  end out_pack_message;

end it_efr;
/

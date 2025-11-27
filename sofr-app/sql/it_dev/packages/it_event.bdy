create or replace package body it_event is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    21.01.2025  Зыков М.В.       BOSS-7457                        BOSS-7453 Разработка. Рефакторинг процедуры мониторинга 
    23.10.2023  Зыков М.В.       BOSS-1230                        BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
    15.05.2023  Зыков М.В.       CCBO-4870                        BIQ-13171. Разработка процедур работы с очередью событий
    02.09.2022  Зыков М.В.       BIQ-9225                         Перевод на универсальную процедуру отправки сообщения
    19.08.2022  Зыков М.В.       BIQ-9225                         Изменение формата сообщения об ошибке
    18.08.2022  Зыков М.В.       BIQ-9225                         Изменение параметров
    03.08.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/
  gb_check_qmanager boolean := false; -- При проблеме один раз пытаемся запустить QManager 
  -- 
  -- Старт QManagera при проблеме 
  procedure start_qmanager as
    vc_qminfo   varchar2(2000);
    vd_last_rem date;
    vc_sid      varchar2(200);
    vc_nproc    varchar2(20) := 'StartQM';
  begin
    if gb_check_qmanager
    then
      return;
    end if;
    vc_sid      := it_event_utils.C_C_QSET_PREFIX || '[' || vc_nproc || ']';
    vd_last_rem := it_q_message.get_qset_data(p_qset_name => vc_sid);
    if vd_last_rem is null
       or sysdate > vd_last_rem + numtodsinterval(5, 'MINUTE')
    then
      it_q_message.set_qset_data(p_qset_name => vc_sid, p_date => sysdate);
      if it_q_manager.startmanager(o_info => vc_qminfo) = 0
         and vc_qminfo is not null
      then
        it_event_utils.send_information_error(p_sqlcode => vc_nproc, p_sqlerrm => vc_qminfo);
      end if;
      gb_check_qmanager := true;
    end if;
  end;

  procedure GetEventParam(p_SystemId         varchar2
                         ,p_ServiceName      varchar2
                         ,o_SystemId         out varchar2
                         ,o_ServiceName      out varchar2
                         ,o_KeyServiceName   out varchar2
                         ,o_EventServiceName out varchar2) as
  begin
    o_systemid         := nvl(trim(upper(p_SystemId)), 'UNKNOWNSYS');
    o_servicename      := nvl(trim(p_servicename), 'UNDEFINEDSERVICE');
    o_KeyServiceName   := o_SystemId || '.' || o_servicename;
    o_EventServiceName := it_event_utils.C_C_REGISTER_SN_PREFIX || '(' || o_KeyServiceName || ')';
  end;

  --регистрирует событие 
  procedure RegisterEvent(p_EventID     varchar2 default null --ключ события внешней системы  , если пустой СОФР сам присваивает GUID
                         ,p_SystemId    varchar2 -- SYSTEMID ключ системы , EventID+ SYSTEMID = суррогатный уникальный ключ который позволяет идентифицировать событие во внешней системе. если пустой, дефалтим в UNKNOWNSYS
                         ,p_ServiceName varchar2 default null -- ключ внешнего сервиса , у одного SYSTEMID м.б. несколько сервисов, . если пустой дефалтим в UNDEFINEDSERVICE
                         ,p_MsgBODY     clob default null -- Текст описания события  
                         ,p_MsgMETA     clob default null -- метаданные события  , XML с единственным узлом и  парами ключ-значение в атрибутах вида <XML param1="value1" param2="value2" .. paramN="valuen"/>
                          -- LevelInfo = "0" - уровень критичности события от 0 - информация до 10 - АВАРИЯ  <0 - бизнес ошибки . Не отправляются в поддержку
                         ,o_errtxt out varchar2 --- Текст ошибки регистрации 
                         ,o_MsgID  out itt_q_message_log.msgid%type) as
    pragma autonomous_transaction;
    vp_systemid      varchar2(128);
    vp_servicename   varchar2(128);
    v_servicename    varchar2(128);
    v_keyservicename varchar2(128);
    vx_msgmeta       xmltype;
    v_qlog           itt_q_message_log%rowtype;
    v_log_id         itt_q_message_log.log_id%type;
    v_LevelInfo      itt_event_log.levelinfo%type := 0;
  begin
    GetEventParam(p_SystemId, p_ServiceName, vp_systemid, vp_servicename, v_keyservicename, v_servicename);
    if p_EventID is not null
    then
      o_MsgID := vp_systemid || '#' || p_EventID;
    end if;
    vx_msgmeta := it_xml.Clob_to_xml(p_MsgMETA, 'p_MsgMETA');
    begin
      select nvl(to_number(EXTRACTVALUE(vx_msgmeta, '/XML/@LevelInfo')), 0) into v_LevelInfo from dual;
    exception
      when others then
        v_LevelInfo := 0;
    end;
    select xmlelement("Event", xmlattributes(p_EventID as "EventID", vp_systemid as "SystemId", vp_servicename as "ServiceName"), xmlelement("MsgMETA", vx_msgmeta))
      into vx_msgmeta
      from dual;
    begin
      for n in 1 .. 1 -- пока отключаем 2 -- Пытаемся запустить сервис 2 раза 
      loop
        begin
          it_q_message.do_a_service(p_servicename => v_servicename
                                    --,p_receiver =>
                                   ,p_messbody => p_MsgBODY
                                   ,p_messmeta => vx_msgmeta
                                   ,p_servicegroup => '##' || upper(v_servicename)
                                   ,p_queue_num => it_event_utils.C_C_QUEUE_NUM
                                    --,p_corrmsgid =>
                                    --,p_comment =>
                                    --,p_delay =>
                                   ,io_msgid => o_MsgID);
          exit;
          /*exception
          when others then
            rollback;
            if gb_check_qmanager
            then
              raise;
            end if;
            start_qmanager;*/
        end;
      end loop;
      v_qlog := it_q_message.messlog_get(p_msgid => o_MsgID, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT);
      insert into itt_event_log l
        (log_id
        ,msgid
        ,SystemId
        ,LevelInfo
        ,reg_status)
      values
        (v_qlog.log_id
        ,v_qlog.msgid
        ,vp_systemid
        ,v_LevelInfo
        ,v_qlog.status);
      commit;
    exception
      -- Сохраняем как мусорное сообщение
      when others then
        rollback;
        it_q_message.messlog_insert_trash(p_message_type => it_q_message.C_C_MSG_TYPE_R
                                         ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                                         ,p_ServiceName => v_servicename
                                         ,p_MESSBODY => p_MsgBODY
                                         ,p_MessMETA => vx_msgmeta
                                         ,p_comment => sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace
                                         ,io_msgid => o_MsgID
                                         ,o_logid => v_log_id);
        insert into itt_event_log
          (log_id
          ,msgid
          ,SystemId
          ,LevelInfo
          ,reg_status)
        values
          (v_log_id
          ,o_MsgID
          ,vp_systemid
          ,v_LevelInfo
          ,it_q_message.C_STATUS_TRASH);
        commit;
    end;
  exception
    when others then
      rollback;
      o_errtxt := it_q_message.get_errtxt(sqlerrm);
      it_information.store_info(p_info_type => 'RegEVENT#' || p_SystemId
                               ,p_info_content => 'ОШИБКА при регистрации события :' || o_errtxt || utl_tcp.crlf || 'MsgBODY=' || it_xml.Clob_to_str(p_MsgBODY, 2000) ||
                                                  utl_tcp.crlf || 'EventID=' || p_EventID || utl_tcp.crlf || 'SystemId=' || p_SystemId || utl_tcp.crlf || 'ServiceName=' ||
                                                  p_ServiceName || utl_tcp.crlf || 'MsgMETA=' || p_MsgMETA);
      it_error.put_error_in_stack;
      it_log.log(p_msg => 'p_EventID=' || p_EventID || utl_tcp.crlf || 'p_SystemId=' || p_SystemId || utl_tcp.crlf || 'p_ServiceName=' || p_ServiceName
                ,p_msg_type => it_log.C_MSG_TYPE__ERROR
                ,p_msg_clob => p_MsgMETA);
      o_MsgID := null;
  end;

  --Информация о событии из журнала событий
  function GetEvent(p_MsgID       varchar2 -- ключ который был получен CALLBACKID RegisterEvent
                   ,p_contentType clob default null -- переключатель структуры выхлопа выходного CLOBa. на первой итерации констатнта - XML , по умолчанию он же . CLOB сериализуем в XML.
                    ) return clob -- содержимое события прямой мапинг из таблицы в софре. сюда же ошибки , если не нашлось , или что-то сломалось в СОФРе . в случае ошибки первое сериализованное поле - литера sofreveneterror
   as
    v_xres       xmltype;
    v_sqlerrm    varchar2(2000);
    v_qlog       itt_q_message_log%rowtype;
    v_log_id     itt_event_log.log_id%type;
    v_info_msgid itt_event_log.info_msgid%type;
  begin
    select max(log_id)
          ,max(info_msgid)
      into v_log_id
          ,v_info_msgid
      from (select log_id
                  ,info_msgid
              from itt_event_log l
             where msgid = p_MsgID
             order by decode(l.reg_status, it_q_message.get_constant_str('C_STATUS_TRASH'), 1, 0))
     where rownum < 2;
    v_qlog := it_q_message.get_msg(p_msgid => p_MsgID);
    if v_log_id is not null
       and v_qlog.log_id is not null
    then
      v_xres := it_xml.Clob_to_xml(v_qlog.messmeta);
      select insertxmlbefore(v_xres, 'Event/MsgMETA', xmlelement("MsgBODY", it_xml.Clob_to_str(v_qlog.messbody))) into v_xres from dual;
      select insertxmlafter(v_xres
                           ,'Event/MsgMETA'
                           ,xmlelement("RegisterEvent"
                                       ,xmlattributes(v_qlog.servicename as "ServiceName"
                                                     ,v_qlog.senderuser as "SenderUser"
                                                     ,v_qlog.workuser as "WorkUser"
                                                     ,it_xml.timestamp_to_char_iso8601(v_qlog.requestdt) as "RequestDt"
                                                     ,v_qlog.status as "Status"
                                                     ,it_xml.timestamp_to_char_iso8601(v_qlog.statusdt) as "StatusDt")))
        into v_xres
        from dual;
      if v_info_msgid is not null
      then
        v_qlog := it_q_message.get_msg(p_msgid => v_info_msgid);
        select insertxmlafter(v_xres
                             ,'Event/RegisterEvent'
                             ,xmlelement("GetInfo"
                                         ,xmlattributes(v_qlog.msgid as "MsgId"
                                                       ,v_qlog.servicename as "ServiceName"
                                                       ,v_qlog.senderuser as "SenderUser"
                                                       ,v_qlog.workuser as "WorkUser"
                                                       ,it_xml.timestamp_to_char_iso8601(v_qlog.requestdt) as "RequestDt"
                                                       ,v_qlog.status as "Status"
                                                       ,it_xml.timestamp_to_char_iso8601(v_qlog.statusdt) as "StatusDt")))
          into v_xres
          from dual;
      end if;
    else
      select xmlelement("SOFREvenetError", 'Событие # ' || p_MsgID || ' не найдено в журнале.') into v_xres from dual;
    end if;
    return v_xres.getClobVal();
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      v_sqlerrm := it_q_message.get_errtxt(sqlerrm);
      select xmlelement("SOFREvenetError", 'Ошибка чтения журнала Событие # ' || p_MsgID || ':' || v_sqlerrm) into v_xres from dual;
      return v_xres.getClobVal();
  end;

  --Список новых сообщений для мониторинга
  function GetNewInfo(p_SystemId itt_event_log.systemid%type default null) return tt_monitoring_info
    pipelined as
    pragma autonomous_transaction;
    vc_proc constant varchar2(100) := 'GNI';
    vrec         tr_monitoring_info;
    v_outmessage it_q_message_t;
    v_errno      integer;
    v_errmsg     varchar2(2000);
    vr_log       itt_q_message_log%rowtype;
    v_SystemId   itt_event_log.systemid%type := upper(p_SystemId);
  begin
    -- Проверяем сформированные ответы контрольных сервмсов предыдущих запусков
    for omsg in (select m.SystemId
                       ,o.QUEUENAME
                       ,o.QMSGID
                       ,o.CORRMSGID
                       ,o.ENQDT
                       ,o.SERVICENAME
                   from itv_q_out o
                   join table(it_event_utils.sel_SystemID_monitiring) m
                     on upper(m.servicename) = o.SERVICENAME
                  where o.MESSAGE_TYPE = it_q_message.get_constant_str('C_C_MSG_TYPE_A')
                    and o.RECEIVER = it_event_utils.get_constant_str('C_C_MONITORINGSYSTEM')
                    and m.SystemId = nvl(v_SystemId, m.SystemId))
    loop
      it_q_message.dequeue_outmessage(p_queuename => omsg.QUEUENAME, p_qmsgid => omsg.QMSGID, o_message => v_outmessage, o_errno => v_errno, o_errmsg => v_errmsg);
      if v_errno = 0
      then
        if v_outmessage.MSGCode = 0
        then
          begin
            vrec.SystemId   := omsg.SystemId;
            vrec.Info_msgid := omsg.CORRMSGID;
            vrec.Info_enqdt := omsg.ENQDT;
            it_event_utils.parsing_Info_BODY(p_BODY => v_outmessage.MessBODY
                                            ,o_Info_txt => vrec.Info_txt
                                            ,o_MaxLevel => vrec.MaxLevel
                                            ,o_MaxLevel_dt => vrec.MaxLevel_dt
                                            ,o_MaxLevel_txt => vrec.MaxLevel_txt);
            vrec.Info_META := it_xml.xml_to_Clob(v_outmessage.MessMETA);
          exception
            when others then
              vrec := it_event_utils.store_error_message(p_SystemId => omsg.SystemId
                                                        ,p_LevelInfo => 6
                                                        ,p_Info_dt => sysdate
                                                        ,p_Info_txt => 'Ошибка разбора данных ответа [' || omsg.SERVICENAME || ']' || omsg.CORRMSGID || ' :' ||
                                                                       it_q_message.get_errtxt(sqlerrm));
          end;
        else
          vr_log := it_q_message.messlog_get(p_msgid => v_outmessage.msgid, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT);
          vrec   := it_event_utils.store_error_message(p_SystemId => omsg.SystemId
                                                      ,p_LevelInfo => 6
                                                      ,p_Info_dt => sysdate
                                                      ,p_Info_txt => 'Ошибка выполнения контрольного сервиса [' || omsg.SERVICENAME || ']' || omsg.CORRMSGID || ' :' ||
                                                                     v_outmessage.MSGText
                                                      ,p_comment => vr_log.commenttxt);
        end if;
        commit;
        pipe row(vrec);
      end if;
    end loop;
    vrec := it_event_utils.check_qmanager(p_SystemId => v_SystemId); -- Проверка QManagera
    if nvl(vrec.MaxLevel, 0) != 0 -- Если проблема возвращаем информацию
    then
      pipe row(vrec);
    end if;
    if nvl(vrec.MaxLevel, 0) < 10 -- Иначе запускаем контрольные сервмсы
    then
      for srv in (select *
                    from (select rownum as tr
                                ,SystemId
                                ,servicename
                            from table(it_event_utils.sel_SystemID_monitiring)
                           order by service_id)
                   where SystemId = nvl(v_SystemId, SystemId))
      loop
        declare
          v_msgid itt_q_message_log.msgid%type := null;
        begin
          it_q_message.load_msg(io_msgid => v_msgid
                               ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                               ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                               ,p_ServiceGroup => it_event_utils.C_C_MESSAGESEND_CORR || srv.systemid
                               ,p_queue_num => it_event_utils.C_C_QUEUE_NUM
                               ,p_Sender => it_event_utils.C_C_MONITORINGSYSTEM
                               ,p_ServiceName => srv.servicename);
          commit;
        exception
          when others then
            rollback;
            it_event_utils.send_information_error(p_sqlcode => vc_proc || sqlcode, p_sqlerrm => sqlerrm);
            it_error.put_error_in_stack;
            it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
        end;
      end loop;
    end if;
  exception
    when others then
      rollback;
      it_event_utils.send_information_error(p_sqlcode => vc_proc || sqlcode, p_sqlerrm => sqlerrm);
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --Список архивных сообщений мониторинга
  function GetInfo(p_dBegin date
                  ,p_dEnd   date
                  ,p_all    number default 0) return tt_monitoring_info
    pipelined as
    pragma autonomous_transaction;
    vc_proc constant varchar2(100) := 'GI';
    vrec tr_monitoring_info;
    --vx_meta xmltype;
    v_all pls_integer := nvl(p_all, 0);
  begin
    -- Проверяем сформированные ответы контрольных сервмсов предыдущих запусков
    for omsg in (select m.SystemID
                       ,o.*
                   from itt_q_message_log o
                   join table(it_event_utils.sel_SystemID_monitiring) m
                     on upper(m.servicename) = o.SERVICENAME
                  where o.enqdt >= p_dBegin
                    and o.enqdt < p_dEnd
                    and o.MESSAGE_TYPE = it_q_message.get_constant_str('C_C_MSG_TYPE_A')
                    and o.RECEIVER = it_event_utils.get_constant_str('C_C_MONITORINGSYSTEM')
                    and o.msgcode = 0
                  order by o.enqdt)
    loop
      begin
        vrec.SystemId   := omsg.SystemId;
        vrec.Info_msgid := omsg.MSGID;
        vrec.Info_enqdt := omsg.ENQDT;
        it_event_utils.parsing_Info_BODY(p_BODY => omsg.MessBODY
                                        ,o_Info_txt => vrec.Info_txt
                                        ,o_MaxLevel => vrec.MaxLevel
                                        ,o_MaxLevel_dt => vrec.MaxLevel_dt
                                        ,o_MaxLevel_txt => vrec.MaxLevel_txt);
        vrec.Info_META := omsg.MessMETA;
      exception
        when others then
          vrec := null;
      end;
      if vrec.SystemId is not null
         and (v_all != 0 or vrec.Info_txt is not null or vrec.MaxLevel is not null or vrec.MaxLevel_txt is not null or dbms_lob.getlength(omsg.MessMETA) != 0)
      then
        pipe row(vrec);
      end if;
    end loop;
  exception
    when others then
      rollback;
      it_event_utils.send_information_error(p_sqlcode => vc_proc || sqlcode, p_sqlerrm => sqlerrm);
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --Новы сообщение для рассылки 
  function GetNewMessageSend(p_wait         number default null -- ожидание в секундах null до появления в очереди
                            ,p_Message_type char default null) return tt_MessageSend
    pipelined as
    pragma autonomous_transaction;
    vc_proc constant varchar2(100) := 'GNMS';
    vr_res       tr_MessageSend;
    v_outmessage it_q_message_t;
    v_errno      integer;
    v_errmsg     varchar2(2000);
    vcur         sys_refcursor;
    v_corr_add   char(1) := nvl(p_Message_type, '%');
  begin
    it_q_message.dequeue_outmessage(p_queuename => it_q_message.C_C_QUEUE_OUT_PREFIX || it_event_utils.C_C_QUEUE_NUM
                                   ,p_correlation => it_event_utils.C_C_MESSAGESEND_CORR || v_corr_add
                                   ,p_wait => p_wait
                                   ,o_message => v_outmessage
                                   ,o_errno => v_errno
                                   ,o_errmsg => v_errmsg);
    if v_errno = 0
       and v_outmessage.MSGCode = 0
    then
      open vcur for
        select v_outmessage.corrmsgid as Info_msgid
              ,v_outmessage.RequestDT as Info_enqdt
              ,sm.*
          from XMLTABLE('/INFO/Message' passing v_outmessage.messmeta columns Message_type char(1) path '@TYPE'
                       ,Message_TO varchar2(2000) path '@TO'
                       ,Subject varchar2(200) path '@Subject'
                       ,Page number path '@Page'
                       ,Text varchar2(4000) path 'Text') sm;
      commit;
      loop
        fetch vcur
          into vr_res;
        exit when vcur%notfound;
        pipe row(vr_res);
      end loop;
      close vcur;
    end if;
  exception
    when others then
      rollback;
      it_event_utils.send_information_error(p_sqlcode => vc_proc || sqlcode, p_sqlerrm => sqlerrm);
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --Парсинг Сообщения для рассылки 
  function GetMessageSend(p_Info_msgid itt_event_log.info_msgid%type -- GUID INFO сообщения 
                          ) return tt_MessageSend
    pipelined as
    vc_proc constant varchar2(100) := 'GMS';
    vr_res tr_MessageSend;
    vr_log itt_q_message_log%rowtype;
    vcur   sys_refcursor;
  begin
    vr_log := it_q_message.messlog_get(p_msgid => p_Info_msgid, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT);
    if vr_log.msgid is not null
    then
      /* select insertchildxml(iox_INFO
      ,'/INFO'
      ,'Message'
      ,XMLElement("Message"
                  ,xmlattributes(p_type as "TYPE", p_Receiver as "TO", p_Title as "Subject", p_pageno as "Page")
                  ,XMLELEment("Text", p_text)))*/
      /*    
      Info_msgid   itt_event_log.info_msgid%type -- GUID INFO сообщения 
      ,Info_enqdt   date -- Сообщение сформировано 
      ,Message_type char(1) -- M - почта / T - telegramm 
      ,Message_TO   varchar2(2000) -- Адресат сообщения 
      ,Subject      varchar2(200) -- Тема сообщения 
      ,Page         number -- Номер страницы сообщения
      ,Text         varchar2(4000) -- Текст сообщения  */
      open vcur for
        select l.msgid as Info_msgid
              ,l.enqdt as Info_enqdt
              ,sm.*
          from itt_q_message_log l
              ,XMLTABLE('/INFO/Message' passing xmltype(l.messmeta) columns Message_type char(1) path '@TYPE'
                       ,Message_TO varchar2(2000) path '@TO'
                       ,Subject varchar2(200) path '@Subject'
                       ,Page number path '@Page'
                       ,Text varchar2(4000) path 'Text') sm
         where l.corrmsgid = vr_log.msgid
           and l.servicename = it_event_utils.get_constant_str('C_C_MESSAGESEND_SN');
      loop
        fetch vcur
          into vr_res;
        exit when vcur%notfound;
        pipe row(vr_res);
      end loop;
      close vcur;
    end if;
  exception
    when others then
      rollback;
      it_event_utils.send_information_error(p_sqlcode => vc_proc || sqlcode, p_sqlerrm => sqlerrm);
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  function GetXMLRegisterError(p_ErrorCode integer
                              ,p_ErrorDesc varchar2
                              ,p_LevelInfo integer -- >= 0 для службы поддержки 0 - сообщение 10 - авария , <0 - бизнес ошибки . Не отправляются в поддержку
                              ,p_backtrace varchar2 default null
                              ,p_MsgBODY   clob default null -- Развернутый текст описания ошибки 
                              ,p_MsgMETA   clob default null -- XML будет добавлено в RootElement <XML> 
                               ) return xmltype as
    vx_MessMETA xmltype;
  begin
    begin
      vx_MessMETA := it_xml.Clob_to_xml(p_MsgMETA);
    exception
      when others then
        select xmlelement("MsgMETA", p_MsgMETA) into vx_MessMETA from dual;
    end;
    begin
      select xmlelement("XML"
                        ,xmlattributes(p_LevelInfo as "LevelInfo", p_ErrorCode as "ErrorCode", p_ErrorDesc as "ErrorDesc")
                        ,xmlelement("BackTrace", p_backtrace)
                        ,xmlelement("MsgBODY", p_MsgBODY)
                        ,vx_MessMETA)
        into vx_MessMETA
        from dual;
    exception
      when others then
        vx_MessMETA := null;
    end;
    return vx_MessMETA;
  end;

  -- Регистрация ОШИБКИ как события мониторинга
  procedure RegisterError(p_SystemId    varchar2 -- SYSTEMID ключ системы 
                         ,p_ServiceName varchar2 -- ключ внешнего сервиса , у одного SYSTEMID м.б. несколько сервисов,
                         ,p_ErrorCode   integer
                         ,p_ErrorDesc   varchar2
                         ,p_LevelInfo   integer -- >= 0 для службы поддержки 0 - сообщение 10 - авария , <0 - бизнес ошибки . Не отправляются в поддержку
                         ,p_backtrace   varchar2 default null
                         ,p_MsgBODY     clob default null -- Развернутый текст описания ошибки 
                         ,p_MsgMETA     clob default null -- XML будет добавлено в RootElement <XML> 
                          ) as
    pragma autonomous_transaction;
    vx_MessMETA xmltype;
    v_errtxt    varchar2(2000);
    v_MsgID     varchar2(128);
  begin
    vx_MessMETA := GetXMLRegisterError(p_ErrorCode => p_ErrorCode, p_ErrorDesc => p_ErrorDesc, p_LevelInfo => p_LevelInfo, p_backtrace => p_backtrace, p_MsgMETA => p_MsgMETA);
    it_event.RegisterEvent( --p_EventID => p_GUID
                           p_SystemId => p_SystemId
                          ,p_ServiceName => p_ServiceName
                          ,p_MsgBODY => nvl(p_MsgBODY, 'Ошибка#' || p_ErrorCode || ':' || p_ErrorDesc)
                          ,p_MsgMETA => case
                                          when vx_MessMETA is not null then
                                           vx_MessMETA.getClobVal
                                        end
                          ,o_errtxt => v_errtxt
                          ,o_MsgID => v_MsgID);
    commit;
  end;

  --  Запись ОШИБКИ в формате события в ITT_LOG .
  procedure AddErrorITLog(p_SystemId    varchar2 -- SYSTEMID ключ системы 
                         ,p_ServiceName varchar2 -- ключ внешнего сервиса , у одного SYSTEMID м.б. несколько сервисов,
                         ,p_ErrorCode   integer
                         ,p_ErrorDesc   varchar2
                         ,p_LevelInfo   integer -- >= 0 для службы поддержки 0 - сообщение 10 - авария , <0 - бизнес ошибки . Не отправляются в поддержку
                         ,p_backtrace   varchar2 default null
                         ,p_MsgBODY     clob default null -- Развернутый текст описания ошибки 
                         ,p_MsgMETA     clob default null -- XML будет добавлено в RootElement <XML> 
                          ) as
    vp_systemid      varchar2(128);
    vp_servicename   varchar2(128);
    v_keyservicename varchar2(128);
    v_servicename    varchar2(128);
    vx_MessMETA      xmltype;
  begin
    vx_MessMETA := GetXMLRegisterError(p_ErrorCode => p_ErrorCode
                                      ,p_ErrorDesc => p_ErrorDesc
                                      ,p_LevelInfo => p_LevelInfo
                                      ,p_backtrace => p_backtrace
                                      ,p_MsgBODY => p_MsgBODY
                                      ,p_MsgMETA => p_MsgMETA);
    GetEventParam(p_SystemId, p_ServiceName, vp_systemid, vp_servicename, v_keyservicename, v_servicename);
    it_log.log_handle(p_object => v_servicename
                     ,p_msg => 'Error(' || p_LevelInfo || ')#' || p_ErrorCode || ':' || p_ErrorDesc
                     ,p_msg_type => it_log.C_MSG_TYPE__ERROR
                     ,p_msg_clob => vx_MessMETA.getClobVal);
  end;

  -- Запись ОШИБКИ в формате события в ITT_LOG и регистрация события мониторинга (RegisterError) с защитой от спама по p_SystemId,p_ServiceName,p_ErrorCode за период p_period) .
  procedure AddErrorITLogMonitoring(p_SystemId    varchar2 -- SYSTEMID ключ системы 
                                   ,p_ServiceName varchar2 -- ключ внешнего сервиса , у одного SYSTEMID м.б. несколько сервисов,
                                   ,p_ErrorCode   integer
                                   ,p_ErrorDesc   varchar2
                                   ,p_LevelInfo   integer -- >= 0 для службы поддержки 0 - сообщение 10 - авария , <0 - бизнес ошибки . Не отправляются в поддержку
                                   ,p_backtrace   varchar2 default null
                                   ,p_MsgBODY     clob default null -- Развернутый текст описания ошибки 
                                   ,p_MsgMETA     clob default null -- XML будет добавлено в RootElement <XML> 
                                   ,p_period      integer default null) as
    vn_period        integer := nvl(p_period, nvl(it_rs_interface.get_parm_number_path(it_event_utils.GC_PARAM_ANTI_SPAM), 600));
    v_cntnsg         integer;
    vp_systemid      varchar2(128);
    vp_servicename   varchar2(128);
    v_servicename    varchar2(128);
    v_keyservicename varchar2(128);
  begin
    GetEventParam(p_SystemId, p_ServiceName, vp_systemid, vp_servicename, v_keyservicename, v_servicename);
    select count(*)
      into v_cntnsg
      from (select ml.log_id
                  ,to_number(extractValue(xmltype(ml.messmeta), 'Event/MsgMETA/XML/@ErrorCode')) as ErrorCode
                  ,extractValue(xmltype(ml.messmeta), 'Event/@ServiceName') as ServiceName
              from itt_event_log lout
              join itt_q_message_log ml
                on lout.log_id = ml.log_id
             where lout.systemid = vp_systemid
               and lout.create_sysdate >= sysdate - numtodsinterval(vn_period, 'SECOND'))
     where ErrorCode = p_ErrorCode
       and upper(ServiceName) = upper(vp_servicename);
    if v_cntnsg = 0
       or p_period is null
    then
      AddErrorITLog(p_SystemId => p_SystemId
                   ,p_ServiceName => p_ServiceName
                   ,p_ErrorCode => p_ErrorCode
                   ,p_ErrorDesc => p_ErrorDesc
                   ,p_LevelInfo => p_LevelInfo
                   ,p_backtrace => p_backtrace
                   ,p_MsgBODY => p_MsgBODY
                   ,p_MsgMETA => p_MsgMETA);
    end if;
    if v_cntnsg = 0
    then
      RegisterError(p_SystemId => p_SystemId
                   ,p_ServiceName => p_ServiceName
                   ,p_ErrorCode => p_ErrorCode
                   ,p_ErrorDesc => p_ErrorDesc
                   ,p_LevelInfo => p_LevelInfo
                   ,p_backtrace => p_backtrace
                   ,p_MsgBODY => p_MsgBODY
                   ,p_MsgMETA => p_MsgMETA);
    end if;
  end;

end it_event;
/

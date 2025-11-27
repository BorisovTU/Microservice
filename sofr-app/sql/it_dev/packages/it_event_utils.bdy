create or replace package body it_event_utils is

  /**
   @file it_event_utils.bdy
   @brief Сервисы для формирования сообщений Мониторинта БО СОФР BIQ-13171
     
   # changeLog
   |date       |author      |tasks           |note                                                        
   |-----------|------------|----------------|-------------------------------------------------------------
   |22.01.2025 |Зыков М.В.  |CCBO-10701      | Реализация мониторинга по итогу СОР, по мотивам дефекта DEF-74069
   |08.12.2023 |Зыков М.В.  |DEF-54477       | BIQ-13171. Формирование оповещений
   |08.11.2023 |Зыков М.В.  |DEF-54476       | BIQ-13171. Доработка механизма отправки сообщений из SiteScope
   |23.10.2023 |Зыков М.В.  |BOSS-1230       | BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
   |2023.09.20 |Зыков М.В.  |CCBO-7636       | BIQ-13171 Story CCBO-4826 - разработка. Настройка параметров проверок                
   |2023.05.15 |Зыков М.В.  |CCBO-4870       | Создание BIQ-13171. Разработка процедур работы с очередью событий                 
    
  */
  GC_SystemId_QMANAGER constant varchar2(100) := 'QMANAGER';

  GC_SystemId_OTHERS constant varchar2(100) := 'OTHERS';

  GC_TEXT_BEGIN_MSG constant varchar2(5) := ' <!>';

  GC_TEXT_END_MSG constant varchar2(5) := chr(13) || chr(10);

  GC_HTML_BEGIN_MSG constant varchar2(5) := '<p>';

  GC_HTML_END_MSG constant varchar2(5) := '</p>';

  g_format_msg           integer := 0; -- 0 - Текст 1- HTML
  gn_low_levelinfo_msg_t integer := 6; -- Минимальный уровень критичности для отправки сообщений Т 
  gn_period_msg_t        integer := 60; -- период повтора событий  для отправки в сообщении Т (мин)
  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic as
    v_ret varchar2(32676);
  begin
    execute immediate ' begin :1 := it_event_utils.' || p_constant || '; end;'
      using out v_ret;
    return v_ret;
  exception
    when others then
      return null;
  end;

  -- Создание сообщений об ошибке
  procedure send_information_error(p_sqlcode varchar2
                                  ,p_sqlerrm varchar2) is
    pragma autonomous_transaction;
    vd_last_rem date;
    vc_sid      varchar2(200);
  begin
    vc_sid      := C_C_QSET_PREFIX || '[ERR#' || p_sqlcode || ']';
    vd_last_rem := it_q_message.get_qset_data(p_qset_name => vc_sid);
    if vd_last_rem is null
       or sysdate > vd_last_rem + numtodsinterval(5, 'MINUTE')
    then
      it_q_message.set_qset_data(p_qset_name => vc_sid, p_date => sysdate);
      it_information.store_info(p_info_type => 'ERR#' || p_sqlcode, p_info_content => p_sqlerrm);
    end if;
    commit;
  end;

  procedure MessageSend(p_CORRmsgid itt_q_message_log.msgid%type
                       ,px_SendInfo xmltype) as
    v_MsgID    itt_q_message_log.msgid%type := null;
    vx_Msg     xmltype;
    v_corr_add char(1);
  begin
    if px_SendInfo is not null
    then
      -- Разбиваем на отдельные сообщения
      for msg in (select column_value as event from XMLTABLE('/INFO/Message' passing px_SendInfo))
      loop
        -- Сначала отправляем пустое сообщение для сброса порогового значения SiteScope
        select EXTRACTVALUE(msg.event, '/Message/@TYPE') into v_corr_add from dual;
        v_MsgID := null;
        it_q_message.load_msg(io_msgid => v_MsgID
                             ,p_message_type => it_q_message.C_C_MSG_TYPE_A
                             ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                             ,p_Correlation => C_C_MESSAGESEND_CORR || v_corr_add
                             ,p_CORRmsgid => p_CORRmsgid
                             ,p_ServiceName => C_C_MESSAGESEND_SN
                             ,p_Receiver => C_C_MONITORINGSYSTEM
                             ,p_queue_num => C_C_QUEUE_NUM);
        select xmlelement("INFO", msg.event) into vx_Msg from dual;
        v_MsgID := null;
        it_q_message.load_msg(io_msgid => v_MsgID
                             ,p_message_type => it_q_message.C_C_MSG_TYPE_A
                             ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                             ,p_Correlation => C_C_MESSAGESEND_CORR || v_corr_add
                             ,p_CORRmsgid => p_CORRmsgid
                             ,p_ServiceName => C_C_MESSAGESEND_SN
                             ,p_Receiver => C_C_MONITORINGSYSTEM
                             ,p_MessMETA => vx_Msg
                             ,p_queue_num => C_C_QUEUE_NUM);
      end loop;
    end if;
  end;

  procedure Insert_INFO_Message(iox_INFO   in out xmltype
                               ,p_type     char -- M - email, T-Telegramm
                               ,p_Receiver varchar2
                               ,p_Title    varchar2 default null
                               ,p_text     varchar2
                               ,p_pageno   integer default null) as
    v_text varchar2(4000);
  begin
    if p_text is null
    then
      return;
    end if;
    if p_type = 'M'
       and g_format_msg = 1
    then
      v_text := '<!DOCTYPE HTML><html><body>' || replace(replace(p_text, GC_TEXT_BEGIN_MSG, GC_HTML_BEGIN_MSG), GC_TEXT_END_MSG, GC_HTML_END_MSG) || '</body></html>';
    else
      v_text := p_text;
    end if;
    if iox_INFO is null
    then
      select XMLELEment("INFO") into iox_INFO from dual;
    end if;
    case
      when p_type in ('M', 'T') then
        select insertchildxml(iox_INFO
                             ,'/INFO'
                             ,'Message'
                             ,XMLElement("Message", xmlattributes(p_type as "TYPE", p_Receiver as "TO", p_Title as "Subject", p_pageno as "Page"), XMLELEment("Text", v_text)))
          into iox_INFO
          from dual;
      else
        raise_application_error(-20000, 'Ошибка параметра процедуры Insert_INFO_Message p_type =' || p_type);
    end case;
  end;

  function get_cur_event_log(p_SystemId   itt_event_log.systemid%type
                            ,p_info_msgid itt_event_log.info_msgid%type default null) return sys_refcursor as
    cur_event sys_refcursor;
  begin
    if p_info_msgid is null
    then
      if p_SystemId = GC_SystemId_OTHERS
      then
        open cur_event for
          select *
            from itt_event_log e
           where upper(e.systemid) not in (select SystemId from table(sel_SystemID_monitiring) where SystemId != GC_SystemId_OTHERS)
             and e.info_msgid is null
             and nvl(e.levelinfo, 0) >= 0
             and e.create_sysdate >= sysdate - 1
           order by e.create_sysdate
             for update skip locked;
      else
        open cur_event for
          select *
            from itt_event_log e
           where upper(e.systemid) = p_SystemId
             and e.info_msgid is null
             and nvl(e.levelinfo, 0) >= 0
             and e.create_sysdate >= sysdate - 1
           order by e.create_sysdate
             for update skip locked;
      end if;
    else
      open cur_event for
        select * from itt_event_log e where e.info_msgid = p_info_msgid order by e.create_sysdate;
    end if;
    return cur_event;
  end;

  --  возвращает событие с максимальным уровенем критичности
  function get_event_MaxLevel(p_SystemId     itt_event_log.systemid%type
                             ,p_MsgId        itt_event_log.msgid%type
                             ,o_MaxLevel_dt  out date
                             ,o_MaxLevel_txt out varchar2) return number as
    v_res          number;
    vr_qlog        itt_q_message_log%rowtype;
    cur_event      sys_refcursor;
    vr_event       itt_event_log%rowtype;
    v_ServiceName  varchar2(128);
    v_MaxLevel_txt varchar2(2000);
    function get_MaxLevel_txt(p_SystemId          varchar2
                             ,p_event_systemid    varchar2
                             ,p_event_servicename varchar2
                             ,p_messbody          clob) return varchar2 as
    begin
      if p_SystemId = GC_SystemId_OTHERS
      then
        return '[' || p_event_systemid || '.' || p_event_servicename || ']:' || it_xml.Clob_to_str(p_messbody, 20);
      else
        return '[' || p_event_servicename || ']:' || it_xml.Clob_to_str(p_messbody, 40);
      end if;
    end;
  
  begin
    cur_event := get_cur_event_log(p_SystemId);
    loop
      fetch cur_event
        into vr_event;
      exit when cur_event%notfound;
      vr_qlog := it_q_message.messlog_get(p_logid => vr_event.log_id);
      select EXTRACTVALUE(xmltype(vr_qlog.messmeta), '/Event/@ServiceName') into v_ServiceName from dual;
      v_MaxLevel_txt := get_MaxLevel_txt(p_SystemId, vr_event.systemid, v_ServiceName, vr_qlog.messbody);
      if nvl(vr_event.levelinfo, 0) > nvl(v_res, 0)
      then
        v_res          := nvl(vr_event.levelinfo, 0);
        o_MaxLevel_dt  := vr_qlog.requestdt;
        o_MaxLevel_txt := v_MaxLevel_txt;
      elsif vr_event.levelinfo = v_res
      then
        o_MaxLevel_txt := it_xml.Clob_to_str(o_MaxLevel_txt || case
                                               when o_MaxLevel_txt is not null then
                                                ' /'
                                             end || v_MaxLevel_txt
                                            ,200);
      end if;
    end loop;
    close cur_event;
    return v_res;
  exception
    when others then
      if cur_event%isopen
      then
        close cur_event;
      end if;
      raise;
  end;

  function get_event_line(p_SystemId         varchar2
                         ,p_len              integer
                         ,p_create_sysdate   date
                         ,p_EventSystemId    varchar2
                         ,p_EventServiceName varchar2
                         ,p_levelinfo        number
                         ,p_messbody         clob) return varchar2 as
  begin
    return GC_TEXT_BEGIN_MSG || to_char(p_create_sysdate, 'dd.mm.yyyy hh24:mi:ss ') || '[' --
    || case when p_SystemId = GC_SystemId_OTHERS then p_EventSystemId || '.' end || --
    p_EventServiceName || ']:' || case when nvl(p_levelinfo, 0) > 0 then '(' || p_levelinfo || ')' end || --
    trim(replace(replace(it_xml.Clob_to_str(p_messbody, p_len - 200), GC_TEXT_BEGIN_MSG), GC_TEXT_END_MSG)) || GC_TEXT_END_MSG;
  end;

  function get_event_msg_t(p_len            integer
                          ,p_create_sysdate date
                          ,p_messbody       clob) return varchar2 as
  begin
    return to_char(p_create_sysdate, 'dd.mm.yyyy hh24:mi:ss ') || ':' || trim(it_xml.Clob_to_str(p_messbody, p_len));
  end;

  -- Возвращает событие и отмечает его как обработанное
  function get_text_event(p_SystemId itt_event_log.systemid%type
                         ,p_MsgId    itt_event_log.msgid%type
                         ,p_len      integer default 3800) return varchar2 as
    vr_qlog       itt_q_message_log%rowtype;
    v_res         varchar2(32676);
    v_line        varchar2(32676);
    cur_event     sys_refcursor;
    vr_event      itt_event_log%rowtype;
    v_ServiceName varchar2(2000);
  begin
    cur_event := get_cur_event_log(p_SystemId);
    loop
      fetch cur_event
        into vr_event;
      exit when cur_event%notfound;
      vr_qlog := it_q_message.messlog_get(p_logid => vr_event.log_id);
      select EXTRACTVALUE(xmltype(vr_qlog.messmeta), '/Event/@ServiceName') into v_ServiceName from dual;
      v_line := get_event_line(p_SystemId => p_SystemId
                              ,p_len => p_len
                              ,p_create_sysdate => vr_event.create_sysdate
                              ,p_EventSystemId => vr_event.systemid
                              ,p_EventServiceName => v_ServiceName
                              ,p_levelinfo => vr_event.levelinfo
                              ,p_messbody => vr_qlog.messbody);
      exit when length(v_res) + length(v_line) > p_len or length(v_res) + length(v_line) > 32676;
      v_res := v_res || v_line;
      update itt_event_log e set e.info_msgid = p_MsgId where e.log_id = vr_event.log_id;
    end loop;
    close cur_event;
    return v_res;
  exception
    when others then
      if cur_event%isopen
      then
        close cur_event;
      end if;
      raise;
  end;

  -- Возвращает списком события и отмечает их как обработанные
  function get_text_event_list(p_SystemId itt_event_log.systemid%type
                              ,p_MsgId    itt_event_log.msgid%type
                              ,p_len      integer default 3800) return varchar2 as
    vr_qlog       itt_q_message_log%rowtype;
    v_res         varchar2(32676);
    v_line        varchar2(32676);
    cur_event     sys_refcursor;
    vr_event      itt_event_log%rowtype;
    v_ServiceName varchar2(2000);
  begin
    cur_event := get_cur_event_log(p_SystemId);
    loop
      fetch cur_event
        into vr_event;
      exit when cur_event%notfound;
      vr_qlog := it_q_message.messlog_get(p_logid => vr_event.log_id);
      select EXTRACTVALUE(xmltype(vr_qlog.messmeta), '/Event/@ServiceName') into v_ServiceName from dual;
      v_line := get_event_line(p_SystemId => p_SystemId
                              ,p_len => p_len
                              ,p_create_sysdate => vr_event.create_sysdate
                              ,p_EventSystemId => vr_event.systemid
                              ,p_EventServiceName => v_ServiceName
                              ,p_levelinfo => vr_event.levelinfo
                              ,p_messbody => vr_qlog.messbody);
      exit when length(v_res) + length(v_line) > p_len or length(v_res) + length(v_line) > 32676;
      v_res := v_res || v_line;
      update itt_event_log e set e.info_msgid = p_MsgId where e.log_id = vr_event.log_id;
    end loop;
    close cur_event;
    return v_res;
  exception
    when others then
      if cur_event%isopen
      then
        close cur_event;
      end if;
      raise;
  end;

  -- Возвращает списком события выше заданного уровня критичности 
  function get_text_event_list_top(p_SystemId itt_event_log.systemid%type
                                  ,p_MsgId    itt_event_log.msgid%type
                                  ,p_LowLevel itt_event_log.levelinfo%type
                                  ,p_len      integer default 3800) return varchar2 as
    vr_qlog       itt_q_message_log%rowtype;
    v_res         varchar2(32676);
    v_line        varchar2(32676);
    cur_event     sys_refcursor;
    vr_event      itt_event_log%rowtype;
    v_ServiceName varchar2(2000);
  begin
    cur_event := get_cur_event_log(p_SystemId, p_MsgId);
    loop
      fetch cur_event
        into vr_event;
      exit when cur_event%notfound;
      if nvl(vr_event.levelinfo, 0) >= p_LowLevel
      then
        vr_qlog := it_q_message.messlog_get(p_logid => vr_event.log_id);
        select EXTRACTVALUE(xmltype(vr_qlog.messmeta), '/Event/@ServiceName') into v_ServiceName from dual;
        v_line := get_event_line(p_SystemId => p_SystemId
                                ,p_len => p_len
                                ,p_create_sysdate => vr_event.create_sysdate
                                ,p_EventSystemId => vr_event.systemid
                                ,p_EventServiceName => v_ServiceName
                                ,p_levelinfo => vr_event.levelinfo
                                ,p_messbody => vr_qlog.messbody);
        exit when length(v_res) + length(v_line) > p_len or length(v_res) + length(v_line) > 32676;
        v_res := v_res || v_line;
      end if;
    end loop;
    close cur_event;
    return v_res;
  exception
    when others then
      if cur_event%isopen
      then
        close cur_event;
      end if;
      raise;
  end;

  function check_MSG_T_send(px_INFO       xmltype
                           ,p_Subject     varchar default null -- Проверка перед отправкой 
                           ,p_SystemId    itt_event_log.systemid%type default null -- Проверка после восстановлением метрики
                           ,p_ServiceName varchar2 default null) return boolean as
    v_n integer := 0;
  begin
    case
      when p_Subject is not null then
        if px_INFO is not null
        then
          select count(1)
            into v_n
            from XMLTable('/INFO/Message' passing px_INFO columns msg_type char(1) path '@TYPE', msg_Subject varchar2(2000) path '@Subject')
           where msg_type = 'T'
             and upper(msg_Subject) = upper(p_Subject);
        end if;
        if v_n = 0
        then
          begin
            select 1
              into v_n
              from (select *
                      from (select enqdt
                                  ,EXTRACTVALUE(xmeta, '/INFO/Message/@TYPE') msg_type
                                  ,EXTRACTVALUE(xmeta, '/INFO/Message/@Subject') msg_Subject
                              from (select t.enqdt
                                          ,xmltype(t.messmeta) as xmeta
                                      from ITT_Q_MESSAGE_LOG t
                                     where t.enqdt >= sysdate - numtodsinterval(gn_period_msg_t, 'MINUTE')
                                       and t.servicename = C_C_MESSAGESEND_SN
                                       and t.correlation = C_C_MESSAGESEND_CORR || 'T'
                                       and t.messmeta is not null))
                     where msg_type = 'T'
                       and upper(msg_Subject) = upper(p_Subject)
                     order by enqdt desc)
             where rownum < 2;
          exception
            when no_data_found then
              v_n := 0;
          end;
        end if;
      when p_SystemId is not null
           and p_ServiceName is not null then
        if px_INFO is not null
        then
          select count(1)
            into v_n
            from XMLTable('/INFO/Message' passing px_INFO columns msg_type char(1) path '@TYPE', msg_Subject varchar2(2000) path '@Subject')
           where msg_type = 'T'
             and upper(msg_Subject) like upper(trim(p_SystemId) || '.' || trim(p_ServiceName) || '(%)');
        end if;
        if v_n = 0
        then
          begin
            select case
                     when upper(msg_Subject) like upper(trim(p_SystemId) || '.' || trim(p_ServiceName) || '(%)') then
                      1
                     else
                      0
                   end
              into v_n
              from (select *
                      from (select enqdt
                                  ,EXTRACTVALUE(xmeta, '/INFO/Message/@TYPE') msg_type
                                  ,EXTRACTVALUE(xmeta, '/INFO/Message/@Subject') msg_Subject
                              from (select t.enqdt
                                          ,xmltype(t.messmeta) as xmeta
                                      from ITT_Q_MESSAGE_LOG t
                                     where t.enqdt >= sysdate - numtodsinterval(gn_period_msg_t * 1.3, 'MINUTE')
                                       and t.servicename = C_C_MESSAGESEND_SN
                                       and t.correlation = C_C_MESSAGESEND_CORR || 'T'
                                       and t.messmeta is not null))
                     where msg_type = 'T'
                       and upper(msg_Subject) like upper(trim(p_SystemId) || '.' || trim(p_ServiceName) || '%')
                     order by enqdt desc)
             where rownum < 2;
          exception
            when no_data_found then
              v_n := 0;
          end;
        end if;
      else
        raise_application_error(-20000, 'Ошибка параметра процедуры check_MSG_T_send ');
    end case;
    return v_n > 0;
  end;

  procedure Insert_INFO_MSG_T(iox_INFO         in out xmltype
                             ,p_SystemId       itt_event_log.systemid%type
                             ,p_ServiceName    varchar2
                             ,p_LevelInfo      integer
                             ,p_create_sysdate date
                             ,p_messbody       clob
                             ,p_messnorm       varchar2 default null
                             ,p_check_hist     boolean default null) as
    v_Title      varchar2(2000);
    v_check_hist boolean := nvl(p_check_hist, true);
  begin
    if nvl(p_LevelInfo, -1) < 0
    then
      return;
    end if;
    if p_LevelInfo >= gn_low_levelinfo_msg_t
    then
      v_Title := trim(p_SystemId) || '.' || trim(p_ServiceName) || '(' || p_LevelInfo || ')';
      if not v_check_hist
         or not check_MSG_T_send(px_INFO => iox_INFO, p_Subject => v_Title)
      then
        Insert_INFO_Message(iox_INFO => iox_INFO
                           ,p_type => 'T'
                           ,p_Receiver => it_event.C_C_INFO_Message_TO
                           ,p_Title => v_Title
                           ,p_text => get_event_msg_t(p_len => 200, p_create_sysdate => p_create_sysdate, p_messbody => p_messbody));
      end if;
    elsif p_messnorm is not null
    then
      if check_MSG_T_send(px_INFO => iox_INFO, p_SystemId => p_SystemId, p_ServiceName => p_ServiceName)
      then
        v_Title := trim(p_SystemId) || '.' || trim(p_ServiceName);
        Insert_INFO_Message(iox_INFO => iox_INFO
                           ,p_type => 'T'
                           ,p_Receiver => it_event.C_C_INFO_Message_TO
                           ,p_Title => v_Title
                           ,p_text => get_event_msg_t(p_len => 200, p_create_sysdate => p_create_sysdate, p_messbody => p_messnorm));
      end if;
    end if;
  end;

  -- Формирует Т сообщения по условию временного лага 
  procedure insert_MSG_T_event(iox_INFO   in out xmltype
                              ,p_SystemId itt_event_log.systemid%type
                              ,p_MsgId    itt_event_log.msgid%type
                              ,p_LowLevel itt_event_log.levelinfo%type) as
    vr_qlog       itt_q_message_log%rowtype;
    cur_event     sys_refcursor;
    vr_event      itt_event_log%rowtype;
    v_ServiceName varchar2(2000);
  begin
    cur_event := get_cur_event_log(p_SystemId, p_MsgId);
    loop
      fetch cur_event
        into vr_event;
      exit when cur_event%notfound;
      if nvl(vr_event.levelinfo, 0) >= p_LowLevel
      then
        vr_qlog := it_q_message.messlog_get(p_logid => vr_event.log_id);
        select EXTRACTVALUE(xmltype(vr_qlog.messmeta), '/Event/@ServiceName') into v_ServiceName from dual;
        Insert_INFO_MSG_T(iox_INFO => iox_INFO
                         ,p_SystemId => vr_event.systemid
                         ,p_ServiceName => v_ServiceName
                         ,p_LevelInfo => vr_event.levelinfo
                         ,p_create_sysdate => vr_event.create_sysdate
                         ,p_messbody => vr_qlog.messbody
                         ,p_check_hist => false);
      end if;
    end loop;
    close cur_event;
  exception
    when others then
      if cur_event%isopen
      then
        close cur_event;
      end if;
      raise;
  end;

  -- Возвращает сообщение для рассылки системы мониторинга и максимально критическое сообщение.
  function get_Info_META(px_SendInfo     xmltype default null
                        ,p_SystemId      itt_event_log.systemid%type
                        ,p_MsgId         itt_event_log.msgid%type
                        ,p_addInfo       varchar2 default null -- Дополнительное сообщение с списку событий если io_MaxLevel >= 0
                        ,io_MaxLevel     in out number
                        ,io_MaxLevel_dt  in out date
                        ,io_MaxLevel_txt in out varchar2) return xmltype as
    vx_res  xmltype := px_SendInfo;
    v_Title varchar2(400);
    vn      number;
    vc      varchar2(32676);
    vd      date;
    vpn     integer := 1;
    -- v_addInfo_Level number := io_MaxLevel;
    v_addInfo varchar2(32000) := p_addInfo;
  begin
    /*v_Title := p_SystemId || '(' || v_addInfo_Level || ')' || io_MaxLevel_txt;
    if p_addInfo is not null
       and nvl(v_addInfo_Level, -1) >= 0
    then
      Insert_INFO_Message(iox_INFO => vx_res, p_type => 'M', p_Receiver => it_event.C_C_INFO_Mail_TO, p_Title => v_Title, p_text => p_addInfo, p_pageno => vpn);
      vpn := vpn+1 ;
    end if;*/
    vn := get_event_MaxLevel(p_SystemId => p_SystemId, p_MsgId => p_MsgId, o_MaxLevel_dt => vd, o_MaxLevel_txt => vc);
    if nvl(vn, 0) > nvl(io_MaxLevel, 0)
    then
      io_MaxLevel     := vn;
      io_MaxLevel_dt  := vd;
      io_MaxLevel_txt := vc;
    end if;
    v_Title := p_SystemId || '(' || io_MaxLevel || ')' || io_MaxLevel_txt;
    loop
      vc        := v_addInfo || get_text_event(p_SystemId => p_SystemId, p_MsgId => p_MsgId, p_len => 3800 - nvl(length(v_addInfo), 0));
      v_addInfo := '';
      exit when vc is null;
      Insert_INFO_Message(iox_INFO => vx_res
                         ,p_type => 'M'
                         ,p_Receiver => it_event.C_C_INFO_Mail_TO
                         ,p_Title => v_Title || case
                                       when vpn > 1 then
                                        '(' || vpn || 'стр.)'
                                     end
                         ,p_text => vc
                         ,p_pageno => vpn);
      vpn := vpn + 1;
    end loop;
    if io_MaxLevel >= gn_low_levelinfo_msg_t -- Есть события для отправки в Т сообщении.
    then
      insert_MSG_T_event(iox_INFO => vx_res, p_SystemId => p_SystemId, p_MsgId => p_MsgId, p_LowLevel => gn_low_levelinfo_msg_t);
      /*-- выводим только те которые вошли в ограничение - весь список с типом M
      vc := get_text_event_list_top(p_SystemId => p_SystemId, p_MsgId => p_MsgId, p_LowLevel => gn_low_levelinfo_msg_t, p_len => 500);
      if v_addInfo_Level >= gn_low_levelinfo_msg_t
      then
        vc := p_addInfo || vc;
      end if;
      Insert_INFO_Message(iox_INFO => vx_res
                         ,p_type => 'T'
                         ,p_Receiver => it_event.C_C_INFO_Message_TO
                         ,p_Title => p_SystemId || '(' || io_MaxLevel || '):' || io_MaxLevel_txt
                         ,p_text => vc);*/
    end if;
    if p_SystemId = GC_SystemId_QMANAGER
    then
      loop
        vc        := v_addInfo || it_information.get_list_info(p_mail_group => it_information.C_C_MAIL_GROUP_DEF
                                                              ,p_len => 3800 - nvl(length(v_addInfo), 0)
                                                              ,p_str_begin => GC_TEXT_BEGIN_MSG
                                                              ,p_str_end => GC_TEXT_END_MSG);
        v_addInfo := '';
        exit when vc is null;
        Insert_INFO_Message(iox_INFO => vx_res
                           ,p_type => 'M'
                           ,p_Receiver => it_event.C_C_INFO_Mail_TO
                           ,p_Title => v_Title || case
                                         when vpn > 1 then
                                          '(' || vpn || 'стр.)'
                                       end
                           ,p_text => vc
                           ,p_pageno => vpn);
        vpn := vpn + 1;
      end loop;
      -- Пока отключаем 
      /*loop
        vc := it_information.get_mess_info(p_mail_group => it_information.C_C_MAIL_GROUP_DEF, o_Title => v_Title);
        exit when vc is null;
        Insert_INFO_Message(iox_INFO => vx_res, p_type => 'M', p_Receiver => it_event.C_C_INFO_Mail_TO, p_Title => v_Title, p_text => vc);
      end loop;*/
    end if;
    return vx_res;
  end;

  -- Возвращает body сообщение для системы мониторинга .
  function get_Info_BODY(p_Info_txt     varchar2
                        ,p_MaxLevel     number
                        ,p_MaxLevel_dt  date
                        ,p_MaxLevel_txt varchar2
                        ,p_XML_add      xmltype default null) return clob as
    vx_body xmltype;
  begin
    select XMLELEment("XML"
                      ,XMLELEment("INFO"
                                 ,xmlattributes(p_MaxLevel_txt as "MaxLevel_txt", p_MaxLevel as "MaxLevel", it_xml.date_to_char_iso8601(p_MaxLevel_dt) as "MaxLevel_dt")
                                 ,p_Info_txt))
      into vx_body
      from dual;
    if p_XML_add is not null
    then
      select insertxmlafter(vx_body, 'XML/INFO', p_XML_add) into vx_body from dual;
    end if;
    return vx_body.getClobVal;
  end;

  -- Разбор  body сообщение для системы мониторинга .
  procedure parsing_Info_BODY(p_BODY         clob
                             ,o_Info_txt     out varchar2
                             ,o_MaxLevel     out number
                             ,o_MaxLevel_dt  out date
                             ,o_MaxLevel_txt out varchar2) as
    vx_body xmltype := it_xml.Clob_to_xml(p_BODY, 'p_BODY');
  begin
    if vx_body is not null
    then
      select EXTRACTVALUE(vx_body, '/XML/INFO')
            ,EXTRACTVALUE(vx_body, '/XML/INFO/@MaxLevel_txt')
            ,EXTRACTVALUE(vx_body, '/XML/INFO/@MaxLevel')
            ,it_xml.char_to_date_iso8601(EXTRACTVALUE(vx_body, '/XML/INFO/@MaxLevel_dt'))
        into o_Info_txt
            ,o_MaxLevel_txt
            ,o_MaxLevel
            ,o_MaxLevel_dt
        from dual;
    end if;
  end;

  function store_error_message(p_SystemId  itt_event_log.systemid%type
                              ,p_LevelInfo number
                              ,p_Info_dt   date
                              ,p_Info_txt  varchar2
                              ,px_SendInfo xmltype default null
                              ,p_comment   varchar2 default null) return it_event.tr_monitoring_info as
    vr_res     it_event.tr_monitoring_info;
    vn         number;
    vx         xmltype := px_SendInfo;
    v_add_info varchar2(4000);
  begin
    v_add_info          := GC_TEXT_BEGIN_MSG || to_char(p_Info_dt, 'dd.mm.yyyy hh24:mi:ss ') || '(' || p_LevelInfo || '): ' || p_Info_txt || GC_TEXT_END_MSG ||
                           it_xml.Clob_to_str(p_comment, 2000);
    vr_res.SystemId     := nvl(p_SystemId, GC_SystemId_QMANAGER);
    vr_res.MaxLevel     := p_LevelInfo;
    vr_res.MaxLevel_dt  := p_Info_dt;
    vr_res.MaxLevel_txt := p_Info_txt;
    vr_res.Info_msgid   := it_q_message.get_sys_guid;
    vx                  := get_Info_META(px_SendInfo => vx
                                        ,p_SystemId => vr_res.SystemId
                                        ,p_MsgId => vr_res.Info_msgid
                                        ,p_addInfo => v_add_Info
                                        ,io_MaxLevel => vr_res.MaxLevel
                                        ,io_MaxLevel_dt => vr_res.MaxLevel_dt
                                        ,io_MaxLevel_txt => vr_res.MaxLevel_txt);
    vr_res.Info_enqdt   := sysdate;
    it_q_message.messlog_insert_trash(p_message_type => it_q_message.C_C_MSG_TYPE_A
                                     ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                                     ,p_Receiver => C_C_MONITORINGSYSTEM
                                     ,p_CORRmsgid => vr_res.Info_msgid
                                     ,p_ServiceName => C_C_INFO_SN_PREFIX || '(' || vr_res.SystemId || ')'
                                     ,p_MESSBODY => get_Info_BODY(p_Info_txt => vr_res.Info_txt
                                                                 ,p_MaxLevel => vr_res.MaxLevel
                                                                 ,p_MaxLevel_dt => vr_res.MaxLevel_dt
                                                                 ,p_MaxLevel_txt => vr_res.MaxLevel_txt)
                                     ,p_queue_num => C_C_QUEUE_NUM
                                     ,io_msgid => vr_res.Info_msgid
                                     ,o_logid => vn);
    MessageSend(p_CORRmsgid => vr_res.Info_msgid, px_SendInfo => vx);
    return vr_res;
  end;

  -- Проверка QManager
  function check_qmanager(p_SystemId itt_event_log.systemid%type) return it_event.tr_monitoring_info as
    pragma autonomous_transaction;
    vr_res      it_event.tr_monitoring_info;
    vn          number;
    vx_SendInfo xmltype;
  begin
    -- Проверка запуска QM
    select count(1)
      into vn
      from user_scheduler_jobs
     where job_name like it_q_manager.С_JOB_MANAGER_PREFIX || '%'
       and state = 'RUNNING';
    if vn = 0
    then
      vr_res.MaxLevel     := 10;
      vr_res.MaxLevel_dt  := sysdate;
      vr_res.MaxLevel_txt := 'Нет работающих обработчиков очередей. Запустите QManager !';
      /*if it_q_manager.startmanager(o_info => vc) = 0 -- Пробуем запустить
         and vc is not null
      then
        vr_res.MaxLevel     := 10;
        vr_res.MaxLevel_dt  := sysdate;
        vr_res.MaxLevel_txt := it_xml.Clob_to_str(vc, 200);
      end if;*/
    end if;
    Insert_INFO_MSG_T(iox_INFO => vx_SendInfo
                     ,p_SystemId => 'QManager'
                     ,p_ServiceName => 'Run QWorker'
                     ,p_LevelInfo => nvl(vr_res.MaxLevel, 0)
                     ,p_create_sysdate => sysdate
                     ,p_messbody => vr_res.MaxLevel_txt
                     ,p_messnorm => 'Обработчики стартовали ( работают )');
    if nvl(vr_res.MaxLevel, 0) = 0 -- Если все ок запускаем тестовый сервис.
    then
      declare
        v_msg        itt_event_log.info_msgid%type;
        v_answerid   itt_event_log.info_msgid%type;
        vd_satart    date := sysdate;
        v_answerbody clob;
        v_answermeta xmltype;
        v_wait       number;
      begin
        it_q_message.do_s_service(p_servicename => 'Test.1'
                                 ,p_priority => it_q_message.C_C_MSG_PRIORITY_N
                                 ,p_timeout => 5
                                 ,io_msgid => v_msg
                                 ,o_answerid => v_answerid
                                 ,o_answerbody => v_answerbody
                                 ,o_answermeta => v_answermeta);
        v_wait := (sysdate - vd_satart) * 24 * 60 * 60;
        if v_wait > 1
        then
          vr_res.MaxLevel_dt  := sysdate;
          vr_res.MaxLevel_txt := 'Время ответа тестового сервиса > ';
          if v_wait > 4
          then
            vr_res.MaxLevel     := 8;
            vr_res.MaxLevel_txt := vr_res.MaxLevel_txt || '4 сек';
          elsif v_wait > 3
          then
            vr_res.MaxLevel     := 6;
            vr_res.MaxLevel_txt := vr_res.MaxLevel_txt || '3 сек';
          elsif v_wait > 2
          then
            vr_res.MaxLevel     := 4;
            vr_res.MaxLevel_txt := vr_res.MaxLevel_txt || '2 сек';
          elsif v_wait > 1
          then
            vr_res.MaxLevel     := 2;
            vr_res.MaxLevel_txt := vr_res.MaxLevel_txt || '1 сек';
          end if;
        end if;
      exception
        when others then
          vr_res.MaxLevel     := 10;
          vr_res.MaxLevel_dt  := sysdate;
          vr_res.MaxLevel_txt := 'QManager:Тестовый сервис не отвечает 5 сек !!!';
      end;
      Insert_INFO_MSG_T(iox_INFO => vx_SendInfo
                       ,p_SystemId => 'QManager'
                       ,p_ServiceName => 'SLA Test.1'
                       ,p_LevelInfo => nvl(vr_res.MaxLevel, 0)
                       ,p_create_sysdate => sysdate
                       ,p_messbody => vr_res.MaxLevel_txt
                       ,p_messnorm => 'Время ответа тестового сервиса в норме');
    end if;
    if nvl(vr_res.MaxLevel, 0) = 10
       or vx_SendInfo is not null -- Регистрируем ответ.
    then
      vr_res := store_error_message(p_SystemId => p_SystemId
                                   ,p_LevelInfo => vr_res.MaxLevel
                                   ,p_Info_dt => vr_res.MaxLevel_dt
                                   ,p_Info_txt => vr_res.MaxLevel_txt
                                   ,px_SendInfo => vx_SendInfo);
    end if;
    commit;
    return vr_res;
  end;

  --Список сервисов мониторинга
  function sel_SystemID_monitiring return tt_SystemID_monitiring
    pipelined as
    vrec tr_SystemID_monitiring;
  begin
    for cur in (select service_id
                      ,rtrim(replace(servicename, C_C_INFO_SN_PREFIX || '('), ')') as SystemId
                      ,servicename
                  from (select s.service_id
                              ,upper(s.servicename) as servicename
                          from itt_q_service s
                         where nvl(s.close_sysdate, sysdate + 1) > sysdate
                           and s.message_type = it_q_message.get_constant_str('C_C_MSG_TYPE_R')
                           and upper(s.servicename) like C_C_INFO_SN_PREFIX || '(%)'))
    loop
      vrec := cur;
      pipe row(vrec);
    end loop;
  end;

  -- Формируем сообщение для мониторинга добавляя данные из журнала событий
  procedure GetEventINFO(p_SystemId  itt_event_log.systemid%type
                        ,p_worklogid integer
                        ,p_addInfo   varchar2 default null
                        ,p_LevelInfo number default null
                        ,p_Info_dt   date default null
                        ,p_Info_txt  varchar2 default null
                        ,px_SendInfo xmltype default null
                        ,p_XML_add   xmltype default null
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype) is
    v_MaxLevel     number := p_LevelInfo;
    v_MaxLevel_dt  date := p_Info_dt;
    v_MaxLevel_txt varchar2(200) := it_xml.Clob_to_str(p_Info_txt, 200);
    vr_qlog        itt_q_message_log%rowtype;
    vx_SendInfo    xmltype;
  begin
    vr_qlog     := it_q_message.messlog_get(p_logid => p_worklogid);
    vx_SendInfo := get_Info_META(px_SendInfo => px_SendInfo
                                ,p_SystemId => p_SystemId
                                ,p_MsgId => vr_qlog.msgid
                                ,p_addInfo => p_addInfo
                                ,io_MaxLevel => v_MaxLevel
                                ,io_MaxLevel_dt => v_MaxLevel_dt
                                ,io_MaxLevel_txt => v_MaxLevel_txt);
    o_messbody  := get_Info_BODY(p_Info_txt => p_addInfo, p_MaxLevel => v_MaxLevel, p_MaxLevel_dt => v_MaxLevel_dt, p_MaxLevel_txt => v_MaxLevel_txt);
    o_messmeta  := p_XML_add;
    -- Создаем еще ответы c SN C_C_MESSAGESEND_SN
    MessageSend(p_CORRmsgid => vr_qlog.msgid, px_SendInfo => vx_SendInfo);
  end;

  procedure addInfo(p_addInfo   varchar
                   ,io_addInfo  in out varchar2
                   ,p_LevelInfo number default null) as
  begin
    io_addInfo := io_addInfo || GC_TEXT_BEGIN_MSG || it_xml.Clob_to_str(case
                                                                          when p_LevelInfo is not null then
                                                                           '(' || p_LevelInfo || ')'
                                                                        end || p_addInfo
                                                                       ,200) || GC_TEXT_END_MSG;
  end;

  procedure MaxLevel(p_LevelInfo   number
                    ,p_Info_dt     date default sysdate
                    ,p_Info_txt    varchar
                    ,io_LevelInfo  in out number
                    ,io_Info_dt    in out date
                    ,io_p_Info_txt in out varchar2) as
  begin
    if p_LevelInfo > nvl(io_LevelInfo, 0)
    then
      io_LevelInfo  := p_LevelInfo;
      io_Info_dt    := p_Info_dt;
      io_p_Info_txt := p_Info_txt;
    elsif p_LevelInfo = nvl(io_LevelInfo, 1)
    then
      io_LevelInfo  := p_LevelInfo;
      io_Info_dt    := p_Info_dt;
      io_p_Info_txt := io_p_Info_txt || case
                         when io_p_Info_txt is not null then
                          ' /'
                       end || trim(p_Info_txt);
    end if;
  end;

  procedure error_param_addInfo(p_error_str  varchar2
                               ,io_addInfo   in out varchar2
                               ,io_LevelInfo in out number
                               ,io_Info_dt   in out date
                               ,io_Info_txt  in out varchar2) as
  begin
    addInfo(p_error_str, io_addInfo, 10);
    MaxLevel(10, sysdate, p_error_str, io_LevelInfo, io_Info_dt, io_Info_txt);
  end;

  -- Контроль формирования сообщений по счетам и проводкам в ЦФТ - Ошибка обработчика событий №10002 
  procedure ChkSOFR_CFT10002(p_SystemID   varchar2
                            ,io_addInfo   in out varchar2
                            ,io_LevelInfo in out number
                            ,io_Info_dt   in out date
                            ,io_Info_txt  in out varchar2
                            ,iox_SendInfo in out xmltype) as
    vcnt         number;
    vn           number;
    vn_period    number;
    vc_error_add varchar2(100) := ' Контроль формирования сообщений по счетам и проводкам в ЦФТ НЕ ВЫПОЛНЕН';
    v_LevelInfo  pls_integer := 0;
    v_mes        varchar2(200);
    v_mesL       varchar2(300);
  begin
    vn_period := nvl(it_rs_interface.get_parm_number_path(GC_PARAM_CFT10002_PERIOD), 30); -- 30 мин;
    select count(*)
          ,sum(decode(t_state, 3, 0, 1))
      into vcnt
          ,vn
      from dss_history_dbt h
     where t_service = 10002
       and t_beginstamp > sysdate - 1 / 24 / 60 * vn_period;
    v_mes  := 'Ошибки формирования сообщений по счетам и проводкам в ЦФТ';
    v_mesL := v_mes || '(' || vn || '/' || vcnt || ' за ' || vn_period || ' мин.)(обработчик событий №10002 uTableProcessEvent_dbt)';
    if vcnt > 0
    then
      case
        when vn / vcnt > 0.8 then
          v_LevelInfo := 10;
        when vn / vcnt > 0.6 then
          v_LevelInfo := 9;
        when vn / vcnt > 0.5 then
          v_LevelInfo := 8;
        when vn / vcnt > 0.4 then
          v_LevelInfo := 7;
        when vn / vcnt > 0.3 then
          v_LevelInfo := 6;
        when vn / vcnt > 0.2 then
          v_LevelInfo := 5;
        when vn / vcnt > 0.1 then
          v_LevelInfo := 4;
        when vn > 0 then
          v_LevelInfo := 3;
        else
          v_LevelInfo := 0;
      end case;
    end if;
    if v_LevelInfo != 0
    then
      addInfo(v_mesL, io_addInfo, v_LevelInfo);
      MaxLevel(v_LevelInfo, sysdate, v_mes, io_LevelInfo, io_Info_dt, io_Info_txt);
    end if;
    Insert_INFO_MSG_T(iox_INFO => iox_SendInfo
                     ,p_SystemId => p_SystemID
                     ,p_ServiceName => 'Формирование сообщений по счетам и проводкам в ЦФТ'
                     ,p_LevelInfo => v_LevelInfo
                     ,p_create_sysdate => sysdate
                     ,p_messbody => v_mesL
                     ,p_messnorm => case
                                      when vcnt = 0 then
                                       'Обработчик событий №10002 uTableProcessEvent_dbt за ' || vn_period || 'мин не запускался '
                                      else
                                       'Восстановление нормальной работы обработчика событий №10002 uTableProcessEvent_dbt'
                                    end);
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      error_param_addInfo(vc_error_add || ' Ошибка кода :' || sqlerrm, io_addInfo, io_LevelInfo, io_Info_dt, io_Info_txt);
  end;

  -- Контроль загрузки по валютному рынку
  procedure ChkSOFR_LOAD_CUR(p_SystemID   varchar2
                            ,io_addInfo   in out varchar2
                            ,io_LevelInfo in out number
                            ,io_Info_dt   in out date
                            ,io_Info_txt  in out varchar2
                            ,iox_SendInfo in out xmltype) as
    vt_start constant timestamp := systimestamp;
    vt           timestamp;
    vd           date;
    vn           number;
    vn_period    number;
    vc_error_add varchar2(100) := ' Контроль онлайн-загрузки по валютному рынку НЕ ВЫПОЛНЕН';
    vc_start     varchar2(100);
    vn_start_h   pls_integer;
    vn_start_m   pls_integer;
    vc_stop      varchar2(100);
    vn_stop_h    pls_integer;
    vn_stop_m    pls_integer;
    v_LevelInfo  pls_integer := 0;
  begin
    -- 3.1 Каждый день 10 утра и до 24:00. Глубина 45 мин.
    vc_start   := trim(it_rs_interface.get_parm_varchar_path(GC_PARAM_LOAD_CUR_START));
    vn_start_h := abs(to_number(substr(vc_start, 1, 2) default -1 on conversion error));
    vn_start_m := abs(to_number(substr(vc_start, 4, 2) default -1 on conversion error));
    if length(nvl(vc_start, ' ')) != 5
       or not vn_start_h between 0 and 23
       or not vn_start_m between 0 and 59
    then
      error_param_addInfo('Ошибка в параметре ' || GC_PARAM_LOAD_CUR_START || '=' || vc_start || vc_error_add, io_addInfo, io_LevelInfo, io_Info_dt, io_Info_txt);
    else
      vc_stop   := trim(it_rs_interface.get_parm_varchar_path(GC_PARAM_LOAD_CUR_STOP));
      vn_stop_h := abs(to_number(substr(vc_stop, 1, 2) default -1 on conversion error));
      vn_stop_m := abs(to_number(substr(vc_stop, 4, 2) default -1 on conversion error));
      if length(nvl(vc_stop, ' ')) != 5
         or not vn_stop_h between 0 and 24
         or not vn_stop_m between 0 and 59
         or (vn_stop_h = 24 and vn_stop_m != 0)
      then
        error_param_addInfo('Ошибка в параметре ' || GC_PARAM_LOAD_CUR_STOP || '=' || vc_stop || vc_error_add, io_addInfo, io_LevelInfo, io_Info_dt, io_Info_txt);
      else
        vn_period := it_rs_interface.get_parm_number_path(GC_PARAM_LOAD_CUR_PERIOD); -- 45;
        if nvl(vn_period, -1) <= 0
           or vn_period > (vn_stop_h - vn_start_h) * 60 + (vn_stop_m - vn_start_m)
        then
          error_param_addInfo('Ошибка в параметре ' || GC_PARAM_LOAD_CUR_PERIOD || '=' || vn_period || vc_error_add
                             ,io_addInfo
                             ,io_LevelInfo
                             ,io_Info_dt
                             ,io_Info_txt);
        else
          if (EXTRACT(HOUR from vt_start) > vn_start_h or (EXTRACT(HOUR from vt_start) = vn_start_h and EXTRACT(MINUTE from vt_start) >= vn_start_m))
             and (EXTRACT(HOUR from vt_start) < vn_stop_h or (EXTRACT(HOUR from vt_start) = vn_stop_h and EXTRACT(MINUTE from vt_start) < vn_stop_m))
          then
            vt := vt_start - numtodsinterval(vn_period, 'MINUTE');
            vd := to_date('01/01/0001 ' || EXTRACT(HOUR from vt) || ':' || EXTRACT(MINUTE from vt) || ':' || trunc(EXTRACT(second from vt)), 'dd/mm/yyyy hh24:mi:ss');
            select count(*)
              into vn
              from DASTSCURR_TRADES_DEL_DBT td
             where td.t_tradedate = trunc(vt_start)
               and to_date('01/01/0001 ' || to_char(td.t_tradetime, 'hh24:mi:ss'), 'dd/mm/yyyy hh24:mi:ss') > vd
               and rownum < 2;
            if vn = 0
            then
              v_LevelInfo := 10;
              addInfo('Не работает онлайн-загрузка по валютному рынку. Нет новых записей в DASTSCURR_TRADES_DEL_DBT'
                     ,io_addInfo
                     ,10);
              MaxLevel(10, sysdate, 'Не работает онлайн-загрузка по валютному рынку', io_LevelInfo, io_Info_dt, io_Info_txt);
            end if;
            Insert_INFO_MSG_T(iox_INFO => iox_SendInfo
                             ,p_SystemId => p_SystemID
                             ,p_ServiceName => 'Oнлайн-загрузка по валютному рынку'
                             ,p_LevelInfo => v_LevelInfo
                             ,p_create_sysdate => sysdate
                             ,p_messbody => 'Не работает онлайн-загрузка по валютному рынку. Нет новых записей в DASTSCURR_TRADES_DEL_DBT'
                             ,p_messnorm => 'Есть новые записи в DASTSCURR_TRADES_DEL_DBT !');
          end if;
        end if;
      end if;
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      error_param_addInfo(vc_error_add || ' Ошибка кода :' || sqlerrm, io_addInfo, io_LevelInfo, io_Info_dt, io_Info_txt);
  end;

  -- Контроль загрузки по валютному рынку
  procedure ChkSOFR_ASTSBridge(p_SystemID   varchar2
                              ,io_addInfo   in out varchar2
                              ,io_LevelInfo in out number
                              ,io_Info_dt   in out date
                              ,io_Info_txt  in out varchar2
                              ,iox_SendInfo in out xmltype
                              ,iox_XML_add  in out xmltype) as
    vt_start constant timestamp := systimestamp;
    vc_error_add   varchar2(100) := ' Контроль обработки планировщиком записей шлюза ASTS НЕ ВЫПОЛНЕН';
    vc_start       varchar2(100);
    vn_start_h     pls_integer;
    vn_start_m     pls_integer;
    vc_stop        varchar2(100);
    vn_stop_h      pls_integer;
    vn_stop_m      pls_integer;
    v_LevelInfo    pls_integer := 0;
    v_cnt_sofr     pls_integer;
    v_delay        pls_integer;
    v_mess         varchar2(2000);
    v_asts_cnt     pls_integer;
    v_asts_id      number;
    v_asts_time    char(8);
    v_asts_tradeno number;
    v_asts_orderno number;
    v_time         char(8);
  begin
    --ASTS. Записи в шлюзе не обрабатываются планировщиком.
    -- текущий день с 10 утра и до 23:59
    vc_start   := trim(it_rs_interface.get_parm_varchar_path(GC_PARAM_ASTS_BRIDGE_START));
    vn_start_h := abs(to_number(substr(vc_start, 1, 2) default -1 on conversion error));
    vn_start_m := abs(to_number(substr(vc_start, 4, 2) default -1 on conversion error));
    if length(nvl(vc_start, ' ')) != 5
       or not vn_start_h between 0 and 23
       or not vn_start_m between 0 and 59
    then
      error_param_addInfo('Ошибка в параметре ' || GC_PARAM_ASTS_BRIDGE_START || '=' || vc_start || vc_error_add
                         ,io_addInfo
                         ,io_LevelInfo
                         ,io_Info_dt
                         ,io_Info_txt);
    else
      vc_stop   := trim(it_rs_interface.get_parm_varchar_path(GC_PARAM_ASTS_BRIDGE_STOP));
      vn_stop_h := abs(to_number(substr(vc_stop, 1, 2) default -1 on conversion error));
      vn_stop_m := abs(to_number(substr(vc_stop, 4, 2) default -1 on conversion error));
      if length(nvl(vc_stop, ' ')) != 5
         or not vn_stop_h between 0 and 24
         or not vn_stop_m between 0 and 59
         or (vn_stop_h = 24 and vn_stop_m != 0)
      then
        error_param_addInfo('Ошибка в параметре ' || GC_PARAM_ASTS_BRIDGE_STOP || '=' || vc_stop || vc_error_add
                           ,io_addInfo
                           ,io_LevelInfo
                           ,io_Info_dt
                           ,io_Info_txt);
      else
        if (EXTRACT(HOUR from vt_start) > vn_start_h or (EXTRACT(HOUR from vt_start) = vn_start_h and EXTRACT(MINUTE from vt_start) >= vn_start_m))
           and (EXTRACT(HOUR from vt_start) < vn_stop_h or (EXTRACT(HOUR from vt_start) = vn_stop_h and EXTRACT(MINUTE from vt_start) < vn_stop_m))
        then
          begin
            select t.cnt
                  ,t.t_id
                  ,to_char(d.t_tradetime, 'hh24:mi:ss')
                  ,d.t_tradeno
                  ,d.t_orderno
                  ,to_char(sysdate, 'hh24:mi:ss')
              into v_asts_cnt
                  ,v_asts_id
                  ,v_asts_time
                  ,v_asts_tradeno
                  ,v_asts_orderno
                  ,v_time
              from DASTSCURR_TRADES_DEL_DBT d
              join (select count(*) cnt
                          ,max(t_id) t_id
                      from DASTSCURR_TRADES_DEL_DBT
                     where t_tradedate = trunc(vt_start)
                       and t_tradetype not in ('S') -- сделкам своп 
                    ) t
                on t.t_id = d.t_id
             where d.t_tradedate = trunc(vt_start);
          exception
            when no_data_found then
              v_asts_cnt := 0;
          end;
          select insertchildxml(iox_XML_add
                               ,'/SOFR'
                               ,'ASTSBridge'
                               ,XMLElement("ASTSBridge"
                                           ,XMLElement("cnt", v_asts_cnt)
                                           ,XMLElement("t_id", v_asts_id)
                                           ,XMLElement("t_time", v_asts_time)
                                           ,XMLElement("t_tradeno", v_asts_tradeno)
                                           ,XMLElement("t_orderno", v_asts_orderno)
                                           ,XMLElement("Time", v_time)))
            into iox_XML_add
            from dual;
          v_delay := nvl(it_rs_interface.get_parm_number_path(GC_PARAM_ASTS_BRIDGE_DELAY), 0);
          if v_delay > 0
          then
            begin
              with meta as
               (select xmltype(messmeta) x
                  from (select l.messmeta
                          from itt_q_message_log l
                         where l.servicename = (select servicename from table(it_event_utils.sel_SystemID_monitiring) where SystemId = p_SystemID)
                           and l.enqdt between trunc(vt_start) and vt_start - numtodsinterval(v_delay, 'MINUTE')
                           and l.message_type = 'A'
                           and l.queuetype = 'OUT'
                         order by l.enqdt desc)
                 where rownum < 2)
              select nvl(to_number(EXTRACTVALUE(meta.x, 'SOFR/ASTSBridge/cnt') default null on CONVERSION ERROR), 0)
                    ,to_number(EXTRACTVALUE(meta.x, 'SOFR/ASTSBridge/t_id') default null on CONVERSION ERROR)
                    ,EXTRACTVALUE(meta.x, 'SOFR/ASTSBridge/t_time')
                    ,to_number(EXTRACTVALUE(meta.x, 'SOFR/ASTSBridge/t_tradeno') default null on CONVERSION ERROR)
                    ,to_number(EXTRACTVALUE(meta.x, 'SOFR/ASTSBridge/t_orderno') default null on CONVERSION ERROR)
                    ,EXTRACTVALUE(meta.x, 'SOFR/ASTSBridge/Time')
                into v_asts_cnt
                    ,v_asts_id
                    ,v_asts_time
                    ,v_asts_tradeno
                    ,v_asts_orderno
                    ,v_time
                from meta;
            exception
              when no_data_found then
                v_asts_cnt := 0;
            end;
          end if;
          if v_asts_cnt > 0
          then
            if v_delay > 0
            then
              select /* index(ndeal DDVNDEAL_DBT_IDX2) */
               count(*)
                into v_cnt_sofr
                from DASTSCURR_TRADES_DEL_DBT d
               where d.t_tradedate = trunc(vt_start)
                 and d.t_id <= v_asts_id
                 and d.t_tradetype not in ('S') -- сделкам своп 
                 and (d.t_tradeno, d.t_orderno) not in (select to_number(ndeal.t_extcode default null on CONVERSION ERROR)
                                                              ,to_number(ndeal.t_reqcode default null on CONVERSION ERROR)
                                                          from ddvndeal_dbt ndeal
                                                         where ndeal.T_PROGNOS = CHR(88)
                                                           and NDEAL.T_STATE = 0
                                                           and ndeal.T_DATE = trunc(vt_start));
              if v_cnt_sofr > 0
              then
                v_LevelInfo := 10;
                v_mess      := 'ASTS. Записи в шлюзе не обрабатываются планировщиком';
                MaxLevel(10, sysdate, v_mess, io_LevelInfo, io_Info_dt, io_Info_txt);
                addInfo(v_mess || ' ( Из ' || v_asts_cnt || ' на ' || v_time || ' не обработано ' || v_cnt_sofr || ' )', io_addInfo, 10);
              end if;
            else
              select count(*)
                into v_cnt_sofr
                from ddvndeal_dbt ndeal
               where ndeal.T_PROGNOS = CHR(88)
                 and NDEAL.T_STATE = 0
                 and ndeal.T_DATE = trunc(vt_start);
              if v_cnt_sofr != v_asts_cnt
              then
                v_LevelInfo := 10;
                v_mess      := 'ASTS. Записи в шлюзе не обрабатываются планировщиком';
                MaxLevel(10, sysdate, v_mess, io_LevelInfo, io_Info_dt, io_Info_txt);
                addInfo(v_mess || ' ( Из ' || v_asts_cnt || ' обработано ' || v_cnt_sofr || ')', io_addInfo, 10);
              end if;
            end if;
            Insert_INFO_MSG_T(iox_INFO => iox_SendInfo
                             ,p_SystemId => p_SystemID
                             ,p_ServiceName => 'Обработка ASTS Bridge'
                             ,p_LevelInfo => v_LevelInfo
                             ,p_create_sysdate => sysdate
                             ,p_messbody => v_mess
                             ,p_messnorm => 'Обработка записей ASTS Bridge в НОРМЕ !');
          end if;
        end if;
      end if;
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      error_param_addInfo(vc_error_add || ' Ошибка кода :' || sqlerrm, io_addInfo, io_LevelInfo, io_Info_dt, io_Info_txt);
  end;

  -- Возвращает набор LevelInfo 
  function get_LevelInfo_number(p_param_path varchar2) return tt_LevelInfo_number as
    vt_LevelInfo tt_LevelInfo_number;
    vn_keyid     number;
    vn           integer := -1;
  begin
    vn_keyid := it_rs_interface.get_keyid_parm_path(p_parm_path => p_param_path);
    if vn_keyid is not null
    then
      for cur in (select t.t_keyid
                        ,to_number(trim(t.t_name)) LevelInfo
                    from dregparm_dbt t
                   where t.t_parentid = vn_keyid
                     and to_number(trim(t.t_name) default vn on conversion error) between 0 and 10
                   order by to_number(trim(t.t_name)) desc)
      loop
        vt_LevelInfo(cur.LevelInfo).param := it_rs_interface.get_parm_number(cur.t_keyid);
      end loop;
    end if;
    return vt_LevelInfo;
  end;

  -- Контроль планировщиков
  procedure ChkSOFR_PLAN(p_SystemID     varchar2
                        ,p_ioper        SOFR_SERVAPP_SHEDULER2_VIEW.T_IOPER%type
                        ,p_work_seconds SOFR_SERVAPP_SHEDULER2_VIEW.t_work_seconds%type
                        ,p_addtext      varchar2
                        ,io_addInfo     in out varchar2
                        ,io_LevelInfo   in out number
                        ,io_Info_dt     in out date
                        ,io_Info_txt    in out varchar2
                        ,iox_SendInfo   in out xmltype) as
    vn               number;
    vn_LI            number;
    vc               varchar2(1000);
    vc_error_add     varchar2(100) := ' Контроль планировщика-' || p_ioper || ' НЕ ВЫПОЛНЕН';
    vt_LevelInfo     tt_LevelInfo_number;
    v_para_plan      varchar2(3000) := get_constant_str('GC_PARAM_PLAN' || p_ioper);
    v_para_LevelInfo varchar2(3000) := v_para_plan || GC_PARAM_LI;
  begin
    if it_rs_interface.get_parm_varchar_path(p_parm_path => v_para_plan) != chr(88)
    then
      return;
    end if;
    vt_LevelInfo := get_LevelInfo_number(v_para_LevelInfo);
    if vt_LevelInfo.count = 0
    then
      error_param_addInfo('В параметре ' || v_para_LevelInfo || ' нет шкалы LevelInfo' || vc_error_add, io_addInfo, io_LevelInfo, io_Info_dt, io_Info_txt);
    else
      vn := vt_LevelInfo.first;
      while vn is not null
      loop
        if p_work_seconds >= vt_LevelInfo(vn).param
        then
          vn_LI := vn;
          exit;
        end if;
        vn := vt_LevelInfo.next(vn);
      end loop;
      if vn_LI is not null
      then
        vc := 'Планировщик-' || p_ioper || ' НЕ работает ';
        addInfo(vc || trunc(p_work_seconds) || ' сек.' || p_addtext, io_addInfo, vn_LI);
        MaxLevel(vn_LI, sysdate, vc, io_LevelInfo, io_Info_dt, io_Info_txt);
      end if;
      Insert_INFO_MSG_T(iox_INFO => iox_SendInfo
                       ,p_SystemId => p_SystemID
                       ,p_ServiceName => 'Планировщик-' || p_ioper
                       ,p_LevelInfo => nvl(vn_LI, 0)
                       ,p_create_sysdate => sysdate
                       ,p_messbody => vc || trunc(p_work_seconds) || ' сек. ' || p_addtext
                       ,p_messnorm => 'Восстановление нормальной работы !');
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      error_param_addInfo(vc_error_add || ' Ошибка кода :' || sqlerrm, io_addInfo, io_LevelInfo, io_Info_dt, io_Info_txt);
  end;

  -- Контроль количества откатываемых операций Предупреждение. Большое количество откатываемых операций через интеграцию
  procedure ChkSOFR_OPER(p_SystemID      varchar2
                        ,p_period        number
                        ,p_OperationName dregparm_dbt.t_name%type
                        ,io_addInfo      in out varchar2
                        ,io_LevelInfo    in out number
                        ,io_Info_dt      in out date
                        ,io_Info_txt     in out varchar2
                        ,iox_SendInfo    in out xmltype) as
    vt_start     timestamp := systimestamp;
    vt           timestamp;
    vn           number;
    vn_LI        number;
    vc           varchar2(1000);
    vc_error_add varchar2(100) := ' Контроль количества откатываемых операций ' || p_OperationName || ' через интеграцию НЕ ВЫПОЛНЕН';
    vt_LevelInfo tt_LevelInfo_number;
    v_para_Oper  varchar2(2000) := GC_PARAM_OPER_SP || '\' || p_OperationName;
    vc_list_ot   varchar2(2000);
    vn_cnt       integer;
  begin
    vt_LevelInfo := get_LevelInfo_number(v_para_Oper || GC_PARAM_LI);
    if vt_LevelInfo.count = 0
    then
      error_param_addInfo('В параметре ' || v_para_Oper || GC_PARAM_LI || ' нет шкалы LevelInfo' || vc_error_add
                         ,io_addInfo
                         ,io_LevelInfo
                         ,io_Info_dt
                         ,io_Info_txt);
    else
      vc_list_ot := it_rs_interface.get_parm_varchar_path(v_para_Oper || '\OBJECTTYPE');
      if vc_list_ot is null
      then
        error_param_addInfo('В параметре ' || v_para_Oper || '\OBJECTTYPE' || ' не определен список значений t_objecttype ' || vc_error_add
                           ,io_addInfo
                           ,io_LevelInfo
                           ,io_Info_dt
                           ,io_Info_txt);
      else
        vt := vt_start - numtodsinterval(p_period, 'MINUTE');
        execute immediate 'select /*+ index(u, UTABLEPROCESSEVENT_DBT_IDX2) */ count(*) from utableprocessevent_dbt u where t_objecttype in (' || vc_list_ot ||
                          ') and t_timestamp > :vt  and t_type = 3' -- откат
          into vn_cnt
          using vt;
        vn := vt_LevelInfo.first;
        while vn is not null
        loop
          if vn_cnt >= vt_LevelInfo(vn).param
          then
            vn_LI := vn;
            exit;
          end if;
          vn := vt_LevelInfo.next(vn);
        end loop;
        if vn_LI is not null
        then
          vc := 'Большое количество откатываемых операций ' || p_OperationName || ' через интеграцию';
          MaxLevel(vn_LI, sysdate, vc, io_LevelInfo, io_Info_dt, io_Info_txt);
          vc := vc || ' (' || vn_cnt || ' за ' || p_period || 'мин.)';
          addInfo(vc, io_addInfo, vn_LI);
        end if;
        Insert_INFO_MSG_T(iox_INFO => iox_SendInfo
                         ,p_SystemId => p_SystemID
                         ,p_ServiceName => 'Операции ' || p_OperationName || ' через интеграцию'
                         ,p_LevelInfo => nvl(vn_LI, 0)
                         ,p_create_sysdate => sysdate
                         ,p_messbody => vc
                         ,p_messnorm => 'Количество откатываемых операций в пределах нормы !');
      end if;
    end if;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      error_param_addInfo(vc_error_add || ' Ошибка кода :' || sqlerrm, io_addInfo, io_LevelInfo, io_Info_dt, io_Info_txt);
  end;

  --Сервис формирования сообщений для мониторинга SOFRа
  procedure GetINFO_SOFR(p_worklogid integer
                        ,p_messbody  clob
                        ,p_messmeta  xmltype
                        ,o_msgid     out varchar2
                        ,o_MSGCode   out integer
                        ,o_MSGText   out varchar2
                        ,o_messbody  out clob
                        ,o_messmeta  out xmltype) is
    v_addInfo   varchar2(4000); -- Текст сообщения для мониторинга
    v_LevelInfo number; -- Если >=0 отправка по почте > 5 - T
    v_Info_dt   date;
    v_Info_txt  varchar2(200); -- Тест для заготовка 
    vx_SendInfo xmltype; -- Сообщения
    vx_XML_add  xmltype; -- Прочие параметры для дальнейшей обработки
    vc_SystemID constant itt_event_log.systemid%type := 'SOFR';
    vn number;
  begin
    select XMLELEment("SOFR") into vx_XML_add from dual;
    if it_rs_interface.get_parm_varchar_path(p_parm_path => GC_PARAM_MONITORING) = chr(88)
    then
      -- 3.1
      if it_rs_interface.get_parm_varchar_path(p_parm_path => GC_PARAM_LOAD_CUR) = chr(88)
      then
        ChkSOFR_LOAD_CUR(vc_SystemID, v_addInfo, v_LevelInfo, v_Info_dt, v_Info_txt, vx_SendInfo);
      end if;
      -- 3.2 3.3
      for v in (select * from SOFR_SERVAPP_SHEDULER2_VIEW)
      loop
        ChkSOFR_PLAN(vc_SystemID
                    ,v.t_ioper
                    ,v.t_work_seconds
                    ,' Последнее задание [' || v.t_name || '] Старт ' || to_char(v.t_startdate, 'dd.mm.yyyy') || to_char(v.t_starttime, ' hh24:mi:ss')
                    ,v_addInfo
                    ,v_LevelInfo
                    ,v_Info_dt
                    ,v_Info_txt
                    ,vx_SendInfo);
      end loop;
      -- CCBO-7027 Предупреждение. Большое количество откатываемых операций через интеграцию
      if it_rs_interface.get_parm_varchar_path(p_parm_path => GC_PARAM_OPER) = chr(88)
      then
        vn := it_rs_interface.get_parm_number_path(GC_PARAM_OPER_PERIOD);
        if vn is null
           or vn not between 1 and 24 * 60
        then
          error_param_addInfo('Ошибка в параметре ' || GC_PARAM_OPER_PERIOD || '=' || vn || ' Контроль количества откатываемых операций через интеграцию НЕ ВЫПОЛНЕН '
                             ,v_addInfo
                             ,v_LevelInfo
                             ,v_Info_dt
                             ,v_Info_txt);
        else
          for cur in (select t.t_keyid
                            ,t.t_name OperationName
                        from dregparm_dbt t
                       where t.t_parentid = it_rs_interface.get_keyid_parm_path(p_parm_path => GC_PARAM_OPER_SP)
                       order by t.t_name)
          loop
            if it_rs_interface.get_parm_varchar(p_keyid => cur.t_keyid) = chr(88)
            then
              ChkSOFR_OPER(vc_SystemID, vn, cur.operationname, v_addInfo, v_LevelInfo, v_Info_dt, v_Info_txt, vx_SendInfo);
            end if;
          end loop;
        end if;
      end if;
      -- DEF-59394 онлайн загрузка сделок валютного рынка через ASTS Bridge
      if it_rs_interface.get_parm_varchar_path(p_parm_path => GC_PARAM_ASTS_BRIDGE) = chr(88)
      then
        ChkSOFR_ASTSBridge(vc_SystemID, v_addInfo, v_LevelInfo, v_Info_dt, v_Info_txt, vx_SendInfo, vx_XML_add);
      end if;
      -- CCBO-10701 Реализация мониторинга по итогу СОР, по мотивам дефекта DEF-74069
      if it_rs_interface.get_parm_varchar_path(p_parm_path => GC_PARAM_CFT10002) = chr(88)
      then
        ChkSOFR_CFT10002(vc_SystemID, v_addInfo, v_LevelInfo, v_Info_dt, v_Info_txt, vx_SendInfo);
      end if;
    end if;
    -- Формирование сообщения
    GetEventINFO(p_SystemId => vc_SystemID
                ,p_worklogid => p_worklogid
                ,p_addInfo => v_addInfo
                ,p_LevelInfo => v_LevelInfo
                ,p_Info_dt => v_Info_dt
                ,p_Info_txt => v_Info_txt
                ,px_SendInfo => vx_SendInfo
                ,p_XML_add => vx_XML_add
                ,o_messbody => o_messbody
                ,o_messmeta => o_messmeta);
  end;

  --Сервис формирования сообщений для мониторинга QMANAGERа
  procedure GetINFO_QMANAGER(p_worklogid integer
                            ,p_messbody  clob
                            ,p_messmeta  xmltype
                            ,o_msgid     out varchar2
                            ,o_MSGCode   out integer
                            ,o_MSGText   out varchar2
                            ,o_messbody  out clob
                            ,o_messmeta  out xmltype) is
    v_addInfo   varchar2(4000); -- Текст сообщения для мониторинга
    v_LevelInfo number; -- Если >=0 отправка по почте > 5 - T
    v_Info_dt   date;
    v_Info_txt  varchar2(200); -- Тест для заготовка
    vx_SendInfo xmltype; -- Сообщения
    vx_XML_add  xmltype; -- Прочие параметры для дальнейшей обработки 
    vc_SystemID constant itt_event_log.systemid%type := 'QMANAGER';
    vn             number;
    vn_cnt_in_task number := 0;
    v_tmpaddInfo varchar2(4000) ;
  begin
    select XMLELEment("QMANAGER") into vx_XML_add from dual;
    select count(*)
      into vn
      from itt_q_worker
     where worker_enabled > 0
       and job_stoptime is null;
    addInfo('Кол-во Worker ' || vn, v_tmpaddInfo);
    select insertchildxml(vx_XML_add, '/QMANAGER', 'CNTWORKER', XMLElement("CNTWORKER", vn)) into vx_XML_add from dual;
    for q in (select column_value queue_num from table(it_q_message.select_queue_num))
    loop
      vn             := it_q_message.get_count_task(p_queue_num => q.queue_num);
      vn_cnt_in_task := vn_cnt_in_task + vn;
      addInfo('IN TASK ' || q.queue_num || ' ' || vn, v_tmpaddInfo);
      select insertchildxml(vx_XML_add, '/QMANAGER', 'INTASK', XMLElement("INTASK", xmlattributes(q.queue_num as "queue_num"), vn)) into vx_XML_add from dual;
    end loop;
    vn := 0;
    if vn_cnt_in_task > 80
    then
      vn := 9;
    elsif vn_cnt_in_task > 50
    then
      vn := 8;
    elsif vn_cnt_in_task > 40
    then
      vn := 7;
    elsif vn_cnt_in_task > 30
    then
      vn := 6;
    elsif vn_cnt_in_task > 20
    then
      vn := 5;
    end if;
    if vn > 0
    then
      MaxLevel(vn, sysdate, 'Во входящих очередях ' || vn_cnt_in_task || 'сообщ. не обработанно ', v_LevelInfo, v_Info_dt, v_Info_txt);
      addInfo(v_tmpaddInfo, v_addInfo);
    end if;
    Insert_INFO_MSG_T(iox_INFO => vx_SendInfo
                     ,p_SystemId => vc_SystemID
                     ,p_ServiceName => 'Сообщения для обработки'
                     ,p_LevelInfo => vn
                     ,p_create_sysdate => sysdate
                     ,p_messbody => 'Во входящих очередях ' || vn_cnt_in_task || 'сообщ. не обработанно '
                     ,p_messnorm => 'Количество сообщений во входящих очередях в пределах нормы');
    GetEventINFO(p_SystemId => vc_SystemID
                ,p_worklogid => p_worklogid
                ,p_addInfo => v_addInfo
                ,p_LevelInfo => v_LevelInfo
                ,p_Info_dt => v_Info_dt
                ,p_Info_txt => v_Info_txt
                ,px_SendInfo => vx_SendInfo
                ,p_XML_add => vx_XML_add
                ,o_messbody => o_messbody
                ,o_messmeta => o_messmeta);
  end;

  --Сервис формирования сообщений для мониторинга систем не вошедших в список
  procedure GetINFO_OTHERS(p_worklogid integer
                          ,p_messbody  clob
                          ,p_messmeta  xmltype
                          ,o_msgid     out varchar2
                          ,o_MSGCode   out integer
                          ,o_MSGText   out varchar2
                          ,o_messbody  out clob
                          ,o_messmeta  out xmltype) is
    v_addInfo   varchar2(4000); -- Текст сообщения для мониторинга
    v_LevelInfo number; -- Если >=0 отправка по почте > 5 - T
    v_Info_dt   date;
    v_Info_txt  varchar2(200); -- Тест для заготовка 
    vx_XML_add  xmltype; -- Прочие параметры для дальнейшей обработки
    vc_SystemID constant itt_event_log.systemid%type := GC_SystemId_OTHERS;
  begin
    GetEventINFO(p_SystemId => vc_SystemID
                ,p_worklogid => p_worklogid
                ,p_addInfo => v_addInfo
                ,p_LevelInfo => v_LevelInfo
                ,p_Info_dt => v_Info_dt
                ,p_Info_txt => v_Info_txt
                ,p_XML_add => vx_XML_add
                ,o_messbody => o_messbody
                ,o_messmeta => o_messmeta);
  end;

begin
  declare
    v_param integer;
  begin
    v_param := it_rs_interface.get_parm_number_path(GC_PARAM_FORMAT_MSG_T_LOWLI);
    if nvl(v_param, -1) > 0
    then
      gn_low_levelinfo_msg_t := v_param;
    end if;
    v_param := it_rs_interface.get_parm_number_path(GC_PARAM_FORMAT_MSG_T_PERIOD);
    if nvl(v_param, -1) > 0
    then
      gn_period_msg_t := v_param;
    end if;
    v_param := nvl(it_rs_interface.get_parm_number_path(GC_PARAM_FORMAT_MSG), 0);
    case v_param
      when 1 then
        g_format_msg := 1;
      else
        g_format_msg := 0;
    end case;
  end;
end it_event_utils;
/

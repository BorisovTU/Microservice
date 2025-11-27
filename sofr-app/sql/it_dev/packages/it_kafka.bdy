create or replace package body it_kafka is

  /***************************************************************************************************\
    Пакет для работы QManagera c KAFKA
   **************************************************************************************************
    Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------
   01.07.2025  Зыков М.В.       BOSS-9662                     Доработка процедуры для QMessage в части взаимодействия с IPS
   04.02.2025  Зыков М.В.       BOSS-7625                     Разработка нового формата входных сообщений из IPS
   27.01.2025  Зыков М.В.       BOSS-7573                     Доработки QMessage в части взаимодействия с адаптером для S3
   05.09.2024  Зыков М.В.       BOSS-5212                     BOSS-1574.9 Доработка СОФР для передачи в sofr_qmngr_mq_adapter параметра кластера платформенной Kafka
   25.03.2024  Зыков М.В.       BOSS-2688                     BOSS-575 СОФР. Доработка формирования отчетной формы "Справка 5798-У" (справка для гос. служащего). Доработка Q-Manager 
   23.10.2023  Зыков М.В.       BOSS-1230                     BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
  \**************************************************************************************************/
  C_C_QUEUE_NUM constant varchar2(2) := '01'; -- Номер рабочей очереди  .
  C_C_KAFKANAME constant varchar2(128) := 'KAFKA';

  C_C_PREF_CORRELATION   constant varchar2(128) := '##KAFKA'; --
  C_C_ADD_S3_CORRELATION constant varchar2(128) := '#S3';

  C_C_THREAD_TOPIC constant varchar(128) := '##KAFKA##TOPIC#';

  G_THREAD_COUNT pls_integer; --  число потоков исполнения TOPIC (Настройка KAFKA_TOPIC_THREADS + динамическая правка ) 
  G_THREAD_EXTREME_SRV_PRICE constant pls_integer := abs(nvl(it_q_message.get_qset_number(p_qset_name => 'KAFKA_TOPIC_SRV_PRICE'), 1000)); --  максимальный уровень цены сервиса для включения потоков исполнения  (Настройка KAFKA_TOPIC_SRV_PRICE) 
  G_MAX_SIZE_MSG             constant pls_integer := abs(nvl(it_q_message.get_qset_number(p_qset_name => 'KAFKA_MAX_SIZE_MSG'), 10)); -- Максимальный размер в МВ Body сообщения 
  type t_thread_kafka_topic_tt is table of pls_integer index by pls_integer; -- 
  gt_thread_kafka_topic t_thread_kafka_topic_tt;

  -- Возвращает значение пакетной переменной по имени
  function get_constant_str(p_constant varchar2) return varchar2 deterministic as
    v_ret varchar2(32676);
  begin
    execute immediate ' begin :1 := it_kafka.' || p_constant || '; end;'
      using out v_ret;
    return v_ret;
  exception
    when others then
      return null;
  end;

  function get_S3_point(p_system_name itt_q_corrsystem.system_name%type) return varchar2 as
  begin
    return it_q_message.get_corrsystem_param(p_system_name => p_system_name, p_param => 'S3_point');
  end;

  -- XML с rootelement из itt_kafka_topic.rootelement и тегами /GUID ,/GUIDReq и т.д.
  procedure Format_XGUID(p_kafka_topic   varchar2
                        ,p_GUID          varchar2
                        ,pcl_message     in clob
                        ,p_system_name   itt_kafka_topic.system_name%type
                        ,p_rootelement   itt_kafka_topic.rootelement%type
                        ,p_cnt_sel       integer
                        ,io_topic_id     in out itt_kafka_topic.topic_id%type
                        ,io_sg_count     in out itt_kafka_topic.sg_count%type
                        ,io_msg_param    in out itt_kafka_topic.msg_param%type
                        ,io_msgid        in out itt_q_message_log.msgid%type
                        ,io_message_type in out itt_q_message_log.message_type%type
                        ,io_CORRmsgid    in out itt_q_message_log.corrmsgid%type
                        ,io_ServiceName  in out itt_q_message_log.servicename%type
                        ,io_MSGCode      in out itt_q_message_log.msgcode%type
                        ,io_MSGText      in out itt_q_message_log.msgtext%type
                        ,io_RequestDT    in out itt_q_message_log.requestdt%type) as
    vx_message       xmltype;
    vx_errlist       xmltype;
    v_msgrootElement itt_kafka_topic.rootelement%type;
    v_dom_doc        dbms_xmldom.domdocument;
    v_dom_eliment    dbms_xmldom.domelement;
  begin
    vx_message       := it_xml.Clob_to_xml(p_clob => pcl_message, p_errparam => 'входящего сообщения из ' || p_system_name);
    v_msgrootElement := vx_message.getRootElement;
    if p_cnt_sel > 1
    then
      begin
        select t.topic_id
              ,t.servicename
              ,nvl2(t.msg_param, 'xmlns="' || t.msg_param || '"', null)
              ,t.sg_count
          into io_topic_id
              ,io_ServiceName
              ,io_msg_param
              ,io_sg_count
          from itt_kafka_topic t
         where upper(t.topic_name) = upper(p_kafka_topic)
           and t.queuetype = it_q_message.C_C_QUEUE_TYPE_IN
           and upper(t.rootelement) = upper(v_msgrootElement);
      exception
        when others then
          raise_application_error(-20901, 'Не удалось определить ServiceName для Topic ' || p_kafka_topic || ' и RootElement ' || v_msgrootElement);
      end;
    end if;
    with in_d as
     (select vx_message as x from dual)
    select extractValue(in_d.x, '*/GUID', io_msg_param)
          ,it_xml.char_to_timestamp(extractValue(in_d.x, '*/RequestTime', io_msg_param))
          ,extractValue(in_d.x, '*/GUIDReq', io_msg_param)
      into io_msgid
          ,io_RequestDT
          ,io_CORRmsgid
      from in_d;
    io_msgid := nvl(io_msgid, p_GUID);
    if io_msgid is null
    then
      raise_application_error(-20908, 'Ошибка валидации сообщения');
    elsif upper(nvl(p_rootElement, v_msgrootElement)) != upper(v_msgrootElement)
          and p_cnt_sel = 1
    then
      raise_application_error(-20902, 'Ошибка  RootElement! Получен ' || v_msgrootElement || ' ожидалось ' || p_rootElement);
    end if;
    v_dom_doc     := dbms_xmldom.newdomdocument(vx_message);
    v_dom_eliment := dbms_xmldom.getdocumentelement(v_dom_doc);
    dbms_xmldom.removeAttribute(v_dom_eliment, 'xmlns');
    vx_errlist := dbms_xmldom.getxmltype(v_dom_doc);
    begin
      select max(abs(ErrorCode))
            ,substr(LISTAGG(trim(ErrorDesc), '\ ') WITHIN group(order by rownum), 1, 2000)
        into io_MSGCode
            ,io_MSGText
        from XMLTable('*/ErrorList/Error' passing(vx_errlist) columns ErrorCode integer path 'ErrorCode', ErrorDesc varchar2(2000) path 'ErrorDesc');
    exception
      when no_data_found then
        io_MSGCode := null;
        io_MSGText := null;
    end;
    case
      when upper(v_msgrootElement) like '%REQ' then
        io_message_type := it_q_message.C_C_MSG_TYPE_R;
        io_MSGCode      := nvl(io_MSGCode, 0);
      when upper(v_msgrootElement) like '%RESP' then
        io_message_type := it_q_message.C_C_MSG_TYPE_A;
        if io_MSGCode is null
        then
          raise_application_error(-20908, 'Не указан обязательный тег ErrorCode');
        end if;
      else
        raise_application_error(-20903, 'Ошибка  RootElement! Получен ' || v_msgrootElement || ' ожидалось %Req или %Resp');
    end case;
  end;

  -- XML по DEF-76759
  procedure Format_XDIASOFT_TAXES(p_kafka_topic   varchar2
                                 ,p_GUID          varchar2
                                 ,pcl_message     in clob
                                 ,p_system_name   itt_kafka_topic.system_name%type
                                 ,p_rootelement   itt_kafka_topic.rootelement%type
                                 ,p_cnt_sel       integer
                                 ,io_topic_id     in out itt_kafka_topic.topic_id%type
                                 ,io_sg_count     in out itt_kafka_topic.sg_count%type
                                 ,io_msg_param    in out itt_kafka_topic.msg_param%type
                                 ,io_msgid        in out itt_q_message_log.msgid%type
                                 ,io_message_type in out itt_q_message_log.message_type%type
                                 ,io_CORRmsgid    in out itt_q_message_log.corrmsgid%type
                                 ,io_ServiceName  in out itt_q_message_log.servicename%type
                                 ,io_MSGCode      in out itt_q_message_log.msgcode%type
                                 ,io_MSGText      in out itt_q_message_log.msgtext%type
                                 ,io_RequestDT    in out itt_q_message_log.requestdt%type) as
    vx_message       xmltype;
    vx_errlist       xmltype;
    v_msgrootElement itt_kafka_topic.rootelement%type;
    v_dom_doc        dbms_xmldom.domdocument;
    v_dom_eliment    dbms_xmldom.domelement;
  begin
    vx_message       := it_xml.Clob_to_xml(p_clob => pcl_message, p_errparam => 'входящего сообщения из ' || p_system_name);
    v_msgrootElement := vx_message.getRootElement;
    if p_cnt_sel > 1
    then
      begin
        select t.topic_id
              ,t.servicename
              ,nvl2(t.msg_param, 'xmlns="' || t.msg_param || '"', null)
              ,t.sg_count
          into io_topic_id
              ,io_ServiceName
              ,io_msg_param
              ,io_sg_count
          from itt_kafka_topic t
         where upper(t.topic_name) = upper(p_kafka_topic)
           and t.queuetype = it_q_message.C_C_QUEUE_TYPE_IN
           and upper(t.rootelement) = upper(v_msgrootElement);
      exception
        when others then
          raise_application_error(-20901, 'Не удалось определить ServiceName для Topic ' || p_kafka_topic || ' и RootElement ' || v_msgrootElement);
      end;
    end if;
    with in_d as
     (select vx_message as x from dual)
    select extractValue(in_d.x, '*/GUID', io_msg_param)
          ,it_xml.char_to_timestamp(extractValue(in_d.x, '*/RequestTime', io_msg_param))
          ,extractValue(in_d.x, '*/GUIDResp', io_msg_param)
      into io_msgid
          ,io_RequestDT
          ,io_CORRmsgid
      from in_d;
    io_msgid := nvl(io_msgid, p_GUID);
    if io_msgid is null
    then
      raise_application_error(-20908, 'Ошибка валидации сообщения');
    elsif upper(nvl(p_rootElement, v_msgrootElement)) != upper(v_msgrootElement)
          and p_cnt_sel = 1
    then
      raise_application_error(-20902, 'Ошибка  RootElement! Получен ' || v_msgrootElement || ' ожидалось ' || p_rootElement);
    end if;
    v_dom_doc     := dbms_xmldom.newdomdocument(vx_message);
    v_dom_eliment := dbms_xmldom.getdocumentelement(v_dom_doc);
    dbms_xmldom.removeAttribute(v_dom_eliment, 'xmlns');
    vx_errlist := dbms_xmldom.getxmltype(v_dom_doc);
    begin
      select max(abs(ErrorCode))
            ,substr(LISTAGG(trim(ErrorDesc), '\ ') WITHIN group(order by rownum), 1, 2000)
        into io_MSGCode
            ,io_MSGText
        from XMLTable('*/ErrorList/Error' passing(vx_errlist) columns ErrorCode integer path 'ErrorCode', ErrorDesc varchar2(2000) path 'ErrorDesc');
    exception
      when no_data_found then
        io_MSGCode := null;
        io_MSGText := null;
    end;
    case
      when upper(v_msgrootElement) like '%REQ' then
        io_message_type := it_q_message.C_C_MSG_TYPE_R;
        io_MSGCode      := nvl(io_MSGCode, 0);
      when upper(v_msgrootElement) like '%RESP' then
        io_message_type := it_q_message.C_C_MSG_TYPE_A;
        if io_MSGCode is null
        then
          raise_application_error(-20908, 'Не указан обязательный тег ErrorCode');
        end if;
      else
        raise_application_error(-20903, 'Ошибка  RootElement! Получен ' || v_msgrootElement || ' ожидалось %Req или %Resp');
    end case;
  end;

  -- JSON с rootelement из itt_kafka_topic.rootelement и тегами /GUID ,/GUIDReq и т.д.
  procedure Format_JGUID(pcl_message     in clob
                        ,p_rootelement   itt_kafka_topic.rootelement%type
                        ,io_msgid        in out itt_q_message_log.msgid%type
                        ,io_message_type in out itt_q_message_log.message_type%type
                        ,io_CORRmsgid    in out itt_q_message_log.corrmsgid%type
                        ,io_MSGCode      in out itt_q_message_log.msgcode%type
                        ,io_MSGText      in out itt_q_message_log.msgtext%type
                        ,io_RequestDT    in out itt_q_message_log.requestdt%type) as
    v_select varchar2(32600);
  begin
    ------------------------------JSON-------------------------------------------------------------
    v_select := 'select j.GUID, j.RequestTime, j.GUIDReq
           from (select :1 as j from dual) json ,
               json_table( json.j, ''$."' || p_rootElement || '"''
                      columns( GUID varchar2(128) path ''$.GUID''
                               ,RequestTime timestamp path ''$.RequestTime''
                               ,GUIDReq varchar2(128) path ''$.GUIDReq''))j';
    begin
      execute immediate v_select
        into io_msgid, io_RequestDT, io_CORRmsgid
        using pcl_message;
    exception
      when others then
        io_msgid := null;
    end;
    if io_msgid is null
    then
      raise_application_error(-20904, 'Ошибка формата сообшщения !  ожидался RootElement $.' || p_rootElement || ' и значение $.GUID');
    end if;
    v_select :=  --
     'select max(abs(j.ErrorCode)), substr(LISTAGG(trim(j.ErrorDesc), ''\ '') WITHIN group(order by rownum), 1, 2000)
              from (select :1 as j from dual) json ,
                   json_table( json.j, ''$."' || p_rootElement || '".ErrorList.Error[*]''
                      columns(ErrorCode number path ''$.ErrorCode''
                              ,ErrorDesc varchar2(2000)  path ''$.ErrorDesc'')
                             )j';
    execute immediate v_select
      into io_MSGCode, io_MSGText
      using pcl_message;
    case
      when upper(p_rootElement) like '%REQ' then
        io_message_type := it_q_message.C_C_MSG_TYPE_R;
        io_MSGCode      := nvl(io_MSGCode, 0);
      when upper(p_rootElement) like '%RESP' then
        io_message_type := it_q_message.C_C_MSG_TYPE_A;
        if io_MSGCode is null
        then
          raise_application_error(-20905, 'У ответа отсутствует обязательный атрибут $.' || p_rootElement || '.ErrorList.ErrorCode');
        end if;
      else
        raise_application_error(-20906, 'Ошибка  RootElement! Получен ' || p_rootElement || ' ожидалось %Req или %Resp');
    end case;
  end;

  -- JSON из IPS параметры сообщения из Heder.
  procedure Format_JIPS(p_kafka_topic varchar2
                       ,p_GUID        varchar2
                       ,p_ESBDT       in timestamp
                       ,pcl_Header    in clob
                       ,pcl_message   in clob
                        --,p_system_name   itt_kafka_topic.system_name%type
                        --,p_rootelement itt_kafka_topic.rootelement%type
                        --,p_cnt_sel     integer
                       ,io_topic_id in out itt_kafka_topic.topic_id%type
                       ,io_sg_count     in out itt_kafka_topic.sg_count%type
                        --,io_msg_param    in out itt_kafka_topic.msg_param%type
                       ,io_msgid        in out itt_q_message_log.msgid%type
                       ,io_message_type in out itt_q_message_log.message_type%type
                       ,io_Sender       in out itt_q_message_log.sender%type
                       ,io_CORRmsgid    in out itt_q_message_log.corrmsgid%type
                       ,io_ServiceName  in out itt_q_message_log.servicename%type
                        --,io_ServiceGroup in out itt_q_message_log.servicegroup%type
                       ,io_MSGCode   in out itt_q_message_log.msgcode%type
                       ,io_MSGText   in out itt_q_message_log.msgtext%type
                       ,io_RequestDT in out itt_q_message_log.requestdt%type) as
    v_header_obj JSON_OBJECT_T;
    v_isSuccess  varchar2(100);
    vr_message   itt_q_message_log%rowtype;
  begin
    ------------------------------JIPS-------------------------------------------------------------
    /* "x-request-id": "11ec46d0-7afb-4725-ac29-01fda8482730 ",
    "x-request-time": "2025-01-30T19:22:35+00:00", */
    begin
      v_header_obj := json_object_t.parse(pcl_Header);
    exception
      when others then
        raise_application_error(-20910, 'Ошибка разбора JSON Headers ообшщения !');
    end;
    io_CORRmsgid := v_header_obj.get_String('x-request-id');
    io_msgid     := coalesce(p_GUID, io_CORRmsgid);
    if io_msgid is null
    then
      io_msgid := it_q_message.get_sys_guid;
    else
      io_msgid := io_msgid || '-IPS';
    end if;
    io_RequestDT    := coalesce(v_header_obj.get_Timestamp('x-request-time'), p_ESBDT, systimestamp);
    io_message_type := it_q_message.C_C_MSG_TYPE_A;
    vr_message      := it_q_message.messlog_get(p_msgid => io_CORRmsgid, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT);
    if vr_message.log_id is null
       or vr_message.message_type != it_q_message.C_C_MSG_TYPE_R
    then
      raise_application_error(-20911
                             ,'Запрос ' || io_CORRmsgid || ' для определения настройки формата ' || C_C_MSG_FORMAT_JIPS || ' топика KAFKA не найден !');
    end if;
    io_Sender := vr_message.receiver;
    begin
      select t.topic_id
            ,t.servicename
            ,t.sg_count
        into io_topic_id
            ,io_ServiceName
            ,io_sg_count
        from itt_kafka_topic t
       where upper(t.topic_name) = upper(p_kafka_topic)
         and t.queuetype = it_q_message.C_C_QUEUE_TYPE_IN
         and upper(trim(t.servicename)) = upper(trim(vr_message.servicename))
         and upper(trim(t.system_name)) = upper(trim(vr_message.receiver));
    exception
      when others then
        raise_application_error(-20912
                               ,'Не удалось определить настройку формата ' || C_C_MSG_FORMAT_JIPS || ' топика KAFKA ' || p_kafka_topic || ' и ServiceName ' ||
                                vr_message.servicename);
    end;
    begin
      select max(abs(j.ErrorCode))
            ,substr(LISTAGG(trim(j.ErrorDesc), '\ ') WITHIN group(order by rownum), 1, 2000)
            ,json_value(json.j, '$.isSuccess') isSuccess
        into io_MSGCode
            ,io_MSGText
            ,v_isSuccess
        from (select pcl_message as j from dual) json
            ,json_table(json.j, '$.errors[*]' columns(ErrorCode number path '$.errorCode', ErrorDesc varchar2(2000) path '$.errorMessage')) j;
    exception
      when others then
        raise_application_error(-20910, 'Ошибка разбора JSON Body cообшщения !');
    end;
    if v_isSuccess = 'true'
    then
      io_MSGCode := 0;
    else
      io_MSGCode := nvl(case
                          when abs(io_MSGCode) != 0 then
                           abs(io_MSGCode)
                        end
                       ,it_q_manager.C_N_ERROR_OTHERS_MSGCODE);
      io_MSGText := nvl(io_MSGText, it_q_manager.C_C_ERROR_OTHERS_MSGTEXT || ' ' || io_ServiceName);
    end if;
  end;

  function get_ServiceGroup_topic_tread(p_topic_id     itt_kafka_topic.topic_id%type
                                       ,p_sg_count     itt_kafka_topic.sg_count%type
                                       ,p_message_type itt_q_message_log.message_type%type
                                       ,p_ServiceName  itt_q_message_log.servicename%type) return varchar2 as
    v_service_stop_apostq integer := 0;
    v_service_price       integer := 0;
    v_thread_count        pls_integer := nvl(p_sg_count, G_THREAD_COUNT);
    v_res                 varchar2(128);
    v_tread               pls_integer;
  begin
    if not gt_thread_kafka_topic.exists(p_topic_id)
    then
      select nvl(max(s.stop_apostq), 0)
            ,nvl(max(s.service_price), 0)
        into v_service_stop_apostq
            ,v_service_price
        from itt_q_service s
       where s.MESSAGE_TYPE = p_message_type
         and UPPER(trim(s.SERVICENAME)) = UPPER(trim(p_ServiceName));
      if v_service_stop_apostq != 0
         or v_service_price > G_THREAD_EXTREME_SRV_PRICE
      then
        gt_thread_kafka_topic(p_topic_id) := round(dbms_random.value(1 - 0.49, v_thread_count + 0.49));
      else
        gt_thread_kafka_topic(p_topic_id) := -1;
        return null;
      end if;
    elsif gt_thread_kafka_topic(p_topic_id) = -1
    then
      return null;
    end if;
    v_tread := gt_thread_kafka_topic(p_topic_id);
    v_res := it_q_message.get_next_servicegroup(p_pref_servicegroup => C_C_THREAD_TOPIC || p_topic_id
                                               ,io_last_thread => v_tread
                                               ,p_max_thread => v_thread_count
                                               ,p_queue_num => C_C_QUEUE_NUM);
    gt_thread_kafka_topic(p_topic_id) := v_tread;
    return v_res;
  end;

  -- Парсинг атрибутов сообщения 
  procedure get_qmess_attribute(p_kafka_topic   varchar2
                               ,p_GUID          varchar2
                               ,p_ESBDT         in timestamp
                               ,pcl_Header      in clob
                               ,pcl_message     in clob
                               ,io_msgid        in out itt_q_message_log.msgid%type
                               ,io_message_type in out itt_q_message_log.message_type%type
                               ,io_Sender       in out itt_q_message_log.sender%type
                               ,io_CORRmsgid    in out itt_q_message_log.corrmsgid%type
                               ,io_ServiceName  in out itt_q_message_log.servicename%type
                               ,io_ServiceGroup in out itt_q_message_log.servicegroup%type
                               ,io_MSGCode      in out itt_q_message_log.msgcode%type
                               ,io_MSGText      in out itt_q_message_log.msgtext%type
                               ,io_RequestDT    in out itt_q_message_log.requestdt%type) as
    v_topic_id    itt_kafka_topic.topic_id%type;
    v_msg_format  itt_kafka_topic.msg_format%type;
    v_system_name itt_kafka_topic.system_name%type;
    v_rootElement itt_kafka_topic.rootelement%type;
    v_msg_param   itt_kafka_topic.msg_param%type;
    v_sg_count    itt_kafka_topic.sg_count%type;
    v_cnt_sel     integer;
  begin
    begin
      select *
        into v_topic_id
            ,v_cnt_sel
            ,v_msg_format
            ,v_system_name
            ,io_ServiceName
            ,v_rootElement
            ,v_msg_param
            ,v_sg_count
        from (select t.topic_id
                    ,count(1) over(partition by upper(t.topic_name), t.queuetype) as cnt_sel
                    ,t.msg_format
                    ,t.system_name
                    ,t.servicename
                    ,t.rootelement
                    ,nvl2(t.msg_param, 'xmlns="' || t.msg_param || '"', null)
                    ,t.sg_count
                from itt_kafka_topic t
               where upper(t.topic_name) = upper(p_kafka_topic)
                 and t.queuetype = it_q_message.C_C_QUEUE_TYPE_IN)
       where rownum < 2;
    exception
      when no_data_found then
        v_msg_format := null;
    end;
    io_Sender := v_system_name;
    case
      when v_msg_format = C_C_MSG_FORMAT_XML then
        Format_XGUID(p_kafka_topic => p_kafka_topic
                    ,p_GUID => p_GUID
                    ,pcl_message => pcl_message
                    ,p_system_name => v_system_name
                    ,p_rootelement => v_rootelement
                    ,p_cnt_sel => v_cnt_sel
                    ,io_topic_id => v_topic_id
                    ,io_sg_count => v_sg_count
                    ,io_msg_param => v_msg_param
                    ,io_msgid => io_msgid
                    ,io_message_type => io_message_type
                    ,io_CORRmsgid => io_CORRmsgid
                    ,io_ServiceName => io_ServiceName
                    ,io_MSGCode => io_MSGCode
                    ,io_MSGText => io_MSGText
                    ,io_RequestDT => io_RequestDT);
      when v_msg_format = C_C_MSG_FORMAT_XDIASOFT_TAXES then
        Format_XDIASOFT_TAXES(p_kafka_topic => p_kafka_topic
                             ,p_GUID => p_GUID
                             ,pcl_message => pcl_message
                             ,p_system_name => v_system_name
                             ,p_rootelement => v_rootelement
                             ,p_cnt_sel => v_cnt_sel
                             ,io_topic_id => v_topic_id
                             ,io_sg_count => v_sg_count
                             ,io_msg_param => v_msg_param
                             ,io_msgid => io_msgid
                             ,io_message_type => io_message_type
                             ,io_CORRmsgid => io_CORRmsgid
                             ,io_ServiceName => io_ServiceName
                             ,io_MSGCode => io_MSGCode
                             ,io_MSGText => io_MSGText
                             ,io_RequestDT => io_RequestDT);
      when v_msg_format = C_C_MSG_FORMAT_JSON then
        Format_JGUID(pcl_message => pcl_message
                    ,p_rootelement => v_rootelement
                    ,io_msgid => io_msgid
                    ,io_message_type => io_message_type
                    ,io_CORRmsgid => io_CORRmsgid
                    ,io_MSGCode => io_MSGCode
                    ,io_MSGText => io_MSGText
                    ,io_RequestDT => io_RequestDT);
      when v_msg_format = C_C_MSG_FORMAT_JIPS then
        Format_JIPS(p_kafka_topic => p_kafka_topic
                   ,p_GUID => p_GUID
                   ,p_ESBDT => p_ESBDT
                   ,pcl_Header => pcl_Header
                   ,pcl_message => pcl_message
                    --,p_system_name => v_system_name
                    --,p_rootelement => v_rootelement
                    --,p_cnt_sel => v_cnt_sel
                   ,io_topic_id => v_topic_id
                   ,io_sg_count => v_sg_count
                    --,io_msg_param => v_msg_param
                   ,io_msgid => io_msgid
                   ,io_message_type => io_message_type
                   ,io_Sender => io_Sender
                   ,io_CORRmsgid => io_CORRmsgid
                   ,io_ServiceName => io_ServiceName
                    --,io_ServiceGroup => io_ServiceGroup
                   ,io_MSGCode => io_MSGCode
                   ,io_MSGText => io_MSGText
                   ,io_RequestDT => io_RequestDT);
      when v_msg_format in (C_C_MSG_FORMAT_SUBSCR_TR, C_C_MSG_FORMAT_SUBSCR_PL) then
        ------------------------------SUBSCR-------------------------------------------------------------
        io_msgid        := nvl(p_GUID, it_q_message.get_sys_guid());
        io_message_type := it_q_message.C_C_MSG_TYPE_R;
        if v_msg_format = C_C_MSG_FORMAT_SUBSCR_TR
        then
          io_ServiceGroup := C_C_THREAD_TOPIC || v_topic_id;
        end if;
        io_MSGCode := 0;
      else
        raise_application_error(-20907, 'Для Topic ' || p_kafka_topic || ' не найден формат сообщения в справочнике !');
    end case;
    if io_ServiceGroup is null
    then
      io_ServiceGroup := get_ServiceGroup_topic_tread(v_topic_id, v_sg_count, io_message_type, io_ServiceName);
    end if;
  end;

  -- Загрузка сообщения в QManager
  procedure qmanager_load_msg(p_kafka_topic varchar2
                             ,p_GUID        varchar2
                             ,p_ESBDT       timestamp
                             ,pcl_header    clob default null -- Header 
                             ,pcl_message   clob -- Body
                             ,o_ErrorCode   out number
                             ,o_ErrorDesc   out varchar2) as
    pragma autonomous_transaction;
    v_msgid        itt_q_message_log.msgid%type;
    v_message_type itt_q_message_log.message_type%type;
    v_Sender       itt_q_message_log.sender%type;
    v_CORRmsgid    itt_q_message_log.corrmsgid%type;
    v_ServiceName  itt_q_message_log.servicename%type;
    v_ServiceGroup itt_q_message_log.servicegroup%type;
    v_MSGCode      itt_q_message_log.msgcode%type;
    v_MSGText      itt_q_message_log.msgtext%type;
    v_RequestDT    itt_q_message_log.requestdt%type;
    v_errtxt       varchar2(2000);
    vx_MessMETA    xmltype;
  begin
    get_qmess_attribute(p_kafka_topic => p_kafka_topic
                       ,p_GUID => p_GUID
                       ,p_ESBDT => p_ESBDT
                       ,pcl_Header => pcl_Header
                       ,pcl_message => pcl_message
                       ,io_msgid => v_msgid
                       ,io_message_type => v_message_type
                       ,io_Sender => v_Sender
                       ,io_CORRmsgid => v_CORRmsgid
                       ,io_ServiceName => v_ServiceName
                       ,io_ServiceGroup => v_ServiceGroup
                       ,io_MSGCode => v_MSGCode
                       ,io_MSGText => v_MSGText
                       ,io_RequestDT => v_RequestDT);
    select xmlelement("KAFKA", xmlattributes(p_kafka_topic as "Topic", p_GUID as "GUID"), xmlelement("Header", pcl_header)) into vx_MessMETA from dual;
    it_q_message.load_msg_inqueue(p_msgid => v_msgid
                                 ,p_message_type => v_message_type
                                 ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                                 ,p_Sender => v_Sender
                                 ,p_Priority => it_q_message.C_C_MSG_PRIORITY_N
                                 ,p_CORRmsgid => v_CORRmsgid
                                  --,p_SenderUser =>
                                 ,p_ServiceName => v_ServiceName
                                 ,p_ServiceGroup => v_ServiceGroup
                                  --,p_BTUID =>
                                 ,p_MSGCode => v_MSGCode
                                 ,p_MSGText => v_MSGText
                                 ,p_MESSBODY => pcl_message
                                 ,p_MessMETA => vx_MessMETA
                                 ,p_queue_num => C_C_QUEUE_NUM
                                 ,p_RequestDT => v_RequestDT
                                 ,p_ESBDT => p_ESBDT);
    o_ErrorCode := 0;
    o_ErrorDesc := '';
    commit;
  exception
    when others then
      rollback; --- !!!!!
      o_ErrorCode := abs(sqlcode);
      v_errtxt    := sqlerrm;
      o_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => v_errtxt);
      v_errtxt    := sys.dbms_utility.format_error_backtrace;
      begin
        select xmlelement("KAFKA", xmlattributes(p_kafka_topic as "Topic", p_GUID as "GUID")) into vx_MessMETA from dual;
      exception
        when others then
          vx_MessMETA := null;
      end;
      it_event.AddErrorITLogMonitoring(p_SystemId => C_C_KAFKANAME
                                      ,p_ServiceName => 'QManager_load_msg'
                                      ,p_ErrorCode => o_ErrorCode
                                      ,p_ErrorDesc => o_ErrorDesc
                                      ,p_LevelInfo => 0
                                      ,p_backtrace => v_errtxt
                                      ,p_MsgBODY => 'Ошибка загрузки сообщения в SOFR из Topic[' || p_kafka_topic || '] : ' || o_ErrorDesc
                                      ,p_MsgMETA => case
                                                      when vx_MessMETA is not null then
                                                       vx_MessMETA.getClobVal
                                                    end);
  end;

  -- Определение TOPIC KAFKA
  function get_kafka_topic(p_message       it_q_message_t
                          ,o_kafka_cluster out varchar2) return varchar2 as
    v_kafka_topic itt_kafka_topic.topic_name%type;
  begin
    select t.topic_name
          ,t.topic_cluster
      into v_kafka_topic
          ,o_kafka_cluster
      from itt_kafka_topic t
     where t.queuetype = it_q_message.C_C_QUEUE_TYPE_OUT
       and t.system_name = p_message.Receiver
       and t.servicename = p_message.ServiceName;
    return v_kafka_topic;
  exception
    when no_data_found then
      return null;
  end;

  procedure read_msg_RegisterError(p_ErrorCode   number
                                  ,p_ErrorDesc   varchar2
                                  ,p_kafka_topic varchar2 default null
                                  ,p_msgID       varchar2 default null
                                  ,p_backtrace   varchar2 default null) as
    vx_MessMETA xmltype;
  begin
    select xmlelement("KAFKA", xmlattributes(p_kafka_topic as "Topic", p_msgID as "MsgID")) into vx_MessMETA from dual;
    it_event.AddErrorITLogMonitoring(p_SystemId => C_C_KAFKANAME
                                    ,p_ServiceName => 'QManager_read_msg'
                                    ,p_ErrorCode => p_ErrorCode
                                    ,p_ErrorDesc => p_ErrorDesc
                                    ,p_LevelInfo => 0
                                    ,p_backtrace => p_backtrace
                                    ,p_MsgBODY => 'Ошибка выгрузки сообщения из SOFR в Topic[' || p_kafka_topic || '] : ' || p_ErrorDesc
                                    ,p_MsgMETA => vx_MessMETA.getClobVal);
  end;

  procedure qmanager_dequeue_msg(p_wait_msg        in number --  Ожидание сообщения (сек)
                                ,p_QUEUENAME       varchar2 default null
                                ,p_QMSGID          in raw default null -- GUID Сообщениz в очереди 
                                ,p_correlation     in varchar2
                                ,o_kafka_cluster   out varchar2 -- cluster
                                ,o_kafka_topic     out varchar2
                                ,o_S3xdatafilename out varchar2
                                ,o_S3_point        out varchar2 -- точка интеграции S3
                                ,o_msgID           out varchar2 -- GUID Сообщения
                                ,ocl_header        out clob -- Header 
                                ,ocl_message       out clob -- Body
                                ,o_ErrorCode       out number -- = 0 ОК
                                ,o_ErrorDesc       out varchar2) as
    v_message  it_q_message_t;
    v_wait_msg number := case
                           when p_qmsgid is not null then
                            sys.dbms_aq.no_wait
                           when p_wait_msg is null
                                or p_wait_msg < 0 then
                            30
                           else
                            p_wait_msg
                         end;
  begin
    it_q_message.dequeue_outmessage(p_queuename => case
                                                     when p_qmsgid is null then
                                                      it_q_message.C_C_QUEUE_OUT_PREFIX || C_C_QUEUE_NUM
                                                     else
                                                      p_QUEUENAME
                                                   end
                                   ,p_qmsgid => p_qmsgid
                                   ,p_correlation => case
                                                       when p_qmsgid is null then
                                                        p_correlation
                                                     end
                                   ,p_wait => v_wait_msg -- Ожидание сообщения в сек.
                                   ,o_message => v_message
                                   ,o_errno => o_ErrorCode
                                   ,o_errmsg => o_ErrorDesc);
    if o_ErrorCode = 0
    then
      o_kafka_topic := get_kafka_topic(v_message, o_kafka_cluster);
      o_msgID       := v_message.msgid;
      o_S3_point    := get_S3_point(v_message.Receiver);
      if v_message.MessMETA is not null
      then
        get_info_Xmessmeta(px_messmeta => v_message.MessMETA, ocl_Нeader => ocl_header, o_S3xdatafilename => o_S3xdatafilename);
      end if;
      o_S3xdatafilename := nvl(o_S3xdatafilename, o_msgID);
      ocl_message       := v_message.MessBODY;
    elsif o_ErrorCode != 25228 -- TimeOut
    then
      read_msg_RegisterError(o_ErrorCode, o_ErrorDesc, o_kafka_topic);
    end if;
  exception
    when others then
      rollback; --- !!!!!
      o_ErrorCode := abs(sqlcode);
      o_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
      read_msg_RegisterError(o_ErrorCode, o_ErrorDesc, o_kafka_topic, p_backtrace => sys.dbms_utility.format_error_backtrace);
  end;

  -- Выгрузка сообщения из QManager
  procedure qmanager_read_msg(p_wait_msg      in number --  Ожидание сообщения (сек)
                             ,p_QUEUENAME     varchar2 default null
                             ,p_QMSGID        in raw default null -- GUID Сообщениz в очереди 
                             ,o_kafka_cluster out varchar2 -- cluster
                             ,o_kafka_topic   out varchar2
                             ,o_msgID         out varchar2 -- GUID Сообщения
                             ,ocl_header      out clob -- Header 
                             ,ocl_message     out clob -- Body
                             ,o_ErrorCode     out number -- = 0 ОК
                             ,o_ErrorDesc     out varchar2) as
    v_S3_point        varchar2(4000);
    v_S3xdatafilename varchar2(4000);
  begin
    qmanager_dequeue_msg(p_wait_msg => p_wait_msg
                        ,p_QUEUENAME => p_QUEUENAME
                        ,p_QMSGID => p_QMSGID
                        ,p_correlation => C_C_PREF_CORRELATION
                        ,o_kafka_cluster => o_kafka_cluster
                        ,o_kafka_topic => o_kafka_topic
                        ,o_S3xdatafilename => v_S3xdatafilename
                        ,o_S3_point => v_S3_point
                        ,o_msgID => o_msgID
                        ,ocl_header => ocl_header
                        ,ocl_message => ocl_message
                        ,o_ErrorCode => o_ErrorCode
                        ,o_ErrorDesc => o_ErrorDesc);
  end;

  -- Выгрузка больщих сообщений из QManager через IPS S3
  procedure qmanager_read_msg_S3(p_wait_msg        in number --  Ожидание сообщения (сек)
                                ,p_QUEUENAME       varchar2 default null
                                ,p_QMSGID          in raw default null -- GUID Сообщениz в очереди 
                                ,o_kafka_cluster   out varchar2 -- cluster
                                ,o_kafka_topic     out varchar2
                                ,o_S3xdatafilename out varchar2
                                ,o_S3_point        out varchar2 -- точка интеграции S3
                                ,o_msgID           out varchar2 -- GUID Сообщения
                                ,ocl_header        out clob -- Header 
                                ,ocl_message       out clob -- Body
                                ,o_ErrorCode       out number -- = 0 ОК
                                ,o_ErrorDesc       out varchar2) as
  begin
    qmanager_dequeue_msg(p_wait_msg => p_wait_msg
                        ,p_QUEUENAME => p_QUEUENAME
                        ,p_QMSGID => p_QMSGID
                        ,p_correlation => C_C_PREF_CORRELATION || C_C_ADD_S3_CORRELATION
                        ,o_kafka_cluster => o_kafka_cluster
                        ,o_kafka_topic => o_kafka_topic
                        ,o_S3xdatafilename => o_S3xdatafilename
                        ,o_S3_point => o_S3_point
                        ,o_msgID => o_msgID
                        ,ocl_header => ocl_header
                        ,ocl_message => ocl_message
                        ,o_ErrorCode => o_ErrorCode
                        ,o_ErrorDesc => o_ErrorDesc);
  end;

  -- Зарегистрировать ошибку при сохранении в транспортной системе
  procedure qmanager_read_msg_error(p_kafka_topic in varchar2
                                   ,p_msgID       in varchar2 -- GUID Сообщения
                                   ,p_ErrorCode   in number -- Код ошибки p_ErrorDesc 
                                   ,p_ErrorDesc   in varchar2) as
    v_message itt_q_message_log%rowtype;
  begin
    v_message := it_q_message.messlog_get(p_msgid => p_msgID, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT);
    it_q_message.messlog_upd_status(p_msgid => p_msgID
                                   ,p_delivery_type => v_message.delivery_type
                                   ,p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT
                                   ,p_status => it_q_message.C_STATUS_ERRSEND
                                   ,p_comment => 'ErrorCode#' || p_ErrorCode || ':' || p_ErrorDesc);
    read_msg_RegisterError(p_ErrorCode, p_ErrorDesc, p_kafka_topic, p_msgID => p_msgID);
  end;

  -- Проверка справочника KAFKA Topic 1- ОК
  function chk_kafka_topic(p_queuetype   itt_q_message_log.queuetype% type
                          ,p_system_name itt_q_corrsystem.system_name%type
                          ,p_ServiceName itt_q_message_log.servicename%type
                          ,o_ErrorCode   out number -- != 0 ошибка o_ErrorDesc
                          ,o_ErrorDesc   out varchar2) return number as
    v_cnt integer;
  begin
    select count(*)
      into v_cnt
      from itt_kafka_topic t
     where t.queuetype = p_queuetype
       and upper(t.system_name) = upper(p_system_name)
       and upper(t.servicename) = upper(p_ServiceName);
    if v_cnt = 0
    then
      o_ErrorCode := 1;
      o_ErrorDesc := 'Для ' || p_queuetype || ' сообщения для бизнеспроцесса ' || p_ServiceName || ' в системе ' || p_system_name || '  не зарегистрирован Topic KAFKA ';
      return 0;
    else
      o_ErrorCode := 0;
      return 1;
    end if;
  end;

  -- процедура размещения сообщения в очередь для отправки через KAFKA
  procedure load_msg(io_msgid       in out itt_q_message_log.msgid%type -- GUID сообщения
                    ,p_message_type itt_q_message_log.message_type%type -- Тип сообщения R или A Запрос/ответ 
                    ,p_ServiceName  itt_q_message_log.servicename%type -- Бизнес-процесс
                    ,p_Receiver     itt_q_message_log.receiver%type -- Система-получатель
                    ,p_MESSBODY     clob -- Бизнес - составляющая сообщения
                    ,o_ErrorCode    out number -- != 0 ошибка o_ErrorDesc
                    ,o_ErrorDesc    out varchar2
                    ,p_CORRmsgid    itt_q_message_log.corrmsgid%type default null -- GUID связанного сообщения 
                    ,p_MSGCode      integer default 0 -- Код результата обработки сообщения. 0 - успех
                    ,p_MSGText      itt_q_message_log.msgtext%type default null -- Текст ошибки, возникший при обработке сообщен
                    ,p_comment      itt_q_message_log.commenttxt%type default null -- коментарии в лог 
                    ,p_MessMETA     xmltype default null -- XML Метаданные сообщения
                    ,p_isquery      pls_integer default null -- сообщение с ожиданием ответа (1/0
                     ) as
  begin
    if chk_kafka_topic(p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT
                      ,p_system_name => p_Receiver
                      ,p_ServiceName => p_ServiceName
                      ,o_ErrorCode => o_ErrorCode
                      ,o_ErrorDesc => o_ErrorDesc) = 1
    then
      it_q_message.load_msg(io_msgid => io_msgid
                           ,p_message_type => p_message_type
                           ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                            --,p_Sender        =>
                            --,p_Priority      => 
                           ,p_Correlation => get_correlation(p_Receiver => p_Receiver, p_len_MessBODY => dbms_lob.getlength(p_MESSBODY))
                           ,p_CORRmsgid => p_CORRmsgid
                            -- ,p_SenderUser    => 
                           ,p_ServiceName => p_ServiceName
                            -- ,p_ServiceGroup  => 
                           ,p_Receiver => p_Receiver
                            -- ,p_BTUID         => 
                           ,p_MSGCode => p_MSGCode
                           ,p_MSGText => p_MSGText
                           ,p_MESSBODY => p_MESSBODY
                           ,p_MessMETA => p_MessMETA
                           ,p_queue_num => C_C_QUEUE_NUM
                           ,p_RequestDT => systimestamp
                           ,p_ESBDT => systimestamp
                            --,p_delay         => 
                           ,p_comment => p_comment
                           ,p_isquery => p_isquery);
      o_ErrorCode := 0;
    end if;
  exception
    when others then
      o_ErrorCode := abs(sqlcode);
      o_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
  end;

  function get_namespace(p_system_name itt_kafka_topic.system_name%type
                        ,p_rootelement itt_kafka_topic.rootelement%type) return varchar2 deterministic as
    v_res itt_kafka_topic.msg_param%type;
  begin
    select nvl2(t.msg_param, 'xmlns="' || t.msg_param || '"', null)
      into v_res
      from itt_kafka_topic t
     where upper(t.rootelement) = upper(p_rootelement)
       and upper(t.system_name) = upper(p_system_name);
    return v_res;
  end;

  function add_KAFKATag_Xmessmeta(px_messmeta xmltype
                                 ,p_Tag       varchar2
                                 ,p_TagValue  varchar2) return xmltype as
    vx_messmeta xmltype := px_messmeta;
  begin
    if p_TagValue is not null
    then
      if px_messmeta is null
      then
        select xmlelement("KAFKA", xmlelement(evalname p_Tag, p_TagValue)) into vx_messmeta from dual;
      else
        if px_messmeta.existsNode('KAFKA') = 1
        then
          select insertchildxml(deleteXML(px_messmeta, 'KAFKA/' || p_Tag), 'KAFKA', p_Tag, xmlelement(evalname p_Tag, p_TagValue)) into vx_messmeta from dual;
        else
          select insertchildxml(deleteXML(px_messmeta, '*/KAFKA/' || p_Tag), '*', 'KAFKA', xmlelement("KAFKA", xmlelement(evalname p_Tag, p_TagValue))) into vx_messmeta from dual;
        end if;
      end if;
    end if;
    return vx_messmeta;
  end;

  -- Возвращает MessMeta c встроенным KafkaHeader
  function add_Header_Xmessmeta(p_Header    varchar2
                               ,px_messmeta xmltype default null) return xmltype as
  begin
    return add_KAFKATag_Xmessmeta(px_messmeta => px_messmeta, p_Tag => 'Header', p_TagValue => p_Header);
  end;

  -- Возвращает MessMeta c встроенным S3 x-data-file-name
  function add_S3xdatafilename_Xmessmeta(p_S3xdatafilename varchar2
                                        ,px_messmeta       xmltype default null) return xmltype as
  begin
    return add_KAFKATag_Xmessmeta(px_messmeta => px_messmeta, p_Tag => 'S3xdatafilename', p_TagValue => p_S3xdatafilename);
  end;

  -- Из MessMeta  KafkaHeader S3x-data-file-name 
  procedure get_info_Xmessmeta(px_messmeta       xmltype
                              ,ocl_Нeader       out clob
                              ,o_S3xdatafilename out varchar2) as
  begin
    with MessMETA as
     (select px_messmeta x from dual)
    select nvl(extractValue(MessMETA.x, 'KAFKA/Header'), extractValue(MessMETA.x, '*/KAFKA/Header'))
          ,nvl(extractValue(MessMETA.x, 'KAFKA/S3xdatafilename'), extractValue(MessMETA.x, '*/KAFKA/S3xdatafilename'))
      into ocl_Нeader
          ,o_S3xdatafilename
      from MessMETA;
  end;

  function get_correlation(p_Receiver     itt_q_message_log.receiver%type
                          ,p_len_MessBODY number) return varchar2 as
    v_correlation  varchar2(128) := C_C_PREF_CORRELATION;
    v_Receiver     itt_q_message_log.receiver%type := p_Receiver;
    v_len_MessBODY number := p_len_MessBODY;
    v_S3_point     varchar2(4000);
  begin
    if nvl(v_Receiver, it_q_message.C_C_SYSTEMNAME) = it_q_message.C_C_SYSTEMNAME
    then
      raise_application_error(-20000, 'Ошибка формата сообщения для отправки в KAFKA ');
    end if;
    if v_len_MessBODY >= G_MAX_SIZE_MSG * 1024 * 1024
    then
      raise_application_error(-20901, 'Отправка сообщения >= ' || G_MAX_SIZE_MSG || 'MБ через KAFKA невозможна !');
    elsif v_len_MessBODY >= 1024 * 1024
    then
      v_S3_point := get_S3_point(v_Receiver);
      if v_S3_point is null
      then
        raise_application_error(-20902
                               ,'Отправка сообщения > 1MБ в ' || v_Receiver || ' через KAFKA невозможна !' || chr(10) || --
                                ' Необходимо настроить IPS S3 для ' || v_Receiver || '.');
      end if;
      v_correlation := v_correlation || C_C_ADD_S3_CORRELATION;
    end if;
    return v_correlation;
  end;

  -- Проверка маршрута через KAFKA S3 ( Если 0 и o_ErrorCode = 0 то KAFKA без S3) 
  -- 20901 - Отправка сообщения >= 10 MБ через KAFKA невозможна !
  -- 20902 - Отправка сообщения > 1MБ в ?????? через KAFKA невозможна Необходимо настроить IPS S3 для ??????
  function chk_from_KAFKAS3(p_Receiver     itt_q_message_log.receiver%type
                           ,p_len_MessBODY number -- dbms_lob.getlength(lob_loc => MessBODY)
                           ,o_ErrorCode    out integer
                           ,o_ErrorDesc    out varchar2) return pls_integer as
  begin
    o_ErrorCode := 0;
    return sys.diutil.bool_to_int(get_correlation(p_Receiver => p_Receiver, p_len_MessBODY => p_len_MessBODY) = C_C_PREF_CORRELATION || C_C_ADD_S3_CORRELATION);
  exception
    when others then
      o_ErrorCode := abs(sqlcode);
      o_ErrorDesc := it_q_message.get_errtxt(sqlerrm);
      return 0;
  end;

  -- Возвращает MessMeta c встроенным msgid и isquery  KAFKA
  function add_KAFKA_Xmessmeta(p_msgid     varchar2
                              ,p_isquery   pls_integer
                              ,px_messmeta xmltype default null) return xmltype as
  begin
    return add_KAFKATag_Xmessmeta(px_messmeta => add_KAFKATag_Xmessmeta(px_messmeta => px_messmeta, p_Tag => 'KAFKAmsgid', p_TagValue => p_msgid)
                                 ,p_Tag => 'KAFKAisquery'
                                 ,p_TagValue => p_isquery);
  end;

  -- процедура размещения сообщения в очередь для отправки через S3 в KAFKA
  procedure load_msg_S3(p_msgid        itt_q_message_log.msgid%type -- GUID сообщения
                       ,p_message_type itt_q_message_log.message_type%type -- Тип сообщения R или A Запрос/ответ 
                       ,p_ServiceName  itt_q_message_log.servicename%type -- Бизнес-процесс
                       ,p_Receiver     itt_q_message_log.receiver%type -- Система-получатель
                       ,p_MESSBODY     clob -- Бизнес - составляющая сообщения
                       ,p_MessMETA     xmltype -- XML Метаданные сообщения
                       ,o_ErrorCode    out number -- != 0 ошибка o_ErrorDesc
                       ,o_ErrorDesc    out varchar2
                       ,p_CORRmsgid    itt_q_message_log.corrmsgid%type default null -- GUID связанного сообщения 
                       ,p_MSGCode      integer default 0 -- Код результата обработки сообщения. 0 - успех
                       ,p_MSGText      itt_q_message_log.msgtext%type default null -- Текст ошибки, возникший при обработке сообщен
                       ,p_isquery      pls_integer default null -- сообщение с ожиданием ответа (1/0
                       ,p_comment      itt_q_message_log.commenttxt%type default null -- коментарии в лог 
                        ) as
    v_msgid    itt_q_message_log.msgid%type := p_msgid;
    v_msgid_S3 itt_q_message_log.msgid%type;
  begin
    if v_msgid is null
    then
      o_ErrorCode := 1;
      o_ErrorDesc := 'Ошибка параметра p_msgid для it_kafka.load_msg_S3';
      return;
    end if;
    o_ErrorCode := 0;
    if chk_kafka_topic(p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT
                      ,p_system_name => p_Receiver
                      ,p_ServiceName => p_ServiceName
                      ,o_ErrorCode => o_ErrorCode
                      ,o_ErrorDesc => o_ErrorDesc) = 1
    then
      if chk_from_KAFKAS3(p_Receiver => p_Receiver, p_len_MessBODY => dbms_lob.getlength(p_MESSBODY), o_ErrorCode => o_ErrorCode, o_ErrorDesc => o_ErrorDesc) = 1
      then
        it_q_message.load_msg(io_msgid => v_msgid_S3
                             ,p_message_type => p_message_type
                             ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                              --,p_Sender        =>
                              --,p_Priority      => 
                             ,p_Correlation => C_C_PREF_CORRELATION || C_C_ADD_S3_CORRELATION
                             ,p_CORRmsgid => p_CORRmsgid
                              -- ,p_SenderUser    => 
                             ,p_ServiceName => p_ServiceName
                              -- ,p_ServiceGroup  => 
                             ,p_Receiver => p_Receiver
                              -- ,p_BTUID         => 
                             ,p_MSGCode => p_MSGCode
                             ,p_MSGText => p_MSGText
                             ,p_MESSBODY => p_MESSBODY
                             ,p_MessMETA => add_KAFKA_Xmessmeta(v_msgid, p_isquery, p_MessMETA)
                             ,p_queue_num => C_C_QUEUE_NUM
                             ,p_RequestDT => systimestamp
                             ,p_ESBDT => systimestamp
                              --,p_delay         => 
                             ,p_comment => p_comment);
      elsif o_ErrorCode = 0
      then
        it_q_message.load_msg(io_msgid => v_msgid
                             ,p_message_type => p_message_type
                             ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                              --,p_Sender        =>
                              --,p_Priority      => 
                             ,p_Correlation => C_C_PREF_CORRELATION
                             ,p_CORRmsgid => p_CORRmsgid
                              -- ,p_SenderUser    => 
                             ,p_ServiceName => p_ServiceName
                              -- ,p_ServiceGroup  => 
                             ,p_Receiver => p_Receiver
                              -- ,p_BTUID         => 
                             ,p_MSGCode => p_MSGCode
                             ,p_MSGText => p_MSGText
                             ,p_MESSBODY => p_MESSBODY
                             ,p_MessMETA => p_MessMETA
                             ,p_queue_num => C_C_QUEUE_NUM
                             ,p_RequestDT => systimestamp
                             ,p_ESBDT => systimestamp
                              --,p_delay         => 
                             ,p_comment => p_comment
                             ,p_isquery => p_isquery);
      end if;
    end if;
  exception
    when others then
      o_ErrorCode := abs(sqlcode);
      o_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
  end;

  -- процедура дублирования сообщения в очередь для отправки header в KAFKA 
  procedure S3_add_msg_KAFKA(p_msgid     itt_q_message_log.msgid%type -- GUID сообщения
                            ,o_ErrorCode out number -- != 0 ошибка o_ErrorDesc
                            ,o_ErrorDesc out varchar2) as
    vr_message itt_q_message_log%rowtype;
    vx_Meta    xmltype;
    v_msgid    itt_q_message_log.msgid%type;
    v_isquery  pls_integer;
  begin
    vr_message := it_q_message.messlog_get(p_msgid => p_msgid, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT);
    if vr_message.log_id is null
    then
      o_ErrorCode := 1;
      o_ErrorDesc := 'Сообщение ' || p_msgid || ' не найдено !';
      return;
    end if;
    if chk_from_KAFKAS3(p_Receiver => vr_message.receiver, p_len_MessBODY => dbms_lob.getlength(vr_message.MESSBODY), o_ErrorCode => o_ErrorCode, o_ErrorDesc => o_ErrorDesc) = 0
    then
      o_ErrorCode := 0;
      o_ErrorDesc := '';
      return;
    end if;
    vx_Meta := it_xml.Clob_to_xml(vr_message.messmeta);
    if vx_Meta is null
    then
      o_ErrorCode := 2;
      o_ErrorDesc := 'Отсутствуют метаданные в сообщении ' || p_msgid || ' !';
      return;
    end if;
    with MessMETA as
     (select vx_Meta x from dual)
    select nvl(extractValue(MessMETA.x, 'KAFKA/KAFKAmsgid'), extractValue(MessMETA.x, '*/KAFKA/KAFKAmsgid'))
          ,to_number(coalesce(extractValue(MessMETA.x, 'KAFKA/KAFKAisquery'), extractValue(MessMETA.x, '*/KAFKA/KAFKAisquery'), '0'))
      into v_msgid
          ,v_isquery
      from MessMETA;
    if v_msgid is null
    then
      o_ErrorCode := 3;
      o_ErrorDesc := 'Cообщение ' || p_msgid || ' не для отпрвки через S3 !';
      return;
    end if;
    o_ErrorCode := 0;
    it_q_message.load_msg(io_msgid => v_msgid
                         ,p_message_type => vr_message.message_type
                         ,p_delivery_type => it_q_message.C_C_MSG_DELIVERY_A
                          --,p_Sender        =>
                          --,p_Priority      => 
                         ,p_Correlation => C_C_PREF_CORRELATION
                         ,p_CORRmsgid => nvl(vr_message.corrmsgid, p_msgid)
                          -- ,p_SenderUser    => 
                         ,p_ServiceName => vr_message.ServiceName
                          -- ,p_ServiceGroup  => 
                         ,p_Receiver => vr_message.Receiver
                          -- ,p_BTUID         => 
                         ,p_MSGCode => vr_message.MSGCode
                         ,p_MSGText => vr_message.MSGText
                          --,p_MESSBODY => p_MESSBODY
                         ,p_MessMETA => vx_Meta
                         ,p_queue_num => vr_message.queue_num
                         ,p_RequestDT => vr_message.requestdt
                         ,p_ESBDT => systimestamp
                          --,p_delay         => 
                         ,p_comment => vr_message.commenttxt
                         ,p_isquery => v_isquery);
  exception
    when others then
      o_ErrorCode := abs(sqlcode);
      o_ErrorDesc := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
  end;

  -- процедура дублирования сообщения в очередь для отправки header в KAFKA после выгрузки боди в S3 ( костыль так как адаптер S3 не хочет отправлять хедеры в KAFKA )
  procedure S3_load_header_msg_KAFKA(p_msgid itt_q_message_log.msgid%type -- GUID сообщения
                                     ) as
    v_ErrorCode integer;
    v_ErrorDesc varchar2(2000);
  begin
    S3_add_msg_KAFKA(p_msgid => p_msgid, o_ErrorCode => v_ErrorCode, o_ErrorDesc => v_ErrorDesc);
    if v_ErrorCode != 0
    then
      qmanager_read_msg_error(p_kafka_topic => null, p_msgID => p_msgid, p_ErrorCode => v_ErrorCode, p_ErrorDesc => v_ErrorDesc);
    end if;
  end;

begin
  declare
    v_worker_load_percent pls_integer := it_q_manager.get_worker_load_percent;
  begin
    G_THREAD_COUNT := greatest(round(abs(nvl(it_q_message.get_qset_number(p_qset_name => 'KAFKA_TOPIC_THREADS'), 12)) / 100 * case
                                       when v_worker_load_percent > 80 then
                                        10
                                       when v_worker_load_percent > 60 then
                                        20
                                       when v_worker_load_percent > 40 then
                                        50
                                       else
                                        100
                                     end)
                              ,1);
  end;
end it_kafka;
/

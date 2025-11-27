create or replace package body it_q_service is

  /**************************************************************************************************\
    BIQ-9225. Разработка очереди и журнала событий
              Пакет с AQ сервисами
    **************************************************************************************************
    Изменения:
    ---------------------------------------------------------------------------------------------------
    Дата        Автор            Jira                             Описание 
    ----------  ---------------  ------------------------------   -------------------------------------
    26.09.2024  Зыков М.В.       BOSS-1585                        Сервис ExecuteCode
    23.10.2023  Зыков М.В.       BOSS-1230                        BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
    03.09.2022  Зыков М.В.       BIQ-9225                         Создание
  \**************************************************************************************************/


  -- Тестовый обработчик № 1
  procedure test1(p_worklogid integer
                 ,p_messbody  clob
                 ,p_messmeta  xmltype
                 ,o_msgid     out varchar2
                 ,o_MSGCode   out integer
                 ,o_MSGText   out varchar2
                 ,o_messbody  out clob
                 ,o_messmeta  out xmltype) is
    v_messbody varchar2(2000);
    v_sleep    number := 0;
  begin
    if dbms_lob.getlength(p_messbody) > 0
    then
      v_messbody := trim(dbms_lob.substr(p_messbody, 100));
    end if;
    case
      when substr(upper(v_messbody), 1, 9) = upper('Здавствуй')
           or substr(upper(v_messbody), 1, 6) = upper('Здоров') then
        o_messbody := 'Привет !';
      when substr(upper(v_messbody), 1, 6) = upper('Привет') then
        o_messbody := 'Здравствуйте';
      when v_messbody is null then
        o_messbody := 'Ожидается приветствие';
      else
        o_MSGCode := 1;
        o_MSGText := 'Ожидалось приветствие  а получено :"' || v_messbody || '"';
        --raise_application_error(-20001, 'Ожидалось приветствие  а получено :"' || v_messbody || '"');
    end case;
    begin
      select nvl(to_number(EXTRACTVALUE(p_messmeta, '*/Sleep')), 0) into v_sleep from dual;
      dbms_lock.sleep(v_sleep);
    exception
      when others then
        null;
    end;
    select xmlelement("OutXMLMeta", xmlelement("InXMLMeta", p_messmeta), xmlelement("Answer", o_messbody)) into o_messmeta from dual;
  end;

  -- обработчик для очистки дублирующих сообщений об ошибках в мониторинге
  procedure MONITOR_Erase_SPAMError(p_worklogid integer
                                   ,p_messbody  clob
                                   ,p_messmeta  xmltype
                                   ,o_msgid     out varchar2
                                   ,o_MSGCode   out integer
                                   ,o_MSGText   out varchar2
                                   ,o_messbody  out clob
                                   ,o_messmeta  out xmltype) is
    v_period        integer ; -- Глубина проверки сообщений (сек)
    vr_message_lin  itt_q_message_log%rowtype;
    vr_message_lout itt_q_message_log%rowtype;
    v_SystemId      itt_event_log.systemid%type;
    v_ServiceName   varchar2(128);
    v_ErrorCode     integer;
    v_res           integer;
    v_eventMeta     xmltype;
    vr_message      itt_q_message_log%rowtype;
  begin
     v_period := nvl(it_rs_interface.get_parm_number_path(it_event_utils.GC_PARAM_ANTI_SPAM), 600);

    vr_message_lin  := it_q_message.messlog_get(p_logid => p_worklogid);
    vr_message_lout := it_q_message.messlog_get(p_msgid => vr_message_lin.msgid, p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT);
    --
    if vr_message_lout.log_id is not null
    then
      select 1 into v_res from itt_event_log l where l.log_id = vr_message_lout.log_id for update nowait;
      vr_message  := vr_message_lout;
      v_eventMeta := xmltype(vr_message_lout.messmeta);
    else
      select 1 into v_res from itt_event_log l where l.log_id = vr_message_lin.log_id for update nowait;
      vr_message  := vr_message_lin;
      v_eventMeta := xmltype(vr_message_lin.messmeta);
    end if;
    --
    select to_number(extractValue(v_eventMeta, 'Event/MsgMETA/XML/@ErrorCode')) into v_ErrorCode from dual;
    update itt_event_log l
       set l.info_msgid = l.msgid
     where l.log_id in (select vr_message.log_id
                          from (select ml.log_id
                                      ,to_number(extractValue(xmltype(ml.messmeta), 'Event/MsgMETA/XML/@ErrorCode')) as ErrorCode
                                  from itt_event_log lout
                                  join itt_event_log el
                                    on lout.systemid = el.systemid
                                   and el.create_sysdate >= lout.create_sysdate - numtodsinterval(v_period, 'SECOND')
                                   and (el.info_msgid is null or el.msgid != el.info_msgid)
                                   and el.log_id != vr_message.log_id
                                  join itt_q_message_log ml
                                    on el.log_id = ml.log_id
                                   and ml.servicename = vr_message.servicename
                                 where lout.log_id = vr_message.log_id)
                         where ErrorCode = v_ErrorCode);
  end;


  -- Сервис для выполнения кода воркерами. Код должен иметь 2 параметра p_worklogid ,p_messmeta ;

  procedure ExecuteCode(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype) is
  begin
   execute immediate p_messbody using p_worklogid ,p_messmeta ;
  end;



end it_q_service;
/

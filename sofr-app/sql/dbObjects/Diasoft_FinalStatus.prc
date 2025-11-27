create or replace procedure Diasoft_FinalStatus(p_messbody clob
                                               ,o_msgid    out varchar2
                                               ,o_MSGCode  out integer
                                               ,o_MSGText  out varchar2
                                               ,o_messbody out clob) is
  --XML
  v_guid                     varchar2(100);
  v_requesttime              varchar2(100);
  v_operationstatuscomment   varchar2(100);
  v_operationexecutionstatus varchar2(100);
  v_sofroperationid          varchar2(100);
  v_custodyorderid           varchar2(100);
  v_finalstatusdate          varchar2(100);
  --Local
  --v_requesttimets               timestamp;
  v_finalstatusdatedt           date;
  v_operationexecutionstatusint number(2);
  v_sofrid                      number(10);
  v_log_message                 varchar2(1000);
  v_description                 varchar2(300) := '';
  -- отправка
  v_msgID     itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
  vx_MESSBODY xmltype;
  ResultCode  number;
  -- логика
  cnt         integer;
  v_xml_in    xmltype;
  v_namespace varchar2(128):= it_kafka.get_namespace(it_diasoft.C_C_SYSTEM_NAME,'SendPkoStatusResultReq');

begin
  v_xml_in := it_xml.Clob_to_xml(p_messbody);
  with topic_mess as
   (select v_xml_in xml from dual)
  select extractValue(t.xml,'SendPkoStatusResultReq/GUID',v_namespace)
        ,extractValue(t.xml,'SendPkoStatusResultReq/RequestTime',v_namespace)
        ,extractValue(t.xml,'SendPkoStatusResultReq/FinalStatusDate',v_namespace)
        ,extractValue(t.xml,'SendPkoStatusResultReq/OperationStatusComment',v_namespace)
        ,extractValue(t.xml,'SendPkoStatusResultReq/OperationExecutionStatus',v_namespace)
        ,extractValue(t.xml,'SendPkoStatusResultReq/SofrOperationId/ObjectId',v_namespace)
        ,extractValue(t.xml,'SendPkoStatusResultReq/CustodyOrderId/ObjectId',v_namespace)
    into v_guid
        ,v_requesttime
        ,v_finalstatusdate
        ,v_operationstatuscomment
        ,v_operationexecutionstatus
        ,v_sofroperationid
        ,v_custodyorderid
    from topic_mess t;
-- TODO: временная зона 
  --v_requesttimets               := to_timestamp(substr(v_requesttime, 1, 19), 'yyyy-mm-dd"T"hh24:mi:ss');
  v_finalstatusdatedt           := to_date(substr(v_finalstatusdate, 1, 10), 'yyyy-mm-dd');
  v_operationexecutionstatusint := to_number(v_operationexecutionstatus);
  v_sofrid                      := to_number(v_sofroperationid);
  select count(*)
    into cnt
    from ddl_tick_dbt t
   where (t.t_dealid = v_sofrid
      or t.t_dealcodets = v_custodyorderid) 
      and t.t_bofficekind = 127;
  if (cnt = 0)
  then
    ResultCode := 20;
    o_MSGText  := 'операция в СОФР не найдена'; --- ?
  else
    ResultCode := 0;
    o_MSGText  := '';
    if (v_sofrid = 0) then
      -- надо найти код операции, так как Diasoft не отправил его, хотя операция нашлась
      select t.t_dealid into v_sofrid
        from ddl_tick_dbt t
        where t.t_dealcodets = v_custodyorderid 
          and t.t_bofficekind = 127
          fetch first row only;
    end if;  
  end if;
 -- Ответ 
  select xmlelement("SendPkoStatusResultResp"
                    ,xmlelement("GUID", v_msgID)
                    ,xmlelement("GUIDReq", v_guid)
                    ,xmlelement("RequestTime", 
                      it_xml.date_to_char_iso8601(sysdate))
                    ,xmlelement("SofrOperationId", 
                      xmlelement("ObjectId", v_sofrid))
                    ,xmlelement("CustodyOrderId", 
                      xmlelement("ObjectId", v_custodyorderid))
                    ,xmlelement("ErrorList", 
                          xmlelement("Error", xmlelement("ErrorCode", ResultCode), 
                                              xmlelement("ErrorDesc", o_MSGText))
                                ))
    into vx_MESSBODY
    from dual;
  o_msgid    := v_msgID;
  o_MSGCode  := ResultCode;
  o_messbody := vx_MESSBODY.getClobVal;

  it_log.log(p_msg => 'отправка ответа на запрос финального статуса', p_msg_clob => o_messbody);


  if (cnt = 1)
  then
    for rec in (select t.t_dealdate
                      ,t.t_dealcodets
                      ,t.t_dealcode
                      ,d.t_shortname
                      ,o.t_code
                      ,f.t_fi_code
                      ,a.t_isin
                      ,l.t_principal
                      ,t.t_dealstatus
                  from ddl_tick_dbt  t
                      ,dparty_dbt    d
                      ,dobjcode_dbt  o
                      ,dfininstr_dbt f
                      ,davoiriss_dbt a
                      ,ddl_leg_dbt   l
                 where 
                   (t.t_dealid = v_sofrid or t.t_dealcodets = v_custodyorderid)
                     and t.t_bofficekind = 127
                   and d.t_partyid = t.t_clientid
                   and o.t_objectid = d.t_partyid
                   and o.t_codekind = 101
                   and o.t_objecttype = 3
                   and o.t_state = 0
                   and f.t_fiid = t.t_pfi
                   and a.t_fiid = f.t_fiid
                   and l.t_dealid = t.t_dealid)
    loop
        if (v_operationexecutionstatusint in (0) and rec.t_dealstatus in (0))
        then
          v_description := 'Поручение отменено клиентом. Необходимо в СОФР удалить операцию.';
        elsif (v_operationexecutionstatusint in (0) and rec.t_dealstatus in (10))
        then
          v_description := 'Поручение отменено клиентом. Необходимо в СОФР перевести операцию в отложенные и удалить операцию.';
        elsif (v_operationexecutionstatusint in (0) and rec.t_dealstatus in (20))
        then
          v_description := 'Поручение отменено клиентом. Необходимо провести корректировочные действия в СОФР.';
        elsif (v_operationexecutionstatusint in (1) and rec.t_dealstatus in (0))
        then
          v_description := 'Поручение не исполнено вышестоящим депозитарием. Необходимо в СОФР удалить операцию.';
        elsif (v_operationexecutionstatusint in (1) and rec.t_dealstatus in (10))
        then
          v_description := 'Поручение не исполнено вышестоящим депозитарием. Необходимо в СОФР перевести операцию в отложенные и удалить операцию.';
        elsif (v_operationexecutionstatusint in (1) and rec.t_dealstatus in (20))
        then
          v_description := 'Поручение не исполнено вышестоящим депозитарием. Необходимо провести корректировочные действия в СОФР.';
        elsif (v_operationexecutionstatusint in (2) and rec.t_dealstatus in (0) 
          and v_finalstatusdatedt = trunc(rec.t_dealdate))
        then
          v_description := 'Поручение исполнено в дату ' || to_char(v_finalstatusdatedt, 'dd.mm.yyyy') ||
                           '. Необходимо в СОФР  исполнить операцию.';
        elsif (v_operationexecutionstatusint in (2) and rec.t_dealstatus in (0) 
          and v_finalstatusdatedt <> trunc(rec.t_dealdate))
        then
          v_description := 'Поручение исполнено в дату ' || to_char(v_finalstatusdatedt, 'dd.mm.yyyy') ||
                           '. Необходимо в СОФР  изменить дату операции и исполнить.';
        elsif (v_operationexecutionstatusint in (2) and rec.t_dealstatus in (10) 
          and v_finalstatusdatedt = trunc(rec.t_dealdate))
        then
          v_description := 'Поручение исполнено в дату ' || to_char(v_finalstatusdatedt, 'dd.mm.yyyy') ||
                           '. Необходимо в СОФР исполнить операцию.';
        elsif (v_operationexecutionstatusint in (2) and rec.t_dealstatus in (10) 
          and v_finalstatusdatedt <> trunc(rec.t_dealdate))
        then
          v_description := 'Поручение исполнено в дату ' || to_char(v_finalstatusdatedt, 'dd.mm.yyyy') ||
                           '. Необходимо в СОФР перевести операцию в отложенные, изменить дату и заново исполнить.';
        elsif (v_operationexecutionstatusint in (2) and rec.t_dealstatus in (20) 
          and v_finalstatusdatedt = trunc(rec.t_dealdate))
        then
          v_description := 'Поручение исполнено в Депозитарии в дату ' || to_char(v_finalstatusdatedt, 'dd.mm.yyyy') ||
                           '. В СОФР не требуется никаких действий.';
        elsif (v_operationexecutionstatusint in (2) and rec.t_dealstatus in (20) 
          and v_finalstatusdatedt <> trunc(rec.t_dealdate))
        then
          v_description := 'Поручение исполнено в дату ' || to_char(v_finalstatusdatedt, 'dd.mm.yyyy') ||
                           '. Необходимо провести корректировочные действия в СОФР.';
        end if;
        v_log_message := 'Получено сообщение из Диасофт об операции списания ц/б N' || rec.t_dealcode || ' (' || rec.t_dealcodets || ') от ' ||
                         to_char(rec.t_dealdate, 'dd.mm.yyyy') || ' по клиенту ' || rec.t_shortname || ' (' || rec.t_code || ') по ц/б ' ||
                         rec.t_fi_code || ' (' || rec.t_isin || ') в количестве ' || to_char(rec.t_principal) || ' штук. ' || v_description;
        rsb_payments_api.InsertEmailNotify(76
                                          ,'Исполнение списания ц/б '||rec.t_dealcode
                                          ,v_log_message);
    end loop;
  end if;
  if (ResultCode<>0) then
          v_log_message := 'Возникла ошибка при обработке финального статуса поручения на списание/зачисление ц/б '
          ||' из Диасофт в СОФР, id поручения в Диасофт '||
            v_custodyorderid||' : код ошибки '||ResultCode||' ('||o_MSGText||')';
     /*rsb_payments_api.InsertEmailNotify(77
    ,'СОФР. BIQ-13034. Ошибка при обработке финального статуса ПКО'
    ,v_log_message);*/
    it_diasoft.SendErrorEvent(p_ErrorCode => ResultCode
                             ,p_Head => 'Ошибка при обработке финального статуса ПКО'
                             ,p_Text => v_log_message);
          
  end if;

end;
/

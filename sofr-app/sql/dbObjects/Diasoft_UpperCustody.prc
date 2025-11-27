create or replace procedure Diasoft_UpperCustody(p_messbody clob
                                                ,o_msgid    out varchar2
                                                ,o_MSGCode  out integer
                                                ,o_MSGText  out varchar2
                                                ,o_messbody out clob) is
  --XML
  v_guid varchar2(100);
  --v_requesttime              varchar2(100);
  v_operationstatuscomment   varchar2(100);
  v_operationexecutionstatus varchar2(100);
  v_sofroperationid          varchar2(100);
  v_custodyorderid           varchar2(100);
  v_finalstatusdate          varchar2(100);
  --Local
  v_requesttimets               timestamp;
  v_finalstatusdatedt           date;
  v_operationexecutionstatusint number(2);
  v_sofrid                      number(10);
  --v_log_message                 varchar2(1000);
  --v_description                 varchar2(300) := '';
  -- отправка
  v_msgID     itt_q_message_log.msgid%type := it_q_message.get_sys_guid;
  vx_MESSBODY xmltype;
  v_ErrorCode number;
  v_ErrorDesc varchar2(32000);
  -- логика
  v_xml_in xmltype;
  -- TODO как будет организован вызов?
  v_namespace      varchar2(128) := it_kafka.get_namespace(it_diasoft.C_C_SYSTEM_NAME, 'SendPkoStatusResultReq');
  v_clientid       ddl_tick_dbt.t_clientid%type; --идентификатор клиента
  v_clientcontrid  ddl_tick_dbt.t_clientcontrid%type; ---Идентфикатор субдоговора
  v_pfi            ddl_leg_dbt.t_pfi %type; -- Идентификатор ценной бумаги
  v_principal      ddl_leg_dbt.t_principal %type; -- Количество ценных бумаг
  v_id_operation   doproper_dbt.t_id_operation %type; -- Идентификатор операции  
  v_marketid       ddlcontrmp_dbt.t_marketid%type;
  v_ClientCode     ddlcontrmp_dbt.t_mpcode%type;
  v_dlcontrid      ddlcontrmp_dbt.t_dlcontrid%type;
  v_PKO_opertype   pko_writeoff.opertype%type;
  v_pko_iscanceled pko_writeoff.iscanceled%type;
  vb_check         boolean;
  v_for_plan1      number;
  v_for_plan2      number;
  v_dealid         ddl_tick_dbt.t_dealid%type;
  v_dealcodets     ddl_tick_dbt.t_dealcodets%type;
  v_dealtype       ddl_tick_dbt.t_dealtype%type;
  procedure SendPkoStatusResultResp as
  begin
    it_diasoft.SendErrorEvent(p_ErrorCode => o_MSGCode
                             ,p_Head => 'Ошибка при обработке финального статуса ПКО'
                             ,p_Text => 'Возникла ошибка при автоматической обработке финального статуса поручения на списание/зачисление ц/б из Диасофт в СОФР, id поручения в Диасофт :' ||
                                        v_custodyorderid || ' ID сделки в СОФР:' || v_sofroperationid || ' код ошибки ' || o_MSGCode || ' (' || o_MSGText ||
                                        '). Необходимо исправить ошибку и выполнить дальнейшие шаги операции вручную.'
                             ,p_monitoring => true);
    -- Ответ 
    select xmlelement("SendPkoStatusResultResp"
                      ,xmlelement("GUID", v_msgID)
                      ,xmlelement("GUIDReq", v_guid)
                      ,xmlelement("RequestTime", it_xml.timestamp_to_char_iso8601(systimestamp))
                      ,xmlelement("SofrOperationId", xmlelement("ObjectId", v_sofrid))
                      ,xmlelement("CustodyOrderId", xmlelement("ObjectId", v_custodyorderid))
                      ,xmlelement("ErrorList", xmlelement("Error", xmlelement("ErrorCode", o_MSGCode), xmlelement("ErrorDesc", o_MSGText))))
      into vx_MESSBODY
      from dual;
    o_msgid    := v_msgID;
    o_messbody := vx_MESSBODY.getClobVal;
  end;

begin
  v_xml_in := it_xml.Clob_to_xml(p_messbody);
  with topic_mess as
   (select v_xml_in xml from dual)
  select extractValue(t.xml, 'SendPkoStatusResultReq/GUID', v_namespace)
        ,it_xml.char_to_timestamp(extractValue(t.xml, 'SendPkoStatusResultReq/RequestTime', v_namespace))
        ,extractValue(t.xml, 'SendPkoStatusResultReq/FinalStatusDate', v_namespace)
        ,extractValue(t.xml, 'SendPkoStatusResultReq/OperationStatusComment', v_namespace)
        ,extractValue(t.xml, 'SendPkoStatusResultReq/OperationExecutionStatus', v_namespace)
        ,extractValue(t.xml, 'SendPkoStatusResultReq/SofrOperationId/ObjectId', v_namespace)
        ,extractValue(t.xml, 'SendPkoStatusResultReq/CustodyOrderId/ObjectId', v_namespace)
    into v_guid
        ,v_requesttimets -- ,v_requesttime
        ,v_finalstatusdate
        ,v_operationstatuscomment
        ,v_operationexecutionstatus
        ,v_sofroperationid
        ,v_custodyorderid
    from topic_mess t;
-- TODO: временная зона 
  -- v_requesttimets               := to_timestamp(substr(v_requesttime, 1, 19), 'yyyy-mm-dd"T"hh24:mi:ss');
  v_finalstatusdatedt           := to_date(substr(v_finalstatusdate, 1, 10), 'yyyy-mm-dd');
  v_operationexecutionstatusint := to_number(v_operationexecutionstatus default null on conversion error);
  v_sofrid                      := to_number(v_sofroperationid default 0 on conversion error);
  o_MSGCode                     := 0;
  o_MSGText                     := '';
  if nvl(v_operationexecutionstatusint, -1) not in (0, 1, 2)
  then
    o_MSGCode := it_q_manager.C_N_ERROR_OTHERS_MSGCODE;
    o_MSGText := it_q_manager.C_C_ERROR_OTHERS_MSGTEXT || ': ' || 'Тег OperationExecutionStatus =' || v_operationexecutionstatus;
  elsif v_finalstatusdatedt is null
  then
    o_MSGCode := it_q_manager.C_N_ERROR_OTHERS_MSGCODE;
    o_MSGText := it_q_manager.C_C_ERROR_OTHERS_MSGTEXT || ': ' || 'Тег FinalStatusDate =' || v_finalstatusdate;
  else
    begin
      select t.t_dealid
        into v_dealid
        from ddl_tick_dbt t
       where t.t_dealcodets = v_custodyorderid
         and t.t_bofficekind = 127;
      v_dealcodets := v_custodyorderid;
    exception
      when no_data_found then
        begin
          select t.t_dealcodets
            into v_dealcodets
            from ddl_tick_dbt t
           where t.t_dealid = v_sofrid
             and t.t_bofficekind = 127;
          v_dealid := v_sofrid;
        exception
          when no_data_found then
            o_MSGCode := 20;
        end;
    end;
    if o_MSGCode = 0
       and (v_dealcodets != v_custodyorderid or v_dealid != v_sofrid)
    then
      o_MSGCode := 28;
    end if;
  end if;
  if o_MSGCode = 0
  then
    v_dealid := -1;
    for rec in (select *
                  from pko_writeoff pko
                 where pko.custodyorderid = v_custodyorderid
                   and nvl(pko.foundCustodyOrderId, chr(0)) != chr(88)
                 order by pko.dealid)
    loop
      v_dealid := rec.dealid;
      exit when v_dealid = v_sofrid;
    end loop;
    if v_dealid = -1
    then
      o_MSGCode := 20;
    elsif v_dealid is null
    then
      o_MSGCode := 29;
    elsif v_dealid != v_sofrid
    then
      o_MSGCode := 28;
    end if;
  end if;
  if o_MSGCode = 0
  then
    for rec in (select t.t_dealdate
                      ,t.t_dealcodets
                      ,t.t_dealcode
                      ,t.t_dealtype
                      ,d.t_shortname
                      ,o.t_code
                      ,f.t_fi_code
                      ,a.t_isin
                      ,l.t_principal
                      ,t.t_dealstatus
                      ,atcor.t_attrid
                  from dparty_dbt    d
                      ,dobjcode_dbt  o
                      ,dfininstr_dbt f
                      ,davoiriss_dbt a
                      ,ddl_leg_dbt   l
                      ,ddl_tick_dbt  t
                  left join dobjatcor_dbt AtCor
                    on AtCor.t_ObjectType = 101
                   and AtCor.t_GroupID = 213 -- Категория "Отказ в проведении операции"
                   and AtCor.t_Object = LPAD(t.t_dealid, 34, '0')
                   and AtCor.t_General = chr(88)
                   and AtCor.t_ValidToDate >= sysdate
                   and AtCor.t_ValidFromDate <= sysdate
                 where t.t_dealid = v_sofrid
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
      if rec.t_dealtype = 2010
      then
        -- Предыдущая версия 
        Diasoft_FinalStatus(p_messbody => p_messbody, o_msgid => o_msgid, o_MSGCode => o_MSGCode, o_MSGText => o_MSGText, o_messbody => o_messbody);
        return;
      end if;
      if v_operationexecutionstatusint in (0, 1)
         and rec.t_attrid = 2
      then
        o_MSGCode := 22;
      elsif v_operationexecutionstatusint in (2)
            and rec.t_attrid = 1
      then
        o_MSGCode := 23;
      elsif nvl(rec.t_attrid, -1) not in (1, 2)
      then
        if rec.t_dealstatus not in (20) --in (0,1))  -- операция в статусе "Открыта/отложена"
        then
          if it_diasoft.get_DealRecv(p_deailid => v_sofrid
                                    ,o_dealtype => v_dealtype
                                    ,o_clientid => v_clientid
                                    ,o_clientcontrid => v_clientcontrid
                                    ,o_pfi => v_pfi
                                    ,o_PKO_opertype => v_PKO_opertype
                                    ,o_principal => v_principal
                                    ,o_id_operation => v_id_operation
                                    ,o_marketid => v_marketid
                                    ,o_ClientCode => v_ClientCode
                                    ,o_dlcontrid => v_dlcontrid) != 0
          then
            --Не заполняем CompletionTimestamp и CancelationTimestamp, если недостаточное количество ц/б для списания 
            --и статус %OperationExecutionStatus =2, оставляем операцию в начальном статусе Открыта/отложена. 
            --Очищаем категорию "Отказ в проведении операции".
            -- пока ставим 1 - Технический пользователь для установки и миграции 
            RsbSessionData.SetOper(1);
            --   нужно для Rsb_Secur.SetDealAttrID
            --   либо вынесем в diasoft_pko_funcobj_creator -> Diasoft.FinalStatus_Close
            vb_check := true;
            if v_operationexecutionstatusint in (2)
               and v_PKO_opertype = 2
            then
              it_diasoft.Get_forPLAN(p_pfi => v_pfi
                                    ,p_clientid => v_clientid
                                    ,p_clientcontrid => v_clientcontrid
                                    ,p_dayCalc => trunc(sysdate)
                                    ,o_for_plan1 => v_for_plan1
                                    ,o_for_plan2 => v_for_plan2);
              vb_check := it_diasoft.get_RestFI(p_pfi => v_pfi, p_clientid => v_clientid, p_clientcontrid => v_clientcontrid, p_dayCalc => trunc(sysdate)) + v_for_plan2 >=
                          v_principal;
            end if;
            if vb_check
            then
              it_diasoft.Set_DealDate(p_deailid => v_sofrid, p_finalstatusdate => v_finalstatusdatedt, o_ErrorCode => v_ErrorCode, o_ErrorDesc => v_ErrorDesc);
              if v_ErrorCode != 0
              then
                o_MSGCode := it_q_manager.C_N_ERROR_OTHERS_MSGCODE;
                o_MSGText := it_q_manager.C_C_ERROR_OTHERS_MSGTEXT || ': ' || trim(v_ErrorDesc);
              else
                if (v_operationexecutionstatusint in (2))
                then
                  -- отметки в Картотеке00
                  update pko_writeoff pko
                     set pko.iscompleted         = chr(88)
                        ,pko.completiontimestamp = nvl(completiontimestamp, v_requesttimets)
                        ,step1_waitstatus        = 1
                        ,step2_reject            = 0
                        ,step3_writeoff          = 1
                   where pko.dealid = v_sofrid;
                  -- заполняем категорию "Отказ в проведении операции" в значение "Нет"
                  Rsb_Secur.SetDealAttrID(v_sofrid, sysdate, 2 /*Нет*/, 213);
                elsif (v_operationexecutionstatusint in (0, 1))
                then
                  -- отметки в Картотеке
                  update pko_writeoff pko
                     set pko.iscanceled           = chr(88)
                        ,pko.cancelationtimestamp = nvl(cancelationtimestamp, v_requesttimets)
                        ,step1_waitstatus         = 1
                        ,step2_reject             = 1
                        ,step3_writeoff           = 0
                   where pko.dealid = v_sofrid;
                  -- заполняем категорию "Отказ в проведении операции" в значение "Да"
                  Rsb_Secur.SetDealAttrID(v_sofrid, sysdate, 1 /*Да*/, 213);
                end if;
                -- выполняем шаг "Ожидание статуса обработки" в Diasoft.FinalStatus_Close
                diasoft_pko_funcobj_creator(4, v_guid, v_sofrid, v_ErrorCode, v_ErrorDesc, '');
                if v_ErrorCode != 0
                then
                  o_MSGCode := it_q_manager.C_N_ERROR_OTHERS_MSGCODE;
                  o_MSGText := it_q_manager.C_C_ERROR_OTHERS_MSGTEXT || ': ' || trim(v_ErrorDesc);
                end if;
              end if;
            else
              Rsb_Secur.SetDealAttrID(v_sofrid, sysdate, -1, 213);
            end if;
          else
            o_MSGCode := 20;
          end if;
        else
          -- операция в статусе "Закрыта"
          begin
            select p.iscanceled into v_pko_iscanceled from pko_writeoff p where p.dealid = v_sofrid;
          exception
            when no_data_found then
              o_MSGCode := 20;
          end;
          if o_MSGCode = 0
          then
            if v_operationexecutionstatusint in (0, 1)
               and nvl(v_pko_iscanceled, chr(0)) != chr(88)
            then
              o_MSGCode := 27;
            elsif v_operationexecutionstatusint in (2)
            then
              o_MSGCode := 26;
            end if;
          end if;
        end if;
      end if;
      exit;
    end loop;
  end if;
  case o_MSGCode
    when 0 then
      o_MSGText := '';
    when 20 then
      o_MSGText := 'операция в СОФР не найдена';
    when 22 then
      o_MSGText := 'Отказ вышестоящего депозитария не обработан - списание ц/б в СОФР уже произошло, операция закрыта';
    when 23 then
      o_MSGText := 'Разрешение списания от вышестоящего депозитария не обработано - в СОФР ранее был отказ в списании';
    when 26 then
      o_MSGText := 'Разрешение списания от вышестоящего депозитария не обработано - Операция закрыта';
    when 27 then
      o_MSGText := 'Отказ вышестоящего депозитария не обработан - Операция закрыта ';
    when 28 then
      o_MSGText := 'Не совпадает один из идентификаторов операции в СОФР или поручения в Диасофт между исходным и конечным сообщением ';
    when 29 then
      o_MSGText := 'Сделка не найдена';
    else
      null;
  end case;
  SendPkoStatusResultResp;
  commit;
exception
  when others then
    rollback;
    o_MSGCode := 20000;
    o_MSGText := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'exception ' || v_guid, p_msg_type => it_log.C_MSG_TYPE__ERROR);
    it_error.clear_error_stack;
    SendPkoStatusResultResp;
    commit;
end;
/

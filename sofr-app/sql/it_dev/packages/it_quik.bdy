create or replace package body it_quik is
 /***************************************************************************************************\
    Пакет для работы QManagera c QUIK
   **************************************************************************************************
    Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------
   23.10.2023  Зыков М.В.       BOSS-1230                     BIQ-15498.BOSS-1230 Доработка QManager для передачи сообщений в Кафку
  \**************************************************************************************************/

  -- Упаковщик исходящх сообшений в QIUK через KAFKA
  procedure out_pack_message(p_message     it_q_message_t
                            ,p_expire      date
                            ,o_correlation out varchar2
                            ,o_messbody    out clob
                            ,o_messmeta    out xmltype) as
    v_rootElement   itt_kafka_topic.rootelement%type;
    vj_in_messbody  clob;
    vj_out_messbody clob;
    vz_GUID         itt_q_message_log.msgid%type;
    vz_GUIDReq      itt_q_message_log.corrmsgid%type;
    vz_ErrorCode    itt_q_message_log.msgcode%type;
    vz_ErrorDesc    itt_q_message_log.msgtext%type;
    v_select        varchar2(2000);
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
                               ,'Сервис ' || p_message.ServiceName || ' не зарегистрирован как ' || it_q_message.C_C_QUEUE_TYPE_OUT || ' для KAFKA/' ||
                                C_C_SYSTEM_NAME);
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
                               ,'Для сервиса ' || p_message.ServiceName || chr(10) || ' зарегистрированого как ' || it_q_message.C_C_QUEUE_TYPE_OUT ||
                                ' для KAFKA/' || C_C_SYSTEM_NAME || chr(10) || ' ожидался RootElement ' || v_rootElement || ' (ошибка формата JSON)');
      elsif p_message.msgid != vz_GUID
      then
        raise_application_error(-20001, 'Значение Msgid != GUID ! ');
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
           and (vz_ErrorDesc is null or vz_ErrorDesc != p_message.MSGText)
        then
          raise_application_error(-20001, 'Значение ErrorDesc должно быть "' || p_message.MSGText || '"! ');
        end if;
      end if;
    else
      if p_message.message_type = it_q_message.C_C_MSG_TYPE_A
         and p_message.MSGCode != 0
      then
        select json_object(v_rootElement value
                           json_object('GUID' value p_message.msgid
                                      ,'GUIDReq' value p_message.CORRmsgid
                                      ,'RequestTime' value p_message.RequestDT
                                      ,'ErrorList' value json_object('Error' value json_array(json_object('ErrorCode' value to_char(p_message.MSGCode))
                                                             ,json_object('ErrorDesc' value p_message.MSGText)))) FORMAT JSON)
          into vj_out_messbody
          from dual;
      else
        raise_application_error(-20001, 'Отправляемое сообщеие не должно быть пустое ! ');
      end if;
    end if;
    o_messbody := vj_out_messbody;
    o_correlation := it_kafka.get_correlation(p_Receiver => C_C_SYSTEM_NAME , p_len_MessBODY => dbms_lob.getlength(o_messbody));
  end;
  
  procedure send_mail (
    p_id_group integer,
    p_text     varchar2
  ) is
  begin
    rsb_payments_api.InsertEmailNotify(p_EmailGroup => p_id_group,
                                       p_Head       => 'Ошибки при обработке поручения на изменение лимитов в QUIK',
                                       p_Text       => p_text);
  end send_mail;
  
  function get_nptxop_by_msg_id (
    p_msg_id quik_sent_order_messages.msg_id%type
  ) return dnptxop_dbt%rowtype is
    l_nptxop dnptxop_dbt%rowtype;
  begin
    select op.*
      into l_nptxop
      from dnptxop_dbt op
      join quik_sent_order_messages m on m.operation_id = op.t_id
     where m.msg_id = p_msg_id;

    return l_nptxop;
  exception
    when no_data_found then
      return null;
  end get_nptxop_by_msg_id;

  procedure set_response_result (
    p_msg_id      quik_sent_order_messages.msg_id%type,
    p_error_descr varchar2
  ) is
  begin
    update quik_sent_order_messages
       set response_time = systimestamp,
           error_descr = p_error_descr
     where msg_id = p_msg_id;
  end set_response_result;
  
  procedure process_quik_result (
    p_json            varchar2,
    po_error_code out integer,
    po_error_desc out varchar2
  ) is
    l_extid            number(10);
    l_errorcode        integer;
    l_errordescription varchar2(2000);
    l_requestTime      timestamp;
    l_nptxop           dnptxop_dbt%rowtype;
    l_tost             timestamp;
    l_message          varchar2(2000);
    l_sysProv          date;
    l_func_id          number(10);
  begin
    po_error_code := 0;
    po_error_desc := null;
    
     -- Парсинг JSON из входящего параметра p_JSON
    select jt.ID,
           jt.RequestTime,
           jt.ResCode,
           jt.ErrDesc
      into l_extid,
           l_requestTime,
           l_errorcode,
           l_errordescription
    from json_table (p_json, '$'
       columns
        ID varchar2(32) path '$.UpdateQuikLimitsNewInstrMonResp.ExtID',
        RequestTime timestamp path '$.UpdateQuikLimitsNewInstrMonResp.RequestTime',
        ResCode integer path '$.UpdateQuikLimitsNewInstrMonResp.ErrorList.Error[0].ErrorCode',
        ErrDesc varchar2(2000) path '$.UpdateQuikLimitsNewInstrMonResp.ErrorList.Error[0].ErrorDesc'
                ) jt;

    set_response_result(p_msg_id      => l_extid,
                        p_error_descr => case when l_errorcode != 0 then l_errordescription end);
    l_nptxop := get_nptxop_by_msg_id(p_msg_id => l_extid);
    if l_nptxop.t_id is null then
      raise_application_error(-20000, 'Неторговое поручение не найдено в СОФР. msg_id = '||to_char(l_extid));
    end if;

    if l_nptxop.t_subkind_operation = 10 then --зачисления
      if (l_errorcode = 0) then
        it_log.log('ExtID('||l_extid||') Пришел успешный ответ из QUIK' );
      else
        l_message := 'Возникла проблема при обработке поручения на изменение'||
          ' лимитов в QUIK  по операции №' || l_nptxop.t_code ||
          '  от ' || to_char(l_nptxop.t_operdate, 'dd.mm.yyyy') || ' Возможно, требуется изменение лимита вручную. '||
          nvl(l_errordescription, 'ErrorCode#' || l_errorcode);

        send_mail(p_id_group => 78, --ОД
                  p_text     => l_message);
        it_log.log('ExtID('||l_extid||')ErrorCode#'||l_errorcode||' Письмо о проблеме в ОД направлено');
        return;
      end if;

      -- дубль в nptxwrt_func.mac 
      -- ВремяОкончанияРасчетаЛимита
      l_tost := rshb_limit_util.GetDT306Limit_dy_nptxop(l_nptxop.t_id);
      if l_tost is not null then -- Найден расчет 
        -- найдем  Момент формирования проводки (Тпров)
        -- дубль в nptxwrt_func.mac 
        select to_date ( TO_CHAR (prov.t_SystemDate, 'YYYYMMDD')
                || TO_CHAR (prov.t_SystemTime, 'HH24MISS'),
                'YYYYMMDDHH24MISS') into l_sysProv
        from doproper_dbt op 
        inner join doprdocs_dbt priv 
              on op.t_id_operation = priv.t_id_operation 
              and op.t_dockind = 4607 and  op.t_kind_operation = 2037 
              and op.t_documentid =lpad(l_nptxop.t_id,34,'0')
        inner join dacctrn_dbt prov 
              on priv.t_acctrnid = prov.t_acctrnid 
              and prov.t_chapter = 1 and prov.t_State=1;
            
        if (trunc(sysdate)>trunc(l_nptxop.t_operdate)
            and l_errorcode=0
            and l_sysProv <= l_tost
            and trunc(l_requestTime) = trunc(sysdate)
            ) then
          l_message := 'По зачислению д/с '||to_char(l_nptxop.t_id)||' от '||to_char(l_nptxop.t_operdate,'dd.mm.yyyy')
                        ||' сформирована проводка в ' || to_char(l_sysProv,'DD.MM.YYYY HH:24MI:SS') || ', 
                        которая вошла в расчет лимитов на начало дня. При этом увеличение лимитов в QUIK по этой операции 
                        было проведено сегодня около ' || to_char(l_requestTime,'DD.MM.YYYY HH:24MI:SS') 
                        || '. Возможно, произошло задвоение зачисления, 
                        необходимо срочно скорректировать лимиты.';
          send_mail(p_id_group => 78, --ОД
                    p_text     => l_message);
        end if;
      end if;
    end if;

    if (l_nptxop.t_status != 2) and (l_errorcode = 0) then
      l_func_id := funcobj_utils.get_func_id(p_code => nontrading_orders_read.get_funcobj_code);
      funcobj_utils.delete_active_task(p_objectid => l_nptxop.t_id,
                                       p_funcid => l_func_id);
      funcobj_utils.save_task(p_objectid => l_nptxop.t_id,
                              p_funcid   => l_func_id,
                              p_param    => null,
                              p_priority => funcobj_utils.get_priority_from_reserve(p_code => nontrading_orders_read.get_funcobj_code));
    end if;
  exception
    when others then
      rollback;
      send_mail(p_id_group => 1, --админы
                p_text     => sqlerrm || utl_tcp.crlf || sys.dbms_utility.format_error_backtrace);
      raise ;
  end process_quik_result;

  -- BIQ-15498 Отправка сообщения поручения на ввод денежных средств  для автоматизации процесса обработки поручения и  корректировки лимитов по денежным средствам 
  procedure LimitsNewInstrMonReq(p_msgID     in varchar2 -- ID Сообщения,
                                ,p_JSON      in varchar2
                                ,o_ErrorCode out number -- != 0 ошибка o_ErrorDesc
                                ,o_ErrorDesc out varchar2
                                ,p_comment   in varchar2 default null) is
    v_msgID itt_q_message_log.msgid%type := p_msgID;
    v_ServiceName constant itt_q_message_log.servicename%type := 'QUIK.LimitsNewInstrMon';
  begin
    it_kafka.load_msg(io_msgid => v_msgid
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                     ,p_ServiceName => v_ServiceName
                     ,p_Receiver => C_C_SYSTEM_NAME
                     ,p_MESSBODY => p_JSON
                     ,o_ErrorCode => o_ErrorCode
                     ,o_ErrorDesc => o_ErrorDesc
                     ,p_comment => p_comment);
  end;

  -- BIQ-15498 Обработка Ответа  поручения на ввод денежных средств  для автоматизации процесса обработки поручения и  корректировки лимитов по денежным средствам 
  procedure LimitsNewInstrMonResp(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype) is
  begin
    process_quik_result(p_json        => it_xml.Clob_to_varchar2(p_messbody,'Входящее сообщение LimitsNewInstrMonResp слишком большое'),
                        po_error_code => o_MSGCode,
                        po_error_desc => o_MSGText);
  end;

   --BIQ-27551 Отправка сообщения с данными об операции списания ценных бумаг ПКО
  procedure sent_nontrade_secure_limits(p_msgID     in varchar2 
                                       ,p_CORRmsgid in varchar2
                                       ,o_ErrorCode out number -- != 0 ошибка o_ErrorDesc
                                       ,o_ErrorDesc out varchar2) is
    C_SYSTEM_NAME constant varchar2(128) := 'QUIK';                                   
    v_msgID itt_q_message_log.msgid%type := p_msgID;
    v_ServiceName constant itt_q_message_log.servicename%type := 'QUIK.nontrade_securities';
    --поручение
    recPKO_WriteOff PKO_WriteOff%rowtype;
                                                                     
    v_json clob;
    --поля отправляемого JSON
    v_GUID varchar2(100) := p_msgID; --генерится
    v_requestTime varchar2(19) := to_char(systimestamp, 'dd.mm.yyyy hh24:mi:ss'); 
    v_extId varchar2(20); 
    v_clientId varchar2(13);
    v_clientBuyId varchar2(13);
    v_instructionType number := 9; 
    v_agreeServ varchar2(128); 
    v_volume double precision; 
    v_valFee double precision := 0; 
    v_valDepoPay double precision := 0;
    v_valCashPay double precision := 0;
    v_payNumber varchar2(128); 
    v_quikClassCode varchar2(13) := 'TQBR';
    v_outSecCode varchar2(13) := 'RUREQTV';
    v_inSecCode varchar2(13);
    v_quikFirm varchar2(13) := 'MC0134700000';
    v_account varchar2(255);
    v_accountIn varchar2(13);
    v_canSplit number := 0;
    v_blockS double precision;
    v_place varchar2(20); 
    v_depoAccount1 varchar2(255);
    v_depoAccount2 varchar2(255);
    v_depoAccount3 varchar2(255);
    v_depoAccount4 varchar2(255);
    v_putReason varchar2(250);
    v_dealCurr varchar2(15);
    v_dealSum double precision;
    v_payer varchar2(100);
    v_cp varchar2(250);
    v_country varchar2(15);
    v_depoCp varchar2(100);
    v_firmId varchar2(13) := 'MC0134700000';
    v_signFlag number;
    v_phone varchar2(30);
    v_commentText varchar2(250);
    v_instrDepoId varchar2(100);
    v_subDepoCp varchar2(100);
    v_iddocTypeCp varchar2(100);
    v_iddocNumCp varchar2(15);
    v_iddocDateCp number;
    v_iddocIssuerCp varchar2(250);
    v_cpType varchar2(100);
    v_responsible varchar2(100);
    v_scaleVal number;
    v_instrFlags number;
    v_dealDate number;
    v_payDate number;
    v_checkType number;  
    v_docComment varchar2(250);
         
  begin
     o_ErrorCode := 0;
     o_ErrorDesc := null;
    --заполняем поля данными об операции
    begin               
      select x.* into recPKO_WriteOff from PKO_WriteOff x where guid = p_CORRmsgid order by x.id desc fetch first row only;
    
      exception when no_data_found then
        o_ErrorCode := 20000;
        o_ErrorDesc := 'Не найдена запись в pko_writeoff с GUID= '||p_CORRmsgid;
        return; 
    end;
    
    v_extId := recPKO_WriteOff.dealid;
    v_volume := recPKO_WriteOff.qnty;
    v_payNumber := recPKO_WriteOff.dealid;
    v_blockS := v_volume;
    v_place := recPKO_WriteOff.market;
    v_checkType := case when recPKO_WriteOff.pkostatus = 7 then 0 else 1 end;
    v_clientId := sfcontr_read.get_ekk_subcontr(p_sfcontr_id => recPKO_WriteOff.idcontract);
                                                   
     begin                
       select bf.t_number into v_agreeServ from(select c.t_number from dsfcontr_dbt c                 
                                              where c.t_id = recPKO_WriteOff.idcontract) bf;
                                              
        exception when no_data_found then
          v_agreeServ := null;
     end;
    
     v_inSecCode := fininstr_read.get_mmvb_code(p_fiid => recPKO_WriteOff.securityid);
     v_account := sfcontr_read.get_depo_trade_acc_subcontr(p_sfcontr_id => recPKO_WriteOff.idcontract);

     --сборка JSON
     select json_object(
           'UpdateQuikLimitsNewInstrSecReq' value
             json_object(
               'GUID'          value v_GUID,
               'RequestTime'      value v_requestTime,
               'ExtID'            value v_extId,
               'ClientId'         value to_char(v_clientId),      
               'ClientBuyId'      value v_clientBuyId,
               'InstructionType'  value to_char(v_instructionType),
               'AgreeServ'        value v_agreeServ,
               'Volume'           value to_char(v_volume),
               'ValFee'           value to_char(v_valFee),
               'ValDepoPay'       value to_char(v_valDepoPay),
               'ValCashPay'       value to_char(v_valCashPay),
               'PayNumber'        value v_payNumber,
               'QuikClassCode'    value v_quikClassCode,
               'OutSecCode'       value v_outSecCode,
               'InSecCode'        value v_inSecCode,
               'QuikFirm'         value v_quikFirm,
               'Account'          value v_account,
               'AccountIn'        value v_accountIn,
               'CanSplit'         value to_char(v_canSplit),
               'BlockS'           value to_char(v_blockS),
               'Place'            value v_place,
               'DepoAccount1'     value v_depoAccount1,
               'DepoAccount2'     value v_depoAccount2,
               'DepoAccount3'     value v_depoAccount3,
               'DepoAccount4'     value v_depoAccount4,
               'PutReason'        value v_putReason,
               'DealCurr'         value v_dealCurr,
               'DealSum'          value to_char(v_dealSum),
               'Payer'            value v_payer,
               'Cp'               value v_cp,
               'Country'          value v_country,
               'DepoCp'           value v_depoCp,
               'FirmId'           value v_firmId,
               'SignFlag'         value to_char(v_signFlag),
               'Phone'            value v_phone,
               'CommentText'      value v_commentText,
               'InstrDepoId'      value v_instrDepoId,
               'SubDepoCp'        value v_subDepoCp,
               'IddocTypeCp'      value v_iddocTypeCp,
               'IddocNumCp'       value v_iddocNumCp,
               'IddocDateCp'      value to_char(v_iddocDateCp),
               'IddocIssuerCp'    value v_iddocIssuerCp,
               'CpType'           value v_cpType,
               'Responsible'      value v_responsible,
               'ScaleVal'         value to_char(v_scaleVal),
               'InstrFlags'       value to_char(v_instrFlags),
               'DealDate'         value to_char(v_dealDate),
               'PayDate'          value to_char(v_payDate),
               'CheckType'        value to_char(v_checkType),
               'DocComment'       value v_docComment) returning clob)
    into v_json
    from dual;
   begin
      insert into nontrade_secur_messages (msg_id, deal_id, system_name, guid, create_time) 
                       values (quik_sent_order_mess_seq.nextval, 
                               recPKO_WriteOff.dealid, 
                               C_SYSTEM_NAME,
                               p_msgID, 
                               systimestamp);
      exception when others then
            o_ErrorCode := 20000;
            o_ErrorDesc := 'Не удалось сохранить сообщение в таблицу nontrade_secur_messages: '||sqlerrm;
            return;
    end;
    
  --отправка сообщения в QUIK
    it_kafka.load_msg(io_msgid => v_msgid
                     ,p_message_type => it_q_message.C_C_MSG_TYPE_R
                     ,p_ServiceName => v_ServiceName
                     ,p_Receiver => C_C_SYSTEM_NAME
                     ,p_MESSBODY => v_json
                     ,p_CORRmsgid => p_CORRmsgid
                     ,o_ErrorCode => o_ErrorCode
                     ,o_ErrorDesc => o_ErrorDesc);
    --в случае успешной отправки фиксируем время отправки                 
    if o_ErrorCode = 0 then
      begin
         update nontrade_secur_messages
         set request_time = systimestamp
         where deal_id = v_extId and guid = v_GUID;

        if sql%rowcount = 0 then
          o_ErrorCode := 20000;
          o_ErrorDesc := 'Не найдена запись nontrade_secur_messages по deal_id='
                     || v_extId || ' и guid=' || v_GUID;
          return;
        end if; 
      end; 
    end if;
    
    exception when others then
      o_ErrorCode := 20000;
      o_ErrorDesc := sqlerrm;               
 end;


    --BIQ-27551. Обработка ответа о получении данных по операции списания ценных бумаг ПКО и корректировки лимитов по ценным бумагам
    procedure nontrade_secur_limits_listener(p_worklogid integer
                                 ,p_messbody  clob
                                 ,p_messmeta  xmltype
                                 ,o_msgid     out varchar2
                                 ,o_MSGCode   out integer
                                 ,o_MSGText   out varchar2
                                 ,o_messbody  out clob
                                 ,o_messmeta  out xmltype) is 
                                 
    v_json varchar2(4000); --ответный json от квика
                                
    v_extID number; --ID операции(наш deal_id)
    v_guidReq varchar2(200); --гуид сообщения (GUID из нашей таблицы)
    v_resCode integer; --код ошибки в ответе (0 - успех)
    v_errDesc varchar2(2000); --описание ошибки (null - успех)
  
    v_pko_row pko_writeoff%rowtype;
  begin
    o_MSGCode := 0;
    o_MSGText := null;
    v_json := it_xml.Clob_to_varchar2(p_messbody,'Входящее сообщение LimitsNewInstrSecResp слишком большое');
    
    select jt.I_ExtID,
           jt.I_GUIDReq,
           jt.I_ResultCode,
           jt.I_ErrDesc
    into v_extID,
           v_guidReq,
           v_resCode,
           v_errDesc
    from json_table (v_json, '$'
       columns
        I_ExtID varchar2(32) path '$.UpdateQuikLimitsNewInstrSecResp.ExtID',
        I_GUIDReq varchar2 path '$.UpdateQuikLimitsNewInstrSecResp.GUIDReq',
        I_ResultCode integer path '$.UpdateQuikLimitsNewInstrSecResp.ErrorList.Error[0].ErrorCode',
        I_ErrDesc varchar2(2000) path '$.UpdateQuikLimitsNewInstrSecResp.ErrorList.Error[0].ErrorDesc'
                ) jt;
             
    v_pko_row := nontrading_secur_orders_utils.get_pko_row_by_dealid(p_deal_id => v_extID);
                
     -- 2) Обновляем нашу запись сообщения
    update nontrade_secur_messages
    set response_time = systimestamp, result_description = nvl(v_errDesc, 'OK')
    where deal_id = v_extID and guid = v_guidReq;

    if sql%rowcount = 0 then
       o_MSGCode := 20000;
       o_MSGText := 'Не найдена запись nontrade_secur_messages по deal_id='
                     || v_extID || ' и guid=' || v_guidReq;
       return;
    end if;

    -- 3) Запускаем бизнес-обработку
    if nontrading_secur_orders_utils.is_voluntary_redemption_by_row(p_pko_row => v_pko_row) = 1
    then
      if v_resCode = 0
      then
        nontrading_secur_orders_utils.set_is_enough_quantity(p_deal_id => v_extID, p_is_enough_quantity => 1);
        nontrading_secur_orders_utils.set_is_limit_corrected(p_pko_id => v_pko_row.id);
        nontrading_secur_orders_utils.set_wait_status(p_pko_id => v_pko_row.id, p_is_wait => 1);
        it_sinv.send_nontrade_limit_state(p_deal_id => v_extID, p_limit_status => 1);
        nontrading_secur_orders_utils.push_to_execute_deal(p_deal_id => v_pko_row.dealid);
      else
        nontrading_secur_orders_utils.set_is_enough_quantity(p_deal_id => v_extID, p_is_enough_quantity => 0);
        --тут должна быть отправка сообщения в it_sinv о недостаточности лимитов. Но сообщение будет отправлено из макроса diasoft_Pko_CancelExpiredOrders.mac
        nontrading_secur_orders_utils.push_to_cancel(p_pko_id => v_pko_row.id);
      end if;
    else
      if v_resCode = 0 then
          -- успех: открываем блокировку лимитов
        begin
          --проставляем в примечании ответ квика
          note_utils.save_note(p_object_type => 101,
                          p_note_kind   => 410,
                          p_document_id => lpad(v_extID, 34, '0'),
                          p_note        => 'Успешно',
                          p_date        => trunc(sysdate));
                          
          it_diasoft.start_Pko_blockSecurities_Open(p_WriteOffid => v_pko_row.id,
                                                    p_operationid => v_pko_row.operationid,             
                                                    p_dealid => v_extID,
                                                    o_ErrorCode => o_MSGCode,
                                                    o_ErrorDesc => o_MSGText);       
          exception when others then
            --не валим ответ в целом, просто отражаем в тексте
            o_MSGCode := 20000;
            o_MSGText := 'Ошибка вызова it_diasoft.start_Pko_blockSecurities_Open: '||sqlerrm;
          return;
        end;
      else
          --ошибка: фиксируем, что бумаг нет
          begin
            note_utils.save_note(p_object_type => 101,
                          p_note_kind   => 410,
                          p_document_id => lpad(v_extID, 34, '0'),
                          p_note        => 'Ц/Б Недостаточно',
                          p_date        => trunc(sysdate));
                          
              it_diasoft.start_Pko_NoSecurities(p_WriteOffid => v_pko_row.id,
                                                p_operationid => v_pko_row.operationid,                   
                                                p_dealid => v_extID,
                                                o_ErrorCode => o_MSGCode,
                                                o_ErrorDesc => o_MSGText);
                                                                                            
          exception when others then
              o_MSGCode := 20000;
              o_MSGText := 'Ошибка вызова it_diasoft.start_Pko_NoSecurities: '||sqlerrm;
              return;
          end;
      end if;
    end if;

   exception when others then
        o_MSGCode := 20000;
        o_MSGText := sqlerrm;
  end; 

end it_quik;
/

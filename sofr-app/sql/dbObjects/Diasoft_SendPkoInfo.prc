create or replace procedure Diasoft_SendPkoInfo(
  p_msgid           itt_q_message_log.msgid%type,
  p_Pkostatus       PKO_WriteOff.Pkostatus%type,
  p_ClientCode      PKO_WriteOff.ClientCode%type,
  p_qnty            PKO_WriteOff.qnty%type,
  p_CustodyOrderId  PKO_WriteOff.CustodyOrderId%type,
  p_guid            PKO_WriteOff.guid%type,
  p_market          PKO_WriteOff.Market%type,
  p_OperationTime   PKO_WriteOff.OperationTime%type,
  p_oper_type       PKO_WriteOff.OperType%type,
  p_client_type     PKO_WriteOff.ClientType%type,
  p_contract_number varchar2,
  p_depo_account    varchar2,
  p_isin            varchar2,
  p_reg_number      varchar2,
  p_expiration_date timestamp,
  p_source_clob     clob,
  o_MSGCode     out integer,
  o_MSGText     out varchar2
) is 
  -- данные для картотеки PKO_WriteOff
  DealCode            PKO_WriteOff.DealCode%type;
  clientid            PKO_WriteOff.clientid%type;
  securityid          PKO_WriteOff.securityid%type;
  foundCustodyOrderId PKO_WriteOff.foundCustodyOrderId%type;
  foundCOId           PKO_WriteOff.foundCOId%type := 0;
  errList             PKO_WriteOff.errList%type;
  idContract          PKO_WriteOff.idContract%type := -1;
  OperationTimeOra    timestamp := systimestamp;

  -- funcobj
  v_ObjectTypeCode varchar2(4) := '8208';
  v_Priority       number(10) := 10;

  v_id             number(10);
  v_ErrorCode      integer;
  v_ErrorDesc      varchar2(2000);
  v_MSGCode        integer;
  v_MSGText        varchar2(2000);
  v_log_message    varchar2(1000) := '';
  v_cnt_contracts  integer := 0;
  v_sec_closed     char;
  v_dealcode_postfix varchar2(15);

  function getWriteOffNumkey(dealDate in date) return varchar2 is
    /*
      Функция нумерации в течение года
      Нумерация начинается с 1 при первой операции в новом году
      Дополнение до 5 знаков нулями
    */
    result number(10) := 0;
    numpad integer := 5;
    l_year varchar2(4);
  begin
    l_year := to_char(dealDate, 'yyyy');

    execute immediate 'select writeoff_' || l_year || '_seq.nextval n from dual'
      into result;
    return lpad(to_char(result), numpad, '0');
  exception
    when others then
      execute immediate 'CREATE SEQUENCE writeoff_' || l_year || '_seq START WITH 1 ' || ' MAXVALUE 9999999999999999999999999999  NOCYCLE NOCACHE';
      execute immediate 'select writeoff_' || l_year || '_seq.nextval n from dual'
        into result;

      return lpad(to_char(result), numpad, '0');
  end getWriteOffNumkey;

  procedure SendPkoInfoResp as
  begin
    v_log_message := 'Возникла ошибка при загрузке поручения на списание/зачисление ц/б из Диасофт в СОФР,' || ' id поручения в Диасофт ' || p_CustodyOrderId || ' : код ошибки ' ||
                     v_MSGCode || ' (' || v_MSGText || '). Необходимо исправить ошибку и повторно загрузить сообщение из Диасофт';
    it_diasoft.SendErrorEvent(p_ErrorCode => v_MSGCode
                             ,p_Head => 'Ошибка при загрузке ПКО. Операция не создана.'
                             ,p_Text => v_log_message
                             ,p_monitoring => v_MSGCode not in (1,2) and ((p_Pkostatus != 7 and v_MSGCode not in (11)) or p_Pkostatus = 7 ));

    if p_oper_type != '102'
    then
      it_diasoft.SendPkoInfoResp(p_GUIDReq => p_msgid
                                ,p_CustodyOrderId => p_CustodyOrderId
                                ,p_SofrOperationId => null
                                ,o_ErrorCode => v_ErrorCode
                                ,o_ErrorDesc => v_ErrorDesc
                                ,p_MSGCode => v_MSGCode
                                ,p_MSGText => v_MSGText);
    end if;
    commit;
    if v_ErrorCode != 0
    then
      it_log.log(p_msg => 'Ошибка #' || v_ErrorCode || v_ErrorDesc, p_msg_type => it_log.C_MSG_TYPE__ERROR, p_msg_clob => 'Ошибка #' || v_MSGCode || v_MSGText);
      v_log_message := 'Возникла ошибка при загрузке ответе об ошибке на списание/зачисление ц/б из Диасофт в СОФР,' || ' id поручения в Диасофт ' || p_CustodyOrderId ||
                       ' : код ошибки ' || v_ErrorCode || ' (' || v_ErrorDesc || ')';
      it_diasoft.SendErrorEvent(p_ErrorCode => it_q_manager.C_N_ERROR_OTHERS_MSGCODE
                               ,p_Head => 'Ошибка при ответе об ошибке ПКО'
                               ,p_Text => v_log_message);
    end if;
    o_MSGCode := v_MSGCode;
    o_MSGText := v_MSGText;
  end SendPkoInfoResp;

begin
  OperationTimeOra  := to_timestamp(p_OperationTime, 'yyyy-mm-dd"T"hh24:mi:ss');

  if to_number(p_Pkostatus default -1 on conversion error) not in (1, 2, 3, 4, 7)
  then
    v_MSGCode := nvl(v_MSGCode, 1);
    v_MSGText := case
                   when v_MSGText is not null then
                    v_MSGText || '/'
                 end || 'Операция не создавалась из-за неподходящего статуса в Диасофт ( PkoStatus=' || p_Pkostatus || ')';
    errList   := errList || '1,'; -- операция не создавалась из-за неподходящего статуса в Диасофт
  end if;
  if nvl(v_MSGCode, 0) = 0
  then
    begin
      select s.t_objectid
        into ClientId
        from dobjcode_dbt s
       where s.t_objecttype = 3
         and s.t_codekind = 101
         and s.t_state = 0
         and s.t_code = p_ClientCode;
    exception
      when no_data_found then
        v_MSGCode := nvl(v_MSGCode, 10);
        v_MSGText := case
                       when v_MSGText is not null then
                        v_MSGText || '/'
                     end || 'Клиент не найден';
        errList   := errList || '10,'; -- не удалось найти клиента, формирование списка ошибок
      -- it_log.log('Клиент не найден');
    end;
  end if;
  if nvl(v_MSGCode, 0) = 0
  then
    begin
      select a.t_fiid
            ,a.t_spisclosed
        into SecurityID
            ,v_sec_closed
        from davoiriss_dbt a
       where a.t_isin = p_isin;
      if (v_sec_closed = chr(88))
      then
        v_MSGCode := nvl(v_MSGCode, 20000);
        v_MSGText := case
                       when v_MSGText is not null then
                        v_MSGText || '/'
                     end || 'ЦБ найдена, но закрыта';
        errList   := errList || '20000,'; -- ЦБ найдена, но закрыта
      end if;
    exception
      when no_data_found then
        begin
          select a.t_fiid
                ,a.t_spisclosed
            into SecurityID
                ,v_sec_closed
            from davoiriss_dbt a
           where a.t_lsin = p_reg_number;
          if (v_sec_closed = chr(88))
          then
            v_MSGCode := nvl(v_MSGCode, 20000);
            v_MSGText := case
                           when v_MSGText is not null then
                            v_MSGText || '/'
                         end || 'ЦБ найдена, но закрыта';
            errList   := errList || '20000,'; -- ЦБ найдена, но закрыта
          end if;
        exception
          when no_data_found then
            v_MSGCode := nvl(v_MSGCode, 13);
            v_MSGText := case
                           when v_MSGText is not null then
                            v_MSGText || '/'
                         end || 'ЦБ не найдена';
            errList   := errList || '13,'; -- не найдена ЦБ
          --  it_log.log('ЦБ не найдена');
        end;
      when too_many_rows then
        begin
          select a.t_fiid
                ,a.t_spisclosed
            into SecurityID
                ,v_sec_closed
            from davoiriss_dbt a
           where a.t_lsin = p_reg_number
             and a.t_isin = p_isin;
          if (v_sec_closed = chr(88))
          then
            errList   := errList || '20000,'; -- ЦБ найдена, но закрыта
            v_MSGCode := nvl(v_MSGCode, 20000);
            v_MSGText := case
                           when v_MSGText is not null then
                            v_MSGText || '/'
                         end || 'ЦБ найдена, но закрыта';
          end if;
        exception
          when no_data_found
               or too_many_rows then
            v_MSGCode := nvl(v_MSGCode, 13);
            v_MSGText := case
                           when v_MSGText is not null then
                            v_MSGText || '/'
                         end || 'ЦБ не найдена';
            errList   := errList || '13,';
            --    it_log.log('ЦБ не найдена');
        end;
    end;
    --  it_log.log('Поиск ЦБ закончен');
  end if;
  if nvl(v_MSGCode, 0) = 0
  then
    select count(*) into v_cnt_contracts from dsfcontr_dbt t where t.t_number = p_contract_number;
    if (v_cnt_contracts = 0)
    then
      v_MSGCode := nvl(v_MSGCode, 11);
      v_MSGText := case
                     when v_MSGText is not null then
                      v_MSGText || '/'
                   end || 'Не найден договор БО';
      errList   := errList || '11,'; -- не найден договор БО
      --   it_log.log('Не найден договор БО');
    end if;
  end if;

  --проверка AccountDepoNumber
  if not (p_depo_account like 'Д-%') then
    v_MSGCode := 20000;
    v_MSGText := 'Счёт: ' || p_depo_account || ' не начинается на Д-';
  end if;

  if nvl(v_MSGCode, 0) = 0
  then
    if (instr(RSB_Common.GetRegStrValue('SECUR/EXCL_PREF_NUMACC_DEPO'), it_xml.token_substr(p_source => p_depo_account, p_delim => '-', p_num => 2)) > 0)
    then
      errList   := errList || '3,'; -- операция НЕ создается, возвращается ошибка 3
    end if;

    idContract := GetSubAgreementByDepoAccount(p_contract_number, p_depo_account, p_market);
    if (idContract = -1)
    then
      v_MSGCode := nvl(v_MSGCode, 12);
      v_MSGText := case
                     when v_MSGText is not null then
                      v_MSGText || '/'
                   end || 'Не найден субдоговор БО';
      errList   := errList || '12,'; -- не найден субдоговор БО
    else
      foundCustodyOrderId := chr(0);
      if p_oper_type != 102
      then
        foundCOId := nontrading_secur_orders_utils.get_deal_id_by_code_ts(p_code_ts => p_CustodyOrderId);
        foundCustodyOrderId := case when foundCOId is null then chr(0) else chr(88) end;

        if (foundCustodyOrderId = chr(88))
        then
          v_MSGCode := nvl(v_MSGCode, 2);
          v_MSGText := case
                          when v_MSGText is not null then
                          v_MSGText || '/'
                        end || 'Найдена операция с таким же внешним кодом';
          errList   := errList || '2,';
        end if;
      end if;

      if (length(errList) is null)
      then
        if p_oper_type = 102
        then
          v_dealcode_postfix := 'ОФЕРТА';
        else
          v_dealcode_postfix := 'ДЕПО';
        end if;
        DealCode := sfcontr_read.get_ekk_subcontr(idContract) || '/' || getWriteOffNumkey(OperationTimeOra) || '/' || v_dealcode_postfix;
      end if;
    end if;
  end if;
  -- Вставка в картотеку
  loop
    begin
      insert into PKO_WriteOff pk
        (DealCode
        ,clientid
        ,securityid
        ,qnty
        ,xml_from_diasoft
        ,guid
        ,Pkostatus
        ,CustodyOrderId
        ,foundCustodyOrderId
        ,foundCOId
        ,errList
        ,idContract
        ,OperationTime
        ,OperationTimeOra
        ,OperType
        ,Market
        ,ExpirationDate
        ,ClientType
        ,ClientCode
        ,StartWriteOffDate)
      values
        (DealCode
        ,clientid
        ,securityid
        ,p_qnty
        ,p_source_clob
        ,p_guid
        ,p_Pkostatus
        ,p_CustodyOrderId
        ,foundCustodyOrderId
        ,foundCOId
        ,errList
        ,idContract
        ,p_OperationTime
        ,OperationTimeOra
        ,p_oper_type
        ,p_market
        ,p_expiration_date
        ,p_client_type
        ,p_ClientCode
        ,OperationTimeOra)
      returning id into v_id;
      it_log.log('PKO_WriteOff.id =' || v_id || ' Вставка в картотеку');
      exit;
    exception
      when DUP_VAL_ON_INDEX then
        begin
          update PKO_WriteOff w
             set errlist = 'УДАЛЕНА'
           where w.custodyorderid = p_CustodyOrderId
             and w.errlist is null;
        end;
    end;
  end loop;
  if nvl(v_MSGCode, 0) = 0
  then
    funcobj_utils.save_task(
      p_objectid => v_id,
      p_funcid => funcobj_utils.get_func_id(p_code => v_ObjectTypeCode),
      p_param => p_guid,
      p_priority => v_Priority
    );
    it_log.log('PKO_WriteOff.id =' || v_id || ' Вставка в funcobj');
    commit;
  else
    SendPkoInfoResp;
  end if;
exception
  when others then
    rollback;
    v_MSGCode := 20000;
    v_MSGText := it_q_message.get_errtxt(p_sqlerrm => sqlerrm);
    it_error.put_error_in_stack;
    it_log.log(p_msg => 'exception ' || p_guid, p_msg_type => it_log.C_MSG_TYPE__ERROR);
    it_error.clear_error_stack;
    SendPkoInfoResp;
end;
/

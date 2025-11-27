create or replace procedure diasoft_CreatePKO_part2(id_ddl_tick ddl_tick_dbt.t_dealid%type
                                                   ,id_PKO_GUID in varchar2
                                                   ,outErr      in out integer) is
  /* к данному моменту операция могла быть создана в макросе diasoft_createPKO_funcobj.mac */
  rec_ddl_tick ddl_tick_dbt%rowtype;
  -- xml
  ISIN               varchar2(25);
  recPKO_WriteOff    PKO_WriteOff%rowtype;
  -- отправка
  o_ErrorCode number(10) := 0;
  o_ErrorDesc varchar2(200) := ''; -- ошибка из вызываемой подпрограммы
  err         number := 0;
  v_err       number; -- для случая ошибки №3
  errDesc     varchar2(1000);
  v_errDesc   varchar2(1000); -- для случая ошибки №3
  v_namespace varchar2(128) := it_kafka.get_namespace('Diasoft', 'SendPkoInfoReq');
  v_log_Head           varchar2(1000) := '';
  v_log_message        varchar2(1000) := '';
  v_dealid             number(10) := 0;
  v_operationid        number(10) := null;
  v_shortname          varchar2(60);
  v_begreplacementdate date;
  v_servkindsub        number;
  v_cnt_deals_on_pko   integer;
  v_is_need_request_limits boolean;

  l_log_object varchar2(50) := 'DIASOFT_CREATEPKO_PART2';
begin
  it_log.log_handle(p_object => l_log_object,
                    p_msg    => 'Начало процедуры diasoft_CreatePKO_part2 id_ddl_tick=' || id_ddl_tick || ' id_PKO_GUID=' || id_PKO_GUID || ' outErr=' || outErr);
  
  if (id_ddl_tick <> 0)
  then
    begin
      select *
        into rec_ddl_tick
        from ddl_tick_dbt dtick
       where dtick.t_dealtype in (2011, 2010, 32743)
         and dtick.t_dealid = id_ddl_tick;

      it_log.log_handle(p_object => l_log_object, p_msg => 'dealtype=' || to_char(rec_ddl_tick.t_dealtype));
    exception
      when no_data_found then
        it_log.log_handle(p_object => l_log_object, p_msg => 'Не найдена операция списания и зачисления в таблице ddl_tick_dbt');
    end;

    begin
      select op.t_id_operation
        into v_operationid
        from doproper_dbt op
       where op.t_kind_operation in (2011, 2010, 32743)
         and op.t_dockind = 127
         and op.t_documentid = lpad(id_ddl_tick, 34, '0');

      it_log.log_handle(p_object => l_log_object, p_msg => 'v_operationid=' || to_char(v_operationid));
    exception
      when no_data_found then
        it_log.log_handle(p_object => l_log_object, p_msg => 'Не найдена операция в таблице doproper_dbt');
    end;

    update PKO_WriteOff x
       set x.dealid      = id_ddl_tick
          ,x.operationid = v_operationid
     where guid = id_PKO_GUID;
    it_log.log_handle(p_object => l_log_object, p_msg => 'Обновлена запись в Картотеке событий взаимодействия с Diasoft');
  else
    update PKO_WriteOff x set x.dealcode = null where guid = id_PKO_GUID;
  end if;
  -- по умолчанию разрешаем первый шаг операции Ожидание статуса обработки 
  -- по умолчанию разрешаем третий шаг операции Списание 
  update PKO_WriteOff x
     set x.step1_waitstatus = 1
        ,x.step3_writeoff   = 1
   where guid = id_PKO_GUID;
  if (outErr <> 0)
  then
    update PKO_WriteOff x set errList = x.errlist || outErr || ',' where guid = id_PKO_GUID;
  end if;

  select x.* into recPKO_WriteOff from PKO_WriteOff x where guid = id_PKO_GUID order by x.id desc fetch first row only;
  if (outErr = 0)
  then
    update ddl_tick_dbt dt
       set dt.t_flag3         = chr(88), --флаг "Учитывать в НУ"
           dt.t_taxownbegdate = dt.t_dealdate
     where dt.t_dealid = id_ddl_tick;
  end if;

  ISIN := fininstr_read.get_isin(p_fiid => recPKO_WriteOff.securityid);

  -- обновление только что созданной операции
  if (outErr = 0)
  then
    update ddl_tick_dbt dt set dt.t_dealcodets = recPKO_WriteOff.CustodyOrderId where dt.t_dealid = id_ddl_tick;
  end if;

  it_log.log_handle(p_object => l_log_object, p_msg => 'PKO_WriteOff.id =' || recPKO_WriteOff.Id || ' Обновлен код внешний в операции');

  err := nvl(regexp_substr(recPKO_WriteOff.Errlist, '[^,]+', 1), 0);
  it_log.log_handle(p_object => l_log_object, p_msg => 'PKO_WriteOff.id =' || recPKO_WriteOff.Id || ' Вычислен код ошибки ' || err || ' - взят первый из списка');

  if (outErr = 0)
  then
    err := 0;
  end if;

  if (err = 0)
  then
    errDesc := null;
  elsif (err = 1)
  then
    errDesc := 'операция не создавалась из-за неподходящего статуса в Диасофт';
  elsif (err = 2)
  then
    errDesc := 'не удалось изменить операцию';
  elsif (err = 3)
  then
    errDesc := 'по указанному счета ДЕПО операции в СОФР не загружаются';
  elsif (err = 10)
  then
    errDesc := 'не удалось найти клиента';
  elsif (err = 11)
  then
    errDesc := 'не удалось найти договор брокерского обслуживания';
  elsif (err = 12)
  then
    errDesc := 'не удалось найти субдоговор брокерского обслуживания';
  elsif (err = 13)
  then
    errDesc := 'не удалось определить ценную бумагу';
  elsif (err = 21)
  then
    errDesc := 'не удалось создать операцию списания/зачисления ц/б';
  elsif (err = 20000)
  then
    errDesc := 'прочие ошибки';
  else
    errDesc := 'неизвестная ошибка';
  end if;

  v_dealid  := recPKO_WriteOff.Dealid;
  v_errDesc := errDesc;
  v_err     := err;
  if (err = 3)
  then
    v_dealid  := 0;
    v_errDesc := null;
    v_err     := 0;
  end if;

  if nontrading_secur_orders_utils.is_voluntary_redemption_by_row(p_pko_row => recPKO_WriteOff) = 0
  then
    it_diasoft.SendPkoInfoResp(recPKO_WriteOff.Guid -- GUID из входящего сообщения  SendPkoInfoReq,
                              ,recPKO_WriteOff.CustodyOrderId -- Id поручения в Диасофт, из CustodyOrderId во входящем xml
                              ,v_dealid -- Id свежесозданной операции в СОФР (ddl_tick_dbt.t_dealid). Не заполняется, если операцию не удалось создать.
                              ,o_ErrorCode -- != 0 ошибка создания сообщения  o_ErrorDesc
                              ,o_ErrorDesc
                              ,v_err -- Код ошибки обработки SendPkoInfoReq
                              ,v_errDesc --recPKO_WriteOff.Errlist -- полный перечень ошибок
                              ,null);
    it_log.log_handle(p_object => l_log_object, p_msg => 'PKO_WriteOff.id =' || recPKO_WriteOff.Id || ' Отправлено сообщение в Диасофт по результату создания операции');
  end if;

  if (err <> 0)
  then
    v_log_message := 'Возникла ошибка при загрузке поручения на списание/зачисление ц/б из Диасофт в СОФР,' || ' id поручения в Диасофт ' || recPKO_WriteOff.CustodyOrderId || ' : код ошибки ' || err || ' (' ||
                     errDesc || ')';
    it_diasoft.SendErrorEvent(p_ErrorCode => err
                             ,p_Head => 'Ошибка при загрузке ПКО'
                             ,p_Text => v_log_message
                             ,p_monitoring => (recPKO_WriteOff.Pkostatus = 7));
  elsif (err = 0 and recPKO_WriteOff.Opertype in (2, 102) -- списание
        and rec_ddl_tick.t_dealtype = 32743 -- новый тип операции
        )
  then
    begin
      select d.t_shortname into v_shortname from dparty_dbt d where d.t_partyid = recPKO_WriteOff.Clientid;
    exception
      when no_data_found then
        Raise_application_error(-20000, 'Не найден клиент по коду ' || to_char(recPKO_WriteOff.Clientid));
    end;
    begin
      select da.t_begplacementdate into v_begreplacementdate from davoiriss_dbt da where da.t_isin = ISIN;
    exception
      when no_data_found then
        Raise_application_error(-20000, 'Не найдена ЦБ по коду ' || ISIN);
    end;
    begin
      select ds.t_servkindsub into v_servkindsub from dsfcontr_dbt ds where ds.t_id = recPKO_WriteOff.Idcontract;
    exception
      when no_data_found then
        Raise_application_error(-20000, 'Не найден договор по коду ' || recPKO_WriteOff.Idcontract);
    end;

    if nvl(recPKO_WriteOff.Pkostatus, -1) != 7
    then
      if nontrading_secur_orders_utils.is_voluntary_redemption_by_row(p_pko_row => recPKO_WriteOff) = 1
      then
        --  проверить, есть ли другая операция с заблокированными лимитами, отменённая по этой же заявке
        select count(1)
          into v_cnt_deals_on_pko
          from pko_writeoff w
          where w.CustodyOrderId = recPKO_WriteOff.CustodyOrderId
            and w.dealid != recPKO_WriteOff.dealid
            and islimitcorrected = 'X'
            and iscanceled = 'X';

        v_is_need_request_limits := v_cnt_deals_on_pko = 0;

        if v_cnt_deals_on_pko > 0
        then
          --если лимиты уже были заблокированы, то делаем тоже самое, что и при получении ответа об успешной блокировке лимитов
          nontrading_secur_orders_utils.set_is_enough_quantity(p_deal_id => recPKO_WriteOff.dealid, p_is_enough_quantity => 1);
          nontrading_secur_orders_utils.set_is_limit_corrected(p_pko_id => recPKO_WriteOff.id);
          nontrading_secur_orders_utils.set_wait_status(p_pko_id => recPKO_WriteOff.id, p_is_wait => 1);
          it_sinv.send_nontrade_limit_state(p_deal_id => recPKO_WriteOff.dealid, p_limit_status => 1);
          nontrading_secur_orders_utils.push_to_execute_deal(p_deal_id => recPKO_WriteOff.dealid);
        end if;
      else
        v_is_need_request_limits := true;
      end if;

      v_log_Head   := 'Получено поручение на списание ц/б ' || recPKO_WriteOff.Dealcode;
      v_log_message :=  'Получено поручение на списание ц/б ' || recPKO_WriteOff.Dealcode || ', клиент ' || v_shortname || ' (' || recPKO_WriteOff.Clientcode || '). Ц/Б ' || ISIN ||
                           ' с датой начала действия ' || to_char(recPKO_WriteOff.Startwriteoffdate, 'dd.mm.yyyy') || '. ' ;
      if v_is_need_request_limits
      then 
        if (v_servkindsub = 9)
        then
          -- внебиржевая операция
          -- запускаем процедуру Diasoft.CheckSecuritiesOTC
          v_log_Head := v_log_Head||' ( Недостаточно ц/б для списания )';
          v_log_message := v_log_message||'Просьба проверить входящие остатки клиента в ВУ и ДУ на предмет расхождений. При необходимости провести корректирующие операции';
          it_diasoft.CheckSecuritiesOTC(recPKO_WriteOff.Id, o_ErrorCode, o_ErrorDesc,p_send_notify => 1,p_Head => v_log_Head,p_Text => v_log_message);
        else
          if it_rs_interface.get_parm_varchar_path(p_parm_path => 'РСХБ\ИНТЕГРАЦИЯ\НЕТОРГОВЫЕ ПОРУЧЕНИЯ\ВЫГРУЗКА В QUIK\ВЫВОД ЦБ') != chr(88) then
            note_utils.save_note(p_object_type => 101,
                          p_note_kind   => 410,
                          p_document_id => lpad(recPKO_WriteOff.Dealid, 34, '0'),
                          p_note        => 'Интеграция выключена',
                          p_date        => trunc(sysdate));
            if (recPKO_WriteOff.Startwriteoffdate <= sysdate and systimestamp < (trunc(sysdate) + interval '19' hour)) --старая логика обработки
            then
              v_log_message :=  v_log_message||
                            'Проверьте достаточность лимитов в QUIK. Если достаточно, уменьшите их на ' || it_xml.number_to_char(recPKO_WriteOff.Qnty, -12) || ' шт., ' ||
                            'после чего в СОФР в списке отложенных операций списания ПКО вызовите по Ctrl-Z ' ||
                            'п.меню "Отметить как заблокированное в лимитах". Если лимитов недостаточно,  ' ||
                            'в СОФР в списке отложенных операций списания ПКО вызовите по Ctrl-Z ' || 'п.меню "Передать сообщение о недостаточности ц/б"';

              rsb_payments_api.InsertEmailNotify(76, v_log_Head, v_log_message);
            end if;
          else --новая логика обработки, в случае включённого рубильника          
            it_quik.sent_nontrade_secure_limits(p_msgID => it_q_message.get_sys_guid,
                                        p_CORRmsgid => recPKO_WriteOff.Guid,
                                        o_ErrorCode => o_ErrorCode,
                                        o_ErrorDesc => o_ErrorDesc);
          end if;
        end if;
      end if;
    end if;
    if o_ErrorCode = 0
    then
      commit;
    else
      it_log.log_error(p_object => l_log_object, p_msg => 'PKO_WriteOff.id =' || recPKO_WriteOff.Id || ' Error#' || o_errorCode || ':' || o_ErrorDesc);  
      rollback;
    end if;
  end if;
  outErr := err;
exception
  when others then
    rollback;
    it_log.log_error(p_object => l_log_object, p_msg => 'PKO_WriteOff.id =' || recPKO_WriteOff.Id || '. Error: ' || sqlerrm);
    Raise_application_error(-20000, 'Error ' || dbms_utility.format_error_stack);
end;
/

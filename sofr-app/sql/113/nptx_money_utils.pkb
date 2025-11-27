create or replace package body nptx_money_utils as

  procedure send_error_mail (
    p_head varchar2,
    p_text varchar2
  ) is
  begin
    if it_rs_interface.get_parm_varchar_path(p_parm_path => 'РСХБ\ИНТЕГРАЦИЯ\НЕТОРГОВЫЕ ПОРУЧЕНИЯ\СОЗДАНИЕ ОПЕРАЦИЙ\ОПОВЕЩЕНИЕ ОД') = chr(88)
    then
      rsb_payments_api.InsertEmailNotify(p_EmailGroup => nptx_money_read.get_email_grp_kafka_err,
                                         p_Head       => p_head,
                                         p_Text       => p_text);
    end if;
  end send_error_mail;

  procedure z_______________buf is
  begin
    null;
  end;
    
  procedure save_nptxop_req (
    p_req dnptxop_req_dbt%rowtype
  ) is
  begin
    insert into dnptxop_req_dbt values p_req;
  end save_nptxop_req;

  function save_transfer_to_buf (
    p_src             dnptxop_req_dbt.src%type,
    p_ext_id          dnptxop_req_dbt.external_id%type,
    p_client_cft_id   dnptxop_req_dbt.client_cft_id%type,
    p_contract        dnptxop_req_dbt.contract%type,
    p_client_code     dnptxop_req_dbt.client_code%type,
    p_is_exchange     dnptxop_req_dbt.is_exchange%type,
    p_is_full_rest    dnptxop_req_dbt.is_full_rest%type,
    p_currency        dnptxop_req_dbt.currency%type,
    p_amount          dnptxop_req_dbt.amount%type,
    p_is_exchange_tgt dnptxop_req_dbt.is_exchange_target%type,
    p_req_date        dnptxop_req_dbt.req_date%type,
    p_req_time        dnptxop_req_dbt.req_time%type,
    p_file_name       dnptxop_req_dbt.file_name%type
  ) return number is
    l_error_id      dnptxop_req_dbt.error_id%type := nptx_money_read.buf_err_no_error;
    l_status_id     dnptxop_req_dbt.status_id%type := nptx_money_read.buf_status_ready;
    l_nptx_req_row  dnptxop_req_dbt%rowtype;
  begin
    if nptx_money_read.get_active_req(p_src    => p_src
                                     ,p_ext_id => p_ext_id
                                     ,p_kind   => nptx_money_read.buf_kind_transfer
                                     ,p_client => p_client_cft_id) is not null
    then
      l_error_id := nptx_money_read.buf_err_duplicate;
      l_status_id := nptx_money_read.buf_status_error;
    end if;
    
    l_nptx_req_row.req_id             := dnptxop_req_seq.nextval;
    l_nptx_req_row.src                := p_src;
    l_nptx_req_row.external_id        := p_ext_id;
    l_nptx_req_row.client_cft_id      := p_client_cft_id;
    l_nptx_req_row.contract           := p_contract;
    l_nptx_req_row.client_code        := p_client_code;
    l_nptx_req_row.is_exchange        := p_is_exchange;
    l_nptx_req_row.is_full_rest       := p_is_full_rest;
    l_nptx_req_row.currency           := p_currency;
    l_nptx_req_row.amount             := p_amount;
    l_nptx_req_row.is_exchange_target := p_is_exchange_tgt;
    l_nptx_req_row.req_date           := p_req_date;
    l_nptx_req_row.req_time           := p_req_time;
    l_nptx_req_row.file_name          := p_file_name;
    l_nptx_req_row.error_id           := l_error_id;
    l_nptx_req_row.status_id          := l_status_id;
    l_nptx_req_row.import_time        := systimestamp;
    l_nptx_req_row.status_changed     := systimestamp;
    l_nptx_req_row.kind               := nptx_money_read.buf_kind_transfer;
    
    save_nptxop_req(p_req => l_nptx_req_row);

    return l_nptx_req_row.req_id;
  exception
    when others then
      it_log.log_error(p_object => 'nptx_money_utils.save_transfer_to_buf',
                       p_msg    => sqlerrm);
      return 0;
  end save_transfer_to_buf;
  
  function save_out_to_buf (
    p_src             dnptxop_req_dbt.src%type,
    p_ext_id          dnptxop_req_dbt.external_id%type,
    p_client_cft_id   dnptxop_req_dbt.client_cft_id%type,
    p_iis             dnptxop_req_dbt.iis%type,
    p_contract        dnptxop_req_dbt.contract%type,
    p_client_code     dnptxop_req_dbt.client_code%type,
    p_is_exchange     dnptxop_req_dbt.is_exchange%type,
    p_is_full_rest    dnptxop_req_dbt.is_full_rest%type,
    p_currency        dnptxop_req_dbt.currency%type,
    p_amount          dnptxop_req_dbt.amount%type,
    p_account         dnptxop_req_dbt.enroll_account%type,
    p_department      dnptxop_req_dbt.department%type,
    p_req_date        dnptxop_req_dbt.req_date%type,
    p_req_time        dnptxop_req_dbt.req_time%type,
    p_file_name       dnptxop_req_dbt.file_name%type
  ) return number is
    l_error_id      dnptxop_req_dbt.error_id%type := nptx_money_read.buf_err_no_error;
    l_status_id     dnptxop_req_dbt.status_id%type := nptx_money_read.buf_status_ready;
    l_nptx_req_row  dnptxop_req_dbt%rowtype;
    l_kind          dnptxop_req_dbt.kind%type;
  begin
    l_kind := case when p_is_exchange = 0 then nptx_money_read.buf_kind_out_otc else nptx_money_read.buf_kind_out_exchange end;
    if nptx_money_read.get_active_req(p_src    => p_src
                                     ,p_ext_id => p_ext_id
                                     ,p_kind   => l_kind
                                     ,p_client => p_client_cft_id) is not null
    then
      l_error_id := nptx_money_read.buf_err_duplicate;
      l_status_id := nptx_money_read.buf_status_error;
    end if;

    l_nptx_req_row.req_id             := dnptxop_req_seq.nextval;
    l_nptx_req_row.src                := p_src;
    l_nptx_req_row.external_id        := p_ext_id;
    l_nptx_req_row.client_cft_id      := p_client_cft_id;
    l_nptx_req_row.iis                := p_iis;
    l_nptx_req_row.contract           := p_contract;
    l_nptx_req_row.client_code        := p_client_code;
    l_nptx_req_row.is_exchange        := p_is_exchange;
    l_nptx_req_row.is_full_rest       := p_is_full_rest;
    l_nptx_req_row.currency           := p_currency;
    l_nptx_req_row.amount             := p_amount;
    l_nptx_req_row.enroll_account     := p_account;
    l_nptx_req_row.department         := p_department;
    l_nptx_req_row.req_date           := p_req_date;
    l_nptx_req_row.req_time           := p_req_time;
    l_nptx_req_row.file_name          := p_file_name;
    l_nptx_req_row.error_id           := l_error_id;
    l_nptx_req_row.status_id          := l_status_id;
    l_nptx_req_row.import_time        := systimestamp;
    l_nptx_req_row.status_changed     := systimestamp;
    l_nptx_req_row.kind               := l_kind;

    save_nptxop_req(p_req => l_nptx_req_row);

    return l_nptx_req_row.req_id;
  exception
    when others then
      it_log.log_error(p_object => 'nptx_money_utils.save_out_to_buf',
                       p_msg    => sqlerrm);
      return 0;
  end save_out_to_buf;
  
  procedure save_error (
    p_req_id   dnptxop_req_dbt.req_id%type,
    p_error_id dnptxop_req_dbt.error_id%type
  ) is
  begin
    update dnptxop_req_dbt r
       set r.error_id  = p_error_id,
           r.status_id = nptx_money_read.buf_status_error
     where r.req_id = p_req_id;
  end save_error;
  
  procedure save_buf_nptxop_id (
    p_req_id    dnptxop_req_dbt.req_id%type,
    p_nptxop_id dnptxop_req_dbt.operation_id%type
  ) is
  begin
    update dnptxop_req_dbt r
       set r.operation_id = p_nptxop_id
     where r.req_id = p_req_id;
  end save_buf_nptxop_id;
  
  procedure set_buf_status (
    p_req_id    dnptxop_req_dbt.req_id%type,
    p_status_id dnptxop_req_dbt.status_id%type
  ) is
  begin
    update dnptxop_req_dbt r
       set r.status_id = p_status_id,
           r.status_changed = systimestamp
     where r.req_id = p_req_id;
  end set_buf_status;
  
  procedure set_buf_error (
    p_req_id    dnptxop_req_dbt.req_id%type,
    p_error_id  dnptxop_req_dbt.error_id%type
  ) is
  begin
    update dnptxop_req_dbt r
       set r.error_id = p_error_id
     where r.req_id = p_req_id;
  end set_buf_error;
  
  procedure set_error_if_not_error (
    p_nptxop_id dnptxop_req_dbt.operation_id%type,
    p_error_id  dnptxop_req_dbt.error_id%type
  ) is
  begin
    update dnptxop_req_dbt r
       set r.error_id = p_error_id
     where r.operation_id = p_nptxop_id
       and r.error_id = nptx_money_read.buf_err_no_error();
  end set_error_if_not_error;
  
  procedure set_status_wait (
    p_nptxop_id dnptxop_req_dbt.operation_id%type
  ) is
   -- pragma autonomous_transaction;
  begin
    update dnptxop_req_dbt r
       set r.status_id = nptx_money_read.buf_status_wait,
           r.status_changed = systimestamp
     where r.operation_id = p_nptxop_id;
  --  commit;
  end set_status_wait;
  
  procedure set_status_done (
    p_nptxop_id dnptxop_req_dbt.operation_id%type
  ) is
  --  pragma autonomous_transaction;
  begin
    update dnptxop_req_dbt r
       set r.status_id = nptx_money_read.buf_status_done,
           r.status_changed = systimestamp
     where r.operation_id = p_nptxop_id;
 
  --  commit;
  end set_status_done;
  
  procedure set_status_reject(
    p_nptxop_id dnptxop_req_dbt.operation_id%type
  ) is
 --   pragma autonomous_transaction;
  begin
    update dnptxop_req_dbt r
       set r.status_id = nptx_money_read.buf_status_reject,
           r.status_changed = systimestamp
     where r.operation_id = p_nptxop_id;
    
    nptx_money_utils.send_order_status(p_operid  => p_nptxop_id,p_status => nptx_money_read.buf_status_reject ) ;
                             
  --  commit;
  end set_status_reject;
  
  procedure set_status_deleted (
    p_nptxop_id dnptxop_req_dbt.operation_id%type
  ) is
   -- pragma autonomous_transaction;
  begin
    update dnptxop_req_dbt r
       set r.status_id = nptx_money_read.buf_status_deleted,
           r.status_changed = systimestamp
     where r.operation_id = p_nptxop_id;
  --  commit;
  end set_status_deleted;
  
  function set_lock (
    p_req_id    dnptxop_req_dbt.req_id%type
  ) return number is
    l_lock_name varchar2(100) := 'nptx_req_lock_' || to_char(p_req_id);
  begin
    if lock_utils.set_lock(p_lockname          => l_lock_name,
                           p_release_on_commit => true,
                           p_exclusive         => true,
                           p_timeout           => 5)
    then
      return 1;
    end if;
    return 0;
  end set_lock;
  
  procedure release_lock (
    p_req_id    dnptxop_req_dbt.req_id%type
  ) is
  begin
    lock_utils.release_lock(p_lockname => 'nptx_req_lock_' || to_char(p_req_id));
  end release_lock;

  procedure z______________operation is
  begin
    null;
  end;
  
  procedure check_global_params
    is
  begin
    if RsbSessionData.oper is null then 
        RsbSessionData.SetOperDprt(1); -- департамент
        RsbSessionData.SetOperDprtNode(1); -- подразделение
        RsbSessionData.SetOper(9997);
    end if;
  exception
    when others then
      raise_application_error(-20000, 'cant define session parameters');
  end;
  
  function save_nptxop (
    p_subkind       dnptxop_dbt.t_subkind_operation%type,
    p_code          dnptxop_dbt.t_code%type,
    p_contract      dnptxop_dbt.t_contract%type,
    p_client        dnptxop_dbt.t_client%type,
    p_currency      dnptxop_dbt.t_currency%type,
    p_account       dnptxop_dbt.t_account%type,
    p_outsum        dnptxop_dbt.t_outsum%type,
    p_iis           dnptxop_dbt.t_iis%type,
    p_partial       dnptxop_dbt.t_partial%type,
    p_placekind     dnptxop_dbt.t_placekind%type,
    p_place         dnptxop_dbt.t_place%type,
    p_marketplace   dnptxop_dbt.t_marketplace%type,
    p_marketsector  dnptxop_dbt.t_marketsector%type,
    p_placekind2    dnptxop_dbt.t_placekind2%type,
    p_place2        dnptxop_dbt.t_place2%type,
    p_marketplace2  dnptxop_dbt.t_marketplace2%type,
    p_marketsector2 dnptxop_dbt.t_marketsector2%type,
    p_calcndfl      dnptxop_dbt.t_calcndfl%type,
    p_flagtax       dnptxop_dbt.t_flagtax%type,
    p_closecontr    dnptxop_dbt.t_closecontr%type,
    p_accounttax    dnptxop_dbt.t_accounttax%type
  ) return dnptxop_dbt.t_id%type is
    l_id  dnptxop_dbt.t_id%type;
  begin
    insert into dnptxop_dbt (t_id,
                             t_dockind,
                             t_kind_operation,
                             t_subkind_operation,
                             t_operdate,
                             t_department,
                             t_iis,
                             t_time,
                             t_currentyear_sum,
                             t_currencysum,
                             t_flagtax,
                             t_partial,
                             t_accounttax,
                             t_status,
                             t_oper,
                             t_code,
                             t_contract,
                             t_client,
                             t_currency,
                             t_fiid,
                             t_account,
                             t_outsum,
                             t_taxsum2,
                             t_placekind,
                             t_place,
                             t_marketplace,
                             t_marketsector,
                             t_placekind2,
                             t_place2,
                             t_marketplace2,
                             t_marketsector2,
                             t_calcndfl,
                             t_closecontr)
    values (dnptxop_dbt_seq.nextval,
            nptx_money_read.dockind,
            2037,
            p_subkind,
            trunc(sysdate),
            1,
            p_iis,
            sysdate,
            0,
            0,
            p_flagtax,
            p_partial,
            p_accounttax,
            0, --TODO: check if right
            RsbSessionData.Oper,
            p_code,
            p_contract,
            p_client,
            p_currency,
            p_currency,
            p_account,
            p_outsum,
            p_outsum,
            p_placekind,
            p_place,
            p_marketplace,
            p_marketsector,
            p_placekind2,
            p_place2,
            p_marketplace2,
            p_marketsector2,
            p_calcndfl,
            p_closecontr)
    returning t_id into l_id;

    return l_id;
  end save_nptxop;

  function save_rq_acc (
    p_nptxop_id  ddlrqacc_dbt.t_docid%type,
    p_client_id  ddlrqacc_dbt.t_party%type,
    p_currency   ddlrqacc_dbt.t_fiid%type,
    p_account    ddlrqacc_dbt.t_account%type,
    p_bank_id    ddlrqacc_dbt.t_bankid%type
  ) return ddlrqacc_dbt.t_id%type is
    l_rqacc_row    ddlrqacc_dbt%rowtype;
    l_id           ddlrqacc_dbt.t_id%type;
  begin
    l_rqacc_row.t_dockind          := nptx_money_read.dockind;
    l_rqacc_row.t_docid            := p_nptxop_id;
    l_rqacc_row.t_party            := p_client_id;
    l_rqacc_row.t_fiid             := p_currency;
    l_rqacc_row.t_account          := p_account;
    l_rqacc_row.t_bankid           := p_bank_id;
    l_rqacc_row.t_bankcodekind     := 3;
    l_rqacc_row.t_bankcode         := nptx_money_read.get_party_code(p_party_id => l_rqacc_row.t_bankid, p_code_kind => l_rqacc_row.t_bankcodekind);
    l_rqacc_row.t_bankname         := nptx_money_read.get_party_name(p_party_id => l_rqacc_row.t_bankid);
    l_rqacc_row.t_dockind          := nptx_money_read.dockind;
    l_rqacc_row.t_subkind          := 0;
    l_rqacc_row.t_type             := 2;
    l_rqacc_row.t_chapter          := 1;
    l_rqacc_row.t_bankcorrid       := 0;
    l_rqacc_row.t_bankcorrcodekind := 0;
    l_rqacc_row.t_bankcorrcode     := chr(1);
    l_rqacc_row.t_bankcorrname     := chr(1);
    l_rqacc_row.t_corracc          := chr(1);
    l_rqacc_row.t_version          := 0;
    
    insert into ddlrqacc_dbt values l_rqacc_row
    returning t_id into l_id;
    
    return l_id;
  end save_rq_acc;

  procedure save_rq (
    p_nptxop_id  ddlrq_dbt.t_docid%type,
    p_amount     ddlrq_dbt.t_amount%type,
    p_currency   ddlrq_dbt.t_fiid%type,
    p_client_id  ddlrq_dbt.t_party%type,
    p_rqacc_id   ddlrq_dbt.t_rqaccid%type
  ) is
  begin
    insert into ddlrq_dbt (t_dockind,
                           t_docid,
                           t_dealpart,
                           t_kind,
                           t_subkind,
                           t_type,
                           t_num,
                           t_amount,
                           t_fiid,
                           t_party,
                           t_rqaccid,
                           t_placeid,
                           t_state,
                           t_plandate,
                           t_factdate,
                           t_changedate,
                           t_version)
    values (nptx_money_read.dockind,
            p_nptxop_id,
            1,
            0,
            0,
            2,
            0,
            p_amount,
            p_currency,
            p_client_id,
            p_rqacc_id,
            1,
            0,
            trunc(sysdate),
            trunc(sysdate),
            trunc(sysdate),
            0);
  end save_rq;
  
  procedure save_payment( p_documentid        dnptxop_dbt.t_id%type,
                          p_code              dnptxop_dbt.t_code%type,
                          p_client            dnptxop_dbt.t_client%type,
                          p_currency          dnptxop_dbt.t_currency%type,
                          p_account_payer     dnptxop_dbt.t_account%type,
                          p_account_receiver  dnptxop_dbt.t_account%type,
                          p_outsum            dnptxop_dbt.t_outsum%type,
                          p_dep_receiver      ddp_dep_dbt.t_name%type,
                          p_valuedate         dpmpaym_dbt.t_valuedate%type)
  is 
    l_payerbankid    dparty_dbt.t_partyid%type;
    l_payercorracc   dbankdprt_dbt.t_coracc%type;
    l_payerbankname  dparty_dbt.t_name%type;
    l_receiverbankid    dparty_dbt.t_partyid%type;
    l_receivercorracc   dbankdprt_dbt.t_coracc%type;
    l_receiverbankname  dparty_dbt.t_name%type;
    
    l_paymentid   dpmpaym_dbt.t_paymentid%type;
    l_clientname  dpmrmprop_dbt.t_payername%type;
    l_clientinn   dpmrmprop_dbt.t_payerinn%type;
    l_ground      dpmrmprop_dbt.t_ground%type;
    
    l_schem_number  dcorschem_dbt.t_number%type;
    l_schem_corrac  dcorschem_dbt.t_account%type;
  begin 
    
    l_payerbankid := payment_utils.get_department_param(  p_depname  => '0000',
                                                          o_bankname => l_payerbankname,
                                                          o_coracc   => l_payercorracc);
    l_receiverbankid := payment_utils.get_department_param( p_depname  => p_dep_receiver,
                                                            o_bankname => l_receiverbankname,
                                                            o_coracc   => l_receivercorracc);
    l_schem_number := payment_utils.get_schem_param( p_bank_id => l_receiverbankid,
                                                     p_code_currency => p_currency,
                                                     o_schem_corrac  => l_schem_corrac);
    l_clientname := nptx_money_read.get_party_name(p_party_id => p_client);
    l_clientinn := nvl(nptx_money_read.get_party_code(p_party_id => p_client, p_code_kind => 16), chr(1));
    l_ground := 'Списание денежных средств по операции № '||p_code;
    
    l_paymentid := payment_utils.save_pmpaym (p_documentid      => p_documentid,
                                              p_dockind         => nptx_money_read.dockind,
                                              p_purpose         => 1,
                                              p_subpurpose      => 0,
                                              p_currency        => p_currency,
                                              p_payer           => p_client,
                                              p_payeraccount    => p_account_payer,
                                              p_payerbankid     => l_payerbankid,
                                              p_receiver        => p_client,
                                              p_receiveraccount => p_account_receiver,
                                              p_receiverbankid  => l_receiverbankid,
                                              p_amount          => p_outsum,
                                              p_valuedate       => p_valuedate,
                                              p_numberpack      => 90,
                                              p_futurepayeraccount => case when l_receiverbankid = l_payerbankid then p_account_payer else l_schem_corrac end,
                                              p_futurereceiveraccount => case when l_receiverbankid = l_payerbankid then p_account_receiver else l_schem_corrac end
                                              );

    payment_utils.save_pmprop(p_paymentid   => l_paymentid,
                              p_bank_id     => l_payerbankid,
                              p_debetcredit => 0,
                              p_corracc     => l_payercorracc,
                              p_schem_number => -1);

    payment_utils.save_pmprop(p_paymentid   => l_paymentid,
                              p_bank_id     => l_receiverbankid,
                              p_debetcredit => 1,
                              p_corracc     => l_receivercorracc,
                              p_schem_number => l_schem_number);
    
    payment_utils.save_pmrmprop(p_paymentid         => l_paymentid,
                                p_number            => '1',
                                p_date              => p_valuedate,
                                p_payername         => l_clientname,
                                p_payerbankname     => l_payerbankname,
                                p_payercorracc      => l_payercorracc,
                                p_payerinn          => l_clientinn,
                                p_receivername      => l_clientname,
                                p_receiverbankname  => l_receiverbankname,
                                p_receiverinn       => l_clientinn,
                                p_receivercorracc   => l_receivercorracc,
                                p_paydate           => p_valuedate,
                                p_ground            => l_ground);
  end save_payment;

  function create_spground_by_buf (
    p_req_row  dnptxop_req_dbt%rowtype,
    p_code     dspground_dbt.t_xld%type,
    p_partyid  dspground_dbt.t_party%type
  ) return dspground_dbt.t_spgroundid%type is
    l_spground_row dspground_dbt%rowtype;
    l_spground_id  dspground_dbt.t_spgroundid%type;
  begin
    l_spground_row.t_doclog       := 513;
    l_spground_row.t_kind         := 251;
    l_spground_row.t_direction    := 1;
    l_spground_row.t_xld          := p_code;
    l_spground_row.t_registrdate  := p_req_row.req_date;
    l_spground_row.t_registrtime  := p_req_row.req_time;
    l_spground_row.t_party        := p_partyid;
    l_spground_row.t_altxld       := p_req_row.external_id;
    l_spground_row.t_signeddate   := p_req_row.req_date;
    l_spground_row.t_backoffice   := 'S';
    l_spground_row.t_partyname    := nptx_money_read.get_party_name(p_party_id => p_partyid);
    l_spground_row.t_partycode    := nvl(nptx_money_read.get_party_code(p_party_id => p_partyid, p_code_kind => 1), chr(1));
    l_spground_row.t_methodapplic := case when p_req_row.src = 'EFR' then 2 else 1 end;
    
    --default values
    l_spground_row.t_signedtime    := to_date('01.01.0001', 'dd.mm.yyyy');
    l_spground_row.t_proxy         := 0;
    l_spground_row.t_division      := 0;
    l_spground_row.t_references    := 1;
    l_spground_row.t_receptionist  := RsbSessionData.Oper;
    l_spground_row.t_copies        := 0;
    l_spground_row.t_sent          := chr(1);
    l_spground_row.t_deliverykind  := 0;
    l_spground_row.t_comment       := chr(1);
    l_spground_row.t_sourcedocid   := 0;
    l_spground_row.t_sourcedockind := 0;
    l_spground_row.t_doctemplate   := 0;
    l_spground_row.t_terminatedate := to_date('01.01.0001', 'dd.mm.yyyy');
    l_spground_row.t_beginningdate := to_date('01.01.0001', 'dd.mm.yyyy');
    l_spground_row.t_sentdate      := to_date('01.01.0001', 'dd.mm.yyyy');
    l_spground_row.t_senttime      := to_date('01.01.0001', 'dd.mm.yyyy');
    l_spground_row.t_department    := 1;
    l_spground_row.t_branch        := 1;
    l_spground_row.t_parent        := 0;
    l_spground_row.t_userlog       := 0;
    l_spground_row.t_version       := 0;
    l_spground_row.t_ismakeauto    := chr(1);
    l_spground_row.t_techautodoc   := 0;
    l_spground_row.t_deponent      := 0;
    l_spground_row.t_havesubjlist  := chr(1);
    l_spground_row.t_subjectid     := 0;
    l_spground_row.t_registerid    := 0;
    l_spground_row.t_depoacntid    := 0;
    l_spground_row.t_msgnumber     := chr(1);
    l_spground_row.t_msgdate       := to_date('01.01.0001', 'dd.mm.yyyy');
    l_spground_row.t_msgtime       := to_date('01.01.0001', 'dd.mm.yyyy');
    
    insert into dspground_dbt values l_spground_row
    returning t_spgroundid into l_spground_id;
    
    return l_spground_id;
  end create_spground_by_buf;
  
  procedure link_spground_to_nptxop (
    p_doc_id    dspgrdoc_dbt.t_sourcedocid%type,
    p_ground_id dspgrdoc_dbt.t_spgroundid%type
  ) is
  begin
    insert into dspgrdoc_dbt (t_sourcedockind,
                              t_sourcedocid,
                              t_spgroundid,
                              t_order,
                              t_debitcredit,
                              t_status,
                              t_version)
    values (nptx_money_read.dockind,
            p_doc_id,
            p_ground_id,
            1,
            0,
            chr(1),
            0);
  end link_spground_to_nptxop;
  
  procedure push_to_auto_executing (
    p_req_id    dnptxop_req_dbt.req_id%type,
    p_op_id     dnptxop_dbt.t_id%type,
    p_client_id dnptxop_dbt.t_client%type
  ) is
  begin
    if nptx_money_read.is_client_allowed_autorun(p_operation_id => p_op_id,
                                                 p_client_id    => p_client_id) = 1
    then
      funcobj_utils.save_task(p_objectid => p_op_id,
                              p_funcid   => nptx_money_read.get_func_run_deal_id,
                              p_param    => to_char(p_client_id));

      set_buf_status(p_req_id    => p_req_id,
                     p_status_id => nptx_money_read.buf_status_executing);
    else
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; client autorun is not allowed');
    end if;
  end push_to_auto_executing;
  
  procedure delete_from_auto_executing (
    p_req_id    dnptxop_req_dbt.req_id%type,
    p_op_id     dnptxop_dbt.t_id%type
  ) is
    pragma autonomous_transaction;
  begin
    funcobj_utils.delete_task(p_objectid => p_op_id,
                              p_funcid   => nptx_money_read.get_func_run_deal_id);

    set_buf_status(p_req_id    => p_req_id,
                   p_status_id => nptx_money_read.buf_status_wait);
    commit;
  end delete_from_auto_executing;

  procedure z______________transfer is
  begin
    null;
  end;

  function save_transfer (
    p_code          dnptxop_dbt.t_code%type,
    p_contract      dnptxop_dbt.t_contract%type,
    p_client        dnptxop_dbt.t_client%type,
    p_currency      dnptxop_dbt.t_currency%type,
    p_account       dnptxop_dbt.t_account%type,
    p_account_tgt   dnptxop_dbt.t_account%type,
    p_outsum        dnptxop_dbt.t_outsum%type,
    p_placekind     dnptxop_dbt.t_placekind%type,
    p_place         dnptxop_dbt.t_place%type,
    p_marketplace   dnptxop_dbt.t_marketplace%type,
    p_marketsector  dnptxop_dbt.t_marketsector%type,
    p_placekind2    dnptxop_dbt.t_placekind2%type,
    p_place2        dnptxop_dbt.t_place2%type,
    p_marketplace2  dnptxop_dbt.t_marketplace2%type,
    p_marketsector2 dnptxop_dbt.t_marketsector2%type
  ) return dnptxop_dbt.t_id%type is
    l_nptxop_id    dnptxop_dbt.t_id%type;
    l_rqacc_id     ddlrqacc_dbt.t_id%type;
  begin
    l_nptxop_id := save_nptxop(
      p_subkind       => nptx_money_read.subkind_transfer,
      p_code          => p_code,
      p_contract      => p_contract,
      p_client        => p_client,
      p_currency      => p_currency,
      p_account       => p_account,
      p_outsum        => p_outsum,
      p_iis           => chr(0),
      p_partial       => chr(0),
      p_placekind     => p_placekind,
      p_place         => p_place,
      p_marketplace   => p_marketplace,
      p_marketsector  => p_marketsector,
      p_placekind2    => p_placekind2,
      p_place2        => p_place2,
      p_marketplace2  => p_marketplace2,
      p_marketsector2 => p_marketsector2,
      p_calcndfl      => chr(0),
      p_flagtax       => chr(0),
      p_closecontr    => chr(0),
      p_accounttax    => chr(0));

    l_rqacc_id := save_rq_acc(p_nptxop_id => l_nptxop_id,
                              p_client_id => p_client,
                              p_currency  => p_currency,
                              p_account   => p_account_tgt,
                              p_bank_id   => 1);

    save_rq(p_nptxop_id => l_nptxop_id,
            p_amount    => p_outsum,
            p_currency  => p_currency,
            p_client_id => p_client,
            p_rqacc_id  => l_rqacc_id);

    return l_nptxop_id;
  end save_transfer;
  
  function check_data_transfer (
    p_req_id       dnptxop_req_dbt.req_id%type,
    p_contract     dnptxop_dbt.t_contract%type,
    p_contract_tgt dnptxop_dbt.t_contract%type,
    p_client       dnptxop_dbt.t_client%type,
    p_currency     dnptxop_dbt.t_currency%type,
    p_account      dnptxop_dbt.t_account%type,
    p_account_tgt  dnptxop_dbt.t_account%type,
    p_ekk          dnptxop_req_dbt.client_code%type,
    p_is_exchange  dnptxop_req_dbt.is_exchange%type,
    p_is_exchange_target  dnptxop_req_dbt.is_exchange_target%type
  ) return dnptxop_err_dbt.error_id%type is
  begin
    if p_contract is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Contract not found');
      return nptx_money_read.buf_err_cntr_not_found;
    end if;

    if p_contract_tgt is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Contract target not found');
      return nptx_money_read.buf_err_cntr_enrl_not_found;
    end if;

    if p_client is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Client not found; contactid = ' || p_contract);
      return nptx_money_read.buf_err_client_not_found;
    end if;

    if p_currency is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Currency not found; contactid = ' || p_contract);
      return nptx_money_read.buf_err_cur_not_found;
    end if;

    if p_account is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Account not found; contractid = ' || p_contract || '; currency = ' || p_currency);
      return nptx_money_read.buf_err_acc_wo_not_found;
    end if;

    if p_account_tgt is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Account not found; contractid = ' || p_contract_tgt || '; currency = ' || p_currency);
      return nptx_money_read.buf_err_acc_enrl_not_found;
    end if;

    if not nptx_money_read.check_ekk_sfcontr(p_sfcontr_id => p_contract_tgt, p_ekk => p_ekk) then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Ekk not matched with contract; contractid = ' || p_contract_tgt);
      return nptx_money_read.buf_err_ekk_not_found;
    end if;

    if p_is_exchange not in (0,1) and nptx_money_read.is_edp_subcontr (p_contract) then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Contract INST is EDP; contractid = ' || p_contract);
      return nptx_money_read.buf_err_cntr_is_edp;
    end if;

    if p_is_exchange_target not in (0,1) and nptx_money_read.is_edp_subcontr (p_contract_tgt) then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Contract INST is EDP; contractid = ' || p_contract_tgt);
      return nptx_money_read.buf_err_cntr_is_edp;
    end if;

    if not nptx_money_read.check_party_sfcontr(p_sfcontr_id => p_contract, p_party_id => p_client) then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; party_id not matched with contract; contractid = ' || p_contract || '; partyid = ' || p_client);
      return nptx_money_read.buf_err_clnt_not_matched_w_cntr;      
    end if;

    if not nptx_money_read.check_party_sfcontr(p_sfcontr_id => p_contract_tgt, p_party_id => p_client) then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; party_id not matched with contract; contractid = ' || p_contract_tgt || '; partyid = ' || p_client);
      return nptx_money_read.buf_err_clnt_not_matched_w_cntr;      
    end if;

    return nptx_money_read.buf_err_no_error;
  end check_data_transfer;
  
  function save_transfer_by_buf (
    p_req_row  dnptxop_req_dbt%rowtype
  ) return number is
    l_log_object      varchar2(100) := 'nptx_money_utils.save_transfer_by_buf';
    l_object_type     number(3) := 131;
    l_contract        dnptxop_dbt.t_contract%type;
    l_contract_tgt    dnptxop_dbt.t_contract%type;
    l_client          dnptxop_dbt.t_client%type;
    l_currency        dnptxop_dbt.t_currency%type;
    l_account         dnptxop_dbt.t_account%type;
    l_account_tgt     dnptxop_dbt.t_account%type;
    l_outsum          dnptxop_dbt.t_outsum%type;
    l_code            dnptxop_dbt.t_code%type;
    l_spground_id     dspground_dbt.t_spgroundid%type;
    l_nptxop_id       dnptxop_dbt.t_id%type;
    l_error_id        dnptxop_err_dbt.error_id%type;
    l_iis             number(1);
  begin
    nptx_money_read.get_buf_data_transfer(p_req_row       => p_req_row,
                                          po_contract     => l_contract,
                                          po_contract_tgt => l_contract_tgt,
                                          po_currency     => l_currency,
                                          po_account      => l_account,
                                          po_account_tgt  => l_account_tgt,
                                          po_outsum       => l_outsum,
                                          po_iis          => l_iis);

    l_client := party_read.get_id_by_cft_code(p_cft_code => p_req_row.client_cft_id);

    l_error_id := check_data_transfer(p_req_id       => p_req_row.req_id,
                                      p_contract     => l_contract,
                                      p_contract_tgt => l_contract_tgt,
                                      p_client       => l_client,
                                      p_currency     => l_currency,
                                      p_account      => l_account,
                                      p_account_tgt  => l_account_tgt,
                                      p_ekk          => p_req_row.client_code,
                                      p_is_exchange  => p_req_row.is_exchange,
                                      p_is_exchange_target  => p_req_row.is_exchange_target);
    if l_error_id != nptx_money_read.buf_err_no_error then
      save_error(p_req_id => p_req_row.req_id, p_error_id => l_error_id);
      
      if l_error_id != nptx_money_read.buf_err_duplicate then
        send_error_mail(p_head => 'Произошла ошибка в работе информационного сервиса неторговых поручений. Не удалось загрузить неторговое поручение в СОФР',
                        p_text => 'Произошла ошибка при попытке завести операцию по поручению: ' || nptx_money_read.get_error_text(p_error_id => l_error_id) || '. '
                               || 'Поручение доступно для просмотра в интерфейсе Буфера Неторговых Поручений, после исправления можно инициировать повторную попытку заведения операции');
      end if;
      return 0;
    end if;

    l_code := user_NPTXFUNC.GenerateOpCodebySyq(pEKK  => p_req_row.client_code,
                                                pYear => to_char(sysdate, 'yyyy'),
                                                pDBO  => nptx_money_read.get_src_name_rus_opcode(p_src_name => p_req_row.src));

    l_nptxop_id := save_transfer(
      p_code          => l_code,
      p_contract      => l_contract,
      p_client        => l_client,
      p_currency      => l_currency,
      p_account       => l_account,
      p_account_tgt   => l_account_tgt,
      p_outsum        => l_outsum,
      p_placekind     => case p_req_row.is_exchange when 0 then nptx_money_read.bank_place_kind else nptx_money_read.rc_place_kind end,
      p_place         => case p_req_row.is_exchange when 0 then nptx_money_read.otc_place else nptx_money_read.nko_nrd_place end,
      p_marketplace   => case p_req_row.is_exchange 
                           when 1 then nptx_money_read.get_micex_id 
                           when 4 then nptx_money_read.get_spbex_id 
                           when 2 then nptx_money_read.get_nrd_id
                           when 3 then nptx_money_read.get_nrd_id 
                           else 0
                         end,
      p_marketsector  => case p_req_row.is_exchange 
                           when 1 then nptx_money_read.main_sector 
                           when 2 then nptx_money_read.forts_sector 
                           when 3 then nptx_money_read.cur_sector
                           else 0 
                         end,
      p_placekind2    => case p_req_row.is_exchange_target 
                           when 1 then nptx_money_read.rc_place_kind 
                           when 2 then nptx_money_read.rc_place_kind
                           when 3 then nptx_money_read.rc_place_kind
                           when 4 then nptx_money_read.rc_place_kind
                           else nptx_money_read.bank_place_kind 
                         end,
      p_place2        => case p_req_row.is_exchange_target when 0 then nptx_money_read.otc_place else nptx_money_read.nko_nrd_place end,
      p_marketplace2  => case p_req_row.is_exchange_target 
                           when 1 then nptx_money_read.get_micex_id 
                           when 4 then nptx_money_read.get_spbex_id 
                           when 2 then nptx_money_read.get_nrd_id
                           when 3 then nptx_money_read.get_nrd_id 
                           else 0 
                         end,
      p_marketsector2 => case p_req_row.is_exchange_target 
                           when 1 then nptx_money_read.main_sector 
                           when 2 then nptx_money_read.forts_sector 
                           when 3 then nptx_money_read.cur_sector
                           else 0 
                         end
    );
      
    l_spground_id := create_spground_by_buf(p_req_row => p_req_row,
                                            p_code    => l_code,
                                            p_partyid => l_client);

    link_spground_to_nptxop(p_doc_id    => l_nptxop_id,
                            p_ground_id => l_spground_id);

    if p_req_row.is_exchange = 0 then
      note_utils.add_new(p_object_type => l_object_type,
                         p_document_id => lpad(l_nptxop_id, 34, '0'),
                         p_note_kind   => 104,
                         p_value       => nptx_money_read.note_enroll_allowed);

      --Enroll allowed = yes
      categ_utils.add_new(p_object_type => l_object_type,
                          p_object      => lpad(l_nptxop_id, 34, '0'),
                          p_group_id    => 103,
                          p_attr_id     => 1);
    end if;
    note_utils.add_new(p_object_type => l_object_type,
                       p_document_id => lpad(l_nptxop_id, 34, '0'),
                       p_note_kind   => 102,
                       p_value       => p_req_row.external_id);

    categ_utils.add_new(p_object_type => l_object_type,
                        p_object      => lpad(l_nptxop_id, 34, '0'),
                        p_group_id    => 102,
                        p_attr_id     => case 
                                          when p_req_row.src = 'EFR' then 1 
                                          when p_req_row.src = 'DBO UL' then 4 
                                          else 3 
                                         end);

    save_buf_nptxop_id(p_req_id    => p_req_row.req_id,
                       p_nptxop_id => l_nptxop_id);

    set_buf_status(p_req_id    => p_req_row.req_id,
                   p_status_id => nptx_money_read.buf_status_wait);

    
    if l_iis = 0 and l_currency = 0 and nptx_money_read.is_allowed_autorun (
                                                p_system_name          => p_req_row.src,
                                                p_exchange_type        => p_req_row.is_exchange,
                                                p_exchange_type_target => p_req_row.is_exchange_target,
                                                p_is_full_rest         => p_req_row.is_full_rest) = 1
    then
      push_to_auto_executing(p_req_id    => p_req_row.req_id,
                             p_op_id     => l_nptxop_id,
                             p_client_id => l_client);
    end if;

    it_log.log_handle(p_object => l_log_object,
                      p_msg    => 'req_id = ' || p_req_row.req_id || '. saved successfully. nptxop_id = ' || l_nptxop_id);
    return 1;
  exception
    when others then
      rollback;
      save_error(p_req_id => p_req_row.req_id, p_error_id => nptx_money_read.buf_err_internal_error);
      send_error_mail(p_head => 'Произошла ошибка в работе информационного сервиса неторговых поручений. Не удалось загрузить неторговое поручение в СОФР',
                      p_text => 'Произошла ошибка при попытке завести операцию по поручению: ' || nptx_money_read.get_error_text(p_error_id => nptx_money_read.buf_err_internal_error) || '. '
                             || 'Поручение доступно для просмотра в интерфейсе Буфера Неторговых Поручений, после исправления можно инициировать повторную попытку заведения операции');
      it_log.log_error(p_object => l_log_object,
                       p_msg    => 'req_id = ' || p_req_row.req_id || '. Error: ' || sqlerrm);
      return 0;
  end save_transfer_by_buf;

  procedure z______________out is
  begin
    null;
  end;

  function save_out (
    p_code          dnptxop_dbt.t_code%type,
    p_contract      dnptxop_dbt.t_contract%type,
    p_client        dnptxop_dbt.t_client%type,
    p_currency      dnptxop_dbt.t_currency%type,
    p_account       dnptxop_dbt.t_account%type,
    p_account_tgt   dnptxop_dbt.t_account%type,
    p_outsum        dnptxop_dbt.t_outsum%type,
    p_placekind     dnptxop_dbt.t_placekind%type,
    p_place         dnptxop_dbt.t_place%type,
    p_dep_code      ddp_dep_dbt.t_name%type,
    p_iis           number,
    p_src           dnptxop_req_dbt.src%type,
    p_isexchange    dnptxop_req_dbt.is_exchange%type
  ) return dnptxop_dbt.t_id%type is
    l_nptxop_id    dnptxop_dbt.t_id%type;
    l_rqacc_id     ddlrqacc_dbt.t_id%type;
    l_account_tax  dnptxop_dbt.t_accounttax%type;
  begin
    l_account_tax := case
                       when party_read.is_legal_entity_clear(p_party_id => p_client) = 1 then
                         chr(1)
                       else
                         nptx_money_read.get_sfcontr_account(p_sfcontr_id => p_contract, p_fiid => 0, p_date => trunc(sysdate))
                     end;

    l_nptxop_id := save_nptxop(
      p_subkind       => nptx_money_read.subkind_out,
      p_code          => p_code,
      p_contract      => p_contract,
      p_client        => p_client,
      p_currency      => p_currency,
      p_account       => p_account,
      p_outsum        => p_outsum,
      p_iis           => case when p_iis = 1 then chr(88) else chr(0) end,
      p_partial       => chr(88),
      p_placekind     => p_placekind,
      p_place         => p_place,
      p_marketplace   => 0,
      p_marketsector  => 0,
      p_placekind2    => 0,
      p_place2        => 0,
      p_marketplace2  => 0,
      p_marketsector2 => 0,
      p_calcndfl      => case p_src when 'DBO UL' then chr(0) else chr(88) end,
      p_flagtax       => case p_src when 'DBO UL' then chr(0) else chr(88) end, 
      p_closecontr    => case when p_iis = 1 then chr(88) else chr(0) end,
      p_accounttax    => l_account_tax);

    if p_src = 'DBO UL' and p_isexchange in (2,3) then
      save_payment( p_documentid        => l_nptxop_id,
                    p_code              => p_code,
                    p_client            => p_client,
                    p_currency          => p_currency,
                    p_account_payer     => p_account,
                    p_account_receiver  => p_account_tgt,
                    p_outsum            => p_outsum,
                    p_dep_receiver      => p_dep_code,
                    p_valuedate         => trunc(sysdate));
    else 
      l_rqacc_id := save_rq_acc(p_nptxop_id => l_nptxop_id,
                                p_client_id => p_client,
                                p_currency  => p_currency,
                                p_account   => p_account_tgt,
                                p_bank_id   => nptx_money_read.get_dep_id_by_code(p_code => p_dep_code));

      save_rq(p_nptxop_id => l_nptxop_id,
              p_amount    => p_outsum,
              p_currency  => p_currency,
              p_client_id => p_client,
              p_rqacc_id  => l_rqacc_id);
    end if;

    return l_nptxop_id;
  end save_out;

  function check_data_out (
    p_req_id       dnptxop_req_dbt.req_id%type,
    p_contract     dnptxop_dbt.t_contract%type,
    p_client       dnptxop_dbt.t_client%type,
    p_currency     dnptxop_dbt.t_currency%type,
    p_account      dnptxop_dbt.t_account%type,
    p_ekk          dnptxop_req_dbt.client_code%type,
    p_is_exchange  dnptxop_req_dbt.is_exchange%type
  ) return dnptxop_err_dbt.error_id%type is
  begin
    if p_contract is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Contract not found');
      return nptx_money_read.buf_err_cntr_not_found;
    end if;

    if p_client is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Client not found; contactid = ' || p_contract);
      return nptx_money_read.buf_err_client_not_found;
    end if;

    if p_currency is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Currency not found; contactid = ' || p_contract);
      return nptx_money_read.buf_err_cur_not_found;
    end if;

    if p_account is null then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Account not found; contractid = ' || p_contract || '; currency = ' || p_currency);
      return nptx_money_read.buf_err_acc_wo_not_found;
    end if;

    if not nptx_money_read.check_ekk_sfcontr(p_sfcontr_id => p_contract, p_ekk => p_ekk) then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Ekk not matched with contract; contractid = ' || p_contract);
      return nptx_money_read.buf_err_ekk_not_found;
    end if;

    if p_is_exchange not in (0,1) and nptx_money_read.is_edp_subcontr (p_contract) then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; Contract INST is EDP; contractid = ' || p_contract);
      return nptx_money_read.buf_err_cntr_is_edp;
    end if;

    if not nptx_money_read.check_party_sfcontr(p_sfcontr_id => p_contract, p_party_id => p_client) then
      it_log.log(p_msg => 'req_id = ' || p_req_id || '; party_id not matched with contract; contractid = ' || p_contract || '; partyid = ' || p_client);
      return nptx_money_read.buf_err_clnt_not_matched_w_cntr;      
    end if;

    return nptx_money_read.buf_err_no_error;
  end check_data_out;

  function save_out_by_buf (
    p_req_row  dnptxop_req_dbt%rowtype
  ) return number is
    l_log_object      varchar2(100) := 'nptx_money_utils.save_out_otc_by_buf';
    l_object_type     number(3) := 131;
    l_contract        dnptxop_dbt.t_contract%type;
    l_client          dnptxop_dbt.t_client%type;
    l_currency        dnptxop_dbt.t_currency%type;
    l_account         dnptxop_dbt.t_account%type;
    l_outsum          dnptxop_dbt.t_outsum%type;
    l_code            dnptxop_dbt.t_code%type;
    l_spground_id     dspground_dbt.t_spgroundid%type;
    l_nptxop_id       dnptxop_dbt.t_id%type;
    l_error_id        dnptxop_err_dbt.error_id%type;
    l_iis             number(1);
    l_place           dnptxop_dbt.t_place%type;
  begin
    nptx_money_read.get_buf_data_out(p_req_row  => p_req_row,
                                    po_contract => l_contract,
                                    po_currency => l_currency,
                                    po_account  => l_account,
                                    po_outsum   => l_outsum,
                                    po_iis      => l_iis);

    l_client := party_read.get_id_by_cft_code(p_cft_code => p_req_row.client_cft_id);

    l_error_id := check_data_out(p_req_id   => p_req_row.req_id,
                                 p_contract => l_contract,
                                 p_client   => l_client,
                                 p_currency => l_currency,
                                 p_account  => l_account,
                                 p_ekk      => p_req_row.client_code,
                                 p_is_exchange  => p_req_row.is_exchange);
    if l_error_id != nptx_money_read.buf_err_no_error then
      save_error(p_req_id => p_req_row.req_id, p_error_id => l_error_id);
      if l_error_id != nptx_money_read.buf_err_duplicate then
        send_error_mail(p_head => 'Произошла ошибка в работе информационного сервиса неторговых поручений. Не удалось загрузить неторговое поручение в СОФР',
                        p_text => 'Произошла ошибка при попытке завести операцию по поручению: ' || nptx_money_read.get_error_text(p_error_id => l_error_id) || '. '
                               || 'Поручение доступно для просмотра в интерфейсе Буфера Неторговых Поручений, после исправления можно инициировать повторную попытку заведения операции');
      end if;
      return 0;
    end if;

    l_code := user_NPTXFUNC.GenerateOpCodebySyq(pEKK  => p_req_row.client_code,
                                                pYear => to_char(sysdate, 'yyyy'),
                                                pDBO  => nptx_money_read.get_src_name_rus_opcode(p_src_name => p_req_row.src));
      
    if p_req_row.src = 'DBO UL' then 
       l_place :=  nptx_money_read.get_dep_id_by_code(p_code => substr(p_req_row.department, 1, 2) || '00');
    else 
       l_place := RsbSessionData.OperDprt;
    end if;
       
    l_nptxop_id := save_out(
      p_code          => l_code,
      p_contract      => l_contract,
      p_client        => l_client,
      p_currency      => l_currency,
      p_account       => l_account,
      p_account_tgt   => p_req_row.enroll_account,
      p_outsum        => l_outsum,
      p_placekind     => nptx_money_read.bank_place_kind,
      p_place         => l_place,
      p_dep_code      => substr(p_req_row.department, 1, 2) || '00',
      p_iis           => l_iis,
      p_src           => p_req_row.src,
      p_isexchange    => p_req_row.is_exchange
     );
      
    l_spground_id := create_spground_by_buf(p_req_row => p_req_row,
                                            p_code    => l_code,
                                            p_partyid => l_client);

    link_spground_to_nptxop(p_doc_id    => l_nptxop_id,
                            p_ground_id => l_spground_id);

    note_utils.add_new(p_object_type => l_object_type,
                       p_document_id => lpad(l_nptxop_id, 34, '0'),
                       p_note_kind   => 102,
                       p_value       => p_req_row.external_id);

    categ_utils.add_new(p_object_type => l_object_type,
                        p_object      => lpad(l_nptxop_id, 34, '0'),
                        p_group_id    => 102,
                        p_attr_id     => case 
                                          when p_req_row.src = 'EFR' then 1 
                                          when p_req_row.src = 'DBO UL' then 4 
                                          else 3 
                                         end);

    save_buf_nptxop_id(p_req_id    => p_req_row.req_id,
                       p_nptxop_id => l_nptxop_id);

    set_buf_status(p_req_id    => p_req_row.req_id,
                   p_status_id => nptx_money_read.buf_status_wait);

    if l_iis = 0 and l_currency = 0 and nptx_money_read.is_allowed_autorun (
                                                p_system_name          => p_req_row.src,
                                                p_exchange_type        => p_req_row.is_exchange,
                                                p_exchange_type_target => p_req_row.is_exchange_target,
                                                p_is_full_rest         => p_req_row.is_full_rest) = 1
    then
      push_to_auto_executing(p_req_id    => p_req_row.req_id,
                             p_op_id     => l_nptxop_id,
                             p_client_id => l_client);
    end if;

    it_log.log_handle(p_object => l_log_object,
                      p_msg    => 'req_id = ' || p_req_row.req_id || '. saved successfully. nptxop_id = ' || l_nptxop_id);
    return 1;
  exception
    when others then
      rollback;
      save_error(p_req_id => p_req_row.req_id, p_error_id => nptx_money_read.buf_err_internal_error);
      send_error_mail(p_head => 'Произошла ошибка в работе информационного сервиса неторговых поручений. Не удалось загрузить неторговое поручение в СОФР',
                      p_text => 'Произошла ошибка при попытке завести операцию по поручению: ' || nptx_money_read.get_error_text(p_error_id => nptx_money_read.buf_err_internal_error) || '. '
                             || 'Поручение доступно для просмотра в интерфейсе Буфера Неторговых Поручений, после исправления можно инициировать повторную попытку заведения операции');
      it_log.log_error(p_object => l_log_object,
                       p_msg    => 'req_id = ' || p_req_row.req_id || '. Error: ' || sqlerrm);
      return 0;
  end save_out_by_buf;

  procedure z______________start is
  begin
    null;
  end;
  
  function save_operation_by_buf (
    p_req_id dnptxop_req_dbt.req_id%type
  ) return number is
    l_req_row         dnptxop_req_dbt%rowtype;
    l_log_object      varchar2(100) := 'nptx_money_utils.save_operation_by_buf';
  begin
    if set_lock(p_req_id => p_req_id) = 0
    then
      it_log.log_error(p_object => l_log_object,
                       p_msg    => 'req_id = ' || p_req_id || '. Couldnt set exclusive lock');
      return 0;
    end if;

    check_global_params;

    l_req_row := nptx_money_read.get_req_row(p_req_id => p_req_id);
    if l_req_row.req_id is null then
      it_log.log_error(p_object => l_log_object,
                       p_msg    => 'record not found; req_id = ' || p_req_id);
      return 0;
    end if;
    
    --nptx_money_read.buf_status_ready - from ui
    --nptx_money_read.buf_status_creating - from get_req_to_work
    if l_req_row.status_id not in (nptx_money_read.buf_status_ready, nptx_money_read.buf_status_creating) then
      it_log.log_error(p_object => l_log_object,
                       p_msg    => 'bad status; req_id = ' || p_req_id);
      return 0;      
    end if;
    
    return case l_req_row.kind
              when nptx_money_read.buf_kind_out_otc      then save_out_by_buf      (p_req_row => l_req_row)
              when nptx_money_read.buf_kind_out_exchange then save_out_by_buf      (p_req_row => l_req_row)
              when nptx_money_read.buf_kind_transfer     then save_transfer_by_buf (p_req_row => l_req_row)
            else
              0
            end;
  end save_operation_by_buf;

  function save_req_to_buf (
    p_src             dnptxop_req_dbt.src%type,
    p_ext_id          dnptxop_req_dbt.external_id%type,
    p_client_cft_id   dnptxop_req_dbt.client_cft_id%type,
    p_iis             dnptxop_req_dbt.iis%type,
    p_contract        dnptxop_req_dbt.contract%type,
    p_client_code     dnptxop_req_dbt.client_code%type,
    p_is_exchange     dnptxop_req_dbt.is_exchange%type,
    p_is_full_rest    dnptxop_req_dbt.is_full_rest%type,
    p_currency        dnptxop_req_dbt.currency%type,
    p_amount          dnptxop_req_dbt.amount%type,
    p_is_exchange_tgt dnptxop_req_dbt.is_exchange_target%type,
    p_account         dnptxop_req_dbt.enroll_account%type,
    p_department      dnptxop_req_dbt.department%type,
    p_req_date        dnptxop_req_dbt.req_date%type,
    p_req_time        dnptxop_req_dbt.req_time%type,
    p_file_name       dnptxop_req_dbt.file_name%type
  ) return number is
    l_dbegin       date;
    l_dend         date;
    l_result       number := 0;
    l_req_id       dnptxop_req_dbt.req_id%type;
    l_result_op    number := 0;
  begin
    if p_is_exchange_tgt is not null then
      l_req_id := nptx_money_utils.save_transfer_to_buf(p_src             => p_src
                                                       ,p_ext_id          => p_ext_id
                                                       ,p_client_cft_id   => p_client_cft_id
                                                       ,p_contract        => p_contract
                                                       ,p_client_code     => p_client_code
                                                       ,p_is_exchange     => p_is_exchange
                                                       ,p_is_full_rest    => p_is_full_rest
                                                       ,p_currency        => p_currency
                                                       ,p_amount          => p_amount
                                                       ,p_is_exchange_tgt => p_is_exchange_tgt
                                                       ,p_req_date        => p_req_date
                                                       ,p_req_time        => p_req_time
                                                       ,p_file_name       => p_file_name);
    else
      l_req_id := nptx_money_utils.save_out_to_buf(p_src           => p_src
                                                  ,p_ext_id        => p_ext_id
                                                  ,p_client_cft_id => p_client_cft_id
                                                  ,p_iis           => p_iis
                                                  ,p_contract      => p_contract
                                                  ,p_client_code   => p_client_code
                                                  ,p_is_exchange   => p_is_exchange
                                                  ,p_is_full_rest  => p_is_full_rest
                                                  ,p_currency      => p_currency
                                                  ,p_amount        => p_amount
                                                  ,p_account       => p_account
                                                  ,p_department    => p_department
                                                  ,p_req_date      => p_req_date
                                                  ,p_req_time      => p_req_time
                                                  ,p_file_name     => p_file_name);
    end if;

    nptx_money_read.get_allowed_period(p_date    => trunc(sysdate),
                                       po_dbegin => l_dbegin,
                                       po_dend   => l_dend);

    if rsi_rsbcalendar.IsWorkDay(p_Date => sysdate) = 1 and
       sysdate > l_dbegin and
       sysdate < l_dend then 
      /* стартуем операцию, но ее результат не влияет на ответ в исходную систему */
      l_result_op := save_operation_by_buf(p_req_id => l_req_id); 
    end if;
    
    return l_result;
  exception
    when others then
      return 1;
  end save_req_to_buf;
  
  function get_req_to_work
    return dnptxop_req_dbt.req_id%type is
    l_req_id    dnptxop_req_dbt.req_id%type;
    pragma autonomous_transaction;
  begin
    update dnptxop_req_dbt r
       set r.status_id = nptx_money_read.buf_status_creating
     where r.rowid = (select rowid
                        from dnptxop_req_dbt
                       where 0 = case when error_id = 0 and operation_id is null then error_id end --index ind_nptx_req_to_create
                         and status_id = nptx_money_read.buf_status_ready
                        order by req_date, req_time
                        fetch next 1 rows only)
       and r.status_id = nptx_money_read.buf_status_ready
    returning req_id into l_req_id;

    commit;
    return l_req_id;
  exception
    when others then
      rollback;
      it_log.log_error(p_object => 'nptx_money_utils.get_req_to_work',
                       p_msg    => 'req_id = ' || l_req_id || '. Error: ' || sqlerrm);
      return null;
  end get_req_to_work;
  
  function run_move_from_buf
    return number is
    l_cnt        number(9) := 0;
    l_start_time pls_integer;
    l_req_id     dnptxop_req_dbt.req_id%type;
  begin
    l_start_time := dbms_utility.get_time;
    
    loop
      l_req_id := get_req_to_work();
      exit when l_req_id is null;

      l_cnt := l_cnt + save_operation_by_buf(p_req_id => l_req_id);
      commit;
    end loop;

    it_log.log(p_msg => 'finish. time: ' || to_char((dbms_utility.get_time - l_start_time) / 100, 'fm9999999990D00') || '. cnt: ' || l_cnt);
    return l_cnt;
  exception
    when others then
      it_log.log_error(p_object => 'nptx_money_utils.run_move_from_buf',
                       p_msg    => 'Error: ' || sqlerrm);
    return l_cnt;
  end run_move_from_buf;

  procedure send_error_notification(p_proc      varchar2
                                   ,p_errcode   integer
                                   ,p_errtxt    varchar2
                                   ,p_nosupport integer) is
    v_SystemId varchar2(128) := 'СОФР. Неторговые поручения';
  begin
    case p_proc
      when 'SAVE_REQ_FROM_JSON' then
        if it_rs_interface.get_parm_varchar_path(p_parm_path => nptx_money_read.GC_PARAM_LOAD_NOTIFICATION_OD) = chr(88)
        then
          nptx_money_utils.send_error_mail(p_head => 'СОФР. Ошибка загрузки неторгового поручения'
                                          ,p_text => 'Произошла ошибка в работе информационного сервиса неторговых поручений. ' || p_errtxt);
        end if;
        if p_nosupport = 0
           and it_rs_interface.get_parm_varchar_path(p_parm_path => nptx_money_read.GC_PARAM_LOAD_NOTIFICATION_SUPPORT) = chr(88)
        then
          it_event.RegisterError(p_SystemId => v_SystemId, p_ServiceName => 'SendNonTradingOrder', p_ErrorCode => p_errcode, p_ErrorDesc => p_errtxt, p_LevelInfo => 8);
        end if;
      when 'SEND_ORDER_STATUS' then
        if it_rs_interface.get_parm_varchar_path(p_parm_path => nptx_money_read.GC_PARAM_STATUS_NOTIFICATION_OD) = chr(88)
        then
          nptx_money_utils.send_error_mail(p_head => 'СОФР. Ошибка отправки статуса обработки неторгового поручения во фронтальную систему'
                                          ,p_text => 'Произошла ошибка в работе информационного сервиса неторговых поручений. ' || p_errtxt);
        end if;
        if p_nosupport = 0
           and it_rs_interface.get_parm_varchar_path(p_parm_path => nptx_money_read.GC_PARAM_STATUS_NOTIFICATION_SUPPORT) = chr(88)
        then
          it_event.RegisterError(p_SystemId => v_SystemId, p_ServiceName => 'SendNonTradingOrderStatusReg', p_ErrorCode => p_errcode, p_ErrorDesc => p_errtxt, p_LevelInfo => 8);
        end if;
      when 'RESP_STATUS_MESSAGE' then
        if it_rs_interface.get_parm_varchar_path(p_parm_path => nptx_money_read.GC_PARAM_STATUS_NOTIFICATION_OD) = chr(88)
        then
          nptx_money_utils.send_error_mail(p_head => 'СОФР. Получена ошибка обработки статуса неторгового поручения из фронтальной системы'
                                          ,p_text => 'Произошла ошибка в работе информационного сервиса неторговых поручений. ' || p_errtxt);
        end if;
        if p_nosupport = 0
           and it_rs_interface.get_parm_varchar_path(p_parm_path => nptx_money_read.GC_PARAM_STATUS_NOTIFICATION_SUPPORT) = chr(88)
        then
          it_event.RegisterError(p_SystemId => v_SystemId, p_ServiceName => 'SendNonTradingOrderStatusResp', p_ErrorCode => p_errcode, p_ErrorDesc => p_errtxt, p_LevelInfo => 8);
        end if;
    end case;
  end send_error_notification;
  
  -- формирования данных для сообщения о статусе 
  procedure send_order_status(p_operid integer -- Идентификатор T_ID из DNPTXOP_DBT
                             ,p_status integer -- Статус поручения 2 - Исполнено 4 - Отклонено
                              ) as
    v_ErrorCode integer;
    v_ErrorDesc varchar2(2000);
  begin
    send_order_status_errtxt(p_operid => p_operid, p_status => p_status, p_automatic => 1, o_ErrorCode => v_ErrorCode, o_ErrorDesc => v_ErrorDesc);
    if nvl(v_ErrorCode,0) != 0 then 
         it_log.log(p_msg => 'SEND_ORDER_STATUS  ' || p_operid||' p_status='||p_status||' Error#'||v_ErrorCode||':'||v_ErrorDesc, p_msg_type => it_log.C_MSG_TYPE__ERROR);
    end if;
  end send_order_status;

  procedure send_order_status_errtxt(p_operid    integer -- Идентификатор T_ID из DNPTXOP_DBT
                                    ,p_status    integer -- Статус поручения 2 - Исполнено 4 - Отклонено
                                    ,p_automatic integer default 0 -- 1 - безинтерфейсный запуск 
                                    ,o_ErrorCode out integer
                                    ,o_ErrorDesc out varchar2) as
    l_req_row        dnptxop_req_dbt%rowtype;
    l_src            varchar2(128);
    l_src_ext_name   varchar2(128);
    l_external_id    dnptxop_req_dbt.external_id%type;
    l_str            varchar2(4000);
    v_decline_reason dnptxop_err_dbt.error_name%type;
    v_clientid       dobjcode_dbt.t_code%type;
  begin
    o_ErrorCode := 0;
    l_req_row   := nptx_money_read.get_req_row(p_oper_id => p_operid);
    if l_req_row.req_id is null
    then
      select max(op.t_code) into l_str from DNPTXOP_DBT op where op.t_id = p_operid;
      l_str := substr(l_str, instr(l_str, '/', -1) + 1);
      l_src := nptx_money_read.get_src_name(p_src_name => l_str);

      select t_altxld
        into l_str
        from (select *
                from (select spg.t_spgroundid
                            ,gr.t_altxld
                            ,min(spg.t_order) over() min_order
                            ,spg.t_order
                        from dspgrdoc_dbt spg
                        join dspground_dbt gr
                          on gr.t_spgroundid = spg.t_spgroundid
                       where spg.t_sourcedocid = p_operid)
               where min_order = t_order
               order by t_spgroundid)
       where rownum < 2;
      if nvl(l_str, 'б/н') != 'б/н'
      then
        l_external_id := l_str;
      end if;
    else
      l_src         := l_req_row.src;
      l_external_id := l_req_row.external_id;
    end if;

    l_src_ext_name := nptx_money_read.get_src_ext_name(p_src_name => l_src);
    if l_src is null
       or l_src_ext_name is null
       or nptx_money_read.is_allowed_send_status(l_src_ext_name) = 0
    then
      o_ErrorCode := 1;
      o_ErrorDesc := 'Источик поручения не в списке систем для отправки статуса. ';
      if p_automatic != 0
      then
        return;
      end if;
    end if;

    if o_ErrorCode = 0
       and l_external_id is null
    then
      o_ErrorCode := 2;
      o_ErrorDesc := 'Не определен идентификатор документа из внешней системы. ';
      if p_automatic != 0
      then
        return;
      end if;
    end if;

    if o_ErrorCode = 0
    then
      begin
        select s.t_code
          into v_clientid
          from Dnptxop_dbt op
          join dobjcode_dbt s
            on (s.t_objecttype = 3 and s.t_codekind = 101 and s.t_state = 0 and s.t_objectid = op.t_client)
         where op.t_id = p_operid
           and rownum < 2;
      exception
        when no_data_found then
          o_ErrorCode := 3;
          o_ErrorDesc := 'Не определен ЦФТ-id клиента ';
      end;
    end if;

    if o_ErrorCode = 0
    then
      if p_status = nptx_money_read.buf_status_reject
      then
        v_decline_reason := note_utils.GetTextID34(p_object_type => 131,
                                               p_id => p_operid,
                                               p_note_kind => 103);
      end if;
      it_frontsystems.form_status_message_json(p_system_origin  =>l_src_ext_name --  "ДБО ФЛ" или "ЕФР" в зависимости от источника
                                              ,p_status         => nptx_money_read.get_status_name(p_status) --  Текстовая расшифровка числового значения из send_order_status()
                                              ,p_decline_reason => v_decline_reason -- Причина отклонения. Определяется по примечанию вида 103 "Отметка об отказе в исполнении"
                                              ,p_clientid       => v_clientid -- ЦФТ-id клиента из поручения. Определяется по коду вида 101 для клиента из DNPTXOP.T_CLIENT
                                              ,p_orderid        => l_external_id -- Идентификатор поручения из внешней системы.
                                              ,p_operid         => p_operid -- Идентификатор T_ID из DNPTXOP_DBT
                                              ,o_ErrorCode      => o_ErrorCode
                                              ,o_ErrorDesc      => o_ErrorDesc);
    end if;

    if o_ErrorCode != 0
    then
      if p_automatic != 0
      then
        send_error_notification(p_proc => 'SEND_ORDER_STATUS', p_errcode => o_ErrorCode, p_errtxt => o_ErrorDesc || ' OPER_ID:' || p_operid, p_nosupport => 1);
      end if;
    else
      o_ErrorDesc := 'Сообщение о статусе <' || nptx_money_read.get_status_name(p_status) || '>' || case
                       when v_decline_reason is not null then
                        '(' || v_decline_reason || ')'
                     end || ' отправлено в ' || l_src_ext_name;
    end if;
  exception
    when others then
      declare
        v_SQLCODE integer;
      begin
        v_SQLCODE := abs(sqlcode);
        it_error.put_error_in_stack;
        it_log.log(p_msg => 'SEND_ORDER_STATUS  ' || p_operid, p_msg_type => it_log.C_MSG_TYPE__ERROR);
        it_error.clear_error_stack;
        o_ErrorCode := v_SQLCODE;
        o_ErrorDesc := 'Ошибка отправки статуса обработки неторгового поручения во фронтальную систему OPER_ID:' || p_operid;
        if p_automatic != 0
        then
          send_error_notification(p_proc => 'SEND_ORDER_STATUS', p_errcode => o_ErrorCode, p_errtxt => o_ErrorDesc, p_nosupport => 0);
        end if;
      end;
  end send_order_status_errtxt;
  
  -- Проверка получения ответа через час на сообщение об изменении статуса запускается сервисом ExecuteCode
  procedure service_chk_send_order_status_resp(p_worklogid integer
                                              ,p_messmeta  xmltype) as
    vr_message     itt_q_message_log%rowtype;
    vr_messageReq  itt_q_message_log%rowtype;
    vr_messageResp itt_q_message_log%rowtype;
    v_SystemOrigin varchar2(1000);
    vx_metaReq     xmltype;
    v_nptxop_id    number;
  begin
    vr_message := it_q_message.messlog_get(p_logid => p_worklogid);
    if vr_message.corrmsgid is not null
    then
      vr_messageReq := it_q_message.messlog_get(p_msgid => vr_message.corrmsgid,p_queuetype => it_q_message.C_C_QUEUE_TYPE_OUT);
      if vr_messageReq.Log_Id is not null
      then
        vx_metaReq := it_xml.Clob_to_xml(vr_messageReq.Messmeta);
        if vx_metaReq is not null
        then
          select to_number(EXTRACTVALUE(vx_metaReq, '/XML/@OperID') default null on conversion error) into v_nptxop_id from dual;
        end if;
        with json as
         (select vr_messageReq.Messbody txt from dual)
        select json_value(json.txt, '$."SendNonTradingOrderStatusReq".SystemOrigin') into v_SystemOrigin from json;
        vr_messageResp := it_q_message.get_answer_msg(p_msgid => vr_messageReq.Msgid);
        if vr_messageResp.Log_Id is null
        then
          nptx_money_utils.send_error_notification(p_proc => 'RESP_STATUS_MESSAGE'
                                                  ,p_errcode => 101
                                                  ,p_errtxt => 'Не получен ответ из системы ' || v_SystemOrigin || ' по ID операции ' || v_nptxop_id
                                                  ,p_nosupport => 1);
        end if;
      end if;
    end if;
  end service_chk_send_order_status_resp;
  
end nptx_money_utils;
/

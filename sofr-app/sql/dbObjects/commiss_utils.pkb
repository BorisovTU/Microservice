create or replace package body commiss_utils as

  procedure insert_dsdef (
    p_id               dsfdef_dbt.t_id%type,
    p_feetype          dsfdef_dbt.t_feetype%type,
    p_commnumber       dsfdef_dbt.t_commnumber%type,
    p_fiid             dsfdef_dbt.t_fiid_sum%type,
    p_sum              dsfdef_dbt.t_sum%type,
    p_sfcontrid        dsfdef_dbt.t_sfcontrid%type,
    p_datefee          dsfdef_dbt.t_datefee%type,
    p_basequant        dsfdef_dbt.t_basequant%type,
    p_code             dsfdef_dbt.t_code%type,
    p_calccomisssumalg dsfdef_dbt.t_calccomisssumalg%type,
    p_concomid         dsfdef_dbt.t_concomid%type
  ) is
  begin
    insert into dsfdef_dbt (t_feetype,
                            t_id,
                            t_commnumber,
                            t_status,
                            t_fiid_sum,
                            t_sum,
                            t_sumnds,
                            t_sfcontrid,
                            t_department,
                            t_ndsratevalue,
                            t_isincluded,
                            t_facturaid,
                            t_datefee,
                            t_basesum,
                            t_fiid_basesum,
                            t_percent,
                            t_sum_per_unit,
                            t_fiid_tarscl,
                            t_invoiceid,
                            t_basequant,
                            t_datepay,
                            t_dateperiodbegin,
                            t_dateperiodend,
                            t_spgroundid,
                            t_extcomid,
                            t_comment,
                            t_skipedbymacro,
                            t_id_operation,
                            t_id_step,
                            t_id_paystep,
                            t_oprcommnumber,
                            t_isdoc,
                            t_fiidpayer,
                            t_accountpayer,
                            t_oper,
                            t_code,
                            t_userfield1,
                            t_userfield2,
                            t_userfield3,
                            t_userfield4,
                            t_closedate,
                            t_fiid_paysum,
                            t_calccomisssumalg,
                            t_concomid,
                            t_discountid,
                            t_nds_quanted)
    values (p_feetype,
            p_id,
            p_commnumber,
            commiss_read.oper_status_deffered,
            p_fiid,
            p_sum,
            0,
            p_sfcontrid,
            nvl(Rsbsessiondata.OperDprt, 1),
            0,
            chr(0),
            0,
            p_datefee,
            0,
            p_fiid,
            0,
            0,
            -1,
            0,
            p_basequant,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            0,
            0,
            chr(0),
            chr(0),
            0,
            0,
            0,
            0,
            chr(0),
            p_fiid,
            chr(0),
            nvl(Rsbsessiondata.oper, 1),
            p_code,
            chr(0),
            chr(0),
            chr(0),
            chr(0),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            p_fiid,
            p_calccomisssumalg,
            p_concomid,
            0,
            chr(0));
  end insert_dsdef;
  
  --что-то наподбии требований/обязательств
  procedure insert_dsfsi (
    p_objecttype         dsfsi_dbt.t_objecttype%type,
    p_objectid           dsfsi_dbt.t_objectid%type,
    p_debetcredit        dsfsi_dbt.t_debetcredit%type,
    p_partyid            dsfsi_dbt.t_partyid%type,
    p_partycodekind      dsfsi_dbt.t_partycodekind%type,
    p_partycode          dsfsi_dbt.t_partycode%type,
    p_partyname          dsfsi_dbt.t_partyname%type,
    p_partyinn           dsfsi_dbt.t_partyinn%type,
    p_bankid             dsfsi_dbt.t_bankid%type,
    p_bankcodekind       dsfsi_dbt.t_bankcodekind%type,
    p_bankcode           dsfsi_dbt.t_bankcode%type,
    p_bankname           dsfsi_dbt.t_bankname%type,
    p_fiid               dsfsi_dbt.t_fiid%type,
    p_account            dsfsi_dbt.t_account%type
  ) is
  
  begin
    insert into dsfsi_dbt (t_objecttype,
                           t_objectid,
                           t_debetcredit,
                           t_partyid,
                           t_partycodekind,
                           t_partycode,
                           t_partyname,
                           t_partyinn,
                           t_bankid,
                           t_bankcodekind,
                           t_bankcode,
                           t_bankname,
                           t_corracc,
                           t_bankcorrid,
                           t_bankcorrcodekind,
                           t_bankcorrcode,
                           t_bankcorrname,
                           t_fiid,
                           t_account,
                           t_istransitacc,
                           t_transitfiid,
                           t_transitaccount,
                           t_receiverndsaccount,
                           t_noaccept,
                           t_department,
                           t_reserve)
    values (p_objecttype,
            p_objectid,
            p_debetcredit,
            p_partyid,
            p_partycodekind,
            p_partycode,
            p_partyname,
            p_partyinn,
            p_bankid,
            p_bankcodekind,
            p_bankcode,
            p_bankname,
            chr(0),
            -1,
            0,
            chr(0),
            chr(0),
            p_fiid,
            p_account,
            chr(0),
            -1,
            chr(0),
            chr(0),
            chr(0),
            nvl(Rsbsessiondata.OperDprt, 1),
            chr(0)
           );
  end insert_dsfsi;

  --На данном этапе заполнение счёта не нужно, поскольку счёт будет вычисляться на шаге операции через категории учёта
  procedure save_dsfsi_by_contr (
    p_objecttype    dsfsi_dbt.t_objecttype%type,
    p_objectid      dsfsi_dbt.t_objectid%type,
    p_debetcredit   dsfsi_dbt.t_debetcredit%type,
    p_sfcontrid     dsfcontr_dbt.t_id%type,
    p_fiid          dsfsi_dbt.t_fiid%type
  ) is
    l_partycodekind dsfsi_dbt.t_partycodekind%type := 1;
    l_setacc_row    dsettacc_dbt%rowtype;
  begin
    l_setacc_row := setacc_read.get_setacc_row_by_id(p_setacc_id => sfcontr_read.get_setacc_id_by_contr(p_sfcontr_id => p_sfcontrid, p_fiid => p_fiid));
  
    insert_dsfsi(p_objecttype    => p_objecttype,
                 p_objectid      => p_objectid,
                 p_debetcredit   => p_debetcredit,
                 p_partyid       => l_setacc_row.t_partyid,
                 p_partycodekind => l_partycodekind,
                 p_partycode     => party_read.get_party_code(p_party_id => l_setacc_row.t_partyid, p_code_kind => l_partycodekind),
                 p_partyname     => party_read.get_party_name(p_partyid => l_setacc_row.t_partyid),
                 p_partyinn      => l_setacc_row.t_inn,
                 p_bankid        => l_setacc_row.t_bankid,
                 p_bankcodekind  => l_setacc_row.t_bankcodekind,
                 p_bankcode      => l_setacc_row.t_bankcode,
                 p_bankname      => l_setacc_row.t_bankname,
                 p_fiid          => p_fiid,
                 p_account       => chr(1)
                 );
  end save_dsfsi_by_contr;

  procedure save_dsfsi_by_party (
    p_objecttype    dsfsi_dbt.t_objecttype%type,
    p_objectid      dsfsi_dbt.t_objectid%type,
    p_debetcredit   dsfsi_dbt.t_debetcredit%type,
    p_partyid       dsfsi_dbt.t_partyid%type,
    p_fiid          dsfsi_dbt.t_fiid%type
  ) is
    l_partycodekind dsfsi_dbt.t_partycodekind%type := 1;
    l_setacc_row    dsettacc_dbt%rowtype;
  begin
    l_setacc_row := setacc_read.get_setacc_row(p_partyid => p_partyid,
                                               p_fiid    => p_fiid,
                                               p_chapter => 1);

    insert_dsfsi(p_objecttype    => p_objecttype,
                 p_objectid      => p_objectid,
                 p_debetcredit   => p_debetcredit,
                 p_partyid       => p_partyid,
                 p_partycodekind => l_partycodekind,
                 p_partycode     => party_read.get_party_code(p_party_id => p_partyid, p_code_kind => l_partycodekind),
                 p_partyname     => party_read.get_party_name(p_partyid => p_partyid),
                 p_partyinn      => l_setacc_row.t_inn,
                 p_bankid        => l_setacc_row.t_bankid,
                 p_bankcodekind  => l_setacc_row.t_bankcodekind,
                 p_bankcode      => l_setacc_row.t_bankcode,
                 p_bankname      => l_setacc_row.t_bankname,
                 p_fiid          => p_fiid,
                 p_account       => chr(1)
                 );
  end save_dsfsi_by_party;
  
  procedure save_additional_info (
    p_feetype      sf_commiss_additional_info.feetype%type,
    p_sfdef_id     sf_commiss_additional_info.sfdef_id%type,
    p_service_type sf_commiss_additional_info.contract_service_type%type
  ) is
  begin
    merge into sf_commiss_additional_info i
    using dual
    on (i.feetype = p_feetype and
        i.sfdef_id = p_sfdef_id)
    when matched then update
      set i.contract_service_type = p_service_type
    when not matched then insert (feetype,
                                  sfdef_id,
                                  contract_service_type)
      values (p_feetype,
              p_sfdef_id,
              p_service_type);
  end save_additional_info;

  /* Сохранение разовой комиссии
    Есть большой костыль для случая, когда банк собирает с клиентов комиссию у себя и от своего имени платит бирже.
    для таких целей подставляется договор обслуживания ид = 25094, где клиент - ммвб
    Через интерфейс такую операцию завести, скоре всего, невозможно

    dsfsi - что-то на подобии требования/обязательств. Фактически, не хранит в себе какой-либо значимой информации.
    Можно рассмотреть вариант не заполнять эту таблицу. При условии, что комиссия будет отображаться в интерфейсе
  */
  procedure save_one_time_commiss (
    pio_id              in out dsfdef_dbt.t_id%type,
    p_feetype                  dsfdef_dbt.t_feetype%type,
    p_commnumber               dsfdef_dbt.t_commnumber%type,
    p_code                     dsfdef_dbt.t_code%type,
    p_sum                      dsfdef_dbt.t_sum%type,
    p_fiid                     dsfdef_dbt.t_fiid_sum%type,
    p_date                     dsfdef_dbt.t_datefee%type,
    p_payer_contract_id        dsfdef_dbt.t_sfcontrid%type,
    p_service_type             sf_commiss_additional_info.contract_service_type%type
  ) is
     l_concom_id    dsfdef_dbt.t_concomid%type;
     l_sfplan_id    dsfplan_dbt.t_sfplanid%type;
     l_service_type sf_commiss_additional_info.contract_service_type%type;
  begin
    if pio_id is null then
      --костыль
      if p_payer_contract_id = 25094 then
        l_sfplan_id := 0;
        l_service_type := p_service_type;
      else
        l_sfplan_id := commiss_read.get_sfcontr_plan_id(p_sfcontr_id => p_payer_contract_id,
                                                        p_date       => p_date);
        l_service_type := commiss_read.get_service_type_by_contract(p_sfcontr_id => p_payer_contract_id);
      end if;
        
      l_concom_id := commiss_read.get_concom_id(p_sfcontr_id => p_payer_contract_id,
                                                p_feetype    => p_feetype,
                                                p_commnumber => p_commnumber,
                                                p_sfplan_id  => l_sfplan_id,
                                                p_date       => p_date);

      if l_concom_id is null then
        raise_application_error(-20000, 'cant found concom id. ' ||
                                        '; p_payer_contract_id = ' || p_payer_contract_id ||
                                        '; p_feetype = ' || p_feetype ||
                                        '; p_commnumber = ' || p_commnumber);
      end if;

      pio_id := dsfdef_dbt_single_seq.nextval;
      insert_dsdef(p_id               => pio_id,
                   p_feetype          => p_feetype,
                   p_commnumber       => p_commnumber,
                   p_fiid             => p_fiid,
                   p_sum              => p_sum,
                   p_sfcontrid        => p_payer_contract_id,
                   p_datefee          => p_date,
                   p_basequant        => 1,
                   p_code             => p_code,
                   p_calccomisssumalg => commiss_read.calccommissalg_calc,
                   p_concomid         => l_concom_id
                   );

    --Первая половина ТО заполняется по договору плательщика комиссии
    save_dsfsi_by_contr(p_objecttype  => commiss_read.once_time_com_objecttype,
                        p_objectid    => lpad(pio_id, 10, '0'),
                        p_debetcredit => 0,
                        p_sfcontrid   => p_payer_contract_id,
                        p_fiid        => p_fiid
                        );

    --вторая половина зполняется по спи субъекта, который указан в получателях комиссии
    save_dsfsi_by_party(p_objecttype  => commiss_read.once_time_com_objecttype,
                        p_objectid    => lpad(pio_id, 10, '0'),
                        p_debetcredit => 1,
                        p_partyid     => commiss_read.get_receiver_id(p_feetype => p_feetype, p_commnumber => p_commnumber),
                        p_fiid        => p_fiid
                        );
    end if;
    
    save_additional_info(p_feetype      => p_feetype,
                         p_sfdef_id     => pio_id,
                         p_service_type => l_service_type);
    
  exception
    when others then
      it_log.log_error(p_object   => 'commiss_utils.save_one_time_commiss',
                       p_msg      => 'commiss wasnt created',
                       p_msg_clob => sqlerrm);
      raise;
  end save_one_time_commiss;

end commiss_utils;
/

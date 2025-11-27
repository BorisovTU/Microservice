create or replace package body secur_deals_utils as

  --should be in party_read
  function get_party_name (
    p_partyid dparty_dbt.t_partyid%type
  ) return dparty_dbt.t_name%type is
    l_name dparty_dbt.t_name%type;
  begin
    select p.t_name
      into l_name
      from dparty_dbt p
     where p.t_partyid = p_partyid;

    return l_name;
  exception
    when no_data_found then
      return null;
  end get_party_name;

  procedure save_client_request (
    pio_req_id in out ddl_req_dbt.t_id%type,
    p_code            ddl_req_dbt.t_code%type,
    p_codets          ddl_req_dbt.t_codets%type,
    p_date            ddl_req_dbt.t_date%type,
    p_time            ddl_req_dbt.t_time%type,
    p_party           ddl_req_dbt.t_party%type,
    p_fiid            ddl_req_dbt.t_fiid%type,
    p_amount          ddl_req_dbt.t_amount%type,
    p_price           ddl_req_dbt.t_price%type,
    p_sourceid        ddl_req_dbt.t_sourceid%type,
    p_contract        ddl_req_dbt.t_clientcontr%type,
    p_status          ddl_req_dbt.t_status%type,
    p_direction       ddl_req_dbt.t_direction%type
  ) is
    l_spground_id dspground_dbt.t_spgroundid%type;
    l_client_id   dparty_dbt.t_partyid%type;
  begin
    if pio_req_id is null then
      l_client_id := sfcontr_read.get_client_by_contract(p_sfcontr_id => p_contract);
      dlreq_utils.save_dlreq(pio_id       => pio_req_id,
                             p_kind       => 350, --todo: constant
                             p_code       => p_code,
                             p_codets     => p_codets,
                             p_date       => trunc(p_date),
                             p_time       => p_time,
                             p_party      => p_party,
                             p_client     => l_client_id,
                             p_fiid       => p_fiid,
                             p_amount     => p_amount,
                             p_price      => p_price,
                             p_pricefiid  => 0,
                             p_sourcekind => 101, --todo:constant
                             p_sourceid   => p_sourceid,
                             p_contract   => p_contract,
                             p_status     => p_status,
                             p_direction  => p_direction); --??

      spground_utils.save_ground(pio_spgroundid => l_spground_id,
                                 p_doclog       => 513,
                                 p_kind         => 251,
                                 p_direction    => p_direction, --??
                                 p_registrdate  => trunc(p_date),
                                 p_registrtime  => p_time,
                                 p_xld          => p_codets,
                                 p_altxld       => p_codets,
                                 p_signeddate   => trunc(p_date),
                                 p_backoffice   => 'S',
                                 p_party        => l_client_id,
                                 p_partyname    => get_party_name(p_partyid => l_client_id),
                                 p_partycode    => party_read.get_party_code(p_party_id => l_client_id, p_code_kind => 1),
                                 p_methodapplic => 1); ----todo: convert to constant. select * from dnamealg_dbt where t_itypealg = 3172

      spground_utils.link_spground_to_doc(p_sourcedockind => 350, --todo:constant
                                          p_sourcedocid   => pio_req_id,
                                          p_spgroundid    => l_spground_id);
    end if;

  exception
    when others then
      it_log.log_error(p_object => 'secur_deals_utils.save_client_request',
                       p_msg    => 'Error ' || sqlerrm);
      raise_application_error(-20000, 'Error ' || sqlerrm, true);
  end save_client_request;
  
  function is_substitution_deal (
    p_dealid ddl_tick_dbt.t_dealid%type
  ) return number is
  begin
    return nvl(categ_read.get_attr_id(p_object_type => 101, --todo:constant
                                      p_group_id    => 120, --todo:constant
                                      p_object      => lpad(p_dealid, 34, '0'),
                                      p_date        => null), 0);
  end is_substitution_deal;
  
  function get_otc_to_match_tick_id (
    p_clientid ddl_tick_dbt.t_clientid%type,
    p_pfi      ddl_tick_dbt.t_pfi%type
  ) return ddl_tick_dbt.t_dealid%type is
    l_id ddl_tick_dbt.t_dealid%type;
  begin
    select t_dealid
      into l_id
      from ddl_tick_dbt t
     where t.t_bofficekind = 101 --todo:constant
       and t.t_clientid = p_clientid
       and t.t_pfi = p_pfi
       and is_substitution_deal(p_dealid => t.t_dealid) = 1
       and not exists (select 1
                         from dspgrdoc_dbt d
                        where d.t_sourcedockind = t.t_bofficekind
                          and d.t_sourcedocid = t.t_dealid);

    return l_id;
  exception
    when too_many_rows then
      return -1;
    when no_data_found then
      return null;
  end get_otc_to_match_tick_id;
  
  function get_tick_row (
    p_dealid ddl_tick_dbt.t_dealid%type
  ) return ddl_tick_dbt%rowtype is
    l_tick_row ddl_tick_dbt%rowtype;
  begin
    select *
      into l_tick_row
      from ddl_tick_dbt t
     where t.t_dealid = p_dealid;

    return l_tick_row;
  exception
    when no_data_found then
      return null;
  end get_tick_row;
  
  function get_otc_not_matched_request_count return number is
    l_cnt number(9);
  begin
    select count(1)
      into l_cnt
      from ddl_req_dbt r
     where r.t_kind = 350 --todo: constant
       and r.t_code like 'НП%';

    return l_cnt;
  end get_otc_not_matched_request_count;

  procedure math_otc_requests_w_deals (
    po_cnt_all            out number,
    po_cnt_too_many_deals out number,
    po_cnt_not_found_deal out number,
    po_cnt_success        out number
  ) is
    l_req_kind        number(3) := 350; --todo: constant
    l_tick_id         ddl_tick_dbt.t_dealid%type;
    l_tick_row        ddl_tick_dbt%rowtype;
    l_req_id          ddl_req_dbt.t_id%type;
    l_spground_id     dspground_dbt.t_spgroundid%type;
    l_spground_row    dspground_dbt%rowtype;
    l_otc_request_cnt number(9);
  begin
    po_cnt_all            := 0;
    po_cnt_too_many_deals := 0;
    po_cnt_not_found_deal := 0;
    po_cnt_success        := 0;

    for r in (select *
                from ddl_req_dbt r
               where r.t_kind = l_req_kind
                 and r.t_code like 'BlockedOTC%'
                 and r.t_status is null
                 and dlreq_utils.is_substitution_secur(p_id => r.t_id) = 1)
    loop
      po_cnt_all := po_cnt_all + 1;
      l_tick_id := get_otc_to_match_tick_id(p_clientid => r.t_client, p_pfi => r.t_fiid);
      l_req_id := r.t_id;

      if l_tick_id = -1 then
        po_cnt_too_many_deals := po_cnt_too_many_deals + 1;
        it_log.log(p_msg => 'found too many deals to match request with id = ' || r.t_id, p_msg_type => it_log.C_MSG_TYPE__ERROR);
        continue;
      elsif l_tick_id is null then
        po_cnt_not_found_deal := po_cnt_not_found_deal + 1;
        l_otc_request_cnt := get_otc_not_matched_request_count();
        dlreq_utils.save_dlreq(pio_id       => l_req_id,
                               p_kind       => l_req_kind,
                               p_code       => 'НП' || to_char(l_otc_request_cnt + 1),
                               p_codets     => 'НП' || to_char(l_otc_request_cnt + 1),
                               p_date       => r.t_date,
                               p_time       => r.t_time,
                               p_party      => r.t_party,
                               p_client     => r.t_client,
                               p_fiid       => r.t_fiid,
                               p_amount     => r.t_amount,
                               p_price      => r.t_price,
                               p_pricefiid  => r.t_pricefiid,
                               p_sourcekind => r.t_sourcekind,
                               p_sourceid   => r.t_sourceid,
                               p_contract   => r.t_clientcontr,
                               p_status     => 'W', --Снята участником торгов todo: constant
                               p_direction  => r.t_direction);
      else
        l_tick_row := get_tick_row(p_dealid => l_tick_id);

        dlreq_utils.save_dlreq(pio_id       => l_req_id,
                               p_kind       => l_req_kind,
                               p_code       => l_tick_row.t_Dealcode,
                               p_codets     => l_tick_row.t_Dealcode,
                               p_date       => r.t_date,
                               p_time       => r.t_time,
                               p_party      => r.t_party,
                               p_client     => r.t_client,
                               p_fiid       => r.t_fiid,
                               p_amount     => r.t_amount,
                               p_price      => r.t_price,
                               p_pricefiid  => r.t_pricefiid,
                               p_sourcekind => 101, --todo: constant
                               p_sourceid   => l_tick_id,
                               p_contract   => r.t_clientcontr,
                               p_status     => 'M', --Заключена сделка todo: constant
                               p_direction  => r.t_direction);

        l_spground_id := spground_utils.get_spground_id_by_source_doc(p_sourcedockind => l_req_kind, p_sourcedocid => l_req_id); --todo: constant
        l_spground_row := spground_utils.get_spground_row(p_spground_id => l_spground_id);

        spground_utils.save_ground(pio_spgroundid => l_spground_id,
                                   p_doclog       => l_spground_row.t_doclog,
                                   p_kind         => l_spground_row.t_kind,
                                   p_direction    => l_spground_row.t_direction,
                                   p_registrdate  => l_spground_row.t_registrdate,
                                   p_registrtime  => l_spground_row.t_registrtime,
                                   p_xld          => l_tick_row.t_Dealcode,
                                   p_altxld       => l_tick_row.t_Dealcode,
                                   p_signeddate   => l_spground_row.t_signeddate,
                                   p_backoffice   => l_spground_row.t_backoffice,
                                   p_party        => l_spground_row.t_party,
                                   p_partyname    => l_spground_row.t_partyname,
                                   p_partycode    => l_spground_row.t_partycode,
                                   p_methodapplic => l_spground_row.t_methodapplic);

        spground_utils.link_spground_to_doc(p_sourcedockind => 101, --todo:constant
                                            p_sourcedocid   => l_tick_id,
                                            p_spgroundid    => l_spground_id);

        po_cnt_success := po_cnt_success + 1;
      end if;
    end loop;
  end math_otc_requests_w_deals;
  
  procedure update_supply_date (
    p_dealid  ddl_leg_dbt.t_dealid%type,
    p_date    ddl_leg_dbt.t_expiry%type
  ) is
  begin
    update ddl_leg_dbt l
       set l.t_expiry = case when l.t_maturityisprincipal = 'X' then l.t_expiry else p_date end,
           l.t_maturity = case when l.t_maturityisprincipal = 'X' then p_date else l.t_maturity end
     where l.t_dealid = p_dealid
       and l.t_legid = 0
       and l.t_legkind = 0;
  end update_supply_date;
  
  procedure update_pay_date (
    p_dealid  ddl_leg_dbt.t_dealid%type,
    p_date    ddl_leg_dbt.t_maturity%type
  ) is
  begin
    update ddl_leg_dbt l
       set l.t_expiry = case when l.t_maturityisprincipal = 'X' then p_date else l.t_expiry end,
           l.t_maturity = case when l.t_maturityisprincipal = 'X' then l.t_maturity else p_date end
     where l.t_dealid = p_dealid
       and l.t_legid = 0
       and l.t_legkind = 0;
  end update_pay_date;

  procedure update_rq_plan_date (
    p_dockind  ddlrq_dbt.t_dockind%type,
    p_docid    ddlrq_dbt.t_docid%type,
    p_type     ddlrq_dbt.t_type%type,
    p_plandate ddlrq_dbt.t_plandate%type
  ) is
  begin
    update ddlrq_dbt r
       set r.t_plandate = p_plandate
     where r.t_dockind = p_dockind
       and r.t_docid = p_docid
       and r.t_type = p_type;
  end update_rq_plan_date;

  procedure update_dlgr_plan_date (
    p_dockind  ddlgrdeal_dbt.t_dockind%type,
    p_docid    ddlgrdeal_dbt.t_docid%type,
    p_templnum ddlgrdeal_dbt.t_templnum%type,
    p_plandate ddlgrdeal_dbt.t_plandate%type
  ) is
  begin
    update ddlgrdeal_dbt g
       set g.t_plandate = p_plandate
     where g.t_dockind = p_dockind
       and g.t_docid = p_docid
       and g.t_templnum = p_templnum;
  end update_dlgr_plan_date;

  procedure update_step_plan_date (
    p_dockind     doproper_dbt.t_dockind%type,
    p_docid       doproper_dbt.t_documentid%type,
    p_step_symbol doprstep_dbt.t_symbol%type,
    p_plan_date   doprstep_dbt.t_plan_date%type
  ) is
  begin
    update doprstep_dbt s
       set s.t_plan_date = p_plan_date
     where s.t_id_operation = (select o.t_id_operation
                                 from doproper_dbt o
                                where o.t_dockind = p_dockind
                                  and o.t_documentid = p_docid)
      and s.t_symbol = p_step_symbol;
  end update_step_plan_date;
  
  function get_deal_type (
    p_dealid ddl_tick_dbt.t_dealid%type
  ) return number is
    l_dealtype ddl_tick_dbt.t_dealtype%type;
  begin
    select t.t_dealtype
      into l_dealtype
      from ddl_tick_dbt t
     where t.t_dealid = p_dealid;

    return l_dealtype;
  exception
    when no_data_found then
      return null;
  end get_deal_type;
  
  function is_buy_deal_type (
    p_dealtype ddl_tick_dbt.t_dealtype%type
  ) return number deterministic is
  begin
    return rsb_secur.IsBuy(OGroup => rsb_secur.get_OperationGroup(oper => rsb_secur.get_OperSysTypes(TypeID => p_dealtype, DocKind => 101)));
  end is_buy_deal_type;
  
  function is_buy_deal (
    p_dealid ddl_tick_dbt.t_dealid%type
  ) return number deterministic is
  begin
    return is_buy_deal_type(p_dealtype => get_deal_type(p_dealid => p_dealid));
  end is_buy_deal;
  
  procedure update_deal_supply_schedule (
    p_dealid  ddl_tick_dbt.t_dealid%type,
    p_date    ddl_leg_dbt.t_expiry%type
  ) is
  begin
    update_supply_date(p_dealid => p_dealid, p_date => p_date);
    update_rq_plan_date(p_dockind  => 101, --todo: constant
                        p_docid    => p_dealid,
                        p_type     => 8, --todo: constant. type == supply
                        p_plandate => p_date);

    update_dlgr_plan_date(p_dockind  => 101,
                          p_docid    => p_dealid,
                          p_templnum => rsi_dlgr.dlgr_templ_delivery,
                          p_plandate => p_date);

    update_dlgr_plan_date(p_dockind  => 101,
                          p_docid    => p_dealid,
                          p_templnum => rsi_dlgr.dlgr_templ_depodraft,
                          p_plandate => p_date);
                          
    update_step_plan_date(p_dockind     => 101,
                          p_docid       => lpad(p_dealid, 34, '0'),
                          p_step_symbol => case when is_buy_deal(p_dealid => p_dealid) = 1 then 'Т' else 'О' end,
                          p_plan_date   => p_date);

  end update_deal_supply_schedule;
  
  procedure update_deal_pay_schedule (
    p_dealid  ddl_tick_dbt.t_dealid%type,
    p_date    ddl_leg_dbt.t_expiry%type
  ) is
  begin
    update_pay_date(p_dealid => p_dealid, p_date => p_date);
    update_rq_plan_date(p_dockind  => 101, --todo: constant
                        p_docid    => p_dealid,
                        p_type     => 2, --todo: constant. type == pay
                        p_plandate => p_date);

    update_dlgr_plan_date(p_dockind  => 101,
                          p_docid    => p_dealid,
                          p_templnum => rsi_dlgr.dlgr_templ_payment,
                          p_plandate => p_date);

    update_dlgr_plan_date(p_dockind  => 101,
                          p_docid    => p_dealid,
                          p_templnum => rsi_dlgr.dlgr_templ_paycom,
                          p_plandate => p_date);
                          
    update_step_plan_date(p_dockind     => 101,
                          p_docid       => lpad(p_dealid, 34, '0'),
                          p_step_symbol => case when is_buy_deal(p_dealid => p_dealid) = 1 then 'О' else 'Т' end,
                          p_plan_date   => p_date);
  end update_deal_pay_schedule;

end secur_deals_utils;
/

create or replace function GetSubAgreementByDepoAccount(p_BrokerContractNumber varchar2 --  номер основного договора
                                                       ,p_AccountDepoNumber    varchar2 -- счет депо
                                                       ,p_Market               varchar2 --обозначение торговой площадки (может быть "MOEX", "SPB", "OTC", "Unknown")
                                                        ) return number is
  -- BIQ-13034 находит субдоговор клиента
  v_Agreement_suffix     u_depoacc_tradeplace.t_agreement_suffix%type;
  v_depoacc_secondletter  u_depoacc_tradeplace.t_depoacc_firstletter%type;
  v_depoacc_middlenumber u_depoacc_tradeplace.t_depoacc_middlenumber%type;
  v_result               number;
begin
  v_depoacc_secondletter  := substr(it_xml.token_substr(p_source => p_AccountDepoNumber, p_delim => '-', p_num => 2), 1, 1);
  v_depoacc_middlenumber := it_xml.token_substr(p_source => p_AccountDepoNumber, p_delim => '-', p_num => 4);
  if p_BrokerContractNumber is null
     or v_depoacc_secondletter is null
     or v_depoacc_middlenumber is null
  then
    return - 1;
  end if;
  begin
    select t_agreement_suffix into v_Agreement_suffix from (
    select s.t_agreement_suffix, s.t_depoacc_firstletter
      from u_depoAcc_tradeplace s
      where  p_AccountDepoNumber like 'Д-' || s.t_depoacc_firstletter || '%' 
      and s.t_depoacc_middlenumber = v_depoacc_middlenumber
      and s.t_market = p_Market order by length (s.t_depoacc_firstletter) desc) where rownum = 1;
  exception
    when no_data_found then
      v_Agreement_suffix := '_в';
  end;
  select decode(cnt_contr, 1, t_id, -1)
    into v_result
    from (select count(*) over() as cnt_contr
                ,t_id
            from DSFCONTR_DBT c
           where c.t_number = p_BrokerContractNumber || v_Agreement_suffix
             and c.t_dateclose = to_date('01010001', 'ddmmyyyy'))
   where rownum < 2;
  return v_result;
exception
  when no_data_found then
    return - 1;
  when others then
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
    it_error.clear_error_stack;
    return - 1;
end GetSubAgreementByDepoAccount;
/

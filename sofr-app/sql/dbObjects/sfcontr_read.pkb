create or replace package body sfcontr_read as

  g_object_type constant number(3) := 659;

  --виды обслуживания
  g_servkind_stock    constant number(1) := 1;  --фондовый рынок
  g_servkind_forts    constant number(2) := 15; --срочный рынок
  
  --подвиды обслуживания 
  g_servsubkind_exchange constant number(1) := 8;

  function subcontr_objecttype
  return number deterministic is
  begin
    return g_object_type;
  end subcontr_objecttype;
     
  function servkind_stock
    return number deterministic is
  begin
    return g_servkind_stock;
  end servkind_stock;
     
  function servkind_forts
    return number deterministic is
  begin
    return g_servkind_forts;
  end servkind_forts;
     
  function servsubkind_exchange
    return number deterministic is
  begin
    return g_servsubkind_exchange;
  end servsubkind_exchange;

  function is_subcontr_iis (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number is
    l_iis number(1);
  begin
    select case when d.t_iis = 'X' then 1 else 0 end
      into l_iis
      from ddlcontrmp_dbt m
      join ddlcontr_dbt d on d.t_dlcontrid = m.t_dlcontrid
     where m.t_sfcontrid = p_sfcontr_id;
    return l_iis;
  exception
    when no_data_found then
      return 0;
  end is_subcontr_iis;

  function is_contr_iis (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number is
    l_iis number(1);
  begin
    select case when d.t_iis = 'X' then 1 else 0 end
      into l_iis
      from ddlcontr_dbt d
     where d.t_sfcontrid = p_sfcontr_id;
    return l_iis;
  exception
    when no_data_found then
      return 0;
  end is_contr_iis;

  function get_subcontr_exchange_id (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return ddlcontrmp_dbt.t_marketid%type is
    l_exchange_id ddlcontrmp_dbt.t_marketid%type;
  begin
    select m.t_marketid
      into l_exchange_id
      from ddlcontrmp_dbt m
     where m.t_sfcontrid = p_sfcontr_id;

    return l_exchange_id;
  exception
    when no_data_found then
      return null;
  end get_subcontr_exchange_id;
  
  function is_subcontr_exchange (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number is
    l_is_exchange number(1);
  begin
    select case when m.t_marketid > 0 then 1 else 0 end
      into l_is_exchange
      from ddlcontrmp_dbt m
     where m.t_sfcontrid = p_sfcontr_id;

    return l_is_exchange;
  exception
    when no_data_found then
      return 0;
  end is_subcontr_exchange;
  
  function get_client_by_contract (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return dsfcontr_dbt.t_partyid%type is
    l_partyid dsfcontr_dbt.t_partyid%type;
  begin
    select s.t_partyid
      into l_partyid
      from dsfcontr_dbt s
     where s.t_id = p_sfcontr_id;

    return l_partyid;
  exception
    when no_data_found then
      return null;
  end get_client_by_contract;

  function get_setacc_id_by_contr (
    p_sfcontr_id dsfcontr_dbt.t_id%type,
    p_fiid       dsfssi_dbt.t_fiid%type
  ) return dsfssi_dbt.t_setaccid%type is
    l_setacc_id dsfssi_dbt.t_setaccid%type;
  begin
    select s.t_setaccid
      into l_setacc_id
      from dsfssi_dbt s
     where s.t_objecttype = subcontr_objecttype
       and s.t_objectid = lpad(p_sfcontr_id, 10, '0')
       and s.t_fiid = p_fiid;

    return l_setacc_id;
  exception
    when no_data_found then
      return null;
  end get_setacc_id_by_contr;
  
  /*
    по субдоговору ищет другой субдоговор этого же договора
    с заданными видом и сабвидом обслуживания
  */
  function get_subcontr_id_with_servkind (
    p_src_sfcontr_id  dsfcontr_dbt.t_id%type,
    p_servkind        dsfcontr_dbt.t_servkind%type,
    p_servkindsub     dsfcontr_dbt.t_servkindsub%type,
    p_market_id       ddlcontrmp_dbt.t_marketid%type
  ) return dsfcontr_dbt.t_id%type is
    l_sfcontr_id dsfcontr_dbt.t_id%type;
  begin
    select c.t_id
      into l_sfcontr_id
      from dsfcontr_dbt c
      join ddlcontrmp_dbt m on m.t_sfcontrid = c.t_id
      join ddlcontrmp_dbt m2 on m2.t_dlcontrid = m.t_dlcontrid
     where m2.t_sfcontrid = p_src_sfcontr_id
       and c.t_servkind = p_servkind
       and c.t_servkindsub = p_servkindsub
       and m.t_marketid = p_market_id;

    return l_sfcontr_id;
  exception
    when others then
      return null;
  end get_subcontr_id_with_servkind;
  
  function get_subcontr_id_by_contr (
    p_contr_id        dsfcontr_dbt.t_id%type,
    p_servkind        dsfcontr_dbt.t_servkind%type,
    p_servkindsub     dsfcontr_dbt.t_servkindsub%type,
    p_market_id       ddlcontrmp_dbt.t_marketid%type
  ) return dsfcontr_dbt.t_id%type is
    l_sfcontr_id dsfcontr_dbt.t_id%type;
  begin
    select sc.t_id
      into l_sfcontr_id
      from ddlcontr_dbt d
      join ddlcontrmp_dbt m on m.t_dlcontrid = d.t_dlcontrid
      join dsfcontr_dbt sc on sc.t_id = m.t_sfcontrid
     where d.t_sfcontrid = p_contr_id
       and sc.t_servkind = p_servkind
       and sc.t_servkindsub = p_servkindsub
       and m.t_marketid = p_market_id;

    return l_sfcontr_id;
  exception
    when others then
      return null;
  end get_subcontr_id_by_contr;
  
  function get_moex_subcontr_id_by_contr (
    p_contr_id        dsfcontr_dbt.t_id%type
  ) return dsfcontr_dbt.t_id%type is
  begin
    return get_subcontr_id_by_contr(p_contr_id    => p_contr_id,
                                    p_servkind    => sfcontr_read.servkind_stock,
                                    p_servkindsub => sfcontr_read.servsubkind_exchange,
                                    p_market_id   => party_read.moex_id);
                                    
  end get_moex_subcontr_id_by_contr;
  
  function get_allowed_broker_asset_usage (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number is
  begin
    return categ_utils.get_attr_id(p_ObjectType => subcontr_objecttype,
                                   p_Object     => lpad(p_sfcontr_id, 10, '0'),
                                   p_GroupID    => 6,
                                   p_Date       => sysdate);
  end get_allowed_broker_asset_usage;
  
  function get_asset_transferred (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number is
  begin
    return categ_utils.get_attr_id(p_ObjectType => subcontr_objecttype,
                                   p_Object     => lpad(p_sfcontr_id, 10, '0'),
                                   p_GroupID    => 7,
                                   p_Date       => sysdate);
  end get_asset_transferred;
  
  /*
    Проверяет признак обособления договора.
    На вход принимается ИД субдоговора с видом обслуживания "Фондовый рынок".
    Проверяет через категории:
      Перевод активов на новый номер ТКС произведен = "Нет" и Заявление клиента о переводе на обособленный учет = "Да"
  */
  function is_separated_contr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number is
  begin
    return case when get_allowed_broker_asset_usage(p_sfcontr_id => p_sfcontr_id) = 2 and get_asset_transferred(p_sfcontr_id => p_sfcontr_id) = 1
                then 1
                else 0
           end;
  end is_separated_contr;
  
  function is_separated_subcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number is
  begin
    return sfcontr_read.is_separated_contr(p_sfcontr_id => sfcontr_read.get_subcontr_id_with_servkind(p_src_sfcontr_id => p_sfcontr_id,
                                                                                                      p_servkind       => sfcontr_read.servkind_stock,
                                                                                                      p_servkindsub    => sfcontr_read.servsubkind_exchange,
                                                                                                      p_market_id      => party_read.moex_id));
  end is_separated_subcontr;
  
  function get_ekk_subcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return ddlobjcode_dbt.t_code%type is
    l_ekk ddlobjcode_dbt.t_code%type;
  begin
    select c.t_code
      into l_ekk
      from ddlcontrmp_dbt m
      join ddlobjcode_dbt c on c.t_objectid = m.t_dlcontrid and
                               c.t_objecttype = 207 and
                               c.t_codekind = 1
     where m.t_sfcontrid = p_sfcontr_id;

    return l_ekk;
  exception
    when others then
      return null;
  end get_ekk_subcontr;
  
  function get_ekk_contr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return ddlobjcode_dbt.t_code%type is
  begin
    return get_ekk_subcontr(p_sfcontr_id => get_moex_subcontr_id_by_contr(p_contr_id => p_sfcontr_id));
  end get_ekk_contr;

  function get_note (
    p_sfcontr_id dsfcontr_dbt.t_id%type,
    p_notekind   dnotetext_dbt.t_notekind%type
  ) return varchar2 is
  begin
    return note_read.get_note(p_object_type => subcontr_objecttype,
                              p_note_kind   => p_notekind,
                              p_document_id => lpad(p_sfcontr_id, 10, 0));
  end get_note;

  function get_moex_stock_client_account (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return varchar2 is
  begin
    return get_note(p_sfcontr_id => p_sfcontr_id,
                    p_notekind  => 5);
  end get_moex_stock_client_account;

  function get_depo_trade_acc_subcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return varchar2 is
    l_trade_acc varchar2(256);
    l_is_separated_contr number(1);
  begin
    l_trade_acc := get_moex_stock_client_account(p_sfcontr_id => p_sfcontr_id);
    if l_trade_acc is not null
    then
      return l_trade_acc;
    end if;

    l_is_separated_contr := is_separated_subcontr(p_sfcontr_id => p_sfcontr_id);

    select t_depoacc
      into l_trade_acc
      from ddl_limitprm_dbt
     where t_marketkind = 1
       and t_marketid = sfcontr_read.get_subcontr_exchange_id(p_sfcontr_id => p_sfcontr_id)
       and t_implkind = case when l_is_separated_contr = 0 then 1 else 2 end;

    return l_trade_acc;
  end get_depo_trade_acc_subcontr;

end sfcontr_read;
/

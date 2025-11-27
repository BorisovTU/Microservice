create or replace package sfcontr_read as

  function subcontr_objecttype
  return number deterministic;
     
  function servkind_stock
    return number deterministic;
     
  function servkind_forts
    return number deterministic;
     
  function servsubkind_exchange
    return number deterministic;

  function is_subcontr_iis (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number;

  function is_contr_iis (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number;

  function get_subcontr_exchange_id (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return ddlcontrmp_dbt.t_marketid%type;
  
  function is_subcontr_exchange (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number;
  
  function get_client_by_contract (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return dsfcontr_dbt.t_partyid%type;

  function get_setacc_id_by_contr (
    p_sfcontr_id dsfcontr_dbt.t_id%type,
    p_fiid       dsfssi_dbt.t_fiid%type
  ) return dsfssi_dbt.t_setaccid%type;

  function get_subcontr_id_with_servkind (
    p_src_sfcontr_id  dsfcontr_dbt.t_id%type,
    p_servkind        dsfcontr_dbt.t_servkind%type,
    p_servkindsub     dsfcontr_dbt.t_servkindsub%type,
    p_market_id       ddlcontrmp_dbt.t_marketid%type
  ) return dsfcontr_dbt.t_id%type;
  
  function get_subcontr_id_by_contr (
    p_contr_id        dsfcontr_dbt.t_id%type,
    p_servkind        dsfcontr_dbt.t_servkind%type,
    p_servkindsub     dsfcontr_dbt.t_servkindsub%type,
    p_market_id       ddlcontrmp_dbt.t_marketid%type
  ) return dsfcontr_dbt.t_id%type;
  
  function get_moex_subcontr_id_by_contr (
    p_contr_id        dsfcontr_dbt.t_id%type
  ) return dsfcontr_dbt.t_id%type;
  
  function is_separated_contr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number;
  
  function is_separated_subcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return number;
  
  function get_ekk_subcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return ddlobjcode_dbt.t_code%type;
  
  function get_ekk_contr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return ddlobjcode_dbt.t_code%type;

  function get_note (
    p_sfcontr_id dsfcontr_dbt.t_id%type,
    p_notekind   dnotetext_dbt.t_notekind%type
  ) return varchar2;

  function get_moex_stock_client_account (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return varchar2;

  function get_depo_trade_acc_subcontr (
    p_sfcontr_id dsfcontr_dbt.t_id%type
  ) return varchar2;

end sfcontr_read;
/

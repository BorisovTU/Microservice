create or replace package party_read as

  function bank_id return dparty_dbt.t_partyid%type deterministic;

  function moex_id return dparty_dbt.t_partyid%type deterministic;

  function party_objecttype
  return number deterministic;

  --субъект - ИП или юр.лицо
  function is_legal_entity (
    p_party_id dparty_dbt.t_partyid%type
  ) return number result_cache;
  
  function is_legal_entity_clear (
    p_party_id dparty_dbt.t_partyid%type
  ) return number;
  
  function is_IP_entity_clear (
    p_party_id dparty_dbt.t_partyid%type
  ) return number;  
  
  function is_person_entity_clear (
    p_party_id dparty_dbt.t_partyid%type
  ) return number;  
  
  function get_id_by_cft_code(
    p_cft_code dobjcode_dbt.t_code%type
  ) return dobjcode_dbt.t_objectid%type;
  
  FUNCTION is_legal_entity_from_bcid (p_bcid dvsbanner_dbt.t_bcid%TYPE)
        RETURN NUMBER
        RESULT_CACHE;
  
  function get_party_code (
    p_party_id   dparty_dbt.t_partyid%type,
    p_code_kind  dobjcode_dbt.t_codekind%type
  ) return dobjcode_dbt.t_code%type;
  
  function get_party_name (
    p_partyid dparty_dbt.t_partyid%type
  ) return dparty_dbt.t_name%type;

end party_read;
/

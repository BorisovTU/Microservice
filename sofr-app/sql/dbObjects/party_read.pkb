create or replace package body party_read as

  g_bank_id  constant dparty_dbt.t_partyid%type := 1;
  g_moex_id  constant dparty_dbt.t_partyid%type := 2;
  
  function bank_id return dparty_dbt.t_partyid%type deterministic is
  begin
    return g_bank_id;
  end bank_id;
  
  function moex_id return dparty_dbt.t_partyid%type deterministic is
  begin
    return g_moex_id;
  end moex_id;

  function party_objecttype
  return number deterministic is
  begin
    return 3;
  end party_objecttype;

  --субъект - ИП или юр.лицо
  function is_legal_entity (
    p_party_id dparty_dbt.t_partyid%type
  ) return number result_cache is
    l_is_le number(1);
  begin
    select count(1)
      into l_is_le
      from dparty_dbt p
     where p.t_partyid = p_party_id
       and (
               (    p.t_legalform = 1
                and not exists (select 1
                                  from dpartyown_dbt po
                                 where po.t_partyid = p.t_partyid
                                   and po.t_partykind = 65)
               )
            or (    p.t_legalform = 2
                and exists (select 1
                              from dpersn_dbt pers
                             where pers.t_personid = p.t_partyid
                               and pers.t_isemployer = 'X')
                )
           );

    return l_is_le;
  end is_legal_entity;
  
  function is_legal_entity_clear (
    p_party_id dparty_dbt.t_partyid%type
  ) return number is
    l_is_le number(1);
  begin
    select count(1)
      into l_is_le
      from dparty_dbt p
     where p.t_partyid = p_party_id
       and p.t_legalform = 1;

    return l_is_le;
  end is_legal_entity_clear;
  
  function is_IP_entity_clear (
    p_party_id dparty_dbt.t_partyid%type
  ) return number is
    l_is_IP number(1);
  begin
    select count(1)
      into l_is_IP
      from dparty_dbt p,
              dpersn_dbt pers
     where p.t_partyid = p_party_id
         and pers.t_personid = p.t_partyid
         and p.t_legalform = 2
         and pers.t_isemployer = chr(88);
    return l_is_IP;
  end is_IP_entity_clear;  
  
  function is_person_entity_clear (
    p_party_id dparty_dbt.t_partyid%type
  ) return number is
    l_is_Pers number(1);
  begin
    select count(1)
      into l_is_Pers
      from dparty_dbt p,
           dpersn_dbt pers
     where p.t_partyid = p_party_id
         and pers.t_personid = p.t_partyid
         and p.t_legalform = 2
         and pers.t_isemployer = chr(0);
    return l_is_Pers;
  end is_person_entity_clear;
  
  function get_id_by_cft_code(
    p_cft_code dobjcode_dbt.t_code%type
  ) return dobjcode_dbt.t_objectid%type is
    l_party_id dobjcode_dbt.t_objectid%type;
  begin
    select c.t_objectid
      into l_party_id
      from dobjcode_dbt c
     where c.t_objecttype = 3
       and c.t_codekind = 101
       and c.t_state = 0
       and c.t_code = p_cft_code;

    return l_party_id;
  exception
    when no_data_found then
      return null;
  end get_id_by_cft_code;
  
  FUNCTION is_legal_entity_from_bcid (p_bcid dvsbanner_dbt.t_bcid%TYPE)
        RETURN NUMBER
        RESULT_CACHE
  IS
        l_is_le   NUMBER (1);
  BEGIN
        SELECT COUNT (1)
          INTO l_is_le
          FROM dparty_dbt p, dvsbanner_dbt vsb
         WHERE     vsb.t_bcid = p_bcid
               AND (   vsb.t_holder > 0 AND p.t_partyid = vsb.t_holder
                    OR     vsb.t_holder <= 0
                       AND p.t_partyid in 
                           (SELECT NVL (ord.t_contractor, -1)
                              FROM ddl_order_dbt ord, dvsordlnk_dbt lnk
                             WHERE     lnk.t_bcid = vsb.t_bcid
                                   AND lnk.t_dockind = 109
                                   AND ord.t_contractstatus > 0
                                   AND ord.t_contractid = lnk.t_contractid))
               AND (   (    p.t_legalform = 1
                        AND NOT EXISTS
                                (SELECT 1
                                   FROM dpartyown_dbt po
                                  WHERE     po.t_partyid = p.t_partyid
                                        AND po.t_partykind = 65))
                    OR (    p.t_legalform = 2
                        AND EXISTS
                                (SELECT 1
                                   FROM dpersn_dbt pers
                                  WHERE     pers.t_personid = p.t_partyid
                                        AND pers.t_isemployer = 'X')));

        RETURN l_is_le;
  END is_legal_entity_from_bcid;
  
  function get_party_code (
    p_party_id   dparty_dbt.t_partyid%type,
    p_code_kind  dobjcode_dbt.t_codekind%type
  ) return dobjcode_dbt.t_code%type is
  begin
    return objcode_read.get_code(p_object_type => party_read.party_objecttype,
                                 p_code_kind   => p_code_kind,
                                 p_object_id   => p_party_id);
  end get_party_code;
  
  function get_party_name (
    p_partyid dparty_dbt.t_partyid%type
  ) return dparty_dbt.t_name%type is
    l_name dparty_dbt.t_name%type;
  begin
    select t_name
      into l_name
      from dparty_dbt p
     where p.t_partyid = p_partyid;

    return l_name;
  exception
    when no_data_found then
      return null;
  end get_party_name;
end party_read;
/

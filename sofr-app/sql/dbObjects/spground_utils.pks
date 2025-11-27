create or replace package spground_utils as

  procedure save_ground (
    pio_spgroundid in out dspground_dbt.t_spgroundid%type,
    p_doclog              dspground_dbt.t_doclog%type,
    p_kind                dspground_dbt.t_kind%type,
    p_direction           dspground_dbt.t_direction%type,
    p_registrdate         dspground_dbt.t_registrdate%type,
    p_registrtime         dspground_dbt.t_registrtime%type,
    p_xld                 dspground_dbt.t_xld%type,
    p_altxld              dspground_dbt.t_altxld%type,
    p_signeddate          dspground_dbt.t_signeddate%type,
    p_backoffice          dspground_dbt.t_backoffice%type,
    p_party               dspground_dbt.t_party%type,
    p_partyname           dspground_dbt.t_partyname%type,
    p_partycode           dspground_dbt.t_partycode%type,
    p_methodapplic        dspground_dbt.t_methodapplic%type
  );
  
  procedure link_spground_to_doc (
    p_sourcedockind dspgrdoc_dbt.t_sourcedockind%type,
    p_sourcedocid   dspgrdoc_dbt.t_sourcedocid%type,
    p_spgroundid    dspgrdoc_dbt.t_spgroundid%type
  );
  
  function get_spground_id_by_source_doc (
    p_sourcedockind dspgrdoc_dbt.t_sourcedockind%type,
    p_sourcedocid   dspgrdoc_dbt.t_sourcedocid%type
  ) return dspground_dbt.t_spgroundid%type;
  
  function get_spground_row (
    p_spground_id dspground_dbt.t_spgroundid%type
  ) return dspground_dbt%rowtype;

end spground_utils;
/

create or replace package body spground_utils as

  procedure insert_ground (
    po_spgroundid  out dspground_dbt.t_spgroundid%type,
    p_doclog           dspground_dbt.t_doclog%type,
    p_kind             dspground_dbt.t_kind%type,
    p_direction        dspground_dbt.t_direction%type,
    p_registrdate      dspground_dbt.t_registrdate%type,
    p_registrtime      dspground_dbt.t_registrtime%type,
    p_xld              dspground_dbt.t_xld%type,
    p_altxld           dspground_dbt.t_altxld%type,
    p_signeddate       dspground_dbt.t_signeddate%type,
    p_backoffice       dspground_dbt.t_backoffice%type,
    p_party            dspground_dbt.t_party%type,
    p_partyname        dspground_dbt.t_partyname%type,
    p_partycode        dspground_dbt.t_partycode%type,
    p_methodapplic     dspground_dbt.t_methodapplic%type
  ) is
  begin
    insert into dspground_dbt(t_doclog,
                              t_kind,
                              t_direction,
                              t_xld,
                              t_registrdate,
                              t_registrtime,
                              t_party,
                              t_altxld,
                              t_signeddate,
                              t_signedtime,
                              t_proxy,
                              t_division,
                              t_references,
                              t_receptionist,
                              t_copies,
                              t_sent,
                              t_deliverykind,
                              t_backoffice,
                              t_comment,
                              t_sourcedocid,
                              t_sourcedockind,
                              t_doctemplate,
                              t_terminatedate,
                              t_partyname,
                              t_partycode,
                              t_beginningdate,
                              t_sentdate,
                              t_senttime,
                              t_department,
                              t_branch,
                              t_parent,
                              t_userlog,
                              t_version,
                              t_ismakeauto,
                              t_techautodoc,
                              t_deponent,
                              t_havesubjlist,
                              t_subjectid,
                              t_registerid,
                              t_depoacntid,
                              t_msgnumber,
                              t_msgdate,
                              t_msgtime,
                              t_methodapplic)
    values (p_doclog,
            p_kind,
            p_direction,
            p_xld,
            p_registrdate,
            p_registrtime,
            p_party,
            p_altxld,
            p_signeddate,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            0,
            0,
            1,
            nvl(RsbSessionData.Oper, 1),
            0,
            chr(1),
            0,
            p_backoffice,
            chr(1),
            0,
            0,
            0,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            p_partyname,
            p_partycode,
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            1,
            1,
            0,
            0,
            0,
            chr(1),
            0,
            0,
            chr(1),
            0,
            0,
            0,
            chr(1),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            to_date('01.01.0001', 'dd.mm.yyyy'),
            p_methodapplic
            )
    returning t_spgroundid into po_spgroundid;
  end insert_ground;
  
  procedure update_ground (
    p_spgroundid  dspground_dbt.t_spgroundid%type,
    p_xld         dspground_dbt.t_xld%type,
    p_altxld      dspground_dbt.t_altxld%type
  ) is
  begin
    update dspground_dbt s
       set s.t_xld = p_xld,
           s.t_altxld = p_altxld
     where s.t_spgroundid = p_spgroundid;
  end update_ground;

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
  ) is
  begin
    if pio_spgroundid is null then
      insert_ground(po_spgroundid  => pio_spgroundid,
                    p_doclog       => p_doclog,
                    p_kind         => p_kind,
                    p_direction    => p_direction,
                    p_registrdate  => p_registrdate,
                    p_registrtime  => p_registrtime,
                    p_xld          => p_xld,
                    p_altxld       => p_altxld,
                    p_signeddate   => p_signeddate,
                    p_backoffice   => p_backoffice,
                    p_party        => p_party,
                    p_partyname    => p_partyname,
                    p_partycode    => p_partycode,
                    p_methodapplic => p_methodapplic);
    else
      update_ground(p_spgroundid => pio_spgroundid,
                    p_xld        => p_xld,
                    p_altxld     => p_altxld);
    end if;

  exception
    when others then
      it_log.log_error(p_object => 'spground_utils.save_ground',
                       p_msg    => 'Error ' || sqlerrm);
      raise_application_error(-20000, 'Error ' || sqlerrm, true);
  end save_ground;
  
  procedure link_spground_to_doc (
    p_sourcedockind dspgrdoc_dbt.t_sourcedockind%type,
    p_sourcedocid   dspgrdoc_dbt.t_sourcedocid%type,
    p_spgroundid    dspgrdoc_dbt.t_spgroundid%type
  ) is
  begin
    insert into dspgrdoc_dbt (t_sourcedockind,
                              t_sourcedocid,
                              t_spgroundid,
                              t_order,
                              t_debitcredit,
                              t_status,
                              t_version)
    values (p_sourcedockind,
            p_sourcedocid,
            p_spgroundid,
            1,
            0,
            chr(1),
            0);

  exception
    when others then
      it_log.log_error(p_object => 'spground_utils.link_spground_to_doc',
                       p_msg    => 'Error ' || sqlerrm);
      raise_application_error(-20000, 'Error ' || sqlerrm, true);
  end link_spground_to_doc;
  
  function get_spground_id_by_source_doc (
    p_sourcedockind dspgrdoc_dbt.t_sourcedockind%type,
    p_sourcedocid   dspgrdoc_dbt.t_sourcedocid%type
  ) return dspground_dbt.t_spgroundid%type is
    l_spgroundid dspground_dbt.t_spgroundid%type;
  begin
    select d.t_spgroundid
      into l_spgroundid
      from dspgrdoc_dbt d
     where d.t_sourcedockind = p_sourcedockind
       and d.t_sourcedocid = p_sourcedocid;

    return l_spgroundid;
  exception
    when others then
      return null;
  end get_spground_id_by_source_doc;
  
  function get_spground_row (
    p_spground_id dspground_dbt.t_spgroundid%type
  ) return dspground_dbt%rowtype is
    l_spground_row dspground_dbt%rowtype;
  begin
    select *
      into l_spground_row
      from dspground_dbt g
     where g.t_spgroundid = p_spground_id;

    return l_spground_row;
  exception
    when no_data_found then
      return null;
  end get_spground_row;

end spground_utils;
/

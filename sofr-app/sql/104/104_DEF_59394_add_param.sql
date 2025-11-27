declare
  vc_ol_load            varchar2(1000) := it_event_utils.GC_PARAM_ASTS_BRIDGE;
  vc_ol_load_cap        varchar2(1000) := 'Контроль обработки планировщиком записей шлюза ASTS ON/OFF';
  vc_ol_load_start      varchar2(1000) := it_event_utils.GC_PARAM_ASTS_BRIDGE_START;
  vc_ol_load_start_cap  varchar2(1000) := 'Время начала контроля(HH24:MI)';
  vc_ol_load_stop       varchar2(1000) := it_event_utils.GC_PARAM_ASTS_BRIDGE_STOP;
  vc_ol_load_stop_cap   varchar2(1000) := 'Время окончания контроля(HH24:MI)';
  --
  vn number;
begin

  vn := it_rs_interface.add_parm_path(p_parm_path => vc_ol_load, p_type => 4, p_description => vc_ol_load_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_ol_load_start, p_type => 2, p_description => vc_ol_load_start_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => '10:00');
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_ol_load_stop, p_type => 2, p_description => vc_ol_load_stop_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => '23:59');
  commit;
exception
  when others then
    rollback;
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
    dbms_output.put_line(sqlerrm);
end;

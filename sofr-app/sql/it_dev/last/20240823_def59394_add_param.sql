declare
  vc_ASTS_BRIDGE_DELAY     varchar2(1000) := it_event_utils.GC_PARAM_ASTS_BRIDGE_DELAY;
  vc_ASTS_BRIDGE_DELAY_cap varchar2(1000) := 'Контроль загрузки сделок с задержкой от ... мин (для синхронизации с "Обработка ЗР источника Шлюз ASTS-2")';
  --
  vn number;
begin
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_ASTS_BRIDGE_DELAY, p_type => 0, p_description => vc_ASTS_BRIDGE_DELAY_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 35);
  commit;
exception
  when others then
    rollback;
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
    dbms_output.put_line(sqlerrm);
end;
/
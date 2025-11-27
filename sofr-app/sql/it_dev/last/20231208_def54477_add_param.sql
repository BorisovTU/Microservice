declare
  vc_param     varchar2(1000) := it_event_utils.GC_PARAM_FORMAT_MSG_T;
  vc_param_cap varchar2(1000) := 'Настройки формирования сообщений для мессенджера';
  vc_param_LowLI      varchar2(1000) := it_event_utils.GC_PARAM_FORMAT_MSG_T_LOWLI;
  vc_param_LowLI_cap  varchar2(1000) := 'Отправка информации о метрике или событии с LevelInfo >= ... ' ;
  vc_param_period      varchar2(1000) := it_event_utils.GC_PARAM_FORMAT_MSG_T_PERIOD;
  vc_param_period_cap  varchar2(1000) := 'Отправка повтора информации о метрике или событии не чаще ... (мин)';

  --
  vn number;
begin
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_param, p_type => 0, p_description => vc_param_cap);
 --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_param_LowLI, p_type => 0, p_description => vc_param_LowLI_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 6);
 --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_param_period, p_type => 0, p_description => vc_param_period_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 60);

  commit;
exception
  when others then
    rollback;
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
    dbms_output.put_line(sqlerrm);
end;

declare
  vc_monitoring     varchar2(1000) := it_event_utils.GC_PARAM_MONITORING;
  vc_monitoring_cap varchar2(1000) := 'Мониторинг бизнес-операций в СОФР ON/OFF';
  --
  vc_ol_load            varchar2(1000) := it_event_utils.GC_PARAM_LOAD_CUR;
  vc_ol_load_cap        varchar2(1000) := 'Контроль OnLine загрузки по валютному рынку ON/OFF';
  vc_ol_load_start      varchar2(1000) := it_event_utils.GC_PARAM_LOAD_CUR_START;
  vc_ol_load_start_cap  varchar2(1000) := 'Время начала контроля(HH24:MI)';
  vc_ol_load_stop       varchar2(1000) := it_event_utils.GC_PARAM_LOAD_CUR_STOP;
  vc_ol_load_stop_cap   varchar2(1000) := 'Время окончания контроля(HH24:MI)';
  vc_ol_load_period     varchar2(1000) := it_event_utils.GC_PARAM_LOAD_CUR_PERIOD;
  vc_ol_load_period_cap varchar2(1000) := 'Проверка данных за последние ..(мин)';
  --
  vc_plan1        varchar2(1000) := it_event_utils.GC_PARAM_PLAN1;
  vc_plan1_cap    varchar2(1000) := 'Контроль Планировщик - 1 ON/OFF';
  vc_plan1_LI     varchar2(1000) := vc_plan1 || it_event_utils.GC_PARAM_LI;
  vc_plan1_LI_cap varchar2(1000) := 'Уровень критичности ошибки от времени последней операции';
  --
  vc_plan2        varchar2(1000) := it_event_utils.GC_PARAM_PLAN2;
  vc_plan2_cap    varchar2(1000) := 'Контроль Планировщик - 2 ON/OFF';
  vc_plan2_LI     varchar2(1000) := vc_plan2 || it_event_utils.GC_PARAM_LI;
  vc_plan2_LI_cap varchar2(1000) := 'Уровень критичности шибки от времени последней операции';
  --
  vc_oper             varchar2(1000) := it_event_utils.GC_PARAM_OPER;
  vc_oper_cap         varchar2(1000) := 'Контроль количества откатываемых операций ON/OFF';
  vc_oper_period      varchar2(1000) := it_event_utils.GC_PARAM_OPER_PERIOD;
  vc_oper_period_cap  varchar2(1000) := 'Проверка данных за последние ..(мин)';
  vc_oper_sp          varchar2(1000) := it_event_utils.GC_PARAM_OPER_SP;
  vc_oper_sp_cap      varchar2(1000) := 'Список операций для контроля';
  vc_oper_sp_1        varchar2(1000) := vc_oper_sp || '\ПРОВОДКИ И ПЛАТЕЖИ';
  vc_oper_sp_1_cap    varchar2(1000) := 'Операции проводки и платежи ON/OFF';
  vc_oper_sp_1_ot     varchar2(1000) := vc_oper_sp_1 || '\OBJECTTYPE';
  vc_oper_sp_1_ot_cap varchar2(1000) := 'Список значений t_objecttype через запятую';
  vc_oper_sp_1_LI     varchar2(1000) := vc_oper_sp_1 || it_event_utils.GC_PARAM_LI;
  vc_oper_sp_1_LI_cap varchar2(1000) := 'Уровень критичностио ошибки от количества откатываемых операций';
  --
  vn number;
begin

  vn := it_rs_interface.add_parm_path(p_parm_path => vc_monitoring, p_type => 4, p_description => vc_monitoring_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_ol_load, p_type => 4, p_description => vc_ol_load_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_ol_load_start, p_type => 2, p_description => vc_ol_load_start_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => '10:00');
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_ol_load_stop, p_type => 2, p_description => vc_ol_load_stop_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => '24:00');
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_ol_load_period, p_type => 0, p_description => vc_ol_load_period_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 45);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_plan1, p_type => 4, p_description => vc_plan1_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
 
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_plan1_LI, p_type => 0, p_description => vc_plan1_LI_cap);
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_plan1_LI || '\10'
                                     ,p_type => 0
                                     ,p_description => ' LevelInfo = 10 при задержке .. . сек');
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 600);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_plan2, p_type => 4, p_description => vc_plan2_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_plan2_LI, p_type => 0, p_description => vc_plan2_LI_cap);
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_plan2_LI || '\10'
                                     ,p_type => 0
                                     ,p_description => ' LevelInfo = 10 при задержке .. . сек');
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 600);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_oper, p_type => 4, p_description => vc_oper_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_oper_period, p_type => 0, p_description => vc_oper_period_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 60);
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_oper_sp, p_type => 0, p_description => vc_oper_sp_cap);
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_oper_sp_1, p_type => 4, p_description => vc_oper_sp_1_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_oper_sp_1_ot, p_type => 2, p_description => vc_oper_sp_1_ot_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => '1,501');
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_oper_sp_1_LI, p_type => 0, p_description => vc_oper_sp_1_LI_cap);
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_oper_sp_1_LI || '\10'
                                     ,p_type => 0
                                     ,p_description => ' LevelInfo = 10 при количестве ...');
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 500);
  commit;
exception
  when others then
    rollback;
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
    dbms_output.put_line(sqlerrm);
end;

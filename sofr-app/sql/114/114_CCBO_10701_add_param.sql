declare
  vc_PARAM_CFT10002     varchar2(1000) := '‘•\Œˆ’ˆƒ SOFR\‘—…’€ ˆ ‚„Šˆ ‚ –”’';
  vc_PARAM_CFT10002_cap     varchar2(1000) := 'Š®­âà®«ì ä®à¬¨à®¢ ­¨ï á®®¡é¥­¨© ¯® áç¥â ¬ ¨ ¯à®¢®¤ª ¬ ¢ –”’ (®¡à ¡®âç¨ª á®¡ëâ¨© ü10002 uTableProcessEvent_dbt)';
  vc_PARAM_CFT10002_PERIOD     varchar2(1000) := vc_PARAM_CFT10002 || '\PERIOD';
  vc_PARAM_CFT10002_PERIOD_cap     varchar2(1000) := 'Š®­âà®«ì áâ âãá®¢ ®¡à ¡®âª¨ á®¡ëâ¨© ü10002 uTableProcessEvent_dbt §  ¯®á«¥¤­¨¥ ... ¬¨­';
  --
  vn number;
begin
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_PARAM_CFT10002, p_type => 4, p_description => vc_PARAM_CFT10002_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);

  vn := it_rs_interface.add_parm_path(p_parm_path => vc_PARAM_CFT10002_PERIOD, p_type => 0, p_description => vc_PARAM_CFT10002_PERIOD_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 30);
end;
/

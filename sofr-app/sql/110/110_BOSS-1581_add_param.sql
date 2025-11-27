declare
GC_PARAM_L1 constant varchar2(128) := '‘•\ˆ’…ƒ€–ˆŸ\…’ƒ‚›… “—…ˆŸ';
GC_PARAM_STATUS constant varchar2(128) := GC_PARAM_L1||'\‚›ƒ“‡Š€ ‘’€’“‘‚';
GC_PARAM_STATUS_DBO constant varchar2(128) := GC_PARAM_STATUS||'\„ ”‹';
vc_cap1       varchar2(1000) := '„«ï á¨áâ¥¬ë „ ”‹. "YES" (¢ª«îç¥­®) / "NO" (¢ëª«îç¥­®)';

GC_PARAM_STATUS_EFR constant varchar2(128) := GC_PARAM_STATUS||'\…”';
vc_cap2       varchar2(1000) := '„«ï á¨áâ¥¬ë …”. "YES" (¢ª«îç¥­®) / "NO" (¢ëª«îç¥­®)';

GC_PARAM_STATUS_NOTIFICATION_OD constant varchar2(128) := GC_PARAM_STATUS||'\‚…™…ˆ… „';
vc_cap3       varchar2(1000) := 'à¨ ®è¨¡ª å ®â¯à ¢«ïâì ®¯®¢¥é¥­¨¥ ¢ „  "YES" (¢ª«îç¥­®) / "NO" (¢ëª«îç¥­®)';

GC_PARAM_STATUS_NOTIFICATION_SUPPORT constant varchar2(128) := GC_PARAM_STATUS||'\‚…™…ˆ…  ‘‚†„…ˆŸ';
vc_cap4       varchar2(1000) := 'à¨ ®è¨¡ª å ®â¯à ¢«ïâì ®¯®¢¥é¥­¨¥ ¢ ‘‚†„…ˆ…  "YES" (¢ª«îç¥­®) / "NO" (¢ëª«îç¥­®)';

GC_PARAM_LOAD constant varchar2(128) := GC_PARAM_L1||'\‡€ƒ“‡Š€';
GC_PARAM_LOAD_DBO constant varchar2(128) := GC_PARAM_LOAD||'\„ ”‹';
vc_cap31       varchar2(1000) := '„«ï á¨áâ¥¬ë „ ”‹. "YES" (¢ª«îç¥­®) / "NO" (¢ëª«îç¥­®)';

GC_PARAM_LOAD_EFR constant varchar2(128) := GC_PARAM_LOAD||'\…”';
vc_cap32       varchar2(1000) := '„«ï á¨áâ¥¬ë …”. "YES" (¢ª«îç¥­®) / "NO" (¢ëª«îç¥­®)';

GC_PARAM_LOAD_NOTIFICATION_OD constant varchar2(128) := GC_PARAM_LOAD||'\‚…™…ˆ… „';
vc_cap33       varchar2(1000) := 'à¨ ®è¨¡ª å ®â¯à ¢«ïâì ®¯®¢¥é¥­¨¥ ¢ „  "YES" (¢ª«îç¥­®) / "NO" (¢ëª«îç¥­®)';

GC_PARAM_LOAD_NOTIFICATION_SUPPORT constant varchar2(128) := GC_PARAM_LOAD||'\‚…™…ˆ…  ‘‚†„…ˆŸ';
vc_cap34       varchar2(1000) := 'à¨ ®è¨¡ª å ®â¯à ¢«ïâì ®¯®¢¥é¥­¨¥  ¢ ‘‚†„…ˆ…  "YES" (¢ª«îç¥­®) / "NO" (¢ëª«îç¥­®)';

GC_PARAM_TIME constant varchar2(128) := GC_PARAM_L1||'\ƒ€”ˆŠ €’Šˆ';

GC_PARAM_TIME_START constant varchar2(128) := GC_PARAM_TIME||'\€—€‹ …ˆ„€';
vc_cap41       varchar2(1000) := '‚à¥¬ï ­ ç «   ¢ ä®à¬ â¥ "HHMM"';

GC_PARAM_TIME_STOP constant varchar2(128) := GC_PARAM_TIME||'\Š—€ˆ… …ˆ„€';
vc_cap42       varchar2(1000) := '‚à¥¬ï ®ª®­ç ­¨ï ¢ ä®à¬ â¥ "HHMM"';

  --
  vn number;
begin
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_L1, p_type => 4, p_description => '‡ £àã§ª  ­¥â®à£®¢ëå ¯®àãç¥­¨©');
 
 vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_LOAD, p_type => 4, p_description => '„®áâã¯­  ¤«ï á¨áâ¥¬ ¨áâ®ç­¨ª®¢ ');
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_LOAD_DBO, p_type => 4, p_description => vc_cap31);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_LOAD_EFR, p_type => 4, p_description => vc_cap32);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 0);
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_LOAD_NOTIFICATION_OD, p_type => 4, p_description => vc_cap33);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_LOAD_NOTIFICATION_SUPPORT, p_type => 4, p_description => vc_cap34);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);

  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_STATUS, p_type => 4, p_description => 'â¯à ¢ª  á®®¡é¥­¨© ®¡ ¨§¬¥­¥­¨¨ áâ âãá  ');
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_STATUS_DBO, p_type => 4, p_description => vc_cap1);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_STATUS_EFR, p_type => 4, p_description => vc_cap2);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 0);
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_STATUS_NOTIFICATION_OD, p_type => 4, p_description => vc_cap3);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_STATUS_NOTIFICATION_SUPPORT, p_type => 4, p_description => vc_cap4);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);

  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_TIME, p_type => 4, p_description => 'â¯à ¢ª  á®®¡é¥­¨© ®¡ ¨§¬¥­¥­¨¨ áâ âãá  ');
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_TIME_START, p_type => 2, p_description => vc_cap41);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => '0800');
  vn := it_rs_interface.add_parm_path(p_parm_path => GC_PARAM_TIME_STOP, p_type => 2, p_description => vc_cap42);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => '1505');

end;
/
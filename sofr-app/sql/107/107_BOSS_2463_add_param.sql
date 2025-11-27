declare
  GC_PARAM_LIMIT constant varchar2(1000) := '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\€‘—…’_‹ˆŒˆ’Ž‚_QUIK';
  GC_PARAM_QMANAGER constant varchar2(1000) := GC_PARAM_LIMIT || '\ˆ‘Ž‹œ‡Ž‚€’œ QMANAGER';
  GC_PARAM_QFORCE constant varchar2(1000) := GC_PARAM_QMANAGER || '\FORCE-…†ˆŒ €‘—…’€';
  GC_PARAM_QDAYCHK constant varchar2(1000) := GC_PARAM_LIMIT || '\ŠŽ’Ž‹œ ‡€“‘ŠŽ‚';
  vc_limit           varchar2(1000) := GC_PARAM_LIMIT;
  vc_limit_cap       varchar2(1000) := ' áâà®©ª  à áç¥â®¢ «¨¬¨â®¢ QUIK';
  vc_qmng            varchar2(1000) := GC_PARAM_QMANAGER;
  vc_qmng_cap        varchar2(1000) := 'ˆá¯®«ì§®¢ âì ®¡à ¡®âç¨ª á¥à¢¨á®¢ QMANAGER ON/OFF';
  vc_qmng_force      varchar2(1000) := GC_PARAM_QFORCE;
  vc_qmng_force_cap  varchar2(1000) := 'ˆá¯®«ì§®¢ âì FORCE à¥¦¨¬ ON/OFF';
  vc_qmng_daychk     varchar2(1000) := GC_PARAM_QDAYCHK;
  vc_qmng_daychk_cap varchar2(1000) := 'ƒ«ã¡¨­  ª®­âà®«ï § ¯ãáª  à áç¥â®¢ ¢ ¤­ïå';
  --
  vn number;
begin
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_limit, p_type => 4, p_description => vc_limit_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 1);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_qmng, p_type => 4, p_description => vc_qmng_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 0);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_qmng_force, p_type => 4, p_description => vc_qmng_force_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 0);
  --
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_qmng_daychk, p_type => 0, p_description => vc_qmng_daychk_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 3);
  --
  commit;
exception
  when others then
    rollback;
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
    dbms_output.put_line(sqlerrm);
end;

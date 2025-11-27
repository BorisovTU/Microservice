declare
  vc_param     varchar2(1000) := '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\ˆ‚…‘’_‘Ž‚…’ˆŠ\€.„…‰ „Ž Ž‹€’›';
  vc_param_cap varchar2(1000) := '  ª ª®© ¯® áç¥âã à ¡®ç¨© ¤¥­ì ¯« ­¨àã¥âáï ®¯« â  ¯® ãá«ã£¥ ˆ­¢¥áâá®¢¥â­¨ª';
  --
  vn number;
begin
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_param, p_type => 0, p_description => vc_param_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 10);
  commit;
exception
  when others then
    rollback;
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
    dbms_output.put_line(sqlerrm);
end;

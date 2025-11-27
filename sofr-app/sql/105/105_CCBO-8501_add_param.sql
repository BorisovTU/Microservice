declare
  vc_param     varchar2(1000) := '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\\„€’€ €‚’. ‚›ƒ“‡Šˆ';
  vc_param_cap varchar2(1000) := '„ â   ¢â®¬ â¨ç¥áª®© ¢ë£àã§ª¨ ¢ Š•„  ¤ ­­ëå ®âç¥â  Š‹-11(¯®àï¤ª®¢ë© ­®¬¥à à ¡®ç¥£® ¤­ï á ­ ç «  â¥ªãé¥£® ¬¥áïæ )';
  --
  vn number;
begin
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_param, p_type => 0, p_description => vc_param_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 3);
  commit;
exception
  when others then
    rollback;
    it_error.put_error_in_stack;
    it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
    dbms_output.put_line(sqlerrm);
end;
/
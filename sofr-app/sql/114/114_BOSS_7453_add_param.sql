declare
  vc_anti_spam varchar2(1000) := '‘•\Œˆ’ˆƒ SOFR\‡€™ˆ’€ ’ SPAM';
  vc_anti_spam_cap varchar2(1000) := 'ˆ§ ®è¨¡®ª ä®à¬¨àãîâáï ®¯®¢¥é¥­¨ï ­¥ ç é¥ 1 à ¢ ... á¥ª ';
  vn           number;
begin
  vn := it_rs_interface.add_parm_path(p_parm_path => vc_anti_spam, p_type => 0, p_description => vc_anti_spam_cap);
  it_rs_interface.set_parm(p_keyid => vn, p_parm => 600);
end;
/
declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\ˆ’…ƒ€–ˆŸ\…’ƒ‚›… “—…ˆŸ\‡€ƒ“‡Š€\‘‚ˆ ˆ‚…‘’ˆ–ˆˆ',
                                            p_type        => 4,
                                            p_description => '‡ £àã§ª  ­¥â®à£®¢®àëå ¯®àãç¥­¨© ®â á¢®¨å ¨­¢¥áâ¨æ¨©');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 1);
  
  
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\ˆ’…ƒ€–ˆŸ\…’ƒ‚›… “—…ˆŸ\‚›ƒ“‡Š€ ‘’€’“‘‚\‘‚ˆ ˆ‚…‘’ˆ–ˆˆ',
                                            p_type        => 4,
                                            p_description => '„«ï á¨áâ¥¬ë ‘¢®¨ ¨­¢¥áâ¨æ¨¨. "YES" (¢ª«îç¥­®) / "NO" (¢ëª«îç¥­®)');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 1);
end;
/
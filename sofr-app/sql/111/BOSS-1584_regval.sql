declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\ˆ’…ƒ€–ˆŸ\…’ƒ‚›… “—…ˆŸ\‚›ƒ“‡Š€ ‚ QUIK\‘ˆ‘€ˆŸ',
                                            p_type        => 4,
                                            p_description => '‚ë£àã§ª  ¢ QUIK ­¥â®à£®¢ëå ¯®àãç¥­¨© ¯® á¯¨á ­¨ï¬');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 1);


  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\ˆ’…ƒ€–ˆŸ\…’ƒ‚›… “—…ˆŸ\‚›ƒ“‡Š€ ‚ QUIK\„”‹ ‚€‹’›• ‘ˆ‘€ˆ‰',
                                            p_type        => 4,
                                            p_description => '‚ë£àã§ª  ¢ QUIK ­¥â®à£®¢ëå ¯®àãç¥­¨© ¯® ¢ «îâ­ë¬ á¯¨á ­¨ï¬ („”‹)');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 1);
end;
/
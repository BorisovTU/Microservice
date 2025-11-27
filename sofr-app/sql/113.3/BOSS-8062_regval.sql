declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\„ˆ…Š’Žˆˆ\‡€Œ…™…ˆ… –\‡€Ÿ‚Šˆ',
                                            p_type        => 2,
                                            p_description => 'Œ¥áâ® åà ­¥­¨ï csv ä ©«  á § ï¢ª ¬¨ ­  § ¬¥é¥­¨¥ æ¥­­ëå ¡ã¬ £');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 'S:\sofr_for_etl\inbox\message');


  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\„ˆ…Š’Žˆˆ\‡€Œ…™…ˆ… –\‘„…‹Šˆ',
                                            p_type        => 2,
                                            p_description => 'Œ¥áâ® åà ­¥­¨ï csv ä ©«  á® á¤¥«ª ¬¨ ­  § ¬¥é¥­¨¥ æ¥­­ëå ¡ã¬ £');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 'S:\sofr_for_etl\inbox\message');
end;
/
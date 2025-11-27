declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\Š…‘Š… ‘‹“†ˆ‚€ˆ…\‚›ƒ“‡Š€ …Š‘ˆ‹ ‘„…‹Š „”\‘ˆ‘Š EMAL „‹Ÿ ’€‚Šˆ EX‘EL',
                                            p_type        => 2,
                                            p_description => 'Email ¤«ï ®â¯à ¢ª¨ ¢ë£àã§ª¨');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => '');

  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\Š…‘Š… ‘‹“†ˆ‚€ˆ…\‚›ƒ“‡Š€ …Š‘ˆ‹ ‘„…‹Š „”\€‚’Œ€’ˆ—…‘Šˆ‰ Š‘’ ‚ CSV',
                                            p_type        => 4,
                                            p_description => '”« £ ¯¥à¥ª«îç â¥«ì à¥¦¨¬  à ¡®âë ¤«ï íªá¯®àâ  ¢ csv: YES - ¢ª«îç¥­, No - ¢ëª«îç¥­');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 0);

  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\Š…‘Š… ‘‹“†ˆ‚€ˆ…\‚›ƒ“‡Š€ …Š‘ˆ‹ ‘„…‹Š „”\€‚’Œ€’ˆ—…‘Šˆ‰ Š‘’ ‚ EXCEL',
                                            p_type        => 4,
                                            p_description => '”« £ ¯¥à¥ª«îç â¥«ì à¥¦¨¬  à ¡®âë ¤«ï íªá¯®àâ  ¢ Excel ¨ ®â¯à ¢ª¨ ¯® ¯®çâ¥: YES - ¢ª«îç¥­, No - ¢ëª«îç¥­');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 0);

  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\Š…‘Š… ‘‹“†ˆ‚€ˆ…\‚›ƒ“‡Š€ …Š‘ˆ‹ ‘„…‹Š „”\€Š€ „‹Ÿ Š‘’€ CSV',
                                            p_type        => 2,
                                            p_description => 'ãâì ¤«ï íªá¯®àâ  ¤ ­­ëå ¢ ä®à¬ â¥ CSV');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => '');

  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\Š…‘Š… ‘‹“†ˆ‚€ˆ…\‚›ƒ“‡Š€ …Š‘ˆ‹ ‘„…‹Š „”\€Š€ „‹Ÿ ˆ‘’ˆˆ',
                                            p_type        => 2,
                                            p_description => 'ãâì ¤«ï á®åà ­¥­¨ï áä®à¬¨à®¢ ­­ëå CSV ¨ Excel');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => '');
end;
/

declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\ˆ’…ƒ€–ˆŸ\…’ƒ‚›… “—…ˆŸ\ˆ‘‹…ˆ… …€–ˆ‰\‡€—ˆ‘‹…ˆŸ € FUNCOBJ',
                                            p_type        => 4,
                                            p_description => 'ˆá¯®«­¥­¨¥ ®¯¥à æ¨© § ç¨á«¥­¨ï „‘ ç¥à¥§ ä ­ª®¡¦');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 1);
end;
/
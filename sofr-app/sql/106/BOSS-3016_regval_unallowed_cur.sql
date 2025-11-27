declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\‘ˆ‘ŽŠ_‚€‹ž’_‡€…’_‘ˆ‘_‡€—',
                                            p_type        => 2,
                                            p_description => '‡ ¯à¥éñ­­ë¥ ¢ «îâë ¤«ï ®¯¥à æ¨© á¯¨á ­¨ï/§ ç¨á«¥­¨ï ¤¥­¥¦­ëå áà¥¤áâ¢');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 'HKD,BYN,KZT,TRY,AED');
  commit;
end;
/

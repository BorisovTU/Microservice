declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\„ˆ…Š’Žˆˆ\REPORT_VU02_CLIENT_IMPORT',
                                            p_type        => 2,
                                            p_description => '‡ £àã§ª  ª«¨¥­â®¢ ¤«ï ®âç¥â  ‚“02 €ªâ á¢¥àª¨');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 'import');

  commit;
end;
/

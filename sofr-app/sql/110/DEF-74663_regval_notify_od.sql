declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\ˆ’…ƒ€–ˆŸ\…’ƒ‚›… “—…ˆŸ\‘‡„€ˆ… …€–ˆ‰\‚…™…ˆ… „',
                                            p_type        => 4,
                                            p_description => '¯®¢¥é¥­¨¥ „ ¯à¨ ®è¨¡ª å ¢ ¯à®æ¥áá¥ á®§¤ ­¨ï ­¥â®à£®¢ëå ¯®àãç¥­¨©');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 1);
end;
/
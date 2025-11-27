declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.get_keyid_parm_path(p_parm_path => 'COMMON\RSXMLRPC\‡€…’ „‚…Œ…ƒ ‡€“‘Š€');
  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 0);
end;
/
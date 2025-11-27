declare
  l_key_id number(10);
begin
  l_key_id := it_rs_interface.get_keyid_parm_path(p_parm_path => '‘•\ˆ’…ƒ€–ˆŸ\…’ƒ‚›… “—…ˆŸ\‚›ƒ“‡Š€ ‚ QUIK\‚›‚„ –');
  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 0);
end;
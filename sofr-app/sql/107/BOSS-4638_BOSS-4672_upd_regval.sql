declare
  l_parm_path varchar2(1000) := '‘•\ŽŠ…‘ŠŽ… Ž‘‹“†ˆ‚€ˆ…\‘ˆ‘ŽŠ_‚€‹ž’_‡€…’_‘ˆ‘_‡€—';
  l_key_id    number(10);
  l_value     varchar2(1000);
begin
  l_key_id := it_rs_interface.get_keyid_parm_path(p_parm_path => l_parm_path);
  l_value := it_rs_interface.get_parm_varchar(p_keyid => l_key_id);
  
  l_value := l_value || ',AMD,UZS,TJS,KGS';
  
  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => l_value);
  commit;
end;
/
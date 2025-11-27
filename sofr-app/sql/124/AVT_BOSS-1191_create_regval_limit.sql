declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\Š€’ˆ‚›… „…‰‘’‚ˆŸ\ˆ‘‹…ˆ…\ƒ€˜…ˆŸ –. ‹Š ‹ˆŒˆ’‚',
                                            p_type        => 4,
                                            p_description => '«®ª¨à®¢ âì «¨¬¨âë ¯à¨ ¨á¯®«­¥­¨¨ ª¤ ¯®£ è¥­¨ï æ¡');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 1);
end;
/
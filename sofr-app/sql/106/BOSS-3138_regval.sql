declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\„ˆ…Š’Žˆˆ\NPTXOP_TRANSFER_IMPORT',
                                            p_type        => 2,
                                            p_description => '®àãç¥­¨ï ª«¨¥­â®¢ ­  ¯¥à¥¢®¤ë „‘ ¬¥¦¤ã áã¡áç¥â ¬¨ „Ž');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => '\\sgo-fc01-r03.go.rshbank.ru\sofr_for_etl\NPTXOP\TRANSFER');
  

  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\„ˆ…Š’Žˆˆ\NPTXOP_OUT_EXCHANGE_IMPORT',
                                            p_type        => 2,
                                            p_description => '®àãç¥­¨ï ª«¨¥­â®¢ ­  ¢ë¢®¤ë „‘ á ¡¨à¦¨');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => '\\sgo-fc01-r03.go.rshbank.ru\sofr_for_etl\NPTXOP\OUT_EXCHANGE');
  

  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => '‘•\„ˆ…Š’Žˆˆ\NPTXOP_OUT_OTC_IMPORT',
                                            p_type        => 2,
                                            p_description => '®àãç¥­¨ï ª«¨¥­â®¢ ­  ¢ë¢®¤ë „‘ á ¢­¥¡¨à¦¨');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => '\\sgo-fc01-r03.go.rshbank.ru\sofr_for_etl\NPTXOP\OUT_OTC');
  commit;
end;
/

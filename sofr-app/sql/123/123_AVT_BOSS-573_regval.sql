declare
  l_key_id dregparm_dbt.t_keyid%type;
begin
  l_key_id := it_rs_interface.add_parm_path(p_parm_path   => 'РСХБ\ДИРЕКТОРИИ\REPORT_SUMCONF_IMPORT',
                                            p_type        => 2,
                                            p_description => 'Загрузка заданий для отчета о сумме подтвержденных расходов');

  it_rs_interface.set_parm(p_keyid => l_key_id, p_parm => 'import');
end;
/

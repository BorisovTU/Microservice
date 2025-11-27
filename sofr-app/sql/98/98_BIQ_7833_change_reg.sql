--изменим текст смс сообщения
begin
 update DREGVAL_DBT  set t_fmtblobdata_xxxx = rsb_struct.putString( t_fmtblobdata_xxxx, 'Временный пароль для входа в торговые терминалы: %v_code_val% Подробнее в уведомлении на электронной почте. ') where t_keyid = 6665;
 commit;
end;/

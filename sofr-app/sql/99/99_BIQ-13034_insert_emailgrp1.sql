declare
  v_cnt integer;
begin
  SELECT COUNT(*)
    INTO v_cnt
    FROM dllvalues_dbt l
   WHERE l.t_list = 5009
     and l.t_element = 77;

  if (v_cnt < 1) then
    INSERT INTO dllvalues_dbt
      (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
    VALUES
      (5009,
       77,
       '77',
       'DiasoftIntegration',
       1,
       'Интеграция с Diasoft',
       chr(1));
    commit;
  else
    dbms_output.put_line('Группа адресов по интеграции с Диасофт уже создана');
  end if;

  DELETE FROM usr_email_addr_dbt tbl_mail WHERE tbl_mail.t_group = 77;
  
  INSERT INTO usr_email_addr_dbt tbl_mail 
    (tbl_mail.t_group,
       tbl_mail.t_email,
       tbl_mail.t_place,
       tbl_mail.t_comment)
     VALUES
       (77, 'sofr-sup@rshb.ru', 'R', '');
  INSERT INTO usr_email_addr_dbt tbl_mail 
    (tbl_mail.t_group,
       tbl_mail.t_email,
       tbl_mail.t_place,
       tbl_mail.t_comment)
     VALUES
       (77, '8888@rshb.ru', 'R', '');
  INSERT INTO usr_email_addr_dbt tbl_mail 
    (tbl_mail.t_group,
       tbl_mail.t_email,
       tbl_mail.t_place,
       tbl_mail.t_comment)
     VALUES
       (77, 'custody@rshb.ru', 'R', '');
  COMMIT;


  SELECT COUNT(*)
    INTO v_cnt
    FROM dllvalues_dbt l
   WHERE l.t_list = 5009
     and l.t_element = 76;

  if (v_cnt < 1) then
    INSERT INTO dllvalues_dbt
      (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
    VALUES
      (5009,
       76,
       '76',
       'CustodyOperations',
       1,
       'Интеграция с Diasoft',
       chr(1));
    commit;
  else
    dbms_output.put_line('Группа адресов по интеграции с Диасофт уже создана');
  end if;

  DELETE FROM usr_email_addr_dbt tbl_mail WHERE tbl_mail.t_group = 76;
  
  INSERT INTO usr_email_addr_dbt tbl_mail 
    (tbl_mail.t_group,
       tbl_mail.t_email,
       tbl_mail.t_place,
       tbl_mail.t_comment)
     VALUES
       (76, 'broker@rshb.ru', 'R', '');
  COMMIT;

end;

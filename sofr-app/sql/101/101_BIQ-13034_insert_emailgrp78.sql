declare
begin

  delete from dllvalues_dbt l 
      WHERE l.t_list = 5009
       and l.t_element = 78;

  INSERT INTO dllvalues_dbt
      (T_LIST, T_ELEMENT, T_CODE, T_NAME, T_FLAG, T_NOTE, T_RESERVE)
    VALUES
      (5009,
       78,
       '78',
       'QuikNonTradeOperations',
       1,
       'Интеграция с МНП QUIK',
       chr(1));
  commit;

  DELETE FROM usr_email_addr_dbt tbl_mail WHERE tbl_mail.t_group = 78;
  
  INSERT INTO usr_email_addr_dbt tbl_mail 
    (tbl_mail.t_group,
       tbl_mail.t_email,
       tbl_mail.t_place,
       tbl_mail.t_comment)
     VALUES
       (78, 'broker@rshb.ru', 'R', '');
  INSERT INTO usr_email_addr_dbt tbl_mail 
    (tbl_mail.t_group,
       tbl_mail.t_email,
       tbl_mail.t_place,
       tbl_mail.t_comment)
     VALUES
       (78, 'bo_securities@rshb.ru', 'R', '');

  COMMIT;

end;

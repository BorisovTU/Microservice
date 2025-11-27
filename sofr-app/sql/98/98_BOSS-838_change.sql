DECLARE
  -- Процедура поля t_MarginCall char(1) в таблицу dbrkrepdeal_u_fm1_tmp
  PROCEDURE addMarginCall
  IS
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE dbrkrepdeal_u_fm1_tmp ADD t_MarginCall char(1)';
  EXCEPTION WHEN others THEN 
    NULL;
  END;
BEGIN
  addMarginCall(); 	-- добавление поля t_MarginCall char(1) в таблицу dbrkrepdeal_u_fm1_tmp
END;
/

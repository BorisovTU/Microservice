DECLARE
  -- Процедура поля t_MarginCall char(1) в таблицу DDL_REGIABUF_DBT
  PROCEDURE addMarginCall
  IS
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE DDL_REGIABUF_DBT ADD t_MarginCall char(1) DEFAULT CHR(0)';
  EXCEPTION WHEN others THEN 
    NULL;
  END;
BEGIN
  addMarginCall(); 	-- добавление поля t_MarginCall char(1) в таблицу DDL_REGIABUF_DBT
END;
/

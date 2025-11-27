DECLARE
  -- Расширить поле t_uniqcodepfi в таблице d_fo07_tmp до 35 символов
  PROCEDURE modifyUniqCodePFI
  IS
  BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE d_fo07_tmp MODIFY t_uniqcodepfi varchar2(35)';
  EXCEPTION WHEN others THEN 
    NULL;
  END;
BEGIN
  modifyUniqCodePFI();    -- расширить поле t_uniqcodepfi в таблице d_fo07_tmp до 35 символов
END;
/
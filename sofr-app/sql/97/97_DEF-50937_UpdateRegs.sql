DECLARE
  v_KeyID NUMBER := 0;
BEGIN
  v_KeyID := RSB_COMMON.GETREGPARM('COMMON/СНОБ/ОТКАТ ПЕРЕСЧЕТА НОБ');

  IF v_KeyID > 0 THEN
    UPDATE DREGPARM_DBT
      SET t_Description = 'Возможность отката операций расчета НОБ с установленным признаком "Пересчет"',
        t_Name = 'ОТКАТ ПЕРЕСЧЕТА НОБ'
      WHERE t_KeyID = v_KeyID;
  END IF;

  COMMIT;
END;
/
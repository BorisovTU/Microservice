-- Удаление типа в dimgtype_dbt (добавлено в поставке по BOSS-194 история 1)
DECLARE
  v_cnt NUMBER;
  logID VARCHAR2(50) := 'DEF-57067 Delete ImgType';
BEGIN
  DELETE
    FROM dImgType_dbt
   WHERE t_objectType = 3 AND t_imageType = 3;
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
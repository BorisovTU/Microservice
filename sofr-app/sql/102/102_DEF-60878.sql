-- Изменения по DEF-60878 (Исправление по договору 08-164312_ф)
DECLARE
  logID VARCHAR2(32) := 'DEF-60878';
  x_Cnt NUMBER;
  x_Stat NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Исправление по договору 08-164312_ф
  PROCEDURE correctDlContrMp
  IS
    x_CloseDate DATE := to_date('11-07-2022', 'dd-mm-yyyy');
    x_OpenDate  DATE := to_date('22-08-2022', 'dd-mm-yyyy');
    x_NullDate  DATE := to_date('01-01-0001', 'dd-mm-yyyy');
    x_dlcontrid NUMBER := 12343;
    x_GoodID NUMBER := 25639;
    x_BadID NUMBER := 721134;
  BEGIN
    LogIt('Исправление по договору 08-164312_ф');
    EXECUTE IMMEDIATE 
       'UPDATE ddlcontrmp_dbt mp SET mp.t_mpregdate = :1, mp.t_mpclosedate = :2 WHERE mp.t_dlcontrid = :3 AND mp.t_sfcontrid = :4'
       USING x_NullDate, x_CloseDate, x_dlcontrid, x_BadID
    ;
    EXECUTE IMMEDIATE 
       'UPDATE ddlcontrmp_dbt mp SET mp.t_mpregdate = :1, mp.t_mpclosedate = :2 WHERE mp.t_dlcontrid = :3 AND mp.t_sfcontrid = :4'
       USING x_OpenDate, x_NullDate, x_dlcontrid, x_GoodID
    ;
    EXECUTE IMMEDIATE 'COMMIT';
    LogIt('Произведено исправление по договору 08-164312_ф');
  EXCEPTION
   WHEN OTHERS THEN 
      LogIt('Ошибка исправления по договору 08-164312_ф');
      EXECUTE IMMEDIATE 'ROLLBACK';
  END;
BEGIN
  correctDlContrMp();           	-- Исправление по договору 08-164312_ф
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/

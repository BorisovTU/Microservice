-- Изменения по DEF-64504
-- Корректировка расчетного кода по BOSS-771_BOSS-773
-- Был создан расчетный код '25834'
-- Должен быть, видимо, 'L00+00002385'
DECLARE
  logID VARCHAR2(32) := 'DEF-61943';
  -- Записать сообщение в itt_log
  PROCEDURE LogIt( p_message VARCHAR2 )
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;
  -- Корректировка расчетного кода
  PROCEDURE CorrectSettleCode(p_OldCode IN varchar2, p_NewCode IN varchar2)
  AS
    x_Cnt number;
  BEGIN
    LogIt('Корректировка расчетного кода, p_OldCode = '||p_OldCode||', p_NewCode = '||p_NewCode);
    UPDATE ddl_extsettlecode_dbt r SET r.t_settlecode = p_NewCode WHERE r.t_settlecode = p_OldCode;
    COMMIT;
    LogIt('Произведена корректировка расчетного кода, p_OldCode = '||p_OldCode||', p_NewCode = '||p_NewCode);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      LogIt('Ошибка корректировки расчетного кода, p_OldCode = '||p_OldCode||', p_NewCode = '||p_NewCode);
  END;
BEGIN
  -- Корректировка расчетного кода
  CorrectSettleCode( 
    '25834'				-- сейчас такой
    , 'L00+00002385' 			-- должен быть такой
  );
EXCEPTION
  WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/

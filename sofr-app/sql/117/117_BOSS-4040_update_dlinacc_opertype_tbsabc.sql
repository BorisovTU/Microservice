--п.3.4.2 ТЗ
DECLARE
  logID VARCHAR2(32) := 'BOSS-4040';
  v_Cnt NUMBER := 0;

  -- Записать сообщение в itt_log
  PROCEDURE LogIt(p_message IN VARCHAR2)
  AS
  BEGIN
    it_log.log(p_msg => logID||': '||p_message);
  END;

BEGIN
  LogIt('Обновление существующих записей РОВУ по переводам ДС (t_OperType)');
  UPDATE DDLINACC_DBT a
     SET a.t_OperType = DECODE(a.t_OperType, 8, 36, 37)
   WHERE a.t_ID IN (SELECT inacc.t_ID
                      FROM DNPTXOP_DBT op, DDLINACC_DBT inacc         
                     WHERE op.t_DocKind = 4607 
                       AND op.t_SubKind_Operation = 30
                       AND inacc.t_DocumentKind = op.t_DocKind
                       AND inacc.t_DocumentID = op.t_ID
                   );

  v_Cnt := SQL%ROWCOUNT;
  LogIt('Обновлено '||to_char(v_Cnt)||' РОВУ по переводам ДС (t_OperType)');

EXCEPTION WHEN OTHERS THEN 
  LogIt('Ошибка при обновлении РОВУ по переводам ДС (t_OperType)');
END;
/
--п.3.5.2 ТЗ
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
  LogIt('Обновление существующих записей РОВУ по удержанию налога при списании (t_OperType)');
  UPDATE DDLINACC_DBT a
     SET a.t_OperType = 17
   WHERE a.t_ID IN (SELECT inacc.t_ID
                      FROM DNPTXOP_DBT op, DOPROPER_DBT oproper, DOPRDOCS_DBT oprd, DACCTRN_DBT acctrn, DDLINACC_DBT inacc         
                     WHERE op.t_DocKind = 4607 
                       AND op.t_SubKind_Operation = 20
                       AND oproper.t_Kind_Operation = op.t_Kind_Operation
                       AND oproper.t_DocumentID = op.t_ID
                       AND oprd.t_ID_Operation = oproper.t_ID_Operation
                       AND oprd.t_DocKind = 1
                       AND acctrn.t_AccTrnID = oprd.t_AccTrnID                                            
                       AND acctrn.t_Chapter = 21                                                                                            
                       AND (acctrn.t_Ground LIKE 'Уде%' OR acctrn.t_Ground LIKE 'Доу%')
                       AND inacc.t_AccTrnID = acctrn.t_AccTrnID
                   );

  v_Cnt := SQL%ROWCOUNT;
  LogIt('Обновлено '||to_char(v_Cnt)||' РОВУ по удержанию налога при списании (t_OperType)');

EXCEPTION WHEN OTHERS THEN 
  LogIt('Ошибка при обновлении РОВУ по удержанию налога при списании (t_OperType)');
END;
/


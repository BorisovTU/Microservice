--п.3.3.3 ТЗ
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
  LogIt('Обновление существующих записей РОВУ по зачислениям (t_OperType)');
  FOR i IN ( SELECT LOWER(u.t_Ground) Ground, inacc.t_ID
               FROM USR_ACC306ENROLL_DBT u, DNPTXOP_DBT op, DDLINACC_DBT inacc         
              WHERE u.t_NptxOpID = op.t_ID 
                AND op.t_DocKind = 4607 
                AND op.t_SubKind_Operation = 10
                AND inacc.t_DocumentKind = op.t_DocKind
                AND inacc.t_DocumentID = op.t_ID
           )
  LOOP
    UPDATE DDLINACC_DBT a
       SET a.t_OperType = CASE WHEN i.Ground LIKE '%купон%' THEN 22
                               WHEN i.Ground LIKE '%погашение%' THEN 23
                               WHEN i.Ground LIKE '%дивиденды%' THEN 24
                               WHEN i.Ground LIKE '%выплата по кд%' THEN 106
                               ELSE 6
                           END
     WHERE a.t_ID = i.t_ID;
    v_Cnt := v_Cnt + 1;
  END LOOP;
  LogIt('Обновлено '||to_char(v_Cnt)||' РОВУ по зачислениям (t_OperType)');

EXCEPTION WHEN OTHERS THEN 
  LogIt('Ошибка при обновлении РОВУ по зачислениям (t_OperType)');
END;
/
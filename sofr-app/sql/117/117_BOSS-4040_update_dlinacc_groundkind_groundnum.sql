--п.3.2.8 ТЗ
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
  LogIt('Обновление существующих записей РОВУ по неторговым операциям (t_GroundKind, t_GroundNum)');
  FOR i IN ( SELECT inacc.t_ID, grnd.t_Kind, grnd.t_AltXld
               FROM DNPTXOP_DBT op, DDLINACC_DBT inacc, DSPGROUND_DBT grnd         
              WHERE op.t_DocKind = 4607 
                AND inacc.t_DocumentKind = op.t_DocKind
                AND inacc.t_DocumentID = op.t_ID
                AND grnd.t_SpGroundID = (SELECT MIN(doc.t_SpGroundID) --Возьмем первый из документов
                                           FROM DSPGRDOC_DBT doc
                                          WHERE doc.t_SourceDocKind = op.t_DocKind
                                            AND doc.t_SourceDocID = op.t_ID)                                                                          
           )
  LOOP
    UPDATE DDLINACC_DBT a
       SET a.t_GroundKind = i.t_Kind,
           a.t_GroundNum = i.t_AltXld
     WHERE a.t_ID = i.t_ID;
    v_Cnt := v_Cnt + 1;
  END LOOP;
  LogIt('Обновлено '||to_char(v_Cnt)||' РОВУ по неторговым операциям (t_GroundKind, t_GroundNum)');

EXCEPTION WHEN OTHERS THEN 
  LogIt('Ошибка при обновлении РОВУ по неторговым операциям (t_GroundKind, t_GroundNum)');
END;
/
/*Обновление шагов*/
DECLARE
BEGIN
UPDATE doprostep_dbt st
   SET st.t_RestrictEarlyExecution = CHR (0)
 WHERE st.t_BlockID IN (SELECT b.t_BlockID
                          FROM doprblock_dbt b
                         WHERE b.t_DocKind IN (4649, 4646));
END;
/
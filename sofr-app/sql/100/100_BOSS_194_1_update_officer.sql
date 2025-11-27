-- Добавление настроек банка в regparm, regval
DECLARE
  logID VARCHAR2(50) := 'BOSS-194-1 Update dOfficer_dbt';
BEGIN
  UPDATE dOfficer_dbt 
     SET t_post = 'Заместитель начальника Управления учета и сопровождения операций на рынках капитала - Начальник отдела учета и сопровождения брокерских операций'
   WHERE t_personID = 149477;

  UPDATE dOfficer_dbt
     SET t_post = 'Начальник Управления учета и сопровождения операций на рынках капитала'
   WHERE t_personID = 322179;

  UPDATE dOfficer_dbt
     SET t_post = 'Заместитель директора Операционного департамента'
   WHERE t_personID = 150175;
   
  COMMIT;
EXCEPTION WHEN OTHERS THEN
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;
/
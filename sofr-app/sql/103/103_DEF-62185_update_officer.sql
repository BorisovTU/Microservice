-- Обновление должностей
DECLARE
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
END;
/
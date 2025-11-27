--Заполнение даты выноса на просрочку на шагах
BEGIN
  INSERT INTO doprdates_dbt (t_id_operation, t_datekindid, t_date)
  (SELECT oper.t_id_operation, 5100003, sfdef.t_planpaydate
     FROM dsfdef_dbt sfdef, dsfcomiss_dbt com, doproper_dbt oper                                                              
    WHERE com.t_code IN ('БрокерФикс','ИнвестСоветник')                                                                            
      AND sfdef.t_feetype = 1                                                                                  
      AND sfdef.t_commnumber = com.t_number                                                                    
      AND sfdef.t_status = 10
      AND oper.t_dockind = 51
      AND oper.t_documentid = LPAD(TO_CHAR(sfdef.t_id), 34, '0')                                                                                  
  );
END;
/
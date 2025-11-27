BEGIN
  UPDATE dsfdef_dbt 
     SET t_PlanPayDate = CASE WHEN t_CommNumber = 1063/*БрокерФикс*/
                              THEN RSI_RSBCALENDAR.GetDateAfterWorkDay(TRUNC(LAST_DAY(t_DatePeriodEnd))/*Последний день текущего месяца*/, 10, 10003)
                              ELSE RSI_RSBCALENDAR.GetDateAfterWorkDay((ADD_MONTHS(TRUNC(t_DatePeriodEnd, 'q'), 3)-1)/*Последний день текущего квартала*/, 10, 10003)  
                          END                                                                                                  
   WHERE t_FeeType = 1                                                                                  
     AND t_CommNumber IN (1063/*БрокерФикс*/, 1078/*ИнвестСоветник*/)                                                                    
     AND t_Status IN (10/*Начисляется*/, 40/*Оплачена*/);

  FOR i IN (SELECT def.t_ID, def.t_PlanPayDate, serv.t_EndDate
              FROM dsfdef_dbt def, ddlcontrmp_dbt mp, ddlcontrserv_dbt serv        
             WHERE def.t_FeeType = 1                                                                                  
               AND def.t_CommNumber = 1078/*ИнвестСоветник*/                                                                    
               AND def.t_Status IN (10/*Начисляется*/, 40/*Оплачена*/)
               AND def.t_SfContrID = mp.t_SfContrID
               AND mp.t_DlContrID = serv.t_DlContrID
               AND serv.t_EndDate <> TO_DATE('01.01.0001','DD.MM.YYYY')
               AND def.t_PlanPayDate > serv.t_EndDate
               AND def.t_DatePeriodEnd > serv.t_BeginDate
               AND def.t_DatePeriodBegin <= serv.t_EndDate               
           )
  LOOP
    UPDATE dsfdef_dbt a 
       SET a.t_PlanPayDate = i.t_EndDate
     WHERE a.t_ID = i.t_ID
       AND a.t_FeeType = 1;
  END LOOP;
END;
/
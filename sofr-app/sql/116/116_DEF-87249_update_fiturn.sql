--Обновление закрытых позиций с остатками
DECLARE
   v_longpositincost_new NUMBER := 0;
   v_shortpositioncost_new NUMBER := 0;
BEGIN
  FOR i IN ( SELECT turn.* 
               FROM DDVFIPOS_DBT pos, ddvfiturn_dbt turn 
              WHERE pos.T_STATE = 2 
                AND turn.t_shortpositioncost <> turn.t_longpositioncost  
                AND turn.t_shortposition = turn.t_longposition 
                AND turn.t_FIID        = pos.t_FIID
                AND turn.t_Department  = pos.t_Department
                AND turn.t_Broker      = pos.t_Broker
                AND turn.t_ClientContr = pos.t_ClientContr
                AND turn.t_GenAgrID    = pos.t_GenAgrID 
                AND turn.t_date =  (SELECT sub.t_date 
                                      FROM ddvfiturn_dbt sub 
                                     WHERE turn.t_FIID        = sub.t_FIID
                                       AND turn.t_Department  = sub.t_Department
                                       AND turn.t_Broker      = sub.t_Broker
                                       AND turn.t_ClientContr = sub.t_ClientContr
                                       AND turn.t_GenAgrID    = sub.t_GenAgrID 
                                  ORDER BY sub.t_date DESC 
                                  FETCH FIRST ROW ONLY)
           )
  LOOP
    IF ( ((i.t_shortpositioncost < i.t_longpositioncost) AND (i.t_shortpositioncost != 0)) OR (i.t_longpositioncost = 0) ) THEN
      v_shortpositioncost_new := i.t_shortpositioncost;
      v_longpositincost_new := i.t_shortpositioncost;
    ELSE
      v_shortpositioncost_new := i.t_longpositioncost;
      v_longpositincost_new := i.t_longpositioncost;
    END IF;
    UPDATE ddvfiturn_dbt SET t_shortpositioncost = v_shortpositioncost_new, t_longpositioncost = v_longpositincost_new WHERE T_ID = i.t_ID;   
  END LOOP;
END;
/
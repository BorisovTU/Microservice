DECLARE
   c   NUMBER (10) := 0;
BEGIN
   FOR i IN (  SELECT  t.*
                 FROM U_CONV_ADR_DATA_DBT t, ddl_leg_dbt leg
                WHERE t.t_flag1 = CHR (88) and t.t_dealid = leg.t_dealid and T.T_TAXOWNBEGDATE <> leg.T_start 
                and t.t_convid = (select min(t_convid) from U_CONV_ADR_DATA_DBT where t_dealid = t.t_dealid and t_flag1 = chr(88))
             ORDER BY t_convid)
   LOOP
      UPDATE ddl_leg_dbt
         SET t_start = i.t_taxownbegdate
       WHERE t_dealid = i.t_dealid AND t_legkind = 0 AND t_legid = 0;

      UPDATE DDLSUM_DBT
         SET t_date = i.t_taxownbegdate
       WHERE     t_dockind = 127
             AND t_docid = i.t_dealid
             AND t_kind IN (1220, 1230, 1240, 1250);

      C := c + 1;
   END LOOP;

   DBMS_OUTPUT.put_line ('count: ' || c);
   
END;
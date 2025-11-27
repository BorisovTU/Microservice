BEGIN
  UPDATE doprostep_dbt
     SET T_POST_MACRO = 'nptxwrt160.mac'
   WHERE T_BLOCKID = 203712
     AND T_NUMBER_STEP = 160
     AND T_POST_MACRO != 'nptxwrt160.mac'
     AND EXISTS (
           SELECT 1
             FROM doprostep_dbt
            WHERE T_BLOCKID = 203712
              AND T_NUMBER_STEP = 160
              AND T_POST_MACRO != 'nptxwrt160.mac'
         ) ;
        END;
/
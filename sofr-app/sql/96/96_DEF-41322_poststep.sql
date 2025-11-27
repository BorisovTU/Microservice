/*постстеп на операции списания/зачисления ц/б*/
DECLARE
BEGIN
  UPDATE doprostep_dbt
      SET t_post_macro = t_carry_macro
    WHERE T_BLOCKID IN (201000, 201100) AND t_number_step in ( 30, 40);

EXCEPTION 
   WHEN OTHERS THEN NULL;
END;
/

BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

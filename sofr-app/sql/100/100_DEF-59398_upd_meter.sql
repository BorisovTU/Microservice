DECLARE 
BEGIN
  update dmeter_dbt set t_lastvalue = (select max(to_number(t_acccode))+1 acccode from dsfcontr_dbt where to_number(t_acccode) < 1000000) where t_seqid=285;
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
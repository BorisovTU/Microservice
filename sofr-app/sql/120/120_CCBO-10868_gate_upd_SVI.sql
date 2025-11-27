declare
    l_id number;
 BEGIN
  SELECT dgt3dsys_dbt_SEQ.NEXTVAL into l_id FROM dual;
END;
/
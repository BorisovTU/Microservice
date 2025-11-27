BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DDIASRESTDEPO_DBT_IDX2';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;

/

DECLARE
  logID VARCHAR2(9) := 'DEF-64505';
BEGIN
  EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX DDIASRESTDEPO_DBT_IDX2 ON DDIASRESTDEPO_DBT (REPORTDATE,ACCDEPOID,ISIN) nologging local';
EXCEPTION
  WHEN OTHERS THEN 
    dbms_output.put_line('error while create index DDIASRESTDEPO_DBT_IDX2');
    it_error.put_error_in_stack;
    it_log.log( p_msg => logID||': exception'
              , p_msg_type => it_log.C_MSG_TYPE__ERROR
              );
    it_error.clear_error_stack;
END;

/
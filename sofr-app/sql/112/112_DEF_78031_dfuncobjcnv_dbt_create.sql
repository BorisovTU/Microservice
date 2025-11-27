/* Linux. Регресс. Замедление более чем в 5 раз Биржевого автомата ММВБ. Загрузка в БО сделок/заявок/клиринга. высокая Загрузка CPU */

DECLARE 
    e_object_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_object_exists, -955); 
BEGIN
    EXECUTE IMMEDIATE 
        'CREATE TABLE DFUNCOBJCNV_DBT' || 
        '(' || 
        'T_TASKID NUMBER(10),' || 
        'T_EXECID NUMBER(10),' || 
        'T_CONVTYPEID NUMBER(5),' || 
        'T_OPERATIONID NUMBER(5),' || 
        'T_PACKID NUMBER(10),' || 
        'T_FUNCOBJID NUMBER(10)' || 
        ')';

  EXECUTE IMMEDIATE 'COMMENT ON TABLE DFUNCOBJCNV_DBT IS ''Параметры операции конвейера для funcobj''';

  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DFUNCOBJCNV_DBT.T_TASKID IS ''ID значений параметров запуска.''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DFUNCOBJCNV_DBT.T_EXECID IS ''ID значений параметров запуска.''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DFUNCOBJCNV_DBT.T_CONVTYPEID IS ''ID вида конвейера.''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DFUNCOBJCNV_DBT.T_OPERATIONID IS ''ID вида операции''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DFUNCOBJCNV_DBT.T_PACKID IS ''ID пачки''';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN DFUNCOBJCNV_DBT.T_FUNCOBJID IS ''Количество нитей.''';

EXCEPTION 
    WHEN e_object_exists THEN NULL; 
END;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DFUNCOBJCNV_DBT_IDX0';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE UNIQUE INDEX DFUNCOBJCNV_DBT_IDX0 ON DFUNCOBJCNV_DBT
(T_EXECID, T_CONVTYPEID, T_OPERATIONID, T_PACKID)
/

BEGIN
  EXECUTE IMMEDIATE 'DROP INDEX DFUNCOBJCNV_DBT_IDX1';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE UNIQUE INDEX DFUNCOBJCNV_DBT_IDX1 ON DFUNCOBJCNV_DBT
(T_FUNCOBJID)
/

DECLARE
   vcnt   NUMBER;
BEGIN
   SELECT COUNT (*)
     INTO vcnt
     FROM user_sequences
    WHERE UPPER (sequence_name) = 'DFUNCOBJCNV_DBT_SEQ';

   IF vcnt = 0
   THEN
      EXECUTE IMMEDIATE 'CREATE SEQUENCE DFUNCOBJCNV_DBT_SEQ
                                START WITH 1
                                MAXVALUE 9999999999999999999999999999
                                MINVALUE 1
                                NOCYCLE
                                NOCACHE
                                NOORDER';
   END IF;
END;
/


CREATE OR REPLACE TRIGGER dfuncobjcnv_dbt_t1_ainc
 BEFORE INSERT OR UPDATE OF t_funcobjid ON DFUNCOBJCNV_DBT FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_funcobjid = 0 OR :new.t_funcobjid IS NULL) THEN
 SELECT dfuncobjcnv_dbt_seq.nextval INTO :new.t_funcobjid FROM dual;
 ELSE
 select last_number into v_id from user_sequences where sequence_name = upper ('dfuncobjcnv_dbt_SEQ');
 IF :new.t_funcobjid >= v_id THEN
 RAISE DUP_VAL_ON_INDEX;
 END IF;
 END IF;
END;
/



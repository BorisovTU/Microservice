-- подменним триггеры и сиквенсы на временные таблицы автоинкрементного ключа на формирование отрицательного ID

-- DFICERT_TMP
DECLARE
  e_sequence_does_not_exist EXCEPTION;
  PRAGMA EXCEPTION_INIT( e_sequence_does_not_exist, -2289 );
BEGIN

  BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE DFICERT_TMP_SEQ';
  EXCEPTION
    WHEN e_sequence_does_not_exist THEN NULL;
  END;

  BEGIN
    EXECUTE IMMEDIATE( ' CREATE SEQUENCE DFICERT_TMP_SEQ'  ||
                       ' START WITH -1' ||
                       ' INCREMENT BY -1 '  ||
                       ' MAXVALUE -1 '  ||
                       ' MINVALUE -9999999999999999999999999999 ' ||
                       ' NOCYCLE' ||
                       ' NOCACHE' ||
                       ' NOORDER' );
    EXCEPTION
      WHEN OTHERS THEN NULL;
  END;
END;
/
CREATE OR REPLACE TRIGGER dficert_tmp_t0_ainc
  BEFORE INSERT OR UPDATE OF t_ficertid ON dficert_tmp FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:new.t_ficertid = 0 OR :new.t_ficertid IS NULL) THEN
    SELECT dficert_tmp_seq.nextval INTO :new.t_ficertid FROM dual;
  ELSE
    select last_number into v_id from user_sequences where sequence_name = upper ('dficert_tmp_SEQ');
    IF :new.t_ficertid <= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
 END IF;
END;
/

-- DVSBANNER_TMP
DECLARE
  e_sequence_does_not_exist EXCEPTION;
  PRAGMA EXCEPTION_INIT( e_sequence_does_not_exist, -2289 );
BEGIN

  BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE DVSBANNER_TMP_SEQ';
  EXCEPTION
    WHEN e_sequence_does_not_exist THEN NULL;
  END;

  BEGIN
    EXECUTE IMMEDIATE( ' CREATE SEQUENCE DVSBANNER_TMP_SEQ'  ||
                       ' START WITH -1' ||
                       ' INCREMENT BY -1 ' ||
                       ' MAXVALUE -1 '  ||
                       ' MINVALUE -9999999999999999999999999999 ' ||
                       ' NOCYCLE' ||
                       ' NOCACHE' ||
                       ' NOORDER' );
    EXCEPTION
      WHEN OTHERS THEN NULL;
  END;
END;
/

CREATE OR REPLACE TRIGGER dvsbanner_tmp_t0_ainc
 BEFORE INSERT OR UPDATE OF t_bcid ON dvsbanner_tmp FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_bcid = 0 OR :new.t_bcid IS NULL) THEN
   SELECT dvsbanner_tmp_seq.nextval INTO :new.t_bcid FROM dual;
 ELSE
   select last_number into v_id from user_sequences where sequence_name = upper ('dvsbanner_tmp_SEQ');
   IF :new.t_bcid <= v_id THEN
     RAISE DUP_VAL_ON_INDEX;
   END IF;
 END IF;
END;
/

-- DDL_LEG_TMP
DECLARE
  e_sequence_does_not_exist EXCEPTION;
  PRAGMA EXCEPTION_INIT( e_sequence_does_not_exist, -2289 );
BEGIN

  BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE DDL_LEG_TMP_SEQ';
  EXCEPTION
    WHEN e_sequence_does_not_exist THEN NULL;
  END;

  BEGIN
    EXECUTE IMMEDIATE( ' CREATE SEQUENCE DDL_LEG_TMP_SEQ'  ||
                       ' START WITH -1' ||
                       ' INCREMENT BY -1 ' ||
                       ' MAXVALUE -1 '  ||
                       ' MINVALUE -9999999999999999999999999999 ' ||
                       ' NOCYCLE' ||
                       ' NOCACHE' ||
                       ' NOORDER' );
    EXCEPTION
      WHEN OTHERS THEN NULL;
  END;
END;
/

CREATE OR REPLACE TRIGGER ddl_leg_tmp_t1_ainc
 BEFORE INSERT OR UPDATE OF t_id ON ddl_leg_tmp FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_id = 0 OR :new.t_id IS NULL) THEN
  SELECT ddl_leg_tmp_seq.nextval INTO :new.t_id FROM dual;
 ELSE
 select last_number into v_id from user_sequences where sequence_name = upper ('ddl_leg_tmp_SEQ');
  IF :new.t_id <= v_id THEN
   RAISE DUP_VAL_ON_INDEX;
  END IF;
 END IF;
END;
/
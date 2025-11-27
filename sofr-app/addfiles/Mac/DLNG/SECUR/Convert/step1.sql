CREATE TABLE DSCIDMAP_TMP
(
  T_TABLE    NUMBER(5),
  T_OLDID    NUMBER(10),
  T_NEWID    NUMBER(10),
  T_ISMOVE   CHAR(1) DEFAULT CHR(0)
);


CREATE UNIQUE INDEX DSCIDMAP_TMP_IDX0 ON DSCIDMAP_TMP ( T_TABLE, T_OLDID );

CREATE UNIQUE INDEX DSCIDMAP_TMP_IDX1 ON DSCIDMAP_TMP ( T_TABLE, T_NEWID );

CREATE INDEX DSCIDMAP_TMP_IDX2 ON DSCIDMAP_TMP ( T_TABLE, T_ISMOVE );

CREATE TABLE DSCIDMAP_ER_TMP
(
  T_ID     NUMBER(10),
  T_TYPE   VARCHAR2(32),
  T_GLOBAL VARCHAR2(32),
  T_FUN    VARCHAR2(64),
  T_MES    VARCHAR2(512)
);

CREATE UNIQUE INDEX DSCIDMAP_ER_TMP_IDX0 ON DSCIDMAP_ER_TMP ( T_ID );

CREATE SEQUENCE DSCIDMAP_ER_TMP_SEQ
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  NOCACHE
  NOORDER;

CREATE OR REPLACE TRIGGER DSCIDMAP_ER_TMP_AINC
  BEFORE INSERT OR UPDATE OF T_ID ON DSCIDMAP_ER_TMP FOR EACH ROW
DECLARE
  v_id INTEGER;
BEGIN
  IF (:new.t_ID = 0 OR :new.t_ID IS NULL) THEN
    SELECT dscidmap_er_tmp_seq.nextval INTO :new.t_ID FROM dual;
  ELSE
    select last_number into v_id from user_sequences where sequence_name = upper('DSCIDMAP_ER_TMP_SEQ');
    IF :new.t_ID >= v_id THEN
      RAISE DUP_VAL_ON_INDEX;
    END IF;
  END IF;
END;
/

CREATE OR REPLACE PACKAGE SCIDMAP
IS

  OldSelfID                 CONSTANT NUMBER := 929;  -- Нужно отредактировать Id ИБТ
  NewSelfID                 CONSTANT NUMBER := 1;     -- Нужно отредактировать Id НБТ
  GlobalFun                 VARCHAR2(32) := '';

  FUNCTION PutObjMappedID( p_Table NUMBER, p_OldID NUMBER, p_NewID NUMBER, p_IsMove CHAR ) RETURN NUMBER;

  FUNCTION GetObjMappedID( p_Table NUMBER, p_OldID NUMBER, p_IsMandatory NUMBER ) RETURN NUMBER;

  PROCEDURE SetError( p_type VARCHAR2, p_fun VARCHAR2, p_mes VARCHAR2 );

END SCIDMAP;
/
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
/*
1 - dparty_dbt
2 - dfininstr_dbt
3 - dsettacc_dbt
4 - dsfcontr_dbt
5 - ddl_tick_dbt
6 - ddl_leg_dbt
7 - dpmpaym_dbt
8 - doproper_dbt
9  -dpmwrtsum_dbt
*/
CREATE OR REPLACE PACKAGE BODY SCIDMAP
IS

  FUNCTION PutObjMappedID( p_Table NUMBER, p_OldID NUMBER, p_NewID NUMBER, p_IsMove CHAR ) RETURN NUMBER
   IS
  BEGIN

    INSERT INTO DSCIDMAP_TMP ( T_TABLE, T_OLDID, T_NEWID, T_ISMOVE ) VALUES ( p_Table, p_OldID, p_NewID, p_IsMove );

    RETURN 0;

  EXCEPTION
    WHEN OTHERS THEN SetError('ERROR','PutObjMappedID','Невозможно вставить запись. Table = '||p_Table||' OldID = '||p_OldID||' NewID = '||p_NewID); RETURN 1;
  END;
  
  FUNCTION GetObjMappedID( p_Table NUMBER, p_OldID NUMBER, p_IsMandatory NUMBER ) RETURN NUMBER
   IS
   v_NewID NUMBER;
  BEGIN

    IF( ( (p_OldID = -1) OR (p_OldID = 0) ) AND (p_Table <> 1) ) THEN

       IF( p_IsMandatory = 0 ) THEN
          RETURN p_OldID;
       ELSE
          SetError('ERROR','GetObjMappedID','Не задана обязательная ссылка. Table = '||p_Table||' OldID = '||p_OldID||' IsMandatory = '||p_IsMandatory);
          RAISE_APPLICATION_ERROR(-20500,'Не задана обязательная ссылка. Table = '||p_Table||' OldID = '||p_OldID||' IsMandatory = '||p_IsMandatory);
          RETURN 0;
       END IF;

    ELSIF( ( (p_OldID = -1) OR (p_OldID = 0) ) AND (p_Table = 1) ) THEN

       RETURN p_OldID;

    ELSIF( (p_OldID = OldSelfID) AND (p_Table = 1) ) THEN

       RETURN NewSelfID;
    
    ELSE

       BEGIN
         SELECT T_NEWID INTO v_NewID
           FROM DSCIDMAP_TMP
          WHERE T_TABLE = p_Table AND
                T_OLDID = p_OldID;
       EXCEPTION
         WHEN OTHERS THEN SetError('ERROR','GetObjMappedID','Не найден объект по ссылке. Table = '||p_Table||' OldID = '||p_OldID||' IsMandatory = '||p_IsMandatory); RAISE_APPLICATION_ERROR(-20501,'Не найден объект по ссылке. Table = '||p_Table||' OldID = '||p_OldID||' IsMandatory = '||p_IsMandatory); RETURN 0;
       END;

       RETURN v_NewID;

    END IF;

  END;

  PROCEDURE SetError( p_type VARCHAR2, p_fun VARCHAR2, p_mes VARCHAR2 )
   IS
   v_Err VARCHAR2(160);
  BEGIN
    v_Err := ' ';
    IF( p_type = 'ERROR' ) THEN
       v_Err := ' SQLCODE = '||SQLCODE||' ERROR = '||SUBSTR(SQLERRM, 1, 100);
    END IF;

    INSERT INTO DSCIDMAP_ER_TMP (T_TYPE, T_GLOBAL, T_FUN, T_MES) VALUES( p_type, GlobalFun, p_fun, p_mes||v_Err );

  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;

END SCIDMAP;
/


INSERT INTO DOBJKCODE_DBT ( T_OBJECTTYPE, T_CODEKIND, T_NAME, T_SHORTNAME, T_MAXCODELEN, T_CASESENSITIVE, T_DEFINITION, T_MACROFILE, T_MACROPROC, T_DUPLICATE ) 
                   VALUES ( 3, 400, 'Код в ИБТ', 'КИБТ', 35, CHR(0), CHR(1), CHR(1), CHR(1), CHR(0) ); 

-- Скрипты на таблицу dcheckaddress_tmp

DECLARE
  e_object_not_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_object_not_exists, -942);
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE dcheckaddress_tmp CASCADE CONSTRAINTS';
EXCEPTION 
  WHEN e_object_not_exists THEN NULL;
END;
/

DECLARE 
  e_object_exists EXCEPTION; 
  PRAGMA EXCEPTION_INIT(e_object_exists, -955);
BEGIN
  EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE dcheckaddress_tmp ' ||
                    '(                                               ' ||
                    '  t_PartyID                 NUMBER(10)          ' ||
                    ' ,t_AddressType             NUMBER(5)           ' ||
                    ' ,t_CheckFlag               NUMBER(5)           ' ||
                    ')                                               ' ||
                    'ON COMMIT PRESERVE ROWS                         ' ;
EXCEPTION 
  WHEN e_object_exists THEN NULL; 
END;
/

DECLARE 
  e_object_exists EXCEPTION; 
  PRAGMA EXCEPTION_INIT(e_object_exists, -955);
BEGIN 
  EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX dcheckaddress_tmp_idx0 ON dcheckaddress_tmp ' ||
                    '(                                                               ' ||
                    '  t_PartyID                                                     ' ||
                    ' ,t_AddressType                                                 ' ||
                    ')                                                               ' ;  
EXCEPTION 
  WHEN e_object_exists THEN NULL; 
END;
/

DECLARE
  e_object_not_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_object_not_exists, -942);
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE drpchkaddress_tmp CASCADE CONSTRAINTS';
EXCEPTION 
  WHEN e_object_not_exists THEN NULL;
END;
/

DECLARE 
  e_object_exists EXCEPTION; 
  PRAGMA EXCEPTION_INIT(e_object_exists, -955);
BEGIN
  EXECUTE IMMEDIATE 'CREATE GLOBAL TEMPORARY TABLE drpchkaddress_tmp ' ||
                    '(                                               ' ||
                    '  t_AutoKey                 NUMBER(10)          ' ||
                    ' ,t_PartyID                 NUMBER(10)          ' ||
                    ' ,t_AddressType             NUMBER(5)           ' ||
                    ' ,t_RowType                 NUMBER(5)           ' ||
                    ' ,t_Title                   VARCHAR2(511)       ' ||
                    ' ,t_Context                 VARCHAR2(511)       ' ||
                    ')                                               ' ||
                    'ON COMMIT PRESERVE ROWS                         ' ;
EXCEPTION 
  WHEN e_object_exists THEN NULL; 
END;
/

DECLARE 
  e_object_exists EXCEPTION; 
  PRAGMA EXCEPTION_INIT(e_object_exists, -955);
BEGIN 
  EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX drpchkaddress_tmp_idx0 ON drpchkaddress_tmp ' ||
                    '(                                                               ' ||
                    '  t_AutoKey                                                     ' ||
                    ')                                                               ' ;  
EXCEPTION 
  WHEN e_object_exists THEN NULL; 
END;
/

DECLARE 
  e_object_exists EXCEPTION; 
  PRAGMA EXCEPTION_INIT(e_object_exists, -955);
BEGIN 
  EXECUTE IMMEDIATE 'CREATE INDEX drpchkaddress_tmp_idx1 ON drpchkaddress_tmp ' ||
                    '(                                                        ' ||
                    '  t_PartyID                                              ' ||
                    ' ,t_AddressType                                          ' ||
                    ')                                                        ' ;  
EXCEPTION 
  WHEN e_object_exists THEN NULL; 
END;
/

DECLARE
  e_sequence_not_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_sequence_not_exists, -2289);
BEGIN
  EXECUTE IMMEDIATE 'DROP SEQUENCE drpchkaddress_tmp_seq';
EXCEPTION
  WHEN e_sequence_not_exists THEN NULL;
END;
/

DECLARE 
  e_object_exists EXCEPTION; 
  PRAGMA EXCEPTION_INIT(e_object_exists, -955);
BEGIN 
  EXECUTE IMMEDIATE 'CREATE SEQUENCE drpchkaddress_tmp_seq START WITH 1';
EXCEPTION 
  WHEN e_object_exists THEN NULL; 
END;
/

CREATE OR REPLACE TRIGGER drpchkaddress_tmp_t0_ainc
 BEFORE INSERT OR UPDATE OF t_AutoKey ON drpchkaddress_tmp FOR EACH ROW
DECLARE
 v_id INTEGER;
BEGIN
 IF (:new.t_AutoKey = 0 OR :new.t_AutoKey IS NULL) THEN
 SELECT drpchkaddress_tmp_seq.nextval INTO :new.t_AutoKey FROM dual;
 ELSE
 select last_number into v_id from user_sequences where sequence_name = upper ('drpchkaddress_tmp_seq');
 IF :new.t_AutoKey >= v_id THEN
 RAISE DUP_VAL_ON_INDEX;
 END IF;
 END IF;
END;
/
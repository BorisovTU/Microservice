DECLARE
  e_exist_field EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_exist_field, -1430);
BEGIN
  EXECUTE IMMEDIATE 'ALTER TABLE USR_ACC306ENROLL_DBT ADD t_IsReliable char(1 byte) default chr(0)';
  EXECUTE IMMEDIATE 'COMMENT ON COLUMN USR_ACC306ENROLL_DBT.t_IsReliable IS ''Параметры операции надежно определены''';
EXCEPTION 
  WHEN e_exist_field THEN NULL;
END;


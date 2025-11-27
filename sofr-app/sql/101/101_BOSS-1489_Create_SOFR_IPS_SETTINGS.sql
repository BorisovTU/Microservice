DECLARE
   table_already_exist EXCEPTION;
   PRAGMA EXCEPTION_INIT(table_already_exist, -955);
BEGIN
   EXECUTE IMMEDIATE ' CREATE TABLE SOFR_IPS_SETTINGS ( ' ||
                     '                                   t_ID NUMBER(5) NOT NULL, ' ||
                     '                                   t_ServiceName VARCHAR2(50), ' ||
                     '                                   t_ProcName VARCHAR2(50), ' ||
                     '                                   t_ProcNameAnswer VARCHAR2(50), ' ||
                     '                                   t_Direction NUMBER(5), ' ||
                     '                                   t_TargetSystem VARCHAR2(100), ' ||
                     '                                   t_Comment VARCHAR2(255) ' ||
                     '                                ) ';
   COMMIT;
EXCEPTION
   WHEN table_already_exist
   THEN NULL;
END;
/
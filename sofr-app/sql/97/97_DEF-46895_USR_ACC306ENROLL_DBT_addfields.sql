/*Создание новых полей*/
DECLARE
  e_exist_field EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_exist_field, -1430);
BEGIN
  EXECUTE IMMEDIATE 'ALTER TABLE USR_ACC306ENROLL_DBT ADD (T_LASTNAME VARCHAR2(24), T_FIRSTNAME VARCHAR2(24), T_MIDDLENAME VARCHAR2(24))';

EXCEPTION 
  WHEN e_exist_field THEN NULL;
END;
/

/*Заполнение новых полей для старых записей*/
BEGIN
  UPDATE USR_ACC306ENROLL_DBT ua
     SET (ua.t_LastName, ua.t_FirstName, ua.t_MiddleName) =
            (SELECT persn.t_Name1, persn.t_Name2, persn.t_Name3
               FROM dnptxop_dbt nptxop, dpersn_dbt persn
              WHERE nptxop.t_ID = ua.t_NptxopID
                AND persn.t_PersonID = nptxop.t_Client)
   WHERE ua.t_NptxopID > 0;

EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

BEGIN
  UPDATE USR_ACC306ENROLL_DBT
     SET t_LastName = SUBSTR(t_ClientName, 1, INSTR(t_ClientName,' ')-1),
         t_FirstName = SUBSTR(t_ClientName, INSTR(t_ClientName,' ')+1, INSTR(t_ClientName,' ',INSTR(t_ClientName,' ')+1)-(INSTR(t_ClientName,' ')+1)),
         t_MiddleName = SUBSTR(t_ClientName, INSTR(t_ClientName,' ',INSTR(t_ClientName,' ')+1)+1)
   WHERE t_NptxopID = -1;

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


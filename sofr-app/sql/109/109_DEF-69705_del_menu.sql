/*Удаление пункта меню "Журнал операций"*/
DECLARE
   v_cnt                 NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 16414;  --Номер модуля

   PROCEDURE DeleteMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
   BEGIN
      
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = C_IDENTPROGRAM_CODE
             AND t_icaseitem = C_CASEITEM;

      IF v_cnt != 0 
      THEN
         DELETE FROM dmenuitem_dbt 
               WHERE t_objectid = p_numberoper 
                 AND t_istemplate = p_istemplate 
                 AND t_iidentprogram = C_IDENTPROGRAM_CODE 
                 AND t_icaseitem = C_CASEITEM;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10011,10012,10013,10014))
   LOOP
      DeleteMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10011,10012,10013,10014)
                AND rol.T_MENUID > 0)
   LOOP
      DeleteMenu (i.t_MenuID, CHR(88));
   END LOOP;

   --Для технического пользователя 1
   DeleteMenu (1, CHR (0));

EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (C_IDENTPROGRAM_CODE || ' ' || C_CASEITEM);
      RAISE;
END;
/
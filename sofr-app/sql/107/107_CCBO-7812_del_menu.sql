/*Удаление меню*/
DECLARE
   v_cnt                 NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 158; --Подсистема
   C_CASEITEM            NUMBER := 26005;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Отчет по переносам по счетам';
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
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      DeleteMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
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

DECLARE
   v_cnt                 NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 78; --Подсистема
   C_CASEITEM            NUMBER := 26005;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Отчет по переносам по счетам';
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
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      DeleteMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
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

DECLARE
   v_cnt                 NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 89; --Подсистема
   C_CASEITEM            NUMBER := 26005;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Отчет по переносам по счетам';
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
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      DeleteMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
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

DECLARE
   v_cnt                 NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 74; --Подсистема
   C_CASEITEM            NUMBER := 26005;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Отчет по переносам по счетам';
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
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      DeleteMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
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

DECLARE
   v_cnt                 NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 26005;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Отчет по переносам по счетам';
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
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      DeleteMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
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

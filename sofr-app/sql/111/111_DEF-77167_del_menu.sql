DECLARE
   C_NAME   VARCHAR2 (255)
               := ' Отчет по переносам по счетам';

   PROCEDURE DeleteMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
   BEGIN
      DELETE FROM dmenuitem_dbt
            WHERE     T_OBJECTID = p_numberoper
                  AND T_IIDENTPROGRAM IN (78, 83, 89, 158)
                  AND T_ICASEITEM = 26005
                  AND T_INUMBERFATHER = 0
                  AND T_SZNAMEITEM = C_NAME
                  AND T_ISTEMPLATE = p_istemplate;
   END;
BEGIN
   --Для пользователей ролей
   FOR i
      IN (SELECT DISTINCT rol.T_OPER
            FROM DACSOPROLE_DBT rol
           WHERE ROL.T_ROLEID IN
                    (10026,
                     10025,
                     10028,
                     10010,
                     10015,
                     10011,
                     10013,
                     10004,
                     10016,
                     10017,
                     10029,
                     10032,
                     10012))
   LOOP
      DeleteMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i
      IN (SELECT DISTINCT rol.T_MENUID
            FROM DACSROLETREE_DBT rol
           WHERE rol.T_ROLEID IN
                    (10026,
                     10025,
                     10028,
                     10010,
                     10015,
                     10011,
                     10013,
                     10004,
                     10016,
                     10017,
                     10029,
                     10032,
                     10012)
                 AND rol.T_MENUID > 0)
   LOOP
      DeleteMenu (i.t_MenuID, CHR (88));
   END LOOP;

   --Для технического пользователя 1
   DeleteMenu (1, CHR (0));
END;
/

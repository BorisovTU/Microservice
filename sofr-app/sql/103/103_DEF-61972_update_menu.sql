/*Обновить пункт меню для отчета*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 507;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Отчеты для уполномоченных представителей БР';
   V_NUMBERFATHER        NUMBER := 0;
   C_NUMBERLINE          NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := -1;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) in ('Отчеты', '~О~тчеты')
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = 0),
                -1)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) in ('Регламентированные','~Р~егламентированные')
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                -1)
        INTO V_NUMBERFATHER
        FROM DUAL;

      IF V_NUMBERFATHER > 0
      THEN
         UPDATE dmenuitem_dbt SET t_inumberfather = V_NUMBERFATHER, t_inumberline = 50
         WHERE t_objectid = p_numberoper AND T_ISTEMPLATE = p_istemplate AND t_iidentprogram = C_IDENTPROGRAM_CODE AND t_inumberfather IN (-1,0) AND trim(t_sznameitem) = 'Отчеты для уполномоченных представителей БР';
          
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10010, 10025, 10026, 10028))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;  

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10010, 10025, 10026, 10028)
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP; 
   
   --Для технического пользователя 1
   InsertMenu (1, CHR (0));

END;
/

/*Обновить пункт меню для отчета*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 507;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Еженедельный отчет об операциях с ценными бумагами (СВОД)';
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
                  WHERE     trim(t_sznameitem) = 'Отчеты'
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
                  WHERE     trim(t_sznameitem) in ('Регламентированные')
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                -1)
        INTO V_NUMBERFATHER
        FROM DUAL;
    
    SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) in ('Отчеты для уполномоченных представителей БР')
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                -1)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = C_IDENTPROGRAM_CODE
             AND t_icaseitem = C_CASEITEM
             AND t_inumberfather = V_NUMBERFATHER
             AND trim(t_sznameitem) = C_NAME;

      
      IF v_cnt = 0 
      THEN
		 
		 UPDATE dmenuitem_dbt SET t_sznameitem = ' Еженедельный отчет об операциях с ценными бумагами (СВОД)', t_inumberfather = V_NUMBERFATHER, t_inumberline = 1
		 WHERE t_objectid = p_numberoper AND T_ISTEMPLATE = p_istemplate AND t_icaseitem = C_CASEITEM AND t_iidentprogram = C_IDENTPROGRAM_CODE AND trim(t_sznameitem) = 'Еженедельный отчет по ЦБ';
		 
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

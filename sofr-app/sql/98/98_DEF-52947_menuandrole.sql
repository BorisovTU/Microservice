/*Связь роли и группы, удаление лишнего меню*/
BEGIN
   INSERT INTO DACSGROUPROLE_DBT (T_GROUPID, T_ROLEID)
               VALUES (10036, 10036);
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

/*Удалить пункт меню для отчетов у роли и её образца*/
DECLARE
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   V_NUMBERFATHER        NUMBER := 0;

   PROCEDURE DeleteMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'СНОБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      IF V_NUMBERFATHER > 0 THEN
        DELETE FROM dmenuitem_dbt
          WHERE     trim(t_sznameitem) = 'Отчеты'
                AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                AND T_ISTEMPLATE = p_istemplate
                AND T_OBJECTID = p_numberoper
                AND t_inumberfather = V_NUMBERFATHER;
      END IF;

      DELETE FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = C_IDENTPROGRAM_CODE
             AND T_ICASEITEM IN (20209, 20210);

    END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10008))
   LOOP
      DeleteMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (10008))
   LOOP
      DeleteMenu (i.t_MenuID, CHR(88));
   END LOOP;

EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
/

BEGIN
 EXECUTE IMMEDIATE 'COMMIT';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/
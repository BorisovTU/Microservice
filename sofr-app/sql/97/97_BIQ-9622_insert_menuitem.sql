/* обновление меню под новые пункты */

BEGIN
   UPDATE dmenuitem_dbt SET t_sznameitem=' Реестр ВУ (старый)' where t_icaseitem=23 and t_iidentprogram in (83, 158);
   DELETE FROM DITEMSYST_DBT WHERE T_CIDENTPROGRAM='S' AND T_ICASEITEM=20210;
   INSERT INTO DITEMSYST_DBT(T_CIDENTPROGRAM,T_ICASEITEM,T_IKINDMETHOD,T_IKINDPROGRAM,T_IHELP,T_RESERVE,T_SZNAMEITEM,T_PARM )
               VALUES('S',20301,1,2,0,CHR(1),'Реестр ВУ','6E726567697374657269615F73632E6D6163000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006E726567697374657269615F73632E6D6163000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');
   INSERT INTO DITEMSYST_DBT(T_CIDENTPROGRAM,T_ICASEITEM,T_IKINDMETHOD,T_IKINDPROGRAM,T_IHELP,T_RESERVE,T_SZNAMEITEM,T_PARM )
               VALUES('Ю',25125,1,2,0,CHR(1),'Реестр ВУ','000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006E726567697374657269615F64762E6D6163000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');
               
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

/*Добавить пункт меню для отчета*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 20301;  --Номер модуля
   C_NAME                VARCHAR2 (255) := ' Реестр ВУ';
   V_NUMBERFATHER        NUMBER := 0;
   C_NUMBERLINE          NUMBER := 50;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := -1;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Внутренний учет'
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
                  WHERE     trim(t_sznameitem) in ('Отчеты','~О~тчеты')
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = C_IDENTPROGRAM_CODE
             AND t_icaseitem = C_CASEITEM
             AND t_inumberfather = V_NUMBERFATHER;

      
      IF v_cnt = 0 AND V_NUMBERFATHER > 0
      THEN
         SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE;

         v_NumberPoint := v_NumberPoint + 1;

         INSERT INTO dmenuitem_dbt (t_objectid,
                                    t_istemplate,
                                    t_iidentprogram,
                                    t_inumberpoint,
                                    t_inumberfather,
                                    t_inumberline,
                                    t_icaseitem,
                                    t_csystemitem,
                                    t_sznameitem,
                                    t_sznameprompt,
                                    t_ihelp,
                                    t_iprogitem)
              VALUES (p_numberoper,
                      p_istemplate,
                      C_IDENTPROGRAM_CODE,
                      v_NumberPoint,
                      V_NUMBERFATHER,
                      C_NUMBERLINE,
                      C_CASEITEM,
                      CHR (88),
                      ' ' || C_NAME,
                      ' ' || C_NAME,
                      0,
                      C_IDENTPROGRAM_CODE);
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10025, 10026, 10008, 10028))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (10025, 10026, 10008, 10028))
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

   --Для технического пользователя 1
   InsertMenu (1, CHR (0));

   COMMIT;
EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
/

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 158; --Подсистема
   C_CASEITEM            NUMBER := 25125;  --Номер модуля
   C_NAME                VARCHAR2 (255) := ' Реестр ВУ';
   V_NUMBERFATHER        NUMBER := 0;
   C_NUMBERLINE          NUMBER := 50;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := -1;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Внутренний учет'
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
                  WHERE     trim(t_sznameitem) in ('Отчеты','~О~тчеты')
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = C_IDENTPROGRAM_CODE
             AND t_icaseitem = C_CASEITEM
             AND t_inumberfather = V_NUMBERFATHER;

      
      IF v_cnt = 0 AND V_NUMBERFATHER > 0
      THEN
         SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE;

         v_NumberPoint := v_NumberPoint + 1;

         INSERT INTO dmenuitem_dbt (t_objectid,
                                    t_istemplate,
                                    t_iidentprogram,
                                    t_inumberpoint,
                                    t_inumberfather,
                                    t_inumberline,
                                    t_icaseitem,
                                    t_csystemitem,
                                    t_sznameitem,
                                    t_sznameprompt,
                                    t_ihelp,
                                    t_iprogitem)
              VALUES (p_numberoper,
                      p_istemplate,
                      C_IDENTPROGRAM_CODE,
                      v_NumberPoint,
                      V_NUMBERFATHER,
                      C_NUMBERLINE,
                      C_CASEITEM,
                      CHR (88),
                      ' ' || C_NAME,
                      ' ' || C_NAME,
                      0,
                      C_IDENTPROGRAM_CODE);
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10025, 10026, 10008, 10028))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (10025, 10026, 10008, 10028))
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

   --Для технического пользователя 1
   InsertMenu (1, CHR (0));

   COMMIT;
EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
/

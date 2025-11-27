/* обновление меню под новый пункт */
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83;
   C_CASEITEM            NUMBER := 950;
   C_NAME                VARCHAR2 (255)
      := 'Обновить биржевые коды в лимитах';
   V_NUMBERFATHER        NUMBER := 0;
   C_NUMBERLINE          NUMBER := 50;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = C_IDENTPROGRAM_CODE
             AND t_icaseitem = C_CASEITEM;

      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT T_INUMBERPOINT
                   FROM dmenuitem_dbt
                  WHERE     t_sznameitem LIKE '%Лимиты QUIK%'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

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
                      CHR (0),
                      ' ' || C_NAME,
                      ' ' || C_NAME,
                      0,
                      C_IDENTPROGRAM_CODE);
      END IF;
   END;
BEGIN
   SELECT COUNT (*)
     INTO v_cnt
     FROM DITEMUSER_DBT
    WHERE T_CIDENTPROGRAM = CHR (C_IDENTPROGRAM_CODE)
          AND T_ICASEITEM = C_CASEITEM;

   IF v_cnt = 0
   THEN
      INSERT INTO DITEMUSER_DBT (T_CIDENTPROGRAM,
                                 T_ICASEITEM,
                                 T_IKINDMETHOD,
                                 T_IKINDPROGRAM,
                                 T_IHELP,
                                 T_RESERVE,
                                 T_SZNAMEITEM,
                                 T_PARM)
           VALUES (
                     CHR (C_IDENTPROGRAM_CODE),
                     C_CASEITEM,
                     1,
                     2,
                     0,
                     CHR (1),
                     C_NAME,
                     '0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000073636C696D757064636F6465732E6D616300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');
   END IF;

   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10010, 10012, 10014))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   InsertMenu (1, CHR (0));

   COMMIT;
EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
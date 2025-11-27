DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 74; --Подсистема
   C_NUMBERLINE          NUMBER := 0;
   V_NUMBERFATHER        NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = 0),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

      C_NUMBERLINE := C_NUMBERLINE + 10;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Контрольные отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO v_NumberPoint
        FROM DUAL;

      IF v_NumberPoint = 0
      THEN
          SELECT NVL (MAX (t_inumberpoint), 0)
            INTO v_NumberPoint
            FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE;

          v_NumberPoint := v_NumberPoint + 1;
          UPDATE dmenuitem_dbt SET T_IIDENTPROGRAM = C_IDENTPROGRAM_CODE, T_IPROGITEM = C_IDENTPROGRAM_CODE, t_inumberline = C_NUMBERLINE, t_inumberfather = 0, T_INUMBERPOINT = v_NumberPoint WHERE T_IPROGITEM = 86
                          AND T_ISTEMPLATE = p_istemplate
                          AND T_OBJECTID = p_numberoper
                          AND trim(t_sznameitem) = 'Контрольные отчеты';
          V_NUMBERFATHER := v_NumberPoint;
          C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHER),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

      C_NUMBERLINE := C_NUMBERLINE + 10;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Отчет о закрытых счетах'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO v_NumberPoint
        FROM DUAL;

      IF v_NumberPoint = 0
      THEN
         SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE;

         v_NumberPoint := v_NumberPoint + 1;
         UPDATE dmenuitem_dbt SET T_IIDENTPROGRAM = C_IDENTPROGRAM_CODE, T_IPROGITEM = C_IDENTPROGRAM_CODE, t_inumberline = C_NUMBERLINE, t_inumberfather = V_NUMBERFATHER, T_INUMBERPOINT = v_NumberPoint WHERE T_IPROGITEM = 86
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND trim(t_sznameitem) = 'Отчет о закрытых счетах';
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

   --Для технического пользователя 1
   InsertMenu (1, CHR (0));

EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
/

/*Исправить подменю*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 158; --Подсистема
   C_CASEITEM            NUMBER := 0;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Контрольные отчеты';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERFATHERREP     NUMBER := 0;
   C_NUMBERLINE          NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;
      V_NUMBERFATHERREP := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = 0
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHERREP
        FROM DUAL;
      
      IF V_NUMBERFATHERREP = 0 THEN 
          SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = 0),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      0,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' Отчеты',
                      ' Отчеты',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHERREP := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = V_NUMBERFATHERREP
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      IF V_NUMBERFATHER = 0 THEN 
          SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHERREP),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      V_NUMBERFATHERREP,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' РСХБ',
                      ' РСХБ',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHER := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHER),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

      C_NUMBERLINE := C_NUMBERLINE + 10;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Контрольные отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO v_NumberPoint
        FROM DUAL;

      IF v_NumberPoint > 0
      THEN
         UPDATE dmenuitem_dbt SET t_inumberfather = V_NUMBERFATHER, t_inumberline = C_NUMBERLINE WHERE T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND T_INUMBERPOINT = v_NumberPoint;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

   --Для технического пользователя 1
   InsertMenu (1, CHR (0));

EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
/

/*Исправить подменю*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 78; --Подсистема
   C_CASEITEM            NUMBER := 0;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Контрольные отчеты';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERFATHERREP     NUMBER := 0;
   C_NUMBERLINE          NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;
      V_NUMBERFATHERREP := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = 0
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHERREP
        FROM DUAL;
      
      IF V_NUMBERFATHERREP = 0 THEN 
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = 0),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      0,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' Отчеты',
                      ' Отчеты',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHERREP := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = V_NUMBERFATHERREP
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      IF V_NUMBERFATHER = 0 THEN 
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHERREP),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      V_NUMBERFATHERREP,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' РСХБ',
                      ' РСХБ',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHER := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHER),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

      C_NUMBERLINE := C_NUMBERLINE + 10;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Контрольные отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO v_NumberPoint
        FROM DUAL;

      IF v_NumberPoint > 0
      THEN
         UPDATE dmenuitem_dbt SET t_inumberfather = V_NUMBERFATHER, t_inumberline = C_NUMBERLINE WHERE T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND T_INUMBERPOINT = v_NumberPoint;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

   --Для технического пользователя 1
   InsertMenu (1, CHR (0));

EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
/

/*Исправить подменю*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 0;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Контрольные отчеты';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERFATHERREP     NUMBER := 0;
   C_NUMBERLINE          NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;
      V_NUMBERFATHERREP := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = 0
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHERREP
        FROM DUAL;
      
      IF V_NUMBERFATHERREP = 0 THEN 
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = 0),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      0,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' Отчеты',
                      ' Отчеты',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHERREP := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = V_NUMBERFATHERREP
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      IF V_NUMBERFATHER = 0 THEN 
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHERREP),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      V_NUMBERFATHERREP,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' РСХБ',
                      ' РСХБ',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHER := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHER),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

      C_NUMBERLINE := C_NUMBERLINE + 10;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Контрольные отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO v_NumberPoint
        FROM DUAL;

      IF v_NumberPoint > 0
      THEN
         UPDATE dmenuitem_dbt SET t_inumberfather = V_NUMBERFATHER, t_inumberline = C_NUMBERLINE WHERE T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND T_INUMBERPOINT = v_NumberPoint;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

   --Для технического пользователя 1
   InsertMenu (1, CHR (0));

EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
/

/*Исправить подменю*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 74; --Подсистема
   C_CASEITEM            NUMBER := 0;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Контрольные отчеты';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERFATHERREP     NUMBER := 0;
   V_NUMBERFATHERRSHB    NUMBER := 0;
   C_NUMBERLINE          NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;
      V_NUMBERFATHERREP := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = 0
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHERREP
        FROM DUAL;
      
      IF V_NUMBERFATHERREP = 0 THEN 
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = 0),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      0,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' Отчеты',
                      ' Отчеты',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHERREP := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = V_NUMBERFATHERREP
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHERRSHB
        FROM DUAL;
      
      IF V_NUMBERFATHERRSHB = 0 THEN 
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHERREP),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      V_NUMBERFATHERREP,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' РСХБ',
                      ' РСХБ',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHERRSHB := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Контрольные отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather in (0, V_NUMBERFATHERRSHB)
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      IF V_NUMBERFATHER = 0 THEN 
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHERRSHB),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      V_NUMBERFATHERRSHB,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' Контрольные отчеты',
                      ' Контрольные отчеты',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHER := v_NumberPoint;
         C_NUMBERLINE := 0;

         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHER),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

         SELECT NVL (
                  (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                     FROM dmenuitem_dbt
                     WHERE     trim(t_sznameitem) = 'Отчет о закрытых счетах'
                           AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                           AND T_ISTEMPLATE = p_istemplate
                           AND T_OBJECTID = p_numberoper),
                  0)
         INTO v_NumberPoint
         FROM DUAL;

         IF v_NumberPoint > 0
         THEN
            UPDATE dmenuitem_dbt SET t_inumberfather = V_NUMBERFATHER WHERE T_IPROGITEM = C_IDENTPROGRAM_CODE
                           AND T_ISTEMPLATE = p_istemplate
                           AND T_OBJECTID = p_numberoper
                           AND T_INUMBERPOINT = v_NumberPoint;
         END IF;
      ELSE
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHERRSHB),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

         IF v_NumberPoint > 0
         THEN
            UPDATE dmenuitem_dbt SET t_inumberfather = V_NUMBERFATHERRSHB, t_inumberline = C_NUMBERLINE WHERE T_IPROGITEM = C_IDENTPROGRAM_CODE
                           AND T_ISTEMPLATE = p_istemplate
                           AND T_OBJECTID = p_numberoper
                           AND T_INUMBERPOINT = V_NUMBERFATHER;
         END IF;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

   --Для технического пользователя 1
   InsertMenu (1, CHR (0));

EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
/

/*Исправить подменю*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 89; --Подсистема
   C_CASEITEM            NUMBER := 0;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Контрольные отчеты';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERFATHERREP     NUMBER := 0;
   C_NUMBERLINE          NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;
      V_NUMBERFATHERREP := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = 0
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHERREP
        FROM DUAL;
      
      IF V_NUMBERFATHERREP = 0 THEN 
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = 0),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      0,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' Отчеты',
                      ' Отчеты',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHERREP := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = V_NUMBERFATHERREP
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      IF V_NUMBERFATHER = 0 THEN 
         SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHERREP),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;
         

         C_NUMBERLINE := C_NUMBERLINE + 10;

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
                      V_NUMBERFATHERREP,
                      C_NUMBERLINE,
                      0,
                      CHR (88),
                      ' РСХБ',
                      ' РСХБ',
                      0,
                      C_IDENTPROGRAM_CODE);
         V_NUMBERFATHER := v_NumberPoint;
         C_NUMBERLINE := 0;
      END IF;

      SELECT NVL (
                (SELECT MAX(t_inumberline)
                   FROM dmenuitem_dbt
                  WHERE  t_objectid = p_numberoper
                   AND t_istemplate = p_istemplate
                   AND t_iidentprogram = C_IDENTPROGRAM_CODE
                   AND t_inumberfather = V_NUMBERFATHER),
                0)
           INTO C_NUMBERLINE
           FROM DUAL;

      C_NUMBERLINE := C_NUMBERLINE + 10;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Контрольные отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO v_NumberPoint
        FROM DUAL;

      IF v_NumberPoint > 0
      THEN
         UPDATE dmenuitem_dbt SET t_inumberfather = V_NUMBERFATHER, t_inumberline = C_NUMBERLINE WHERE T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND T_INUMBERPOINT = v_NumberPoint;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10025,10026,10028,10010,10015,10011,10013,10004,10016,10017,10029,10032,10012)
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

   --Для технического пользователя 1
   InsertMenu (1, CHR (0));

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
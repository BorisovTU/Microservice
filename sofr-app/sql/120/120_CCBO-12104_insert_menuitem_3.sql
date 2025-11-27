DELETE FROM DMENUITEM_DBT WHERE T_IPROGITEM = 83 AND t_iidentprogram = 83 AND ((t_csystemitem = chr(88) AND t_icaseitem = 26110) OR (t_csystemitem = chr(88) AND t_icaseitem = 26111) OR (t_csystemitem = chr(0) AND t_icaseitem = 519) OR (t_csystemitem = chr(0) AND t_icaseitem = 523) OR (t_csystemitem = chr(0) AND t_icaseitem = 552) OR (t_csystemitem = chr(0) AND t_icaseitem = 117))
/

DELETE FROM DITEMSYST_DBT WHERE T_CIDENTPROGRAM = 'S' AND T_ICASEITEM IN (26110, 26111)
/

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 26110;  --Номер модулей
   C_NAME                VARCHAR2 (255) := ' [ОД_1] Расшифровка вложений в ценные бумаги по состоянию на отчетную дату';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERLINE          NUMBER := 50;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = 0),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'МСФО'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
		
      SELECT NVL (MAX (t_inumberline), 0)
             INTO v_NumberLine
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE
                  AND t_inumberfather = V_NUMBERFATHER;
      
       v_NumberLine := v_NumberLine + 1;

            UPDATE dmenuitem_dbt 
               SET t_sznameitem = ' OLD[ОД_2] Расшифровка вложений в акции и доли', t_inumberfather = V_NUMBERFATHER, t_inumberline = v_NumberLine
             WHERE t_objectid   = p_numberoper
               AND t_istemplate = p_istemplate
			   AND t_csystemitem = chr(0)
               AND t_icaseitem  = 117
               AND t_iidentprogram = C_IDENTPROGRAM_CODE;
   END;
BEGIN

   INSERT INTO DITEMSYST_DBT(T_CIDENTPROGRAM,T_ICASEITEM,T_IKINDMETHOD,T_IKINDPROGRAM,T_IHELP,T_RESERVE,T_SZNAMEITEM,T_PARM )
               VALUES('S',C_CASEITEM,1,2,0,CHR(1),'[ОД_1] Расшифровка вложений в цб','6F64315F322E6D6163000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006F64315F322E6D6163000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');

   FOR i IN (SELECT DISTINCT t_objectid, t_istemplate FROM DMENUITEM_DBT WHERE t_icaseitem IN (117) AND t_iidentprogram = C_IDENTPROGRAM_CODE AND t_iprogitem = C_IDENTPROGRAM_CODE AND t_csystemitem = chr(0))
   LOOP
      InsertMenu (i.t_objectid, i.t_istemplate);
   END LOOP;

END;
/

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 26110;  --Номер модулей
   C_NAME                VARCHAR2 (255) := ' [ОД_1] Расшифровка вложений в ценные бумаги по состоянию на отчетную дату';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERLINE          NUMBER := 50;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = 0),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'МСФО'
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
             AND t_inumberfather = V_NUMBERFATHER
			 AND t_csystemitem = chr(88)
             AND trim(replace(t_sznameitem, '~', '')) = C_NAME;
      
      IF v_cnt = 0 
      THEN
         SELECT COUNT(1)
           INTO v_cnt
           FROM dmenuitem_dbt
          WHERE t_objectid = p_numberoper
            AND t_istemplate = p_istemplate
            AND t_iidentprogram = C_IDENTPROGRAM_CODE
			AND t_csystemitem = chr(88)
            AND t_icaseitem = C_CASEITEM;
         
         IF v_cnt = 0
         THEN

           SELECT NVL (MAX (t_inumberpoint), 0)
             INTO v_NumberPoint
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE;

            SELECT NVL (MAX (t_inumberline), 0)
             INTO v_NumberLine
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE
                  AND t_inumberfather = V_NUMBERFATHER;

           v_NumberPoint := v_NumberPoint + 1;

           v_NumberLine := v_NumberLine + 1;

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
                        V_NUMBERLINE,
                        C_CASEITEM,
                        CHR (88),
                        C_NAME,
                        C_NAME,
                        0,
                        C_IDENTPROGRAM_CODE);
        END IF;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/))
   LOOP
      InsertMenu (i.t_oper, CHR(0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/)) LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

END;
/

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 26111;  --Номер модулей
   C_NAME                VARCHAR2 (255) := ' [ОД_3] Реестр сделок РЕПО';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERLINE          NUMBER := 50;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = 0),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'МСФО'
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
             AND t_inumberfather = V_NUMBERFATHER
			 AND t_csystemitem = chr(88)
             AND trim(replace(t_sznameitem, '~', '')) = C_NAME;
      
      IF v_cnt = 0 
      THEN
         SELECT COUNT(1)
           INTO v_cnt
           FROM dmenuitem_dbt
          WHERE t_objectid = p_numberoper
            AND t_istemplate = p_istemplate
            AND t_iidentprogram = C_IDENTPROGRAM_CODE
			AND t_csystemitem = chr(88)
            AND t_icaseitem = C_CASEITEM;
         
         IF v_cnt = 0
         THEN

           SELECT NVL (MAX (t_inumberpoint), 0)
             INTO v_NumberPoint
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE;

            SELECT NVL (MAX (t_inumberline), 0)
             INTO v_NumberLine
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE
                  AND t_inumberfather = V_NUMBERFATHER;

           v_NumberPoint := v_NumberPoint + 1;

           v_NumberLine := v_NumberLine + 1;

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
                        V_NUMBERLINE,
                        C_CASEITEM,
                        CHR (88),
                        C_NAME,
                        C_NAME,
                        0,
                        C_IDENTPROGRAM_CODE);
        END IF;
      END IF;
   END;
BEGIN

   INSERT INTO DITEMSYST_DBT(T_CIDENTPROGRAM,T_ICASEITEM,T_IKINDMETHOD,T_IKINDPROGRAM,T_IHELP,T_RESERVE,T_SZNAMEITEM,T_PARM )
               VALUES('S',C_CASEITEM,1,2,0,CHR(1),'[ОД_3] Реестр сделок РЕПО','000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006F6430335F5265706F72742E6D6163000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/))
   LOOP
      InsertMenu (i.t_oper, CHR(0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/)) LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

END;
/

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 519;  --Номер модулей
   C_NAME                VARCHAR2 (255) := ' OLD[ОД_1] Расшифровка долговых ценных бумаг';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERLINE          NUMBER := 50;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = 0),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'МСФО'
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
             AND t_inumberfather = V_NUMBERFATHER
			 AND t_csystemitem = chr(0)
             AND trim(replace(t_sznameitem, '~', '')) = C_NAME;
      
      IF v_cnt = 0 
      THEN
         SELECT COUNT(1)
           INTO v_cnt
           FROM dmenuitem_dbt
          WHERE t_objectid = p_numberoper
            AND t_istemplate = p_istemplate
            AND t_iidentprogram = C_IDENTPROGRAM_CODE
			AND t_csystemitem = chr(0)
            AND t_icaseitem = C_CASEITEM;
         
         IF v_cnt = 0
         THEN

           SELECT NVL (MAX (t_inumberpoint), 0)
             INTO v_NumberPoint
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE;

            SELECT NVL (MAX (t_inumberline), 0)
             INTO v_NumberLine
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE
                  AND t_inumberfather = V_NUMBERFATHER;

           v_NumberPoint := v_NumberPoint + 1;

           v_NumberLine := v_NumberLine + 1;

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
                        V_NUMBERLINE,
                        C_CASEITEM,
                        CHR (0),
                        C_NAME,
                        C_NAME,
                        0,
                        C_IDENTPROGRAM_CODE);
        END IF;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/))
   LOOP
      InsertMenu (i.t_oper, CHR(0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/)) LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

END;
/

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 523;  --Номер модулей
   C_NAME                VARCHAR2 (255) := ' OLD[ОД_2] Расшифровка вложений в акции и доли';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERLINE          NUMBER := 50;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = 0),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'МСФО'
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
             AND t_icaseitem in (C_CASEITEM, 117)
             AND t_inumberfather = V_NUMBERFATHER
			 AND t_csystemitem = chr(0)
             AND trim(replace(t_sznameitem, '~', '')) = C_NAME;
      
      IF v_cnt = 0 
      THEN
         SELECT COUNT(1)
           INTO v_cnt
           FROM dmenuitem_dbt
          WHERE t_objectid = p_numberoper
            AND t_istemplate = p_istemplate
            AND t_iidentprogram = C_IDENTPROGRAM_CODE
			AND t_csystemitem = chr(0)
            AND t_icaseitem in (C_CASEITEM, 117);
         
         IF v_cnt = 0
         THEN

           SELECT NVL (MAX (t_inumberpoint), 0)
             INTO v_NumberPoint
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE;

            SELECT NVL (MAX (t_inumberline), 0)
             INTO v_NumberLine
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE
                  AND t_inumberfather = V_NUMBERFATHER;

           v_NumberPoint := v_NumberPoint + 1;

           v_NumberLine := v_NumberLine + 1;

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
                        V_NUMBERLINE,
                        C_CASEITEM,
                        CHR (0),
                        C_NAME,
                        C_NAME,
                        0,
                        C_IDENTPROGRAM_CODE);
        END IF;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/))
   LOOP
      InsertMenu (i.t_oper, CHR(0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/)) LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

END;
/

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 552;  --Номер модулей
   C_NAME                VARCHAR2 (255) := ' OLD[ОД_3] Сделки РЕПО';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERLINE          NUMBER := 50;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = 0),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'РСХБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'МСФО'
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
             AND t_inumberfather = V_NUMBERFATHER
			 AND t_csystemitem = chr(0)
             AND trim(replace(t_sznameitem, '~', '')) = C_NAME;
      
      IF v_cnt = 0 
      THEN
         SELECT COUNT(1)
           INTO v_cnt
           FROM dmenuitem_dbt
          WHERE t_objectid = p_numberoper
            AND t_istemplate = p_istemplate
            AND t_iidentprogram = C_IDENTPROGRAM_CODE
			AND t_csystemitem = chr(0)
            AND t_icaseitem = C_CASEITEM;
         
         IF v_cnt = 0
         THEN

           SELECT NVL (MAX (t_inumberpoint), 0)
             INTO v_NumberPoint
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE;

            SELECT NVL (MAX (t_inumberline), 0)
             INTO v_NumberLine
             FROM dmenuitem_dbt
            WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE
                  AND t_inumberfather = V_NUMBERFATHER;

           v_NumberPoint := v_NumberPoint + 1;

           v_NumberLine := v_NumberLine + 1;

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
                        V_NUMBERLINE,
                        C_CASEITEM,
                        CHR (0),
                        C_NAME,
                        C_NAME,
                        0,
                        C_IDENTPROGRAM_CODE);
        END IF;
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/))
   LOOP
      InsertMenu (i.t_oper, CHR(0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (1, 10010/*[10] Прикладной администратор*/,10026/*[26] Специались по формированию отчетности (ценные бумаги)*/, 10028/*[28] Аналитик*/)) LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

END;
/
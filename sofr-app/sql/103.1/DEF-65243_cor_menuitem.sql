BEGIN
   DELETE FROM DMENUITEM_DBT
         WHERE T_ICASEITEM = 26002;
END;
/

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_CASEITEM            NUMBER := 26002;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Отчет-сверка проводок глав А и Г';
   V_NUMBERFATHER        NUMBER := 0;
   C_NUMBERLINE          NUMBER := 20;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR, p_identprogram_code NUMBER)
   IS
      v_cnt              NUMBER := 0;
      v_NumberPoint      NUMBER := 0;
      V_NUMBERFATHERTMP  NUMBER := 0;
      v_numberline       NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Отчеты'
                        AND T_IPROGITEM = p_identprogram_code
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = 0),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      IF (V_NUMBERFATHER = 0) THEN
         RETURN;
      END IF;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'РСХБ'
                        AND T_IPROGITEM = p_identprogram_code
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHERTMP
        FROM DUAL;

      if (V_NUMBERFATHERTMP = 0) THEN
         SELECT NVL (MAX (t_inumberpoint), 0) + 1
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = p_identprogram_code;
         SELECT NVL (MAX (t_inumberline), 0)
           INTO v_numberline
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = p_identprogram_code
                AND t_inumberfather = V_NUMBERFATHER;
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
                      p_identprogram_code,
                      v_NumberPoint,
                      V_NUMBERFATHER,
                      v_numberline + 10,
                      0,
                      CHR (88),
                      ' РСХБ',
                      ' РСХБ',
                      0,
                      p_identprogram_code);
          V_NUMBERFATHER := v_NumberPoint;
      ELSE
        V_NUMBERFATHER := V_NUMBERFATHERTMP;
      END IF;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Контрольные отчеты'
                        AND T_IPROGITEM = p_identprogram_code
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = V_NUMBERFATHER),
                0)
        INTO V_NUMBERFATHERTMP
        FROM DUAL;

      if (V_NUMBERFATHERTMP = 0) THEN
         SELECT NVL (MAX (t_inumberpoint), 0) + 1
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = p_identprogram_code;
         SELECT NVL (MAX (t_inumberline), 0)
           INTO v_numberline
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = p_identprogram_code
                AND t_inumberfather = V_NUMBERFATHER;
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
                      p_identprogram_code,
                      v_NumberPoint,
                      V_NUMBERFATHER,
                      v_numberline + 10,
                      0,
                      CHR (88),
                      ' Контрольные отчеты',
                      ' Контрольные отчеты',
                      0,
                      p_identprogram_code);
          V_NUMBERFATHER := v_NumberPoint;
      ELSE
        V_NUMBERFATHER := V_NUMBERFATHERTMP;
      END IF;
      
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = p_identprogram_code
             AND t_icaseitem = C_CASEITEM
             AND t_inumberfather = V_NUMBERFATHER
             AND t_sznameitem = C_NAME;

      
      IF v_cnt = 0 and V_NUMBERFATHER <> 0
      THEN
         SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = p_identprogram_code;

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
                      p_identprogram_code,
                      v_NumberPoint,
                      V_NUMBERFATHER,
                      C_NUMBERLINE,
                      C_CASEITEM,
                      CHR (88),
                      ' ' || C_NAME,
                      ' ' || C_NAME,
                      0,
                      p_identprogram_code);
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10026,10025,10028,10010,10015,10011,10013,10004,10016,10032,10012))
   LOOP
      InsertMenu (i.t_oper, CHR (0), 83);
      InsertMenu (i.t_oper, CHR (0), 78);
      InsertMenu (i.t_oper, CHR (0), 158);
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (10026,10025,10028,10010,10015,10011,10013,10004,10016,10032,10012) AND T_MENUID is not null and T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88), 83);
      InsertMenu (i.t_MenuID, CHR(88), 78);
      InsertMenu (i.t_MenuID, CHR(88), 158);
   END LOOP;

   InsertMenu (1, CHR (0),  83);
   InsertMenu (1, CHR (0),  78);
   InsertMenu (1, CHR (0),  158);
   InsertMenu (1, CHR (88), 83);
   InsertMenu (1, CHR (88), 78);
   InsertMenu (1, CHR (88), 158);

END;
/
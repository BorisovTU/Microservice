/* обновление меню под новые пункты */
BEGIN
   DELETE FROM DMENUITEM_DBT WHERE T_IIDENTPROGRAM=83 AND ((T_CSYSTEMITEM=chr(0) AND T_ICASEITEM IN (925,926,927,539) AND T_IPROGITEM=83) OR (T_CSYSTEMITEM=chr(88) AND T_ICASEITEM=1700  AND T_IPROGITEM=73));
EXCEPTION
   WHEN OTHERS THEN NULL;
END;
/

/*Добавить пункт меню для отчета*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
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
                  WHERE     trim(t_sznameitem) in ('Формы отчётов, представляемые в Банк России', '~Ф~ормы отчётов, представляемые в Банк России')
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
             AND t_icaseitem in (925,926,927,539,1700)
             AND t_inumberfather = V_NUMBERFATHER;

      
      IF v_cnt = 0 AND V_NUMBERFATHER > 0
      THEN
         SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE;

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
                      v_NumberPoint+1,
                      V_NUMBERFATHER,
                      C_NUMBERLINE,
                      1700,
                      CHR (88),
                      ' ' || ' Форма №0409711',
                      ' ' || ' Форма №0409711',
                      0,
                      73);
                      
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
                      v_NumberPoint+2,
                      V_NUMBERFATHER,
                      C_NUMBERLINE + 10,
                      925,
                      CHR (0),
                      ' ' || '  Загрузка закладных для 711 формы',
                      ' ' || '  Загрузка закладных для 711 формы',
                      0,
                      C_IDENTPROGRAM_CODE);
                      
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
                      v_NumberPoint+3,
                      V_NUMBERFATHER,
                      C_NUMBERLINE + 20,
                      926,
                      CHR (0),
                      ' ' || 'Загрузка залогов для 711 формы',
                      ' ' || 'Загрузка залогов для 711 формы',
                      0,
                      C_IDENTPROGRAM_CODE);
                      
                      
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
                      v_NumberPoint+4,
                      V_NUMBERFATHER,
                      C_NUMBERLINE + 30,
                      927,
                      CHR (0),
                      ' ' || 'Загрузка иерархии СС для 711 формы',
                      ' ' || 'Загрузка иерархии СС для 711 формы',
                      0,
                      C_IDENTPROGRAM_CODE);
                      
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
                      v_NumberPoint+5,
                      V_NUMBERFATHER,
                      C_NUMBERLINE + 40,
                      539,
                      CHR (0),
                      ' ' || 'Загрузка остатков ДУ для 711 формы',
                      ' ' || 'Загрузка остатков ДУ для 711 формы',
                      0,
                      C_IDENTPROGRAM_CODE);
                      
            v_NumberPoint :=  v_NumberPoint + 6;           
                      
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10010, 10025, 10026, 10008, 10028))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (10010, 10025, 10026, 10008, 10028))
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

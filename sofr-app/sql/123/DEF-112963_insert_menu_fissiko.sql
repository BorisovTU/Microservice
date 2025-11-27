/*Добавить пункт меню для отчета*/
DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;
   v_NumberFather        NUMBER := 0;
   v_NumberLine          NUMBER := 0;
   v_CaseItem            NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 158; --Подсистема ФИСС и КО

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN

      select nvl(
        (select nvl(t_icaseitem, 0) from ditemuser_dbt where t_parm = (select utl_raw.cast_to_raw(rpad(lpad(chr(0), 259, chr(0)) || lower('dl_report725.mac'), 400, chr(0))) from dual)),
      0) into v_CaseItem from dual;
      IF( v_CaseItem = 0) THEN
        RETURN;
      END IF;

      v_NumberFather := -1;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'Отчеты'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = 0),
                -1)
        INTO v_NumberFather
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) in ('Регламентированные')
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = v_NumberFather),
                -1)
        INTO v_NumberFather
        FROM DUAL;
        
        SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) in ('Формы отчётов, представляемые в Банк России', '~Ф~ормы отчётов, представляемые в Банк России')
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND t_inumberfather = v_NumberFather),
                -1)
        INTO v_NumberFather
        FROM DUAL;
      
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = C_IDENTPROGRAM_CODE
             AND t_icaseitem = v_CaseItem
             AND t_inumberfather = v_NumberFather;
      
      IF v_cnt = 0 AND v_NumberFather > 0
      THEN
         SELECT NVL (MAX (t_inumberpoint), 0)+1
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE;

         SELECT NVL (MAX (t_inumberline), 0)+1
           INTO v_NumberLine
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE
                AND t_inumberfather = v_NumberFather;
				  		
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
                      v_NumberFather,
                      v_NumberLine,
                      v_CaseItem,
                      CHR (0),
                      ' ' || ' Сведения о маржинальных сделках клиентов (ф.725)',
                      ' ' || ' Сведения о маржинальных сделках клиентов (ф.725)',
                      0,
                      83);
 
      END IF;
   END;
BEGIN

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10012, 10014))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (10012, 10014))
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
/* обновление меню под новые пункты */
DECLARE
   --v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   V_NUMBERFATHER        NUMBER := 0;
   V_ICASEITEM           NUMBER := 20214;
   V_IKINDMETHOD         NUMBER := 0;
   V_IKINDPROGRAM        NUMBER := 1;
   V_IHELP               NUMBER := 0;
   V_SZNAMEITEM          VARCHAR(60) := 'Операции технической сверки СНОБ';
   V_PARM                RAW(400)    := '00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000';
   V_SZNAMEITEM_MENUITEM VARCHAR(80) := 'Операции технической сверки СНОБ';
   
   --Найдём все родительские пункты меню (Налоговый учет, НДФЛ, СНОБ, Отчеты), если не нашли, то добавляем
   FUNCTION GetNumberFather(p_sznameitem VARCHAR2, p_numberfather NUMBER, p_numberoper NUMBER, p_istemplate CHAR) RETURN NUMBER
   IS
      v_NumberFather_Node NUMBER := 0;
      v_NumberPoint_Node  NUMBER := 0;
   BEGIN
      it_log.log('BOSS-1489: ищем пункт меню ''' || p_sznameitem || ''' у пользователя ' || p_numberoper);
      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = p_sznameitem
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper
                        AND T_INUMBERFATHER = p_numberfather),
                0)
        INTO v_NumberFather_Node
        FROM DUAL;
      
      IF v_NumberFather_Node = 0
      THEN
         it_log.log('BOSS-1489: не нашли пункт меню ''' || p_sznameitem || ''' у пользователя ' || p_numberoper || '. Добавляем');
         
         SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint_Node
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE;

         v_NumberPoint_Node := v_NumberPoint_Node + 1;
         
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
                      v_NumberPoint_Node,
                      p_numberfather,
                      10,
                      0,
                      CHR (88),
                      p_sznameitem,
                      p_sznameitem,
                      0,
                      C_IDENTPROGRAM_CODE);
                      
         v_NumberFather_Node := v_NumberPoint_Node;
      END IF;
      
      return v_NumberFather_Node;
   END;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR, p_CaseItem IN NUMBER, p_NameItem IN VARCHAR2)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
      v_NumberLine    NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;
        
      V_NUMBERFATHER := GetNumberFather('Налоговый учет', 0, p_numberoper, p_istemplate);
      V_NUMBERFATHER := GetNumberFather('НДФЛ', V_NUMBERFATHER, p_numberoper, p_istemplate);
      V_NUMBERFATHER := GetNumberFather('СНОБ', V_NUMBERFATHER, p_numberoper, p_istemplate);

      /*SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(t_sznameitem) = 'СНОБ'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;*/
      
      IF V_NUMBERFATHER > 0
      THEN
         SELECT COUNT (1)
           INTO v_cnt
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE
                AND t_icaseitem = p_CaseItem
                AND t_inumberfather = V_NUMBERFATHER;

         IF v_cnt = 0 
         THEN
            it_log.log('BOSS-1489: добавление меню ' || p_CaseItem || ' для пользователя ' || p_numberoper);
            
            BEGIN
               SELECT MAX(t_INumberLine) + 10
                 INTO v_NumberLine
                 FROM dmenuitem_dbt
                WHERE     t_objectid = p_numberoper
                      AND t_istemplate = p_istemplate
                      AND t_iidentprogram = C_IDENTPROGRAM_CODE
                      AND t_inumberfather = V_NUMBERFATHER;
            EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_NumberLine := 10;
            END;

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
                         v_NumberLine,
                         p_CaseItem,
                         CHR (88),
                         ' ' || p_NameItem,
                         ' ' || p_NameItem,
                         0,
                        C_IDENTPROGRAM_CODE);
         END IF;
      END IF;
   END;
BEGIN
   BEGIN
      INSERT INTO ditemsyst_dbt (
                                   t_CIdentProgram,
                                   t_ICaseItem,
                                   t_IKindMethod,
                                   t_IKindProgram,
                                   t_IHelp,
                                   t_szNameItem,
                                   t_Parm
                                )
                          VALUES(
                                   CHR(C_IDENTPROGRAM_CODE),   --t_CIdentProgram
                                   V_ICASEITEM,                --t_ICaseItem
                                   V_IKINDMETHOD,              --t_IKindMethod
                                   V_IKINDPROGRAM,             --t_IKindProgram
                                   V_IHELP,                    --t_IHelp
                                   V_SZNAMEITEM,               --t_szNameItem
                                   V_PARM                      --t_Parm
                                );
                             
      it_log.log('BOSS-1489: Вставлен системный модуль ' || V_ICASEITEM);
   EXCEPTION
      WHEN DUP_VAL_ON_INDEX
      THEN
         it_log.log('BOSS-1489: Ошибка вставки системного модуля '  || V_ICASEITEM || '. Такой модуль уже есть в системе');
   END;

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10036))
   LOOP
      InsertMenu (i.t_oper, CHR (0), V_ICASEITEM, V_SZNAMEITEM_MENUITEM);
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10036)
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR (88), V_ICASEITEM, V_SZNAMEITEM_MENUITEM);
   END LOOP;

   --Для технического пользователя 1
   InsertMenu (1, CHR (0), 20209, V_SZNAMEITEM_MENUITEM);

EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (v_NumberPoint);
      RAISE;
END;
/
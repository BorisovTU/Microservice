DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;
   v_NumberLine          NUMBER := 50;

   C_IDENTPROGRAM_CODE   NUMBER := 158; --Подсистема
   C_CASEITEM            NUMBER := 25200; --Номер модуля
   C_NAME                VARCHAR2(255) := 'Справочник балансовых счетов для отбора';
   V_NUMBERFATHER        NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR, p_IDENTPROGRAM_CODE IN NUMBER)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Справочники'
                        AND t_iidentprogram = p_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = 0
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = p_IDENTPROGRAM_CODE
             AND t_icaseitem = C_CASEITEM
             AND t_inumberfather = V_NUMBERFATHER;

      IF (v_cnt = 0 AND V_NUMBERFATHER != 0)
      THEN
         SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = p_IDENTPROGRAM_CODE;

         SELECT NVL (MAX (t_inumberline), 0) + 10
           INTO v_NumberLine
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = p_IDENTPROGRAM_CODE
                AND t_inumberfather = V_NUMBERFATHER;

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
                      p_IDENTPROGRAM_CODE,
                      v_NumberPoint,
                      V_NUMBERFATHER,
                      v_NumberLine,
                      C_CASEITEM,
                      CHR (88),
                      ' ' || C_NAME,
                      ' ' || C_NAME,
                      0,
                      C_IDENTPROGRAM_CODE);
      END IF;
   END;

BEGIN
   INSERT INTO DITEMSYST_DBT(T_CIDENTPROGRAM,T_ICASEITEM,T_IKINDMETHOD,T_IKINDPROGRAM,T_IHELP,T_RESERVE,T_SZNAMEITEM,T_PARM)
               VALUES('Ю',C_CASEITEM,0,1,0,CHR(1),C_NAME,'00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE rol.T_ROLEID IN (10010/*[10] Прикладной администратор*/                                    
                                    ))
   LOOP
      InsertMenu (i.t_oper, CHR(0), ASCII('S'));
      InsertMenu (i.t_oper, CHR(0), ASCII('Ю'));
      InsertMenu (i.t_oper, CHR(0), ASCII('Y'));
      InsertMenu (i.t_oper, CHR(0), ASCII('N'));
      InsertMenu (i.t_oper, CHR(0), ASCII('J'));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10010/*[10] Прикладной администратор*/
                                    )
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88), ASCII('S'));
      InsertMenu (i.t_MenuID, CHR(88), ASCII('Ю'));
      InsertMenu (i.t_MenuID, CHR(88), ASCII('Y'));
      InsertMenu (i.t_MenuID, CHR(88), ASCII('N'));
      InsertMenu (i.t_MenuID, CHR(88), ASCII('J'));
   END LOOP;
   
   InsertMenu (1, CHR(0), ASCII('S'));
   InsertMenu (1, CHR(0), ASCII('Ю'));
   InsertMenu (1, CHR(0), ASCII('Y'));
   InsertMenu (1, CHR(0), ASCII('N'));
   InsertMenu (1, CHR(0), ASCII('J'));

   InsertMenu (1, CHR(88), ASCII('S'));
   InsertMenu (1, CHR(88), ASCII('Ю'));
   InsertMenu (1, CHR(88), ASCII('Y'));
   InsertMenu (1, CHR(88), ASCII('N'));
   InsertMenu (1, CHR(88), ASCII('J'));
END;
/

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;
   v_NumberLine          NUMBER := 50;

   C_IDENTPROGRAM_CODE   NUMBER := 158; --Подсистема
   C_CASEITEM            NUMBER := 25201; --Номер модуля
   C_NAME                VARCHAR2(255) := 'Контроль по привязке парных счетов';
   V_NUMBERFATHER        NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR, p_IDENTPROGRAM_CODE IN NUMBER)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Отчеты'
                        AND t_iidentprogram = p_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = 0
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      IF (V_NUMBERFATHER != 0) THEN
        SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'РСХБ'
                        AND t_iidentprogram = p_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = V_NUMBERFATHER
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

        IF (V_NUMBERFATHER != 0) THEN 

          SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Контрольные отчеты'
                        AND t_iidentprogram = p_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND t_inumberfather = V_NUMBERFATHER
                        AND T_OBJECTID = p_numberoper),
                0)
          INTO V_NUMBERFATHER
          FROM DUAL;
      
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid = p_numberoper
             AND t_istemplate = p_istemplate
             AND t_iidentprogram = p_IDENTPROGRAM_CODE
             AND t_icaseitem = C_CASEITEM
             AND t_inumberfather = V_NUMBERFATHER;

      IF v_cnt = 0 AND V_NUMBERFATHER != 0
      THEN
         SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = p_IDENTPROGRAM_CODE;

         SELECT NVL (MAX (t_inumberline), 0) + 10
           INTO v_NumberLine
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = p_IDENTPROGRAM_CODE
                AND t_inumberfather = V_NUMBERFATHER;

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
                      p_IDENTPROGRAM_CODE,
                      v_NumberPoint,
                      V_NUMBERFATHER,
                      v_NumberLine,
                      C_CASEITEM,
                      CHR (88),
                      ' ' || C_NAME,
                      ' ' || C_NAME,
                      0,
                      C_IDENTPROGRAM_CODE);
      END IF;

      END IF;

      END IF;
   END;

BEGIN
   INSERT INTO DITEMSYST_DBT(T_CIDENTPROGRAM,T_ICASEITEM,T_IKINDMETHOD,T_IKINDPROGRAM,T_IHELP,T_RESERVE,T_SZNAMEITEM,T_PARM)
               VALUES('Ю',C_CASEITEM,1,2,0,CHR(1),C_NAME,'0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000047656E6572617465436E74726C5061726965644163635265702E6D616300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE rol.T_ROLEID IN (10026/*[26] Специалист по формированию отчетности (ценные бумаги)*/,
                                     10025/*[25] Специалист по формированию отчетности ДО*/,
                                     10028/*[28] Аналитик*/,
                                     10010/*[10] Прикладной администратор*/,
                                     10015/*[15] Специалист БУ ПФИ*/,
                                     10011/*[11] Специалист БУ ЦБ*/,
                                     10013/*[13] Специалист БУ БО*/,
                                     10004/*[04] Специалист по сопровождению и учёту операций с векселями*/,
                                     10016/*[16] Специалист ПФИ*/,
                                     10017/*[17] Специалист МБК*/,
                                     10029/*[29] Просмотр МБК*/,
                                     10032/*[32] Работник отдела последующего контроля*/,
                                     10012/*[12] Специалист ЦБ*/                            
                                    ))
   LOOP
      InsertMenu (i.t_oper, CHR(0), ASCII('S'));
      InsertMenu (i.t_oper, CHR(0), ASCII('Ю'));
      InsertMenu (i.t_oper, CHR(0), ASCII('Y'));
      InsertMenu (i.t_oper, CHR(0), ASCII('N'));
      InsertMenu (i.t_oper, CHR(0), ASCII('J'));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10026/*[26] Специалист по формированию отчетности (ценные бумаги)*/,
                                     10025/*[25] Специалист по формированию отчетности ДО*/,
                                     10028/*[28] Аналитик*/,
                                     10010/*[10] Прикладной администратор*/,
                                     10015/*[15] Специалист БУ ПФИ*/,
                                     10011/*[11] Специалист БУ ЦБ*/,
                                     10013/*[13] Специалист БУ БО*/,
                                     10004/*[04] Специалист по сопровождению и учёту операций с векселями*/,
                                     10016/*[16] Специалист ПФИ*/,
                                     10017/*[17] Специалист МБК*/,
                                     10029/*[29] Просмотр МБК*/,
                                     10032/*[32] Работник отдела последующего контроля*/,
                                     10012/*[12] Специалист ЦБ*/     
                                    )
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88), ASCII('S'));
      InsertMenu (i.t_MenuID, CHR(88), ASCII('Ю'));
      InsertMenu (i.t_MenuID, CHR(88), ASCII('Y'));
      InsertMenu (i.t_MenuID, CHR(88), ASCII('N'));
      InsertMenu (i.t_MenuID, CHR(88), ASCII('J'));
   END LOOP;
   
   InsertMenu (1, CHR(0), ASCII('S'));
   InsertMenu (1, CHR(0), ASCII('Ю'));
   InsertMenu (1, CHR(0), ASCII('Y'));
   InsertMenu (1, CHR(0), ASCII('N'));
   InsertMenu (1, CHR(0), ASCII('J'));

   InsertMenu (1, CHR(88), ASCII('S'));
   InsertMenu (1, CHR(88), ASCII('Ю'));
   InsertMenu (1, CHR(88), ASCII('Y'));
   InsertMenu (1, CHR(88), ASCII('N'));
   InsertMenu (1, CHR(88), ASCII('J'));
END;
/
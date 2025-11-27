DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83; --Подсистема
   C_CASEITEM            NUMBER := 20257;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Справочник особых условий';
   V_NUMBERFATHER        NUMBER := 0;
   C_NUMBERLINE          NUMBER := 15;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Налоговый учет'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Настройки'
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
                  WHERE     trim(replace(t_sznameitem, '~', '')) = 'Настройки для применения льгот'
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
             AND trim(replace(t_sznameitem, '~', '')) = C_NAME;
      
      IF v_cnt = 0 
      THEN
         SELECT COUNT(1)
           INTO v_cnt
           FROM dmenuitem_dbt
          WHERE t_objectid = p_numberoper
            AND t_istemplate = p_istemplate
            AND t_iidentprogram = C_IDENTPROGRAM_CODE
            AND t_icaseitem = C_CASEITEM;
         
         IF v_cnt > 0
         THEN
           UPDATE dmenuitem_dbt
              SET t_inumberfather = V_NUMBERFATHER,
                  t_inumberline = C_NUMBERLINE
            WHERE t_objectid = p_numberoper
              AND t_istemplate = p_istemplate
              AND t_iidentprogram = C_IDENTPROGRAM_CODE
              AND t_icaseitem = C_CASEITEM;
         ELSE

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
      END IF;
   END;
BEGIN

   INSERT INTO DITEMSYST_DBT(T_CIDENTPROGRAM,T_ICASEITEM,T_IKINDMETHOD,T_IKINDPROGRAM,T_IHELP,T_RESERVE,T_SZNAMEITEM,T_PARM )
               VALUES('S',C_CASEITEM,0,1,0,CHR(1),C_NAME,'00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');

         
   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10010/*[10] Прикладной администратор*/,10014 /*[14] Специалист БО*/, 1))
   LOOP
      InsertMenu (i.t_oper, CHR(0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE ROL.T_ROLEID IN (10010/*[10] Прикладной администратор*/,10014 /*[14] Специалист БО*/, 1)) LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;

END;
/
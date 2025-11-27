DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 158; --Подсистема
   C_CASEITEM            NUMBER := 26100; --Номер модуля
   C_NAME                VARCHAR2(255) := 'Обработка валютного рынка СПВБ';
   V_NUMBERFATHER        NUMBER := 0;
   V_NUMBERSPFI          NUMBER := 0;
   C_NUMBERLINE          NUMBER := 200;
   V_ISFREE              NUMBER := 1;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Сервисные операции'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
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

      IF v_cnt = 0 
      THEN
         SELECT NVL (
                (SELECT NVL(MAX(t_inumberline), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Обработка сделок рынка СПФИ по итогам торгов'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERSPFI
        FROM DUAL;

        if V_NUMBERSPFI <> 0
        THEN
          C_NUMBERLINE := V_NUMBERSPFI;
          WHILE V_ISFREE > 0
          LOOP
            C_NUMBERLINE := C_NUMBERLINE + 1;
            SELECT COUNT (1)
              INTO V_ISFREE
              FROM dmenuitem_dbt
             WHERE     t_objectid = p_numberoper
                  AND t_istemplate = p_istemplate
                  AND t_iidentprogram = C_IDENTPROGRAM_CODE
                  AND t_inumberline = C_NUMBERLINE
                  AND t_inumberfather = V_NUMBERFATHER;
          END LOOP;
        END IF;

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
   INSERT INTO DITEMSYST_DBT(T_CIDENTPROGRAM,T_ICASEITEM,T_IKINDMETHOD,T_IKINDPROGRAM,T_IHELP,T_RESERVE,T_SZNAMEITEM,T_PARM)
               VALUES('Ю',C_CASEITEM,1,2,0,CHR(1),C_NAME,'000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004175746F50726F63657373535056422E6D61630000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE rol.T_ROLEID IN (1/*[1] Администратор*/,
                                     10016/*[16] Специалист ПФИ*/
                                    ))
   LOOP
      InsertMenu (i.t_oper, CHR(0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (1/*[1] Администратор*/,
                                     10016/*[16] Специалист ПФИ*/
                                    )
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu (i.t_MenuID, CHR(88));
   END LOOP;
END;
/
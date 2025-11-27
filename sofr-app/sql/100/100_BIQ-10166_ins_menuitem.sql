/* обновление меню под новые пункты */

DECLARE
   v_cnt                 NUMBER := 0;
   v_NumberPoint         NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 131; --Подсистема
   C_CASEITEM            NUMBER := 11000;  --Номер модуля
   C_NAME                VARCHAR2 (255) := 'Настройка правил блокировки проводок';
   V_NUMBERFATHER        NUMBER := 0;
   C_NUMBERLINE          NUMBER := 0;

   PROCEDURE InsertMenu (p_numberoper IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
      v_NumberPoint   NUMBER := 0;
   BEGIN
      
      V_NUMBERFATHER := 0;

      SELECT NVL (
                (SELECT NVL(MAX(T_INUMBERPOINT), 0)
                   FROM dmenuitem_dbt
                  WHERE     replace(trim(t_sznameitem), '~', '') = 'Процедуры'
                        AND T_IPROGITEM = C_IDENTPROGRAM_CODE
                        AND T_ISTEMPLATE = p_istemplate
                        AND T_OBJECTID = p_numberoper),
                0)
        INTO V_NUMBERFATHER
        FROM DUAL;
      
	  IF V_NUMBERFATHER = 0 THEN 
		SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE;

         v_NumberPoint := v_NumberPoint + 1;
		 
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
                      ' Процедуры',
                      ' Процедуры',
                      0,
                      C_IDENTPROGRAM_CODE);
	     V_NUMBERFATHER := v_NumberPoint;
		 C_NUMBERLINE := 0;
		 v_NumberPoint := 0;
	  END IF;
		 
	  
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
         SELECT NVL (MAX (t_inumberpoint), 0)
           INTO v_NumberPoint
           FROM dmenuitem_dbt
          WHERE     t_objectid = p_numberoper
                AND t_istemplate = p_istemplate
                AND t_iidentprogram = C_IDENTPROGRAM_CODE;

         v_NumberPoint := v_NumberPoint + 1;
		 
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

   --Для пользователей ролей
   FOR i IN (SELECT DISTINCT rol.T_OPER
               FROM DACSOPROLE_DBT rol
              WHERE ROL.T_ROLEID IN (10027,10030,10010,10004,10011,10013,10015,10017,10026,10025,10008,10028,10032))
   LOOP
      InsertMenu (i.t_oper, CHR (0));
   END LOOP;

   --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10027,10030,10010,10004,10011,10013,10015,10017,10026,10025,10008,10028,10032)
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

/*Удаление пункта меню "Отчет об операциях на валютных и денежных рынках (ф.701)"*/
DECLARE
   v_cnt                 NUMBER := 0;

   C_IDENTPROGRAM_CODE   NUMBER := 83;    --Подсистема
   C_CASEITEM            NUMBER := 16994; --Номер модуля

   PROCEDURE DeleteMenu (p_objectid IN NUMBER, p_inumberfather IN NUMBER, p_istemplate IN CHAR)
   IS
      v_cnt           NUMBER := 0;
   BEGIN
      --Удаление строки меню
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt
       WHERE     t_objectid      = p_objectid
             AND t_istemplate    = p_istemplate
             AND t_iidentprogram = C_IDENTPROGRAM_CODE
             AND t_icaseitem     = C_CASEITEM;

      IF v_cnt != 0 
      THEN
         DELETE FROM dmenuitem_dbt 
               WHERE t_objectid      = p_objectid 
                 AND t_istemplate    = p_istemplate 
                 AND t_iidentprogram = C_IDENTPROGRAM_CODE 
                 AND t_icaseitem     = C_CASEITEM;
      END IF;
      --Удаление "родительского" раздела
      SELECT COUNT (1)
        INTO v_cnt
        FROM dmenuitem_dbt                                
       WHERE     t_objectid      = p_objectid
             AND t_istemplate    = p_istemplate
             AND t_iidentprogram = C_IDENTPROGRAM_CODE
             AND t_inumberpoint  = p_inumberfather;

      IF v_cnt != 0 
      THEN
         DELETE FROM dmenuitem_dbt 
               WHERE t_objectid      = p_objectid 
                 AND t_istemplate    = p_istemplate 
                 AND t_iidentprogram = C_IDENTPROGRAM_CODE 
                 AND t_inumberpoint  = p_inumberfather;
      END IF;
   END;
BEGIN

   --Для пользователей
   FOR i IN (SELECT menu.t_ObjectID, menu.t_INumberFather, menu.t_IsTemplate
               FROM DMENUITEM_DBT menu
              WHERE menu.t_iCaseItem     = C_CASEITEM 
                AND menu.t_IIdentProgram = C_IDENTPROGRAM_CODE )
   LOOP
      DeleteMenu(i.t_ObjectID, i.t_INumberFather, i.t_IsTemplate);
   END LOOP;

EXCEPTION
   WHEN DUP_VAL_ON_INDEX
   THEN
      DBMS_OUTPUT.put_line (C_IDENTPROGRAM_CODE || ' ' || C_CASEITEM);
      RAISE;
END;
/
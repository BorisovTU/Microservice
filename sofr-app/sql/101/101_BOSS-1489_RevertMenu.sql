DECLARE
   v_INumberFather NUMBER := 0;

   FUNCTION GetMaxNumberLine(p_INumberFather IN NUMBER, p_IsTemplate IN CHAR, p_Oper IN NUMBER, p_IIdentProgram IN NUMBER) RETURN NUMBER
   IS
      v_INumberLine NUMBER := 0;
   BEGIN
      SELECT NVL(MAX(t_INumberLine), 0)
      INTO v_INumberLine
      FROM DMENUITEM_DBT
      WHERE t_ObjectID = p_Oper
        AND t_INumberFather = p_INumberFather
        AND t_IsTemplate = p_IsTemplate
        AND t_IIdentProgram = p_IIdentProgram;
        
      RETURN v_INumberLine;
   END;
   
   FUNCTION GetNumberFather(p_SzNameItem IN VARCHAR2, p_INumberFather IN NUMBER, p_Oper IN NUMBER, p_IsTemplate IN CHAR, p_IIdentProgram IN NUMBER) RETURN NUMBER
   IS
      v_INumberFather NUMBER := 0;
   BEGIN
      SELECT NVL(MAX(t_INumberPoint), 0)
      INTO v_INumberFather
      FROM DMENUITEM_DBT
      WHERE t_ObjectID = p_Oper
        AND t_INumberFather = p_INumberFather
        AND t_IsTemplate = p_IsTemplate
        AND LTRIM(t_SzNameItem) = p_SzNameItem
        AND t_IIdentProgram = p_IIdentProgram;
        
      IF v_INumberFather = 0
      THEN
         it_log.log('Откат BOSS-1489. Не найден родительский пункт меню ''' || p_SzNameItem || ''' для пользователя ' || p_Oper);
      END IF;
      COMMIT;
        
      RETURN v_INumberFather;
   END;

   FUNCTION GetOrInsertNumberFather(p_SzNameItem IN VARCHAR2, p_INumberFather IN NUMBER, p_Oper IN NUMBER, p_IsTemplate IN CHAR, p_IIdentProgram IN NUMBER) RETURN NUMBER
   IS
      v_INumberFather NUMBER := 0;
      v_INumberLine NUMBER := 0;
   BEGIN
      v_INumberFather := GetNumberFather(p_SzNameItem, p_INumberFather, p_Oper, p_IsTemplate, p_IIdentProgram);
      
      IF v_INumberFather = 0
      THEN 
         it_log.log('Откат BOSS-1489. Вставляем родительский пункт меню ''' || p_SzNameItem || ''' для пользователя ' || p_Oper);
         COMMIT;
         v_INumberLine := GetMaxNumberLine(p_INumberFather, p_IsTemplate, p_Oper, p_IIdentProgram) + 1;
           
         SELECT NVL(MAX(t_INumberPoint), 0)
         INTO v_INumberFather
         FROM DMENUITEM_DBT
         WHERE t_ObjectID = p_Oper
           AND t_IsTemplate = p_IsTemplate
           AND t_IIdentProgram = p_IIdentProgram;
           
         v_INumberFather := v_INumberFather + 1;
      
         INSERT INTO dmenuitem_dbt (
                                      t_ObjectID,
                                      t_IsTemplate,
                                      t_IIdentProgram,
                                      t_INumberPoint,
                                      t_INumberFather,
                                      t_InumberLine,
                                      t_ICaseItem,
                                      t_CSystemItem,
                                      t_SzNameItem,
                                      t_SzNamePrompt,
                                      t_IHelp,
                                      t_IProgItem
                                   )
                            VALUES (
                                      p_Oper,
                                      p_IsTemplate,
                                      p_IIdentProgram,
                                      v_INumberFather,
                                      p_INumberFather,
                                      v_INumberLine,
                                      0,
                                      CHR(88),
                                      ' ' || p_SzNameItem,
                                      ' ' || p_SzNameItem,
                                      0,
                                      p_IIdentProgram
                                   );
      END IF;
      
      RETURN v_INumberFather;
   END;

   PROCEDURE InsertMenu(p_Oper IN NUMBER, p_IsTemplate IN CHAR, p_ICaseItem IN NUMBER, p_SzNameItem IN VARCHAR2, p_IIdentProgram IN NUMBER)
   IS
      v_INumberPoint NUMBER := 0;
      v_INumberFather NUMBER := 0;
      v_INumberLine NUMBER := 0;
      v_MenuItemExist NUMBER := 0;
   BEGIN
      v_INumberFather := GetNumberFather('Налоговый учет', v_INumberFather, p_Oper, p_IsTemplate, p_IIdentProgram);
      v_INumberFather := GetNumberFather('НДФЛ', v_INumberFather, p_Oper, p_IsTemplate, p_IIdentProgram);
      v_INumberFather := GetOrInsertNumberFather('СНОБ', v_INumberFather, p_Oper, p_IsTemplate, p_IIdentProgram);
      v_INumberFather := GetOrInsertNumberFather('Отчеты', v_INumberFather, p_Oper, p_IsTemplate, p_IIdentProgram);
      
      v_INumberLine := GetMaxNumberLine(v_INumberFather, p_IsTemplate, p_Oper, p_IIdentProgram) + 1;

      BEGIN
         SELECT COUNT(1)
         INTO v_MenuItemExist
         FROM DMENUITEM_DBT
         WHERE t_ObjectID = p_Oper
           AND t_IsTemplate = p_IsTemplate
           AND t_IIdentProgram = p_IIdentProgram
           AND LTRIM(t_SzNameItem) = p_SzNameItem;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN v_MenuItemExist := 0;
      END;

      IF v_MenuItemExist > 0
      THEN
         INSERT INTO DMENUITEM_DBT (
                                      t_ObjectID,
                                      t_IsTemplate,
                                      t_IIdentProgram,
                                      t_INumberPoint,
                                      t_INumberFather,
                                      t_InumberLine,
                                      t_ICaseItem,
                                      t_CSystemItem,
                                      t_SzNameItem,
                                      t_SzNamePrompt,
                                      t_IHelp,
                                      t_IProgItem
                                   )
                            VALUES (
                                      p_Oper,
                                      p_IsTemplate,
                                      p_IIdentProgram,
                                      v_INumberFather,
                                      v_INumberFather,
                                      v_INumberLine,
                                      p_ICaseItem,
                                      CHR(88),
                                      ' ' || p_SzNameItem,
                                      ' ' || p_SzNameItem,
                                      0,
                                      p_IIdentProgram
                                   );
                                   
         it_log.log('Откат BOSS-1489. Пункт меню ''' || p_SzNameItem || ''' для пользователя ' || p_Oper || ' вставлен успешно');
      ELSE
         it_log.log('Откат BOSS-1489. Пункт меню ''' || p_SzNameItem || ''' для пользователя ' || p_Oper || ' уже существует');
      END IF;
      
      COMMIT;
   END;
   
   PROCEDURE DeleteMenuItem(p_SZNameItem IN VARCHAR2, p_INumberFather IN NUMBER, p_IsTemplate IN CHAR, p_Oper IN NUMBER, p_IIdentProgram NUMBER)
   IS
      v_INumberPoint NUMBER := 0;
   BEGIN
      SELECT t_INumberPoint
      INTO v_INumberPoint
      FROM DMENUITEM_DBT
      WHERE LTRIM(t_SZNameItem) = p_SZNameItem
        AND t_IsTemplate = p_IsTemplate
        AND t_ObjectID = p_Oper
        AND t_IIdentProgram = p_IIdentProgram
        AND t_INumberFather = p_INumberFather;
   
      DELETE
      FROM DMENUITEM_DBT
      WHERE t_INumberPoint = v_INumberPoint
        AND t_IsTemplate = p_IsTemplate
        AND t_ObjectID = p_Oper
        AND t_IIdentProgram = p_IIdentProgram;
        
      it_log.log('Откат BOSS-1489. Пункт меню ''' || p_SZNameItem || ' для пользователя ' || p_Oper || ' успешно удален');
      COMMIT;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         it_log.log('Откат BOSS-1489. Пункт меню ''' || p_SZNameItem || ''' с t_IsTemplate = ''' || CASE WHEN p_IsTemplate = CHR(0) THEN '' ELSE p_IsTemplate END || ''', t_ObjectID = ' || p_Oper || ', t_IIdentProgram = ' || p_IIdentProgram || ' не найден. Удаление не возможно');
         COMMIT;
   END;
begin
  it_log.log('Откат BOSS-1489. Удаляем меню для пользователей роли 10036');
  COMMIT;
  for i in (SELECT DISTINCT T_OPER
              FROM DACSOPROLE_DBT rol
             WHERE ROL.T_ROLEID IN (10036))
  loop
     v_INumberFather := GetNumberFather('Налоговый учет', 0, i.t_Oper, CHR(0), 83);
     v_INumberFather := GetNumberFather('НДФЛ', v_INumberFather, i.t_Oper, CHR(0), 83);
     v_INumberFather := GetNumberFather('СНОБ', v_INumberFather, i.t_Oper, CHR(0), 83);
     v_INumberFather := GetNumberFather('Отчеты', v_INumberFather, i.t_Oper, CHR(0), 83);
     DeleteMenuItem('Отчет по СНОБ и удержанному НДФЛ', v_INumberFather, CHR(0), i.t_Oper, 83);
     DeleteMenuItem('История операций по ФЛ', v_INumberFather, CHR(0), i.t_Oper, 83);
  end loop;

  it_log.log('Откат BOSS-1489. Возвращаем на место пункты меню для пользователей с ролями 10008, 10026');
  COMMIT;

  --Для пользователей ролей
  for i in (SELECT DISTINCT T_OPER
              FROM DACSOPROLE_DBT rol
             WHERE ROL.T_ROLEID IN (10008, 10026))
  loop
     InsertMenu(i.t_Oper, CHR(0), 20209, ' Отчет по СНОБ и удержанному НДФЛ', 83);
     InsertMenu(i.t_Oper, CHR(0), 20210, ' История операций по ФЛ', 83);
  end loop;
  
  --Для образцов меню ролей
   FOR i IN (SELECT DISTINCT rol.T_MENUID
               FROM DACSROLETREE_DBT rol
              WHERE rol.T_ROLEID IN (10008, 10026)
                AND rol.T_MENUID > 0)
   LOOP
      InsertMenu(i.t_MenuID, CHR(88), 20209, ' Отчет по СНОБ и удержанному НДФЛ', 83);
      InsertMenu(i.t_MenuID, CHR(88), 20210, ' История операций по ФЛ', 83);
   END LOOP;
   
   --Для технического пользователя 1
   InsertMenu (1, CHR(0), 20209, ' Отчет по СНОБ и удержанному НДФЛ', 83);
   InsertMenu (1, CHR(0), 20210, ' История операций по ФЛ', 83);
   
   it_log.log('Откат BOSS-1489. Конец скрипта 101_BOSS-1489_RevertMenu.sql');
end;
/
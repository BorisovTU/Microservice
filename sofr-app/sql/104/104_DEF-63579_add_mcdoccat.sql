-- Добавление в таблицу "Виды документов категорий" (dmcdoccat_dbt) записи для категорий "+ПереоценкаА" и "-ПереоценкаА" с t_dockind = 117 (Погашение ЦБ)
DECLARE
  OBJTYPE_RETIRE NUMBER := 117; -- Погашение ЦБ
  
  PROCEDURE AddDockindForCategory(p_CategoryCode IN VARCHAR2, p_DocKind IN NUMBER)
  AS
    recCount NUMBER;
    catID NUMBER;
    catNumber NUMBER;
  BEGIN
    SELECT t_ID, t_Number
      INTO catID, catNumber
      FROM dMcCateg_dbt
     WHERE t_Code = p_CategoryCode;
     
    dbms_output.put_line(p_CategoryCode || ', id = ' || catID || ', number = ' || catNumber);
    IF catID IS NULL THEN
      RETURN;
    END IF;
    
    SELECT COUNT(1)
      INTO recCount
      FROM dMcDocCat_dbt
     WHERE t_catID = catID
       AND t_catNum = catNumber
       AND t_docKind = p_DocKind;
    
    dbms_output.put_line('count = ' || recCount);
    IF recCount = 0 THEN
      dbms_output.put_line('inserting');
      INSERT INTO dmcdoccat_dbt
             (t_CatID, t_CatNum, t_DocKind, t_NotInUse, t_Reserve)
      VALUES (catID, catNumber, p_DocKind, CHR(0), CHR(1));
    END IF;
  END;
  
BEGIN
  AddDockindForCategory('+ПереоценкаА', OBJTYPE_RETIRE);
  AddDockindForCategory('-ПереоценкаА', OBJTYPE_RETIRE);
END;
/

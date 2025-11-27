--Добавление и удаление видов НДР
DECLARE 
BEGIN
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(402,'Plusi','Доходы от реализации ц/б (i года (лет) в собственности)',3,'X',CHR(0),CHR(0),0);
  
END;
/

DECLARE 
BEGIN
  DELETE FROM DNPTXKIND_DBT 
   WHERE T_ELEMENT IN (403, 404, 405, 406);
  
END;
/

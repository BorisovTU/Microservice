--Добавление видов НДР
DECLARE 
BEGIN
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(1190,'PlusG_2792','Весь процентный расход  по коротким позициям по РЕПО (обращающиеся ц/б) (ИИС)',5,CHR(0),'X',CHR(0),0);
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(1174,'FullMinus_239','Вся сумма убытка по  РЕПО в уменьшение НОБ по обращающимся ц/б  (ИИС)',5,CHR(0),'X',CHR(0),0);
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(1175,'FullMinus_226','Все расходы по необращающимся ц/б (ИИС)',5,CHR(0),'X',CHR(0),0);
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(1176,'FullMinus_234','Весь процентный расход  по коротким позициям по РЕПО (не обращающиеся ц/б)',5,CHR(0),'X',CHR(0),0);
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(1177,'FullMinus_240','Вся сумма убытка по  РЕПО в уменьшение НОБ по не обращающимся ц/б  (ИИС)',5,CHR(0),'X',CHR(0),0);
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(1178,'FullMinus_236','Все убытки по обращающимся ц/б, уменьшающие НБ по потерявшим обращаемость ц/б (ИИС)',5,CHR(0),'X',CHR(0),0);
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(1179,'FullMinus_235','Все расходы по необращающимся ПИ (ИИС)',5,CHR(0),'X',CHR(0),0);
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(1180,'FullMinus_230','Все расходы по процентам РЕПО  (ИИС)',5,CHR(0),'X',CHR(0),0);
  INSERT INTO DNPTXKIND_DBT (T_ELEMENT,T_CODE,T_NAME,T_LEVEL,T_INCOME,T_EXPENDITURE,T_DONOTUSE,T_TAXBASEKIND)
                VALUES(1181,'FullMinus_231','Все расходы по коротким позициям по РЕПО (ИИС)',5,CHR(0),'X',CHR(0),0);
END;
/

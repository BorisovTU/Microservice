--Обновление видов НДР
DECLARE 
BEGIN
  UPDATE DNPTXKIND_DBT
     SET t_Income = 'X'
   WHERE t_Element = 435; --Proc_Sec_Short
END;
/

DECLARE 
BEGIN
  UPDATE DNPTXKIND_DBT
     SET t_TaxBaseKind = 2
   WHERE t_Element = 632; --PlusG_1539
END;
/

DECLARE 
BEGIN
  UPDATE DNPTXKIND_DBT
     SET t_TaxBaseKind = 2
   WHERE t_Element = 743; --MinusG_213
END;
/

DECLARE 
BEGIN
  UPDATE DNPTXKIND_DBT
     SET t_TaxBaseKind = 2
   WHERE t_Element = 1020; --MinusG_218
END;
/

DECLARE 
BEGIN
  UPDATE DNPTXKIND_DBT
     SET t_TaxBaseKind = 2
   WHERE t_Element = 1030; --MinusG_219
END;
/

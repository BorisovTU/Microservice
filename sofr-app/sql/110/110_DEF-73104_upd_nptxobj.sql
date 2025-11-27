/*Обновление объектов НДР*/
BEGIN
   
   DELETE FROM DNPTXOBJ_DBT
    WHERE T_CLIENT = 159119
      AND T_DATE = TO_DATE('27.09.2024','DD.MM.YYYY')
      AND T_SUM = -1
      AND T_KIND IN (1150,1162);
   
   UPDATE DNPTXOBJ_DBT 
      SET T_SUM = 1,
          T_SUM0 = 1
    WHERE T_CLIENT = 159119
      AND T_DATE = TO_DATE('27.09.2024','DD.MM.YYYY')
      AND T_SUM = 2
      AND T_KIND = 870;
END;
/



BEGIN
   FOR rec
      IN (SELECT r1.t_OtherFI, r1.t_rateid, r2.t_rateid t_rateid_NotRub, r1.T_ISINVERSE, r1.T_RATE, r1.T_SCALE, r1.T_POINT, r1.T_INPUTDATE, r1.T_INPUTTIME, r1.T_OPER, r1.T_SINCEDATE, r1.T_ISMANUALINPUT, 
                 case when exists (select 1 from DRATEHIST_DBT where T_RATEID = r2.t_rateid and T_SINCEDATE = r1.T_SINCEDATE) or r2.T_SINCEDATE = r1.T_SINCEDATE then 1 else 0 end t_FinalRateExists 
            FROM dratedef_dbt r1, dratedef_dbt r2, dfininstr_dbt fin
           WHERE r1.t_Type IN (2,3,6,18)                       -- Виды курсов
             AND r1.t_Type = r2.t_Type                         -- Одинаковый вид курса
             AND r1.t_FIID != r2.t_FIID                        -- Разный котируемый ФИ
             AND r1.t_FIID = 0                                 -- Рубли, т.е. неправильный
             AND r1.t_market_Place = r2.t_market_Place         -- Одинаковая торговая площадка
             AND r1.t_OtherFI = r2.t_OtherFI                   -- Одна бумага      
             AND fin.t_FIID = r1.t_OtherFI
             AND fin.t_FIID IN (select t_FIID from dfininstr_dbt subfin where subfin.t_facevaluefi  IN (76813,2583, 2589, 16371) and subfin.t_drawingdate >= TO_DATE('01/01/2024 00:00:00', 'MM/DD/YYYY HH24:MI:SS')) )
             
   LOOP               
      /*Перепишем историю неправильного курса на правильный, хотя по идее она должна быть одинаковой*/
      UPDATE DRATEHIST_DBT H 
         SET H.T_RATEID = REC.t_rateid_NotRub
       WHERE H.T_RATEID = REC.T_RATEID
         AND NOT EXISTS (SELECT 1 FROM DRATEHIST_DBT H1 WHERE H1.T_RATEID = REC.t_rateid_NotRub AND H1.T_SINCEDATE = H.T_SINCEDATE);

      /*Удалим дублирующуюся историю неправильного курса*/
      DELETE FROM DRATEHIST_DBT
            WHERE t_rateid = REC.T_RATEID;

      /*Запишем последнее значение неправильного курса в историю правильного (опять же должна быть одинаковой)*/ 
      IF REC.t_FinalRateExists = 0 THEN
         INSERT INTO DRATEHIST_DBT (T_RATEID, T_ISINVERSE, T_RATE, T_SCALE, T_POINT, T_INPUTDATE, T_INPUTTIME, T_OPER, T_SINCEDATE, T_ISMANUALINPUT)  
                            VALUES (REC.t_rateid_NotRub, REC.T_ISINVERSE, REC.T_RATE, REC.T_SCALE, REC.T_POINT, REC.T_INPUTDATE, REC.T_INPUTTIME, REC.T_OPER, REC.T_SINCEDATE, REC.T_ISMANUALINPUT);
      END IF;

      /*Удалим неправильный курс*/
      DELETE FROM dratedef_dbt
            WHERE t_rateid = REC.T_RATEID;

   END LOOP; 

   UPDATE dratedef_dbt rt
      SET rt.t_fiid = 7 /*USD*/ 
    WHERE rt.t_Type IN (2,3,6,18) /*Расчётная, минимальная и максимальная цены, цена закрытия*/ 
      AND rt.t_FIID = 0/*RUB*/ 
      AND rt.t_OtherFI IN (select subfin.t_FIID from dfininstr_dbt subfin where subfin.t_facevaluefi IN (2583,2589,16371) and subfin.t_drawingdate >= TO_DATE('01/01/2024 00:00:00', 'MM/DD/YYYY HH24:MI:SS'));

   UPDATE dratedef_dbt rt
      SET rt.t_fiid = 11 /*CNY*/ 
    WHERE rt.t_Type IN (2,3,6,18) /*Расчётная, минимальная и максимальная цены, цена закрытия*/
      AND rt.t_FIID = 0/*RUB*/ 
      AND rt.t_OtherFI IN (select subfin.t_FIID from dfininstr_dbt subfin where subfin.t_facevaluefi = 76813 and subfin.t_drawingdate >= TO_DATE('01/01/2024 00:00:00', 'MM/DD/YYYY HH24:MI:SS'));

END;
/
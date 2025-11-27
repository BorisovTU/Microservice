--добавление пользовательского метода выполнения мультивалютных проводок
declare
begin
  INSERT INTO dmultymtdcat_dbt (T_METHODID,T_CHAPTER,T_CATPLUS,T_CATMINUS) VALUES (100,1,1001,1002);

  INSERT INTO dmultymtd_dbt (T_METHODID,T_MACRONAME,T_CLASSNAME,T_NAME,T_SHORT_NAME,T_TYPEUSERMETHOD,T_PARENTMETHODID,T_GROUNDPOSITIVE,T_GROUNDNEGATIVE) 
  VALUES (100,chr(1),chr(1),'Доходы и расходы по депо комиссиям','Депо.комисс',0,0,'{Ground}','{Ground}');
   commit;
EXCEPTION
  WHEN OTHERS THEN NULL;     

end;

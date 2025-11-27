create or replace package RSHB_USERMsfo7 as

  -- Заполоняет основную таблицу данных отчета
  procedure InsIntoTmpTable(BegDate in date
                           ,EndDate in date);

end;
/

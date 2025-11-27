--Удалить записи из tRSHB_Portfolio_PKL_2CHD, чтобы отчет ОД1 мог сформироваться заново
BEGIN
  delete from tRSHB_Portfolio_PKL_2CHD e where not exists (select 1 from DPORTFOLIO_TMP t where t.report_dt = e.report_dt);

END;
/

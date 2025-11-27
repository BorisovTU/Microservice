create or replace package QB_DWH_EXPORT_PFI is

  -- Author  : MALASHKEVICH-DP
  -- Created : 05.06.2020 16:20:40
  -- Purpose : Выгрузка ПФИ в DWH

  -- Событие - Выгрузка ПФИ
  cEvent_EXPORT_PFI pls_integer := 14;
  cPFI      pls_integer := 14;

  procedure RunExport(in_Date date, procid number, export_mode number default 0);

end QB_DWH_EXPORT_PFI;
/

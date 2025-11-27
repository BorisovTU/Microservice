create or replace package QB_DWH_EXPORT_DU is

  -- Author  : MALASHKEVICH-DP
  -- Created : 05.06.2020 16:20:40
  -- Purpose : Выгрузка данных по ц/б в ДУ в DWH

  -- Событие - Выгрузка ДУ
  cEvent_EXPORT_DU pls_integer := 15;
  cDU      pls_integer := 15;

  function InConst(cname qb_dwh_const4exp.name%type,
                 val qb_dwh_const4exp_val.value%type) return boolean;

  procedure RunExport(in_Date date, procid number);

end QB_DWH_EXPORT_DU;
/

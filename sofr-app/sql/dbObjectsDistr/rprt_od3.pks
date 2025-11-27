create or replace package RPRT_OD3 as

  --Маленький пакет для отчета ОД_3
  REPOPART_DOCKIND constant integer := 176;

  --p_leg - Ид записи в ddl_leg_dbt
  function f_get_dealrepo_account(p_dealid      in integer
                                 ,p_leg         in integer
                                 ,p_dealtype    in integer
                                 ,p_bofficekind in integer
                                 ,p_date        in date default sysdate) return varchar2;

  --***************************************************************************-
  --Корректировка до суммы ЭПС. По лоту сделки РЕПО  
  function f_get_corrinttoeir(p_dealid in integer
                             ,p_fiid   in integer
                             ,p_date   in date default sysdate) return number;

  --Корректировка до суммы оценочного резерва. По лоту сделки РЕПО  
  function f_get_correstreserve(p_dealid in integer
                               ,p_fiid   in integer
                               ,p_date   in date default sysdate) return number;

  --Сумма оценочного резерва. По лоту сделки РЕПО  
  function f_get_estreserve(p_dealid in integer
                           ,p_fiid   in integer
                           ,p_date   in date default sysdate) return number;

  -- Формирование отчета в CSV 
  function make_report(p_dt     date
                      ,o_errmsg out varchar2) return number;

end RPRT_OD3;
/

-- доработка сделок АРНУ

update ddl_tick_Dbt set t_flag3 = chr(88) where t_dealcode like 'ARNU%' and t_dealdate < to_date('01.01.2019','dd.mm.yyyy')   and t_bofficekind=127

update ddl_tick_Dbt set t_flag3 = chr(88) where t_dealcode like '5ЕР01/282/1' and t_dealdate = to_date('24.12.2018','dd.mm.yyyy')   and t_bofficekind=127 
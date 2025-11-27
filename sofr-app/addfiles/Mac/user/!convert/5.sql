-- удаление налоговых сделок

delete from ddlrq_dbt where t_docid in
(select t_dealid from ddl_tick_Dbt where t_dealdate < to_date('01.01.2019','dd.mm.yyyy') and t_clientid=-1  and t_bofficekind=127 and  t_dealcode like 'ARNU%')

delete from ddl_leg_dbt where t_dealid in
(select t_dealid from ddl_tick_Dbt where t_dealdate < to_date('01.01.2019','dd.mm.yyyy') and t_clientid=-1  and t_bofficekind=127 and  t_flag3=chr(88) and  t_dealcode like 'ARNU%' )

delete from ddl_tick_Dbt where t_dealdate < to_date('01.01.2019','dd.mm.yyyy') and t_clientid=-1 and  t_bofficekind=127  and  t_flag3=chr(88) and  t_dealcode like 'ARNU%'
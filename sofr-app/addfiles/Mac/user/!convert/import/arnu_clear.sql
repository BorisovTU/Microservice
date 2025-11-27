-- очистка загруженных сделок

--------------------------------------------------------------------------------------------------------
delete from ddlrq_dbt where t_dockind=127 and t_docid in
(select t_dealid from ddl_tick_Dbt where   t_dealcode like 'ARNU/%' );

delete from ddl_leg_Dbt where t_dealid in 
(select t_dealid from ddl_tick_Dbt where    t_dealcode like 'ARNU/%' );

delete from ddl_tick_Dbt where     t_dealcode like 'ARNU/%' ;
--------------------------------------------------------------------------------------------------------
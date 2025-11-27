-- закрытие платежей
update ddlrq_Dbt set t_state=2 where t_id in (
select t_id from ddlrq_Dbt where t_docid in (
select t_dealid  from ddl_tick_dbt  where  t_dealcode like 'ARNU/%' --and t_dealdate < to_date('01.01.2019','dd.mm.yyyy')
)) and t_state=0

update ddl_tick_dbt set t_dealstatus=20, t_flag3=chr(88) where t_dealstatus=0 and  t_dealcode like 'ARNU/%'




begin
    rsb_sctx.TXCreateLots( to_date('01.01.1980','dd.mm.yyyy'),  to_date('01.01.2020','dd.mm.yyyy'), -1, -1, 0, 0, 1);
end;





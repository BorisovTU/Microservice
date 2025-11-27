--откат изменений скрипта 112_dss_sheduler_rudata 
begin
delete from DSS_SHEDULER_DBT where t_id in (10058,10059,10061,10062,10063,10064);
delete from DSIMPLESERVICE_DBT where t_id in (10058,10059,10061,10062,10063,10064);
delete  from dfunc_dbt where t_funcid in (5200,5201,5202,5203,5204);
update dss_sheduler_dbt set T_SHEDULERTYPE = 2 where t_id = 10009;
delete  from DSS_FUNC_DBT  where t_id in (100058,100059,100061,100062,100063,100064);
commit;
end;
/
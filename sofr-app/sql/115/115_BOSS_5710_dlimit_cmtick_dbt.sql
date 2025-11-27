declare
  n integer;
begin
  select count(*) into n from user_tables t where t.TABLE_NAME = upper('DLIMIT_CMTICK_DBT');
  if n > 0
  then
    execute immediate 'DROP table DLIMIT_CMTICK_DBT';
  end if;
  execute immediate 'create table DLIMIT_CMTICK_DBT(t_dealid number(10)
                                ,t_dockind number(5)
                                ,t_sumtype number(1)
                                ,t_clientid number(10)
                                ,t_dlcontrid number(10)
                                ,t_clientcontrid number(10)
                                ,t_plan_date date
                                ,t_isfactpaym varchar2(1)
                                ,t_fiid number(10)
                                ,t_plan_plus number
                                ,t_plan_minus number
                                ,t_calc_sid varchar2(128) default ''X'' not null)
                      partition by list(T_CALC_SID)(partition P99999999X values(''X''))';
  execute immediate 'create index DLIMIT_CMTICK_IDX1 on DLIMIT_CMTICK_DBT (t_clientcontrid, t_fiid) local';
  execute immediate 'create index DLIMIT_CMTICK_IDX2 on DLIMIT_CMTICK_DBT(t_dlcontrid) local';
end;
/

declare
  n      number;
  t_name varchar2(100) := 'DDL_LIMITOP_DBT';
begin
  select count(*) into n from user_tables t where t.table_name = upper(t_name);
  if n = 0
  then
    execute immediate 'create table DDL_LIMITOP_DBT
(
  t_startdt        date  default  sysdate ,
  t_calc_direct    varchar2(128)  not null,
  t_Calcdate       date  not null,
  t_CalcParam      clob ,
  t_user           varchar2(128),
  t_status         varchar2(32)   not null,
  t_statusdt       date  default  sysdate ,
  t_statustxt      varchar2(2000),
  t_Calclog        clob ,
  t_Contrlog       clob 
)';
    execute immediate 'create unique index DDL_LIMITOP_IDX0 on DDL_LIMITOP_DBT (t_calc_direct) tablespace INDX';
    execute immediate 'create index DDL_LIMITOP_IDX1 on DDL_LIMITOP_DBT (t_startdt) tablespace INDX';
    execute immediate 'create index DDL_LIMITOP_IDX2 on DDL_LIMITOP_DBT (t_Calcdate,t_startdt) tablespace INDX';
  end if;
end;
/

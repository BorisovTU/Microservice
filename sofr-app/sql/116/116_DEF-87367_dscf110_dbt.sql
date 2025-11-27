-- Дубль 97/97_def50034_dscf110_dbt.sql
declare
  n      number;
  t_name varchar2(100) := 'DSCF110_DBT';
begin
  select count(*) into n from user_tables t where t.table_name = upper(t_name);
  if n != 0
  then
    execute immediate 'drop table dscf110_dbt' ;
  end if;
   execute immediate 'create table dscf110_dbt
( t_sysdate        DATE default sysdate ,
  t_sessionid      NUMBER default sys_context(''USERENV'',''SESSIONID''),
  t_id             NUMBER default its_log.nextval , 
  t_acctrnid       NUMBER(10),
  t_carrysum       NUMBER(32,12),
  t_id_operation   NUMBER(10),
  t_id_step        NUMBER(5),
  t_fiid           NUMBER(10),
  t_portfolio      NUMBER(5),
  t_amount         NUMBER(32,12),
  t_cost           NUMBER(32,12),
  t_nkdamount      NUMBER(32,12),
  t_interestincome NUMBER(32,12),
  t_discountincome NUMBER(32,12),
  t_outlay         NUMBER(32,12),
  t_corrvalue      NUMBER(32,12),
  t_corrinttoeir   NUMBER(32,12),
  t_s385_16        NUMBER(32,12),
  t_s386_17        NUMBER(32,12),
  t_s175_16        NUMBER(32,12),
  t_s176_17        NUMBER(32,12),
  t_s_4_4          NUMBER(32,12),
  t_s_4_2          NUMBER(32,12)
)';
    execute immediate 'comment on table dscf110_dbt
  is ''Для промежуточных расчетов в scf110_data.mac (ф 110) ''';
end;
/

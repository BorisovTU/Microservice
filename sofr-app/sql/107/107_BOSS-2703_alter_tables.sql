declare
begin
  execute immediate 'alter table ddl_comm_dbt add t_sumprecision number(5)';
end;


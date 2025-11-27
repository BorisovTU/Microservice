--Изменение urefill_manual_dbt
begin
  execute immediate 'alter table urefill_manual_dbt rename to urefill_dbt';
end;
/

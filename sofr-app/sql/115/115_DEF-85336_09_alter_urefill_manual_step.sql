--Изменение urefill_manual_step_dbt
begin
  execute immediate 'alter table urefill_manual_step_dbt rename to urefill_step_dbt';
end;
/
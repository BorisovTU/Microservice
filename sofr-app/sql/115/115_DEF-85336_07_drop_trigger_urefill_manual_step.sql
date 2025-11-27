--Изменение urefill_manual_step_dbt
begin
  execute immediate 'drop trigger "UREFILL_MANUAL_STEP_DBT_T0_AINC"';
end;
/

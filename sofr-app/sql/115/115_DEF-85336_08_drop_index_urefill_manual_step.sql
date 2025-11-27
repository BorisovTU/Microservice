--Изменение urefill_manual_step_dbt
begin
  execute immediate 'drop index UREFILL_MANUAL_STEP_IDX0';
  execute immediate 'drop index UREFILL_MANUAL_STEP_IDX1';
  execute immediate 'drop index UREFILL_MANUAL_STEP_IDX2';
end;
/
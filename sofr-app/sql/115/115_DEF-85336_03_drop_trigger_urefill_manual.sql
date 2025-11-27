--Изменение urefill_manual_dbt
begin
  execute immediate 'drop trigger "UREFILL_MANUAL_T0_AINC"';
end;
/
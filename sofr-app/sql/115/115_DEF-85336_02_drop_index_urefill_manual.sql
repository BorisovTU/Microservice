--Изменение urefill_manual_dbt
begin
  execute immediate 'drop index UREFILL_MANUAL_IDX0';
  execute immediate 'drop index UREFILL_MANUAL_IDX1';
  execute immediate 'drop index UREFILL_MANUAL_IDX2';
  execute immediate 'drop index UREFILL_MANUAL_IDX3';
end;
/
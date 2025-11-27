--Изменение urefill_dbt
begin
  execute immediate 'CREATE UNIQUE INDEX UREFILL_IDX0 ON UREFILL_DBT (T_ID)';
  execute immediate 'CREATE INDEX UREFILL_IDX1 ON UREFILL_DBT (T_DATE_OPERATION)';
  execute immediate 'CREATE INDEX UREFILL_IDX2 ON UREFILL_DBT (T_STATUS)';
  execute immediate 'CREATE INDEX UREFILL_IDX3 ON UREFILL_DBT (T_CONTRACT)';
end;
/

--Изменение unptxop_payment_link_step_dbt
begin
  execute immediate 'CREATE INDEX UNPTXOP_PAYMENT_LINK_STEP_DBT_IDX1 ON UNPTXOP_PAYMENT_LINK_STEP_DBT (T_REFILLID)';
end;
/
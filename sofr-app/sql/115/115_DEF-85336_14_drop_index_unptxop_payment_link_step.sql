--Изменение unptxop_payment_link_step_dbt
begin
  execute immediate 'drop index UNPTXOP_PAYMENT_LINK_STEP_DBT_IDX1';
end;
/
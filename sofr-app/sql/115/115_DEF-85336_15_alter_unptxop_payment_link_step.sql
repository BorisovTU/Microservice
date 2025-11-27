--Изменение unptxop_payment_link_step_dbt
begin
  execute immediate 'alter table UNPTXOP_PAYMENT_LINK_STEP_DBT rename column T_NPTXOPID to T_REFILLID';
end;
/
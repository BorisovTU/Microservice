--Изменение unptxop_payment_link_dbt
begin
  execute immediate 'drop index UNPTXOP_PAYMENT_LINK_IDX0';
end;
/
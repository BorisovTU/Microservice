--Изменение unptxop_payment_link_dbt
begin
  execute immediate 'alter table unptxop_payment_link_dbt add t_refillid number(10)';
  execute immediate 'comment on column unptxop_payment_link_dbt.t_refillid is ''ID операции подкрепления''';
end;
/
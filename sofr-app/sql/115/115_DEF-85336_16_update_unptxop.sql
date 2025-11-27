--Изменение unptxop_payment_link_dbt, unptxop_payment_link_step_dbt
begin
  update unptxop_payment_link_step_dbt set t_refillid = -t_refillid;
  update unptxop_payment_link_dbt set t_refillid = -t_nptxopid, t_nptxopid = 0 where t_nptxopid < 0;
  update unptxop_payment_link_dbt set t_refillid = -t_nptxopid where t_nptxopid > 0;
end;
/
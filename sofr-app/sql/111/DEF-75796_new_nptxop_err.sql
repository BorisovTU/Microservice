begin
  delete dnptxop_err_dbt where error_id = 11;
  insert into dnptxop_err_dbt (error_id, error_name) values (11, 'Договор не соответствует клиенту');
end;
/
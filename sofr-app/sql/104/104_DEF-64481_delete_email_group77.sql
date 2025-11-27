
begin
  DELETE FROM dllvalues_dbt where 
    T_LIST = 5009 and T_ELEMENT = 77
    and T_CODE = '77' and T_NAME = 'DiasoftIntegration';
  DELETE FROM usr_email_addr_dbt tbl_mail WHERE tbl_mail.t_group = 77;
end;

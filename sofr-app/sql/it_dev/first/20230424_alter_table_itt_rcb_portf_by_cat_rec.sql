declare
n number;
begin
  select count(*) into n from user_tab_columns t where t.TABLE_NAME ='ITT_RCB_PORTF_BY_CAT_REC' and t.COLUMN_NAME  = upper('t_facevaluefi_ccy');
  if n = 0 then
    execute immediate 'alter table itt_rcb_portf_by_cat_rec add t_facevaluefi_ccy varchar(50)';
    execute immediate 'comment on column itt_rcb_portf_by_cat_rec.t_facevaluefi is ''ID валюты номинала ''';
    execute immediate 'comment on column itt_rcb_portf_by_cat_rec.t_facevaluefi_ccy is ''Валюта номинала ''';
  end if;
exception
  when no_data_found then
    null;
end;
/

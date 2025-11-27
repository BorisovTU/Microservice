declare
n number;
begin
  select count(*) into n from user_tab_columns t where t.TABLE_NAME ='ITT_RCB_PORTF_BY_CAT_REC' and t.COLUMN_NAME  = upper('t_requirement_rest');
  if n = 0 then
    execute immediate 'alter table itt_rcb_portf_by_cat_rec add t_requirement_rest number';
    execute immediate 'comment on column itt_rcb_portf_by_cat_rec.t_requirement_rest is ''Баланс требований/обязательств в штуках (корректировка к t_rest)''';
  end if;
  select count(*) into n from user_tab_columns t where t.TABLE_NAME ='ITT_RCB_PORTF_BY_CAT_REC' and t.COLUMN_NAME  = upper('requirement_summ');
  if n = 0 then
    execute immediate 'alter table itt_rcb_portf_by_cat_rec add requirement_summ number';
    execute immediate 'comment on column itt_rcb_portf_by_cat_rec.requirement_summ is ''Баланс требований/обязательств в валюте инструмента (корректировка к summ)''';
  end if;
  select count(*) into n from user_tab_columns t where t.TABLE_NAME ='ITT_RCB_PORTF_BY_CAT_REC' and t.COLUMN_NAME  = upper('requirement_summ_rur_cb');
  if n = 0 then
    execute immediate 'alter table itt_rcb_portf_by_cat_rec add requirement_summ_rur_cb number';
    execute immediate 'comment on column itt_rcb_portf_by_cat_rec.requirement_summ_rur_cb is ''Баланс требований/обязательств в рублях по курсу ЦБ (корректировка к summ_rur_cb)''';
   end if;
  select count(*) into n from user_tab_columns t where t.TABLE_NAME ='ITT_RCB_PORTF_BY_CAT_REC' and t.COLUMN_NAME  = upper('is_upd');
  if n = 0 then
    execute immediate 'alter table itt_rcb_portf_by_cat_rec add is_upd number';
   end if;
  select count(*) into n from user_tab_columns t where t.TABLE_NAME ='ITT_RCB_PORTF_BY_CAT_REC' and t.COLUMN_NAME  = upper('rate_abs');
  if n = 0 then
    execute immediate 'alter table itt_rcb_portf_by_cat_rec add rate_abs number';
    execute immediate 'comment on column itt_rcb_portf_by_cat_rec.rate_abs is ''Абсолютный курс''';
  end if;
  select count(*) into n from user_tab_columns t where t.TABLE_NAME ='ITT_RCB_PORTF_BY_CAT_REC' and t.COLUMN_NAME  = upper('foreign_priz');
  if n = 0 then
    execute immediate 'alter table itt_rcb_portf_by_cat_rec add foreign_priz number';
    execute immediate 'comment on column itt_rcb_portf_by_cat_rec.foreign_priz is ''Признак иностр эмитента''';
  end if;
  select count(*) into n from user_tab_columns t where t.TABLE_NAME ='ITT_RCB_PORTF_BY_CAT_REC' and t.COLUMN_NAME  = upper('ETF_priz');
  if n = 0 then
    execute immediate 'alter table itt_rcb_portf_by_cat_rec add ETF_priz number';
    execute immediate 'comment on column itt_rcb_portf_by_cat_rec.ETF_priz is ''Признак паи-etf ''';
  end if;
exception
  when no_data_found then
    null;
end;
/

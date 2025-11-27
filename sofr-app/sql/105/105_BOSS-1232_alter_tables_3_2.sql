begin

  execute immediate 'alter table DSCDLFI_DBT add t_sumprecision number(5)';
  execute immediate 'alter table DSCDLFI_TMP add t_sumprecision number(5)';
  execute immediate 'alter table DSCDLFI_TMP add t_sumprecision_from number(5)';

  execute immediate 'alter table ddl_comm_dbt add t_corpactiondate date';
  execute immediate 'alter table ddl_comm_dbt add t_considerfirstbuydate CHAR(1)';
  execute immediate 'comment on column DDL_COMM_DBT.t_corpactiondate is ''Дата корпоративного действия''';
  execute immediate 'comment on column DDL_COMM_DBT.t_considerfirstbuydate is ''Учитывать первоначальные даты покупки при расчете НОБ: 1 - признак установлен, 0 - признак сброшен''';

end;
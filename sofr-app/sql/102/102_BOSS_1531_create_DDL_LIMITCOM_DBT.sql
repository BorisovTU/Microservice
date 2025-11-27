declare
  n      number;
  t_name varchar2(100) := 'DDL_LIMITCOM_DBT';
begin
  select count(*) into n from user_tables t where t.table_name = upper(t_name);
  if n = 0
      then
        execute immediate 'create table DDL_LIMITCOM_DBT
        (
          t_marketid  NUMBER not null,
          t_client    NUMBER not null,
          t_sfcontrid NUMBER not null,
          t_commnumber NUMBER not null,
          t_sum       NUMBER not null,
          t_fiid      NUMBER not null,
          t_plandate  DATE not null
        )';
    execute immediate 'comment on table DDL_LIMITCOM_DBT is ''Неоплаченные комиссии для расчета резервов''';

    execute immediate 'comment on column DDL_LIMITCOM_DBT.t_client  is ''ID клиента ''';
    execute immediate 'comment on column DDL_LIMITCOM_DBT.t_sfcontrid is ''ID субдоговора''';
    execute immediate 'comment on column DDL_LIMITCOM_DBT.t_commnumber  is ''Номер комиссии''';
    execute immediate 'comment on column DDL_LIMITCOM_DBT.t_sum  is ''Сумма неоплаченной комиссии''';
    execute immediate 'comment on column DDL_LIMITCOM_DBT.t_fiid  is ''Валюта комиссии''';
    execute immediate 'comment on column DDL_LIMITCOM_DBT.t_plandate  is ''Плановая дата оплаты ''';
    execute immediate 'comment on column DDL_LIMITCOM_DBT.t_marketid  is ''ID биржи''';

    execute immediate 'create index DDL_LIMITCOM_DBT_IDX1 on DDL_LIMITCOM_DBT (t_marketid, t_sfcontrid,t_fiid,t_plandate) tablespace INDX';
 end if;
end;
/

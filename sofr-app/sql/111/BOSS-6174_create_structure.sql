declare

  procedure drop_table_if_exists( p_table_name varchar2) is
    l_cnt number(1);
  begin
    select count(*) into l_cnt from user_tables WHERE upper(table_name) = upper(p_table_name);
    if l_cnt = 1 then
      execute immediate 'DROP TABLE ' || p_table_name;
      it_log.log_handle(p_object => 'drop_table_if_exists',
                        p_msg    => 'table ' || p_table_name || ' dropped');
    end if;
  end drop_table_if_exists;
begin
  drop_table_if_exists(p_table_name => 'nontrading_autorun_config');

  execute immediate '
    create table nontrading_autorun_config (
      src                  varchar2(50) not null,
      exchange_type        number(10) not null,
      exchange_type_target number(10),
      is_full_rest         number(1) not null,
      is_allowed           number(1) not null
    )';
end;
/

begin
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO',    0, null, 0, 1);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO',    0, null, 1, 1);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO',    1, null, 0, 1);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO',    1, null, 1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO',    0, 1,    0, 1);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO',    0, 1,    1, 1);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO',    1, 0,    0, 1);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO',    1, 0,    1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('EFR',    0, null, 0, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('EFR',    0, null, 1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('EFR',    1, null, 0, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('EFR',    1, null, 1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('EFR',    0, 1,    0, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('EFR',    0, 1,    1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('EFR',    1, 0,    0, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('EFR',    1, 0,    1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 0, null, 0, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 0, null, 1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 1, null, 0, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 1, null, 1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 0, 1,    0, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 0, 1,    1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 1, 0,    0, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 1, 0,    1, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 1, 1,    0, 0);
  insert into nontrading_autorun_config (src, exchange_type, exchange_type_target, is_full_rest, is_allowed) values ('DBO UL', 1, 1,    1, 0);

  execute immediate 'comment on table nontrading_autorun_config is ''Настроечная таблица для определения, доступен ли автозапуск неторгового поручения''';
  execute immediate 'comment on column nontrading_autorun_config.src                  is ''Система-источник поручения''';
  execute immediate 'comment on column nontrading_autorun_config.exchange_type        is ''Тип биржевого обслуживания договора''';
  execute immediate 'comment on column nontrading_autorun_config.exchange_type_target is ''Тип биржевого обслуживания договора зачисления. Только для переводов''';
  execute immediate 'comment on column nontrading_autorun_config.is_full_rest         is ''Полный вывод средств''';
  execute immediate 'comment on column nontrading_autorun_config.is_allowed           is ''0 - автозапуск запрещён. 1 - разрешён''';
end;
/

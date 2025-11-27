create table nontrading_autorun_config (
  src                  varchar2(50) not null,
  exchange_type        number(10) not null,
  exchange_type_target number(10),
  is_full_rest         number(1) not null,
  is_allowed           number(1) not null,
  currency             varchar2(3),
  is_iis               number(1) not null
);

comment on table nontrading_autorun_config is 'Настроечная таблица для определения, доступен ли автозапуск неторгового поручения';
comment on column nontrading_autorun_config.src                  is 'Система-источник поручения';
comment on column nontrading_autorun_config.exchange_type        is 'Тип биржевого обслуживания договора';
comment on column nontrading_autorun_config.exchange_type_target is 'Тип биржевого обслуживания договора зачисления. Только для переводов';
comment on column nontrading_autorun_config.is_full_rest         is 'Полный вывод средств';
comment on column nontrading_autorun_config.is_allowed           is '0 - автозапуск запрещён. 1 - разрешён';
comment on column nontrading_autorun_config.currency             is 'Валюта';
comment on column nontrading_autorun_config.is_iis               is 'Признак ИИС';

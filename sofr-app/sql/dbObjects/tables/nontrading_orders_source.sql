--Не нормализованная таблица. Пока очень мало систем-источников и маловероятно, что станет больше. Можно оставить таблицу в таком виде
create table nontrading_orders_source (
  incoming_name     varchar2(100),
  buffer_name       varchar2(100),
  opercode_name     varchar2(100)
);

comment on table nontrading_orders_source is 'Виды систем-источников для неторговых поручений';
comment on column nontrading_orders_source.incoming_name is 'Входящее имя';
comment on column nontrading_orders_source.buffer_name   is 'Имя для сохранения в буфере';
comment on column nontrading_orders_source.opercode_name is 'Имя для фомирования кода операции';
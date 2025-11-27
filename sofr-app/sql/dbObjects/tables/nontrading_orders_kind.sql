create table nontrading_orders_kind (
  kind_id   number(9),
  kind_name varchar2(100) not null
);

alter table nontrading_orders_kind add constraint pk_nptx_kind_id primary key (kind_id) using index tablespace INDX;

create table nontrading_orders_status (
  status_id      number(9),
  status_name    varchar2(30) not null,
  status_comment varchar2(100)
);

alter table nontrading_orders_status add constraint pk_nptx_status_id primary key (status_id) using index tablespace INDX;

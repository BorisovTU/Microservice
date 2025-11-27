create table nontrading_orders_error (
  error_id   number(9),
  error_name varchar2(100)
);

alter table nontrading_orders_error add constraint pk_nptx_err_id primary key (error_id) using index tablespace INDX;

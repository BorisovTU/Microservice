create table nontrading_orders_buffer (
 req_id                 number(9),
 src                    varchar2(3) not null,
 external_id            varchar2(30) not null,
 client_cft_id          varchar2(30),
 iis                    number(1),
 contract               varchar2(50), 
 client_code            varchar2(30),
 marketplace_withdrawal number(1),
 marketplace_enroll     number(1),
 is_full_rest           number(1),
 currency               varchar2(3),
 amount                 number(32,12),
 enroll_account         varchar2(20),
 department             varchar2(50),
 req_date               date,
 req_time               date,
 status_id              number(9),
 kind                   number(2),
 last_feed_back         timestamp,
 import_time            timestamp default systimestamp not null,
 status_changed         timestamp default systimestamp not null,
 error_id               number(9),
 operation_id           number(10), --нет fk, потому что на dnptxop_dbt.t_id нет pk или unique key
 file_name              varchar2(100)
);

alter table nontrading_orders_buffer add constraint pk_nptx_req_id primary key (req_id) using index tablespace INDX;

alter table nontrading_orders_buffer add constraint fk_nptx_req_status_id foreign key (status_id)    references nontrading_orders_status(status_id);
alter table nontrading_orders_buffer add constraint fk_nptx_req_error_id  foreign key (error_id)     references nontrading_orders_error(error_id);
alter table nontrading_orders_buffer add constraint fk_nptx_req_kind      foreign key (kind)         references nontrading_orders_kind(kind_id);

create index ind_nptx_req_status_id  on nontrading_orders_buffer (status_id) tablespace INDX;
create index ind_nptx_req_error_id   on nontrading_orders_buffer (error_id) tablespace INDX;
create index ind_nptx_req_sec_ext_id on nontrading_orders_buffer (src, external_id) tablespace INDX;
create index ind_nptx_req_to_create  on nontrading_orders_buffer (case when error_id = 0 and operation_id is null then error_id end) tablespace INDX;
create index ind_nptx_req_imp_time   on nontrading_orders_buffer (import_time) tablespace INDX;
create index ind_nptx_req_kind       on nontrading_orders_buffer (kind) tablespace INDX;
create index ind_nptx_req_oper_id    on nontrading_orders_buffer (operation_id) tablespace INDX;

comment on table nontrading_orders_buffer is 'Входящие поручения на выводы/переврды денежных средств клиентов' ;
comment on column nontrading_orders_buffer.src                    is 'DBO/EFR - источник поручения';
comment on column nontrading_orders_buffer.contract               is 'Номер договора';
comment on column nontrading_orders_buffer.client_code            is 'ЕКК клиента';
comment on column nontrading_orders_buffer.marketplace_withdrawal is '0 Внебиржевой рынок; 1 Фондовый рынок; 2 Валютный рынок; 3 Срочный рынок; 4 СПБ';
comment on column nontrading_orders_buffer.marketplace_enroll     is '0 Внебиржевой рынок; 1 Фондовый рынок; 2 Валютный рынок; 3 Срочный рынок; 4 СПБ';
comment on column nontrading_orders_buffer.kind                   is 'Вид поручения. 0 - Вывод ДС с биржевого счета. 1 - Вывод ДС с внебиржевого счета. 2 - Перевод ДС между субсчетами ДБО';
comment on column nontrading_orders_buffer.last_feed_back         is 'Дата и время последнего возврата статуса в систему-источник';
comment on column nontrading_orders_buffer.status_changed         is 'Дата последнего изменения статуса записи. ';
comment on column nontrading_orders_buffer.operation_id           is 'Наименование исходного файла поручений';
comment on column nontrading_orders_buffer.file_name              is 'ID операции DNPTXOP_DBT';

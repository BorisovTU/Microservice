declare
  procedure drop_table_if_exists( p_table_name varchar2) is
    l_cnt number(1);
  begin
    select count(*) into l_cnt from user_tables WHERE upper(table_name) = upper(p_table_name);
    if l_cnt = 1 then
      execute immediate 'DROP TABLE ' || p_table_name;
      it_log.log_handle(p_object => 'BOSS-3138_create_structure',
                        p_msg    => 'table ' || p_table_name || ' dropped');
    end if;
  end drop_table_if_exists;
begin
  drop_table_if_exists(p_table_name => 'dnptxop_req_dbt');
  drop_table_if_exists(p_table_name => 'dnptxop_err_dbt');
  drop_table_if_exists(p_table_name => 'dnptxop_status_dbt');
end;
/

begin
  execute immediate '
    create table dnptxop_err_dbt (
      error_id   number(9),
      error_name varchar2(100)
    )';
end;
/

begin
  execute immediate 'alter table dnptxop_err_dbt add constraint pk_nptx_err_id primary key (error_id) using index tablespace INDX';
end;
/

begin
  execute immediate '
    create table dnptxop_status_dbt (
      status_id      number(9),
      status_name    varchar2(30) not null,
      status_comment varchar2(200)
    )';
end;
/

begin
  execute immediate 'alter table dnptxop_status_dbt add constraint pk_nptx_status_id primary key (status_id) using index tablespace INDX';
end;
/

begin
  execute immediate '
    create table dnptxop_req_dbt (
     req_id             number(9),
     src                varchar2(3) not null,
     external_id        varchar2(30) not null,
     client_cft_id      varchar2(30),
     iis                number(1),
     contract           varchar2(50), 
     client_code        varchar2(30),
     is_exchange        number(1),
     is_exchange_target number(1),
     is_full_rest       number(1),
     currency           varchar2(3),
     amount             number(32,12),
     enroll_account     varchar2(20),
     department         varchar2(50),
     req_date           date,
     req_time           date,
     status_id          number(9),
     kind               number(2),
     last_feed_back     timestamp,
     import_time        timestamp default systimestamp not null,
     status_changed     date,
     error_id           number(9),
     operation_id       number(10),
     file_name          varchar2(300)
    )';
end;
/

begin
  execute immediate 'alter table dnptxop_req_dbt add constraint pk_nptx_req_id primary key (req_id) using index tablespace INDX';

  execute immediate 'alter table dnptxop_req_dbt add constraint fk_nptx_req_status_id foreign key (status_id) references dnptxop_status_dbt(status_id)';
  execute immediate 'alter table dnptxop_req_dbt add constraint fk_nptx_req_error_id  foreign key (error_id) references dnptxop_err_dbt(error_id)';

  execute immediate 'create index ind_nptx_req_status_id  on dnptxop_req_dbt (status_id) tablespace INDX';
  execute immediate 'create index ind_nptx_req_error_id   on dnptxop_req_dbt (error_id) tablespace INDX';
  execute immediate 'create index ind_nptx_req_sec_ext_id on dnptxop_req_dbt (src, external_id) tablespace INDX';
  execute immediate 'create index ind_nptx_req_to_create  on dnptxop_req_dbt (case when error_id = 0 and operation_id is null then error_id end) tablespace INDX';
  execute immediate 'create index ind_nptx_req_imp_time   on dnptxop_req_dbt (import_time) tablespace INDX';

  execute immediate 'comment on table dnptxop_req_dbt is ''Входящие поручения на выводы и переврды денежных средств клиентов''';
  execute immediate 'comment on column dnptxop_req_dbt.src                is ''DBO или EFR - источник поручения''';
  execute immediate 'comment on column dnptxop_req_dbt.contract           is ''Номер договора''';
  execute immediate 'comment on column dnptxop_req_dbt.client_code        is ''ЕКК клиента''';
  execute immediate 'comment on column dnptxop_req_dbt.is_exchange        is ''Флаг биржевой или внебиржевой рынок''';
  execute immediate 'comment on column dnptxop_req_dbt.is_exchange_target is '' Флаг биржевой или внебиржевой рынок цель - для переводов''';
  execute immediate 'comment on column dnptxop_req_dbt.kind               is ''Вид поручения. 0 - Вывод ДС с биржевого счета. 1 - Вывод ДС с внебиржевого счета. 2 - Перевод ДС между субсчетами ДБО''';
  execute immediate 'comment on column dnptxop_req_dbt.last_feed_back     is ''Дата и время последнего возврата статуса в систему-источник (возврат в текущей задаче не реализуется)''';
  execute immediate 'comment on column dnptxop_req_dbt.status_changed     is ''Дата последнего изменения статуса записи. ''';
  execute immediate 'comment on column dnptxop_req_dbt.operation_id       is ''Наименование исходного файла поручений''';
  execute immediate 'comment on column dnptxop_req_dbt.file_name          is ''ID операции DNPTXOP_DBT''';
end;
/

begin
  insert into dnptxop_status_dbt (status_id, status_name, status_comment) values (0, 'Готово к обработке', 'Поручение заведено в буфер без ошибок');
  insert into dnptxop_status_dbt (status_id, status_name, status_comment) values (1, 'Ожидает исполнения', 'На основе поручения заведена операция без ошибок');
  insert into dnptxop_status_dbt (status_id, status_name, status_comment) values (2, 'Исполнено', 'Операция по поручению исполнена');
  insert into dnptxop_status_dbt (status_id, status_name, status_comment) values (3, 'Ошибка', 'При заведении операции произошла ошибка');
  insert into dnptxop_status_dbt (status_id, status_name, status_comment) values (4, 'Отклонено', 'Пользователь сам отклонил поручение и указал ошибку');
  insert into dnptxop_status_dbt (status_id, status_name, status_comment) values (5, 'Исполняется', 'Операция находится в процессе исполнения или в очереди на исполнение конвейером');
  insert into dnptxop_status_dbt (status_id, status_name, status_comment) values (6, 'Создание операции', 'Операция в данный момент создаётся');
  commit;
end;
/

begin
  insert into dnptxop_err_dbt (error_id, error_name) values (0, 'Нет ошибки');
  insert into dnptxop_err_dbt (error_id, error_name) values (1, 'Найден дубль');
  insert into dnptxop_err_dbt (error_id, error_name) values (2, 'Не найден клиент');
  insert into dnptxop_err_dbt (error_id, error_name) values (3, 'Не найден договор');
  insert into dnptxop_err_dbt (error_id, error_name) values (4, 'Не найден ЕКК');
  insert into dnptxop_err_dbt (error_id, error_name) values (5, 'Не найдена валюта');
  insert into dnptxop_err_dbt (error_id, error_name) values (6, 'Не найден счет для списания');
  insert into dnptxop_err_dbt (error_id, error_name) values (7, 'Недостаточно средств');
  insert into dnptxop_err_dbt (error_id, error_name) values (8, 'Не найден счет для зачисления');
  insert into dnptxop_err_dbt (error_id, error_name) values (9, 'Не найден договор для зачисления');
  insert into dnptxop_err_dbt (error_id, error_name) values (999, 'Внутренняя ошибка загрузки');
  commit;
end;
/

declare

  procedure create_sq_if_not_exists (p_sq_name varchar2) is
    l_cnt number(1);
  begin
    select count(1)
      into l_cnt
      from user_sequences s
     where s.SEQUENCE_NAME = upper(p_sq_name);

    if l_cnt = 0 then
      execute immediate 'create sequence ' || p_sq_name || ' nocache';
      it_log.log_handle(p_object => 'BOSS-3138_create_structure',
                        p_msg    => 'sequence ' || p_sq_name || ' created');
    end if;
  end create_sq_if_not_exists;
begin
  create_sq_if_not_exists(p_sq_name => 'dnptxop_req_seq');
end;
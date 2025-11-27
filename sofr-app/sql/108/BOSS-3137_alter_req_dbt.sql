begin
  execute immediate 'alter table dnptxop_req_dbt modify status_changed timestamp';
end;
/

begin
  update dnptxop_req_dbt r set r.status_changed = r.import_time
    where r.status_changed is null;
  commit;
end;
/

declare
  e_object_not_null exception;
  pragma exception_init( e_object_not_null, -01442);
begin
  execute immediate 'alter table dnptxop_req_dbt modify status_changed default systimestamp not null';
exception
  when e_object_not_null then null;
end;
/

declare
  procedure drop_fk_if_exists (
    p_table_name varchar2,
    p_fk_name    varchar2
  ) is
    e_fk_not_exists exception;
    pragma exception_init( e_fk_not_exists, -02443);
  begin
    execute immediate 'alter table ' || p_table_name || ' drop constraint ' || p_fk_name;
    it_log.log_handle(p_object => 'install_script',
                      p_msg    => 'fk ' || p_fk_name || ' dropped from table ' || p_table_name);
  exception
    when e_fk_not_exists then null;
  end drop_fk_if_exists;
begin
  drop_fk_if_exists(p_table_name => 'dnptxop_req_dbt', p_fk_name => 'fk_nptx_req_kind');
end;
/

declare
  procedure drop_ind_if_exists (
    p_name varchar2
  ) is
    e_ind_not_exists exception;
    pragma exception_init( e_ind_not_exists, -01418);
  begin
    execute immediate 'drop index ' || p_name;
    it_log.log_handle(p_object => 'install_script',
                      p_msg    => 'index ' || p_name || ' dropped');
  exception
    when e_ind_not_exists then null;
  end drop_ind_if_exists;
begin
  drop_ind_if_exists(p_name => 'ind_nptx_req_kind');
  drop_ind_if_exists(p_name => 'ind_nptx_req_oper_id');
end;
/

declare
  procedure drop_table_if_exists( p_table_name varchar2) is
    l_cnt number(1);
  begin
    select count(*) into l_cnt from user_tables WHERE upper(table_name) = upper(p_table_name);
    if l_cnt = 1 then
      execute immediate 'DROP TABLE ' || p_table_name;
      it_log.log_handle(p_object => 'install_script',
                        p_msg    => 'table ' || p_table_name || ' dropped');
    end if;
  end drop_table_if_exists;
begin
  drop_table_if_exists(p_table_name => 'dnptxop_kind_dbt');
end;
/

begin
  execute immediate '
    create table dnptxop_kind_dbt (
      kind_id   number(9),
      kind_name varchar2(100) not null
        )';
end;
/

begin
  execute immediate 'alter table dnptxop_kind_dbt add constraint pk_nptx_kind_id primary key (kind_id) using index tablespace INDX';
end;
/

begin
  insert into dnptxop_kind_dbt (kind_id, kind_name) values (0, 'Вывод ДС с биржевого счета');
  insert into dnptxop_kind_dbt (kind_id, kind_name) values (1, 'Вывод ДС с внебиржевого счета');
  insert into dnptxop_kind_dbt (kind_id, kind_name) values (2, 'Перевод ДС между субсчетами ДБО');
  commit;
end;
/

begin
  execute immediate 'alter table dnptxop_req_dbt add constraint fk_nptx_req_kind      foreign key (kind)         references dnptxop_kind_dbt(kind_id)';
  execute immediate 'create index ind_nptx_req_kind       on dnptxop_req_dbt (kind) tablespace INDX';
  execute immediate 'create index ind_nptx_req_oper_id    on dnptxop_req_dbt (operation_id) tablespace INDX';
end;
/

create or replace type t_number_list as table of number(32, 12);
/

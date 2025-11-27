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

  procedure create_sq_if_not_exists (
    p_sq_name varchar2
  ) is
    l_cnt number(1);
  begin
    select count(1)
      into l_cnt
      from user_sequences s
     where s.SEQUENCE_NAME = upper(p_sq_name);

    if l_cnt = 0 then
      execute immediate 'create sequence ' || p_sq_name || ' nocache';
      it_log.log_handle(p_object => 'create_sq_if_not_exists',
                        p_msg    => 'sequence ' || p_sq_name || ' created');
    end if;
  end create_sq_if_not_exists;
begin
  drop_table_if_exists(p_table_name => 'uddl_wrtenr_links');
  create_sq_if_not_exists(p_sq_name => 'uddl_wrtenr_links_seq');
end;
/

declare
    e_pk_already_exists exception;
    pragma exception_init( e_pk_already_exists, -02260);
begin
  begin
    execute immediate 'alter table ddl_tick_dbt add constraint DDL_TICK_DBT_IDX0 primary key (T_DEALID) using index tablespace INDX';
  exception
    when e_pk_already_exists then
      null;
  end;

  execute immediate '
create table uddl_wrtenr_links (
  link_id        number(12),
  wrtoff_deal_id number(12),
  wrtoff_guid    varchar2(256),
  enroll_deal_id number(12),
  enroll_guid    varchar2(256)
)';

  execute immediate 'alter table uddl_wrtenr_links add constraint pk_uddl_wrtenr_link_id primary key (link_id) using index tablespace INDX';

  execute immediate 'alter table uddl_wrtenr_links add constraint fk_uddl_enr_deal_id  foreign key (enroll_deal_id) references ddl_tick_dbt(t_dealid) on delete cascade';
  execute immediate 'alter table uddl_wrtenr_links add constraint fk_uddl_wrt_deal_id  foreign key (wrtoff_deal_id) references ddl_tick_dbt(t_dealid) on delete cascade';
  
  execute immediate 'create index ind_uddl_enr_deal_id on uddl_wrtenr_links (enroll_deal_id) tablespace INDX';
  execute immediate 'create index ind_uddl_enr_guid    on uddl_wrtenr_links (enroll_guid) tablespace INDX';
  execute immediate 'create index ind_uddl_wrt_deal_id on uddl_wrtenr_links (wrtoff_deal_id) tablespace INDX';
  execute immediate 'create index ind_uddl_wrt_guid    on uddl_wrtenr_links (wrtoff_guid) tablespace INDX';
end;
/

create table uddl_wrtenr_links (
  link_id        number(12),
  wrtoff_deal_id number(12),
  wrtoff_guid    varchar2(256),
  enroll_deal_id number(12),
  enroll_guid    varchar2(256)
);

alter table uddl_wrtenr_links add constraint pk_uddl_wrtenr_link_id primary key (link_id) using index tablespace INDX;

alter table uddl_wrtenr_links add constraint fk_uddl_enr_deal_id  foreign key (enroll_deal_id) references ddl_tick_dbt(t_dealid) on delete cascade;
alter table uddl_wrtenr_links add constraint fk_uddl_wrt_deal_id  foreign key (wrtoff_deal_id) references ddl_tick_dbt(t_dealid) on delete cascade;

create index ind_uddl_enr_deal_id on uddl_wrtenr_links (enroll_deal_id) tablespace INDX;
create index ind_uddl_enr_guid    on uddl_wrtenr_links (enroll_guid) tablespace INDX;
create index ind_uddl_wrt_deal_id on uddl_wrtenr_links (wrtoff_deal_id) tablespace INDX;
create index ind_uddl_wrt_guid    on uddl_wrtenr_links (wrtoff_guid) tablespace INDX;

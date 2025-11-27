create table itt_rcb_portf_by_cat_pack (
  id_rcb_portf_by_cat_pack      NUMBER,
  id_file_in                    NUMBER,
  id_file_cb_portf_by_cat       NUMBER,
  id_file_cover                 NUMBER,
  id_file_detail                NUMBER,
  report_date                   DATE, 
  create_user                   NUMBER,
  start_date                    DATE,
  end_date                      DATE
);

comment on table itt_rcb_portf_by_cat_pack is 'Атрибуты запуска отчета по концентрации активов 6-ЭП';

comment on column itt_rcb_portf_by_cat_pack.id_rcb_portf_by_cat_pack
  is 'Идентификатор записи';
comment on column itt_rcb_portf_by_cat_pack.id_file_in
  is 'Запуск отчета. Таблица itt_file';   
comment on column itt_rcb_portf_by_cat_pack.id_file_cb_portf_by_cat
  is 'Выходная форма отчета для ЦБ'; 
comment on column itt_rcb_portf_by_cat_pack.id_file_cover
  is 'Выходная форма отчета статистика'; 
comment on column itt_rcb_portf_by_cat_pack.id_file_detail
  is 'Выходная форма отчета развернутая';     
comment on column itt_rcb_portf_by_cat_pack.report_date
  is 'Дата отчета';  
comment on column itt_rcb_portf_by_cat_pack.create_user
  is 'Пользователь создавший запись';
comment on column itt_rcb_portf_by_cat_pack.start_date
  is 'Дата создания файла';
comment on column itt_rcb_portf_by_cat_pack.end_date
  is 'Дата окончания файла';  
   
create unique index iti_rcb_portf_cat_pack_pk on itt_rcb_portf_by_cat_pack (id_rcb_portf_by_cat_pack);
create index iti_rcb_portf_cat_pack_id_file on itt_rcb_portf_by_cat_pack (id_file_in); 
  
  

--Пакет запуска выгрузки лимитов в QUIK
create table itt_lim_exp_pack(
  id_lim_exp_pack number,
  id_file         number,  
  create_user     number,
  start_date      date,
  end_date        date,
  delete_date     date,
  create_sysdate  date,
  update_sysdate  date,
  error_txt       clob
);

comment on table itt_lim_exp_pack
  is 'Пакет запуска выгрузки лимитов в QUIK';

comment on column itt_lim_exp_pack.id_lim_exp_pack
  is 'Идентификатор пакета';
comment on column itt_lim_exp_pack.id_file
  is 'Идентификатор файла';
comment on column itt_lim_exp_pack.create_user
  is 'Пользователь создавший запись';
comment on column itt_lim_exp_pack.start_date
  is 'Дата начала формирования выгрузки';
comment on column itt_lim_exp_pack.end_date
  is 'Дата окончания формирования выгрузки ';
comment on column itt_lim_exp_pack.delete_date
  is 'Дата удаления';
comment on column itt_lim_exp_pack.create_sysdate
  is 'Дата создания записи';
comment on column itt_lim_exp_pack.create_sysdate
  is 'Дата обновления записи';   
comment on column itt_lim_exp_pack.error_txt
  is 'Текст ошибки';    
  
create unique index iti_id_lim_exp_pack_pk on itt_lim_exp_pack (id_lim_exp_pack);
create index iti_itt_lim_exp_pack_id_file on itt_lim_exp_pack (id_file);
create index iti_itt_lim_exp_pack_start_date on itt_lim_exp_pack (start_date);
 

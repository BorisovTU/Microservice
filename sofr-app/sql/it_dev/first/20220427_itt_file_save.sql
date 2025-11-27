--Журнал сохранения файла
create table itt_file_save (
  id_file_save   number,
  id_file        number,
  file_dir       varchar2(2000),
  file_name      varchar2(1000),
  save_user      number,
  save_user_name varchar2(250), 
  save_date      date,
  create_sysdate date,
  update_sysdate date,
  error_clob     clob  
);


create unique index iti_file_save on itt_file_save(id_file_save);
create index iti_file_save_id_file on itt_file_save(id_file); 

alter table itt_file_save add constraint itt_file_save_pk unique (id_file_save);

comment on table ITT_FILE_SAVE
  is 'Журнал сохранения файла';
-- Add comments to the columns 
comment on column ITT_FILE_SAVE.id_file_save
  is 'Идентификатор строки';
comment on column ITT_FILE_SAVE.id_file
  is 'Идентификатор файла';
comment on column ITT_FILE_SAVE.file_dir
  is 'Каталог';
comment on column ITT_FILE_SAVE.file_name
  is 'Название файла';
comment on column ITT_FILE_SAVE.save_user
  is 'ID пользователя сохранившего файл';
comment on column ITT_FILE_SAVE.save_user_name
  is 'Имя пользователя сохранившего файл';
comment on column ITT_FILE_SAVE.save_date
  is 'Дата сохранения файла';
comment on column ITT_FILE_SAVE.create_sysdate
  is 'Системная дата создания';
comment on column ITT_FILE_SAVE.update_sysdate
  is 'Системная дата обновления';
comment on column ITT_FILE_SAVE.error_clob
  is 'Описание ошибки';  

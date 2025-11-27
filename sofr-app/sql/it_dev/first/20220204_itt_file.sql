  /**************************************************************************************************\
  Обмен файлами для всего функционала разработки РСХБ-Интех
  **************************************************************************************************
  Изменения:
  ---------------------------------------------------------------------------------------------------
  Дата        Автор            Jira                          Описание 
  ----------  ---------------  ---------------------------   ----------------------------------------
  04.02.2022  Мелихова О.С.    BIQ-6664 CCBO-506             Создание
 \**************************************************************************************************/

create table itt_file(
id_file        number,
file_dir       varchar2(1000),
file_name      varchar2(250),
file_clob      clob,
file_blob      blob,
from_system    varchar2(250),
from_module    varchar2(250),
to_system      varchar2(250),
to_module      varchar2(250),
create_sysdate date,
create_user    varchar2(250),
note           varchar2(4000)
)
LOB (file_clob) STORE AS  (DISABLE STORAGE IN ROW)
LOB (file_blob) STORE AS  (DISABLE STORAGE IN ROW);

alter table itt_file add constraint itt_file_pk primary key (id_file);

create index iti_file_create_sysdate on itt_file (create_sysdate);
create index iti_file_file_name on itt_file (file_name);

comment on table itt_file is 'Загрузка файлов';

comment on column itt_file.id_file      is 'Иденттификатор строки ';
comment on column itt_file.file_dir     is 'Название директории файла';
comment on column itt_file.file_name    is 'Название файла';
comment on column itt_file.file_clob    is 'Тело CLOB файла ';
comment on column itt_file.file_blob    is 'Тело BLOB файла';
comment on column itt_file.from_system  is 'Система, откуда загрузили файл';
comment on column itt_file.from_module  is 'Название макроса/модуля, откуда загрузили файл';
comment on column itt_file.to_system    is 'Система, куда загрузили файл';
comment on column itt_file.to_module    is 'Название макроса/модуля, куда загрузили файл';
comment on column itt_file.create_sysdate is 'Дата создания записи';
comment on column itt_file.create_user    is 'Пользователь, создавший запись';
comment on column itt_file.note           is 'Комментарий';

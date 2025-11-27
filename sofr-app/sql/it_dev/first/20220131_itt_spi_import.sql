 /**************************************************************************************************\
  BIQ-6664 / Загрузка справочника платежных инструкций СПИ из файла
  **************************************************************************************************
  Изменения:
  ---------------------------------------------------------------------------------------------------
  Дата        Автор            Jira                             Описание 
  ----------  ---------------  ------------------------------   -------------------------------------
  31.01.2022  Мелихова О.С.    BIQ-6664 CCBO-506                Создание
 \**************************************************************************************************/
--drop table itt_spi_import; 
create table itt_spi_import (
  id_spi_import        number,
  id_file              number,
  excel_rownum         number,
  parentgroup          varchar2(1000),
  groupname            varchar2(1000),
  fullname             varchar2(1000),
  account_name         varchar2(1000),
  instrument           varchar2(1000),
  si_external          varchar2(1000),
  si_internal          varchar2(1000),
  is_default           varchar2(1000),
  disabled             varchar2(1000),
  cft_file_name        varchar2(1000),
  currency             varchar2(1000),
  account_no           varchar2(1000),
  bank_name            varchar2(1000),
  bic                  varchar2(1000),
  corr_account         varchar2(1000),
  corr_bank_name       varchar2(1000),
  corr_bic             varchar2(1000),
  swift                varchar2(1000),
  asudr                varchar2(1000),
  spi_id               varchar2(1000),
  create_sysdate       date,
  t_list               number,
  t_element            number,
  t_settaccid          number,
  t_partyid            number,
  result_status        varchar2(1000),  
  result_msg           varchar2(4000)
   );   


alter table itt_spi_import
  add constraint itt_spi_import_pk primary key (id_spi_import);

create index iti_spi_import_id_file on itt_spi_import (id_file);

  
comment on table itt_spi_import is 'Загрузка СПИ из файла'; 
  
comment on column itt_spi_import.id_spi_import  is 'Идентификатор записи';
comment on column itt_spi_import.id_file        is 'Идентификатор записи таблицы TIT_FILE';
comment on column itt_spi_import.excel_rownum   is 'Номер строки файла EXCEL';
comment on column itt_spi_import.parentgroup    is 'Содержимое поля "PARENTGROUP" входящего файла ';
comment on column itt_spi_import.groupname      is 'Содержимое поля "GROUPNAME" входящего файла ';
comment on column itt_spi_import.fullname       is 'Содержимое поля "FULLNAME" входящего файла ';
comment on column itt_spi_import.account_name   is 'Содержимое поля "ACCOUNTNAME" входящего файла ';
comment on column itt_spi_import.instrument     is 'Содержимое поля "INSTRUMENT" входящего файла ';
comment on column itt_spi_import.si_external    is 'Содержимое поля "SI_EXTERNAL" входящего файла ';
comment on column itt_spi_import.si_internal    is 'Содержимое поля "SI_INTERNAL" входящего файла ';
comment on column itt_spi_import.is_default     is 'Признак';
comment on column itt_spi_import.disabled       is 'Признак';
comment on column itt_spi_import.cft_file_name  is 'Файл ЦФТ';
comment on column itt_spi_import.currency       is 'Валюта счета';
comment on column itt_spi_import.account_no     is 'Расчетный счет банка';
comment on column itt_spi_import.bank_name      is 'Название банка';
comment on column itt_spi_import.bic            is 'БИК банка';
comment on column itt_spi_import.corr_account   is 'Корр счет банка';
comment on column itt_spi_import.corr_bank_name is 'Банк-корреспондент';
comment on column itt_spi_import.corr_bic       is 'БИК банка-корреспондента';
comment on column itt_spi_import.swift          is 'SWIFT';
comment on column itt_spi_import.asudr          is 'ASUDR';
comment on column itt_spi_import.spi_id         is 'Идентификатор СПИ';
comment on column itt_spi_import.create_sysdate is 'Дата создания записи';
comment on column itt_spi_import.t_list         is 'dllvalues_dbt.t_list (Справочник) если строка в неё была добавлена';
comment on column itt_spi_import.t_element      is 'dllvalues_dbt.t_element (Номер элемента) если строка в неё была добавлена';
comment on column itt_spi_import.t_settaccid    is 'dsettacc_dbt.t_settaccid (Идентификатор счета расчетов)';
comment on column itt_spi_import.t_partyid      is 'dsettacc_dbt.t_partyid (Идентификатор бенефициара)';
comment on column itt_spi_import.result_status  is 'Статус обработки INSERT/UPDATE/ERROR';
comment on column itt_spi_import.result_msg     is 'Сообщение о результате обработки';


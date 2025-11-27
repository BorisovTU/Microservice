
create table itt_rcb_portf_by_cat_rec
( id_rcb_portf_by_cat_rec       NUMBER,
  id_rcb_portf_by_cat_pack      NUMBER,
  t_dlcontrid                   number,  
  t_dlcontr_name                varchar2(50),
  t_partyid                     NUMBER,
  t_party_code                  varchar2(250),
  t_legalform                   Varchar2(10),
  t_shortname                   VARCHAR2(4000),
  is_qual                       Varchar2(10),
  t_notresident                 VARCHAR2(10), --рез/нерез 
  t_okato                       VARCHAR2(20),--если неизвестно (по-умолчанию) литера "00"
  t_fiid                        NUMBER, ----идентификатор ценной бумаги
  t_fiid_name                   VARCHAR2(250), ----описание ценной бумаги
  t_isin                        VARCHAR2(250),--ISIN ценной бумаги
  t_rest                        NUMBER,--кол-во ценных бумаг на договоре клиента в рамках департамента(он всегда =1) 
  t_facevaluefi                 NUMBER,--валюта номинала
  t_facevalue                   number,--номинал
  t_rate_fiid                   NUMBER, -- приоритетный фин инструмент 
  t_rate_ccy                    VARCHAR2(3),    --приоритетная валюта
  t_rate_isrelative             VARCHAR2(250),
  t_rate                        NUMBER,-- qt-приоритетный курс  qt цена(котировка) актива , для валюты оф.курс ЦБ, для облигаций в % , для остальных абсолютное значение в валюте
  t_rate_type                   NUMBER,-- приоритетный тип цены /*Рыночная цена / Цена Bloomberg для ф.707*/   
  t_rate_market_place /*qt_src*/number,-- приоритетный MP - market place /*СПБ / Мосбиржа/ Bloomberg*/ qt_src
  t_rate_sincedate              DATE, -- приоритетная дата котировки / дата последнего обновления qt /*qt_date*/
  is_rate_sincedate90           number, -- признак плохого курса 
  t_rate_definition_lst         varchar2(4000),-- qt_lst --текст с листом котировок которые принимал СОФР
  t_fi_kind                     NUMBER,
  t_avoirkind                   NUMBER,
  t_avrkind_root                number,
  t_kind_name                   VARCHAR2(250),--/*облигация / акция/ */ins_typ тип инструмента
  summ                          NUMBER, --/*amt     /*qt - оригина  валюта  */ 
  summ_rur_cb                   NUMBER,--объм в рублях по курсу ЦБ на указанную дату отчета =amt * (rub/ccy)
  t_rate_cb                     NUMBER,  --курс валют ЦБ  РФ
  t_nkd_rate_isrelative         varchar2(250),
  t_nkd_rate                    NUMBER,
  t_nkd_rate_fiid               NUMBER,
  t_nkd_rate_ccy                varchar2(50),
  nkd_summ                      NUMBER,
  nkd_summ_rur_cb               NUMBER,
  t_nkd_rate_cb                 NUMBER      
 )
 ;
 
comment on table ITT_RCB_PORTF_BY_CAT_REC
  is 'Атрибуты для отчета по отчету по концентрации активов';
-- Add comments to the columns 
comment on column ITT_RCB_PORTF_BY_CAT_REC.id_rcb_portf_by_cat_rec
  is 'Идентификатор записи';
comment on column ITT_RCB_PORTF_BY_CAT_REC.id_rcb_portf_by_cat_pack
  is 'Идентификатор атрибутов';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_dlcontrid
  is 'Идентификатор договор';  
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_dlcontr_name
  is 'Договор';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_partyid
  is 'Идентификатор клиента';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_legalform
  is 'Категория клиента: 1 - Физик; 2 - Юрик ';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_shortname
  is 'Краткое наименование клиента';
comment on column ITT_RCB_PORTF_BY_CAT_REC.is_qual
  is 'Категория клиента: 1 - Квал; 2 - Неквал';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_notresident
  is 'Категория клиента: 1 - Резидент; 2 - Нерезидент';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_okato
  is 'Код ОКАТО / если неизвестно литера "00"';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_fiid
  is 'Идентификатор финансового инструмента';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_fiid_name
  is 'Наименование финансовго инструмента';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_isin
  is 'ISIN ';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_rest
  is 'кол-во ценных бумаг на договоре клиента в рамках департамента(он всегда =1) ';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_facevaluefi
  is 'валюта номинала';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_facevalue
  is 'номинал';  
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_rate_fiid
  is 'приоритетный фин инструмент ';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_rate_ccy
  is 'приоритетная валюта';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_rate
  is 'qt-приоритетный курс  qt цена(котировка) актива , для валюты оф.курс ЦБ, для облигаций в % , для остальных абсолютное';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_rate_cb
  is 'оф.курс ЦБ';  
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_rate_type
  is 'приоритетный тип цены /*Рыночная цена / Цена Bloomberg для ф.707*/   ';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_rate_market_place
  is 'приоритетный MP - market place /*СПБ / Мосбиржа/ Bloomberg*/ qt_src';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_rate_sincedate
  is 'приоритетная дата котировки / дата последнего обновления qt /*qt_date*/';
comment on column ITT_RCB_PORTF_BY_CAT_REC.is_rate_sincedate90
  is 'признак плохого курса(дата больше 90 дней даты отчета)';  
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_rate_definition_lst
  is 'qt_lst --текст с листом котировок которые принимал СОФР';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_fi_kind
  is 'идентификатор типа инструмента';
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_Avoirkind
  is 'идентификатор типа инструмента';  
comment on column ITT_RCB_PORTF_BY_CAT_REC.t_kind_name
  is '/*облигация / акция/ */ins_typ тип инструмента';
comment on column ITT_RCB_PORTF_BY_CAT_REC.summ
  is '/*amt     /*qt - оригина  валюта  */ ';
comment on column ITT_RCB_PORTF_BY_CAT_REC.summ_rur_cb
  is 'объм в рублях по курсу ЦБ на указанную дату отчета =amt * (rub/ccy)';

  
create unique index iti_rcb_portf_by_cat_rec_pk on itt_rcb_portf_by_cat_rec (id_rcb_portf_by_cat_rec);
create index iti_rcb_portf_by_cat_rec_id_file on itt_rcb_portf_by_cat_rec (id_rcb_portf_by_cat_pack);



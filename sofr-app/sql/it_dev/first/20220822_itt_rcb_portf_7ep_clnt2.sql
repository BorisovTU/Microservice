
create table itt_rcb_portf_7ep_clnt
(
  id_rcb_portf_7ep_pack  number not null,
  t_partyid         number not null,
  t_dlcontrid        number ,
  t_dlcontr_name      varchar2(50),
  sfcontr_begin     date ,
  sfcontr_end       date ,
  client_agegrp     number not null,
  client_birth_date date not null,
  client_gender     char(1) not null,
  portf_begin       number not null,
  portf_end         number not null,
  portf_end_frgn    number not null,
  portf_end_etf     number not null,
  cash_end          number not null,
  cash_end_frgn     number not null,
  fii_count         number ,
  portf_add         number,
  portf_add_cnt     number,
  portf_subtract    number
);
comment on column itt_rcb_portf_7ep_clnt.id_rcb_portf_7ep_pack
  is 'Идентификатор отчета';
comment on column itt_rcb_portf_7ep_clnt.t_partyid
  is 'Ключ клиента';
comment on column itt_rcb_portf_7ep_clnt.t_dlcontrid
  is 'Ключ контракта';
comment on column itt_rcb_portf_7ep_clnt.t_dlcontr_name
  is 'Контракт';
comment on column itt_rcb_portf_7ep_clnt.sfcontr_begin
  is 'Дата заключения договора';
comment on column itt_rcb_portf_7ep_clnt.sfcontr_end
  is 'Дата закрытия договора';
comment on column itt_rcb_portf_7ep_clnt.client_agegrp
  is 'Возрастная группа';
comment on column itt_rcb_portf_7ep_clnt.client_birth_date
  is 'ДР';
comment on column itt_rcb_portf_7ep_clnt.client_gender
  is 'Пол';
comment on column itt_rcb_portf_7ep_clnt.portf_begin
  is 'Порьфель на начало';
comment on column itt_rcb_portf_7ep_clnt.portf_end
  is 'Порьфель на отчетную дату';
comment on column itt_rcb_portf_7ep_clnt.portf_end_frgn
  is 'Бумаги иностронных эмитентов на на отчетную дату';
comment on column itt_rcb_portf_7ep_clnt.portf_end_etf
  is 'Инструменты коллективного инвестирования (ETF/ПИФ)';
comment on column itt_rcb_portf_7ep_clnt.cash_end
  is 'Ден. средства на отчетную дату';
comment on column itt_rcb_portf_7ep_clnt.cash_end_frgn
  is 'Инвалюта  на отчетную дату';
comment on column itt_rcb_portf_7ep_clnt.portf_add
  is 'Пополнение за отчетный период';
comment on column itt_rcb_portf_7ep_clnt.portf_add_cnt
  is 'Кол-во пополнений за отчетный период в разрезе клиента';
comment on column itt_rcb_portf_7ep_clnt.portf_subtract
  is 'Вывод средств за отчетный период';
comment on column itt_rcb_portf_7ep_clnt.fii_count
  is 'Кол-во ФИ в разрезе t_partyid ';
create unique index iti_rcb_portf_7ep_clnt_pk on itt_rcb_portf_7ep_clnt (id_rcb_portf_7ep_pack,t_partyid,t_dlcontrid);

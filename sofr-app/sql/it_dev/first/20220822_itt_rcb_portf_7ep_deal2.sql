create table itt_rcb_portf_7ep_deal
(
  id_rcb_portf_7ep_pack  NUMBER not null,
  t_partyid         NUMBER not null,
  sfcontr_id       NUMBER not null,
  t_dealdate        date not null,
  cnt_deal          number not null,
  cnt_margin        number not null,
  cnt_short_sel     number not null,
  cnt_Margin_call   Number not null
);

comment on column itt_rcb_portf_7ep_deal.id_rcb_portf_7ep_pack
  is 'Идентификатор отчета';
comment on column itt_rcb_portf_7ep_deal.t_partyid
  is 'Ключ клиента';
comment on column itt_rcb_portf_7ep_deal.sfcontr_id
  is 'Ключ контракта';
comment on column itt_rcb_portf_7ep_deal.t_dealdate
  is 'Дата сделок';
comment on column itt_rcb_portf_7ep_deal.cnt_deal
  is 'Кол-во всего';
comment on column itt_rcb_portf_7ep_deal.cnt_margin
  is 'Кол-во маржинальных';
comment on column itt_rcb_portf_7ep_deal.cnt_short_sel
  is 'Кол-во коротких продаж';
comment on column itt_rcb_portf_7ep_deal.cnt_Margin_call
  is 'Кол-во закрытые по margin call';

  create unique index ITI_RCB_PORTF_7ЕР_DEAL_PK on itt_rcb_portf_7ep_deal (id_rcb_portf_7ep_pack,t_partyid,sfcontr_id,t_dealdate);

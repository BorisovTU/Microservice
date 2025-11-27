alter table itt_rcb_portf_by_cat_rec add t_requirement_rest number;

comment on column itt_rcb_portf_by_cat_rec.t_requirement_rest is 'Баланс требований/обязательств в штуках (корректировка к t_rest)';

alter table itt_rcb_portf_by_cat_rec add requirement_summ number;

comment on column itt_rcb_portf_by_cat_rec.requirement_summ is 'Баланс требований/обязательств в валюте инструмента (корректировка к summ)';

alter table itt_rcb_portf_by_cat_rec add requirement_summ_rur_cb number;

alter table itt_rcb_portf_by_cat_rec add is_upd number;

comment on column itt_rcb_portf_by_cat_rec.requirement_summ_rur_cb is 'Баланс требований/обязательств в рублях по курсу ЦБ (корректировка к summ_rur_cb)';

alter table itt_rcb_portf_by_cat_rec add rate_abs number;
comment on column itt_rcb_portf_by_cat_rec.rate_abs is 'Абсолютный курс';

alter table itt_rcb_portf_by_cat_rec add foreign_priz number;
comment on column itt_rcb_portf_by_cat_rec.foreign_priz is 'Признак иностр эмитента';

alter table itt_rcb_portf_by_cat_rec add ETF_priz number;
comment on column itt_rcb_portf_by_cat_rec.ETF_priz is 'Признак паи-etf ';

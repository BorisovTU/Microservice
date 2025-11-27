--Исправление даты сделки и даты начала размещения ЦБ
update ddl_tick_dbt set t_dealdate = TO_DATE('21;11;2018', 'DD;MM;YYYY') where t_dealcode = 'LSEB/31T1/01'/

update davoiriss_dbt set t_begplacementdate = TO_DATE('21;11;2018', 'DD;MM;YYYY') where t_isin = 'RU000A0ZZUS8'/
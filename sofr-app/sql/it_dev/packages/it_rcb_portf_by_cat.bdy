create or replace package body it_rcb_portf_by_cat is

  /**************************************************************************************************\
   Отчет - Информация по концентрации активов клиентов на брокерском обслуживании 
           по разным категориям инвесторов по запросу Банка России
   **************************************************************************************************
   Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание 
   ----------  ---------------  ---------------------------   ----------------------------------------
   26.12.2024  Макринский В.А.  DEF-77215                      Уменьшено кол-во строк в файле детализации (1M->500K) для Астры
   04.04.2023  Зыков М.В.       DEF-30097                      BIQ-11362 (Доработки отчетности 6ЭП в рамках BIQ-11362)
   29.08.2022  Зыков М.В.       BIQ-12884                      Правка условия  требований и обязательств
   22.08.2022  Зыков   М.В.     BIQ-12884                      Правка условия "относительный курс" 
   05.08.2022  Зыков   М.В.     BIQ-12884                      Добавлены столбцы foreign_priz и etf_priz 
   07.07.2022  Мелихова О.С.    BIQ-11362                     Добавлена процедура update_security 
   27.04.2022  Зотов Ю.Н.       BIQ-11362                     Выделение требований и обязательств в отдельные 3 колонки в таблице детализации:
                                                                t_requirement_rest, requirement_summ, requirement_summ_rur_cb
                                                              Изменение основного select в make_process:
                                                                теперь отталкиваемся от договоров и всё (клиентов, ЦБ, счета, требования и обзятельства) фильтруем по отобранным договорам.
   29.03.2022  Мелихова О.С.    BIQ-11362                     Создание
               Зотов Ю.Н.
  */
  s_rep_date date;

  s_rep_cb_portf_by_cat clob;

  s_rep_cover_stat clob;

  -- Признак иностранного эмитента
  function get_priz_notrezident(p_isin in varchar2) return varchar2 deterministic is
    v_res        varchar2(10);
    v_avoirkind  number;
    v_t_parentfi number;
    v_issuer     number;
  begin
    --it_log.log('START'); 
    if p_isin is not null
    then
      select f.t_avoirkind
            ,f.t_parentfi
            ,f.t_issuer
        into v_avoirkind
            ,v_t_parentfi
            ,v_issuer
        from dfininstr_dbt f
       where f.t_fi_code = p_isin;
      if v_avoirkind in (45, 46, 10, 47) /*расписка*/
         and v_t_parentfi != 0
      then
        select f.t_issuer into v_issuer from dfininstr_dbt f where f.t_fiid = v_t_parentfi;
      end if;
      select p.t_notresident into v_res from dparty_dbt p where p.t_partyid = v_issuer;
    end if;
    return v_res;
  exception
    when others then
      return null;
  end;

  function make_process(p_repdate date) return number is
    v_id_rep_pack               number;
    v_cnt                       number;
    v_itt_rcb_portf_by_cat_pack itt_rcb_portf_by_cat_pack%rowtype;
  begin
    it_log.log('START');
    v_id_rep_pack                                        := its_main.nextval();
    v_itt_rcb_portf_by_cat_pack                          := null;
    v_itt_rcb_portf_by_cat_pack.id_rcb_portf_by_cat_pack := v_id_rep_pack;
    v_itt_rcb_portf_by_cat_pack.report_date              := p_repdate;
    v_itt_rcb_portf_by_cat_pack.start_date               := sysdate;
    v_itt_rcb_portf_by_cat_pack.create_user              := 1;
    -- Формирование отчета
    it_log.log('Формирование отчета');
    insert into itt_rcb_portf_by_cat_rec
      (id_rcb_portf_by_cat_rec
      ,id_rcb_portf_by_cat_pack
      ,t_dlcontrid
      ,t_dlcontr_name
      ,t_partyid
      ,t_party_code
      ,t_legalform
      ,t_shortname
      ,is_qual
      ,t_notresident
      ,t_okato
      ,t_fiid
      ,t_fiid_name
      ,t_isin
      ,t_rest --кол-во 
      ,t_facevaluefi -- ID валюты номинала
      ,t_facevaluefi_ccy -- валюта номинала
      ,t_facevalue --номинал         
      ,t_rate_fiid
      ,t_rate_ccy
      ,t_rate_isrelative
      ,t_rate
      ,t_rate_type
      ,t_rate_market_place
      ,t_rate_sincedate
      ,is_rate_sincedate90
      ,t_rate_definition_lst
      ,t_fi_kind
      ,t_avoirkind
      ,t_avrkind_root
      ,t_kind_name
      ,rate_abs
      ,summ
      ,summ_rur_cb
      ,t_rate_cb
      ,t_nkd_rate_isrelative
      ,t_nkd_rate
      ,t_nkd_rate_fiid
      ,t_nkd_rate_ccy
      ,nkd_summ
      ,nkd_summ_rur_cb
      ,t_nkd_rate_cb
      ,t_requirement_rest --баланс требований и обязательств (корректировка к кол-ву) 
      ,requirement_summ
      ,requirement_summ_rur_cb
      ,foreign_priz
      ,etf_priz)
      with contract as
       ( --Договоры обслуживания
        --175456
        select c.t_id              as t_sfcontr_id
               ,c.t_number          as t_sfcontr_number
               ,dc_root.t_dlcontrid
               ,c_root.t_id         as t_sfcontr_root_id
               ,c_root.t_number     as t_sfcontr_root_number
               ,c.t_partyid
          from dsfcontr_dbt c --Договор обслуживания (ДО)
          left join ddlcontrmp_dbt c2dc
            on c2dc.t_sfcontrid = c.t_id --Площадки Договора Брокерского Обслуживания(ДБО)/Связь ДО с ДБО через площадку
          left join ddlcontr_dbt dc_root
            on dc_root.t_dlcontrid = c2dc.t_dlcontrid --ДБО
          left join dsfcontr_dbt c_root
            on c_root.t_id = dc_root.t_sfcontrid --родительский ДО, на который ссылается ДБО
         where c.t_servkind in (1 /*Фондовый дилинг*/, 15 /*Срочные контракты*/, 21 /*Валютный рынок*/) -- select * from dservkind_dbt        
           and p_repdate >= c.t_datebegin
           and (c.t_dateclose = to_date('01.01.0001', 'dd.mm.yyyy') or p_repdate < c.t_dateclose)
        --   and ad.t_owner = 153570
        --   and ad.t_owner = 152956
        --   and ad.t_account = '30601810699000134958'
        )
      /*, root_contract as (select c.t_dlcontrid
             , nvl(c.t_sfcontr_root_id, c.t_sfcontr_id) as t_sfcontr_root_id
             , nvl(c.t_sfcontr_root_number, t_sfcontr_number) as sfcontr_root_number
             , c.t_partyid
             , listagg(c.t_sfcontr_number,', ') within group (order by c.t_sfcontr_number) as sfcontr_number_list
          from contract c
         group by c.t_dlcontrid
                , nvl(c.t_sfcontr_root_id, c.t_sfcontr_id)
                , nvl(c.t_sfcontr_root_number, t_sfcontr_number)
                , c.t_partyid 
      )*/
      ,
      party_contracts as
       (select c.t_partyid from contract c group by c.t_partyid),
      client as
       ( --Клиенты (физики и юрики, резиденты и нерезиденты, квалы и неквалы)
        --204060
        select p.t_partyid
               ,j.t_code as t_party_code
               ,p.t_shortname
               ,case
                  when p.t_notresident = chr(0) then
                   'Резидент'
                  else
                   'Нерезидент'
                end t_notresident -- резидент или нерезидент 
               ,case
                  when p.t_legalform = 1 then
                   'ЮР'
                  else
                   'ФИЗ'
                end t_legalform -- физические или юридические лица   
               ,case
                  when l.t_partyid is not null then
                   'Квал'
                  else
                   'Неквал'
                end kval
               ,p.t_nrcountry --Код страны нерезидента
                -- , cl.t_department --Филиал, в котором ведется учет
               ,c.t_codenum3
               ,case
                  when p.t_notresident = chr(0) /*Резидент*/
                       and p.t_legalform = 1 /*ЮЛ*/
                   then
                   substr(coalesce(trim(chr(1) from ct.t_okato)
                                  ,trim(chr(1) from ad1.t_okato)
                                  ,trim(chr(1) from ad5.t_okato)
                                  ,trim(chr(1) from ad2.t_okato)
                                  ,trim(chr(1) from ad.t_okato))
                         ,1
                         ,2)
                  when p.t_notresident = chr(0) /*Резидент*/
                       and p.t_legalform = 2 /*ФИЗ*/
                   then
                   substr(coalesce(trim(chr(1) from ct.t_okato)
                                  ,trim(chr(1) from ad5.t_okato)
                                  ,trim(chr(1) from ad2.t_okato)
                                  ,trim(chr(1) from ad1.t_okato)
                                  ,trim(chr(1) from ad.t_okato)
                                  ,'00')
                         ,1
                         ,2)
                  when p.t_notresident = chr(88) /*Нерезидент*/
                       and p.t_legalform = 1 /*ЮЛ*/
                       and po34.t_partyid is not null then
                   '998'
                  when p.t_notresident = chr(88) /*Нерезидент*/
                       and p.t_legalform = 1 /*ЮЛ*/
                       and po52.t_partyid is not null then
                   '999'
                  when p.t_notresident = chr(88) /*Нерезидент*/ /*and p.t_legalform = 1 \*ЮЛ*\ */
                   then
                   to_char(coalesce(c.t_codenum3, ad.t_codenum3))
                  when p.t_notresident = chr(0) /*Резидент*/
                   then
                   '00'
                  when p.t_notresident = chr(88) /*Нерезидент*/
                   then
                   to_char(coalesce(c.t_codenum3, ad.t_codenum3))
                  when trim(chr(1) from ad.t_okato) is not null then
                   substr(ad.t_okato, 1, 2)
                end t_okato
          from dparty_dbt p
         inner join party_contracts c
            on c.t_partyid = p.t_partyid
        /*inner join dclient_dbt cl on cl.t_partyid = p.t_partyid --клиенты
        and cl.t_department = 1 --филил 0000 (все клиенты сидят в одном фиилале АО "РОССЕЛЬХОЗБАНК")
        and cl.t_branch=0 
        and cl.t_servicekind = 1 --клиент  */
          left join dobjcode_dbt j
            on j.t_objectid = p.t_partyid
           and j.t_codekind = 1
           and j.t_objecttype = 3
           and j.t_state = 0
          left join dscqinv_dbt l
            on p.t_partyid = l.t_partyid
           and nvl(l.t_state, 0) = 1 -- квалифицированные или неквалифицированные
          left join dcountry_dbt c
            on c.t_codelat3 = p.t_nrcountry
           and c.t_codelat3 is not null
           and c.t_codelat3 != chr(1)
          left join dadress_dbt ad1
            on ad1.t_partyid = p.t_partyid
           and ad1.t_type = 1 -- Юридический
          left join dadress_dbt ad2
            on ad2.t_partyid = p.t_partyid
           and ad2.t_type = 2 -- Фактический 
          left join dadress_dbt ad5
            on ad5.t_partyid = p.t_partyid
           and ad5.t_type = 5 -- Место регистрации
        --Код ОКАТО по категории субъекта
          left join (select /*+ full(d)*/
                      dd.t_nameobject t_okato
                     ,d.t_object
                       from dobjatcor_dbt d
                      inner join dobjattr_dbt dd
                         on d.t_groupid = dd.t_groupid
                        and d.t_attrid = dd.t_attrid
                        and d.t_objecttype = dd.t_objecttype
                      where d.t_objecttype = 3
                           -- and d.t_object = lpad('137591',10,'0') 
                        and d.t_groupid = 12 --справочник ОКАТО
                        and p_repdate between t_validfromdate and d.t_validtodate) ct
            on ct.t_object = lpad(p.t_partyid, 10, '0')
        --поиск ОКАТО любого типа адреса 
          left join (select d.t_partyid
                           ,d.t_okato
                           ,d.t_coderegion
                           ,d.t_country
                           ,substr(d.t_okato, 1, 2) as t_okato2
                           ,cc.t_codenum3
                           ,row_number() over(partition by d.t_partyid order by null) rn
                       from dadress_dbt d
                       left join dcountry_dbt cc
                         on cc.t_codelat3 = d.t_country
                        and cc.t_codelat3 is not null
                        and cc.t_codelat3 != chr(1)
                      where d.t_okato != chr(1)) ad
            on ad.t_partyid = p.t_partyid
           and rn = 1
          left join dpartyown_dbt po34
            on po34.t_partyid = p.t_partyid
           and po34.t_partykind = 34 --Международной организацией
          left join dpartyown_dbt po52
            on po52.t_partyid = p.t_partyid
           and po52.t_partykind = 52 --Международной фин.организацией                  
         where 1 = 1
        --and p.t_partyid = 152956 
        ),
      acc as
       ( --Счета с Денежными средствами клиента по отобранным договорам
        --604401 (271611 по договорам)
        select /*+ materialize leading(c ad)*/
        distinct --т.к. одни счёт может быть связан с несколькими contract
                  a.t_accountid
                 ,a.t_account
                 ,a.t_code_currency
                 ,a.t_client
                 ,c.t_dlcontrid
                 ,c.t_sfcontr_root_id
                 ,c.t_sfcontr_root_number
          from contract c
        --В dmcaccdoc_dbt индекс составной из 3-х полей
          left join dmcaccdoc_dbt ad
            on ad.t_clientcontrid = c.t_sfcontr_id
           and ad.t_owner = c.t_partyid
           and ad.t_catid = 70 /*ДС клиента, Ц/Б*/
        --and ad.t_catid in (70/*ДС клиента, Ц/Б*/,364/*ЦБ Клиента, ВУ*/)
        /* --Добавляем фильтр по датам счетов--*/
        --из-за косяков, что счета открываются в СОФР через неделю после реального открытия, дата активации не корректна, при это дата открытия счёта проставлена старой верной датой
        /*and p_repdate >= ad.t_activatedate 
        and (ad.t_disablingdate = to_date('01.01.0001','dd.mm.yyyy') or p_repdate < ad.t_disablingdate)*/
        /*-------------------------------------*/
        --В daccount_dbt индекс составной из 3-х полей
          left join daccount_dbt a
            on a.t_chapter = ad.t_chapter
           and a.t_account = ad.t_account
           and a.t_code_currency = ad.t_currency
        --фильтр по дате не нужен, если счёта не было, то на нём в отчётную дату не будет остатка, а клиента нам нужно посчитать, если договор попал в выборку
        /*and p_repdate >= a.t_open_date
        and (a.t_close_date = to_date('01.01.0001','dd.mm.yyyy') or p_repdate < a.t_close_date)*/
         where 1 = 1
           and ad.t_iscommon = 'X' --and a.t_docid=0 and a.t_dockind=0
           and ad.t_isusable = 'X'
        --   and ad.t_owner = 153570
        --  and ad.t_owner = 152956
        --   and ad.t_account = '30601810699000134958'
        )
      --остатки на счетах на дату
      ,
      acc_rest1 as
       ( --
        select r.t_accountid
               ,r.t_restcurrency
               ,r.t_restdate
               ,nvl((lead(r.t_restdate) over(partition by r.t_accountid, r.t_restcurrency order by r.t_restdate)) - 1
                   ,to_date('31.12.9999', 'dd.mm.yyyy')) as dend
               ,abs(r.t_rest) as t_rest
               ,r.t_planrest
          from drestdate_dbt r),
      acc_rest as
       (select *
          from acc_rest1 r
         where p_repdate between r.t_restdate and r.dend --на дату
           and r.t_rest != 0),
      acc_asset as
       ( --Денежные средства на клиентах  
        select /*+ ordered leading(a) use_hash(a,f)*/
         a.t_account
        ,a.t_client as t_party
        ,a.t_dlcontrid
        ,a.t_sfcontr_root_id
        ,a.t_sfcontr_root_number
        ,f.t_fiid
        ,f.t_name
        ,null as t_isin
        ,nvl(r.t_rest, 0) as t_rest
        ,case
            when f.t_fi_kind = 1 /*Валюты*/
             then
             f.t_fiid
            else
             f.t_facevaluefi
          end as t_facevaluefi
        ,case
            when f.t_fi_kind = 1 /*Валюты*/
             then
             1
            else
             f.t_facevalue
          end as t_facevalue
        ,f.t_ccy
        ,fk.t_fi_kind
        ,fk.t_name as t_fi_kind_name
        ,av.t_avoirkind
        ,av.t_name as t_avoirkind_name
        ,av.t_root t_avrkind_root
        -- distinct substr(a.t_account,1,5) --30601, 30606
        -- count(1)
          from acc a
         inner join dfininstr_dbt f
            on f.t_fiid = a.t_code_currency
          left join dfikinds_dbt fk
            on fk.t_fi_kind = f.t_fi_kind
          left join davrkinds_dbt av
            on av.t_fi_kind = f.t_fi_kind
           and f.t_avoirkind = av.t_avoirkind
          left join acc_rest r
            on r.t_accountid = a.t_accountid
           and r.t_restcurrency = a.t_code_currency
         where 1 = 1),
      avoiriss_asset as
       ( --Ценные бумаги на клиентах
        select s.t_party --идентификатор клиента
                --, s.t_contract --идентификатор договора
               ,c.t_dlcontrid
               ,c.t_sfcontr_root_id
               ,c.t_sfcontr_root_number
               ,f.t_fiid --идентификатор ценной бумаги
               ,f.t_name --наименование ценной бумаги
               ,substr(a.t_isin, 1, 12) t_isin --ISIN ценной бумаги
               ,s.t_amount as t_rest --кол-во ценных бумаг на договоре клиента в рамках департамента(он всегда =1)
               ,s.t_department --Филиал, в котором ведется учет
               ,f.t_facevaluefi --валюта номинала
               ,f.t_facevalue --номинал
               ,f.t_ccy
               ,fk.t_fi_kind
               ,fk.t_name as t_fi_kind_name
               ,av.t_avoirkind
               ,av.t_name as t_avoirkind_name
               ,av.t_root t_avrkind_root
          from DPMWRTCL_DBT s --DPMWRTSUM_DBT
         inner join contract c
            on c.t_sfcontr_id = s.t_contract
         inner join dfininstr_dbt f
            on f.t_fiid = s.t_fiid
          left join dfikinds_dbt fk
            on fk.t_fi_kind = f.t_fi_kind
          left join davrkinds_dbt av
            on av.t_fi_kind = f.t_fi_kind
           and f.t_avoirkind = av.t_avoirkind
          left join davoiriss_dbt a
            on a.t_fiid = f.t_fiid
         where 1 = 1
              --   and s.t_party = 115762
              --and s.t_fiid in (23091,274,2102)
              --and p_repdate between s.t_begdate and s.t_enddate --ценные бумаги на дату
           and p_repdate between s.t_begdate and s.t_enddate --ценные бумаги на дату
        ),
      requirement1 as
       (select d.t_clientid
              ,c.t_dlcontrid
              ,c.t_sfcontr_root_id
              ,c.t_sfcontr_root_number
              ,r.t_fiid
              ,r.t_kind
              ,r.t_docid
              ,r.t_amount
              ,d.t_dealdate
              ,d.t_department
              ,r.t_plandate
              ,r.t_type
              ,r.t_kind
              ,r.t_subkind
              ,case
                 when r.t_kind = 0 then
                  1 --требование
                 when r.t_kind = 1 then
                  -1 --обязательство  
               end as sgn
              ,ko.t_name
          from ddlrq_dbt r --Требования и обязательства по операциям с ЦБ
         inner join ddl_tick_dbt d
            on d.t_dealid = r.t_docid --сделки 
         inner join contract c
            on c.t_sfcontr_id = d.t_clientcontrid --фильтр по отобранным договорам
         inner join doprkoper_dbt ko
            on ko.t_kind_operation = d.t_dealtype
         where 1 = 1
           and r.t_type in (2 /*DLRQ_TYPE_PAYMENT оплата*/, 8 /*DLRQ_TYPE_DELIVERY поставка*/, 6 /*DLRQ_TYPE_COMISS комиссия*/)
           and r.t_plandate > p_repdate
           and d.t_dealdate <= p_repdate
           and (d.t_closedate > p_repdate or d.t_closedate = to_date('01.01.0001', 'dd.mm.yyyy'))
           and r.t_dockind not in (4607, 4608)
        /*and r.t_plandate > to_date('20.02.2022','dd.mm.yyyy')
        and d.t_dealdate <= to_date('20.02.2022','dd.mm.yyyy')
        and (d.t_closedate > to_date('20.02.2022','dd.mm.yyyy') or d.t_closedate = to_date('01.01.0001','dd.mm.yyyy'))*/
        --and d.t_clientid = 152956 --Каленский Анатолий
        --order by r.t_docid
        ),
      requirement2 as
       (select r.t_clientid
              ,r.t_dlcontrid
              ,r.t_sfcontr_root_id
              ,r.t_sfcontr_root_number
              ,r.t_fiid
              ,sum(r.t_amount * r.sgn) as amount
          from requirement1 r
         group by r.t_clientid
                 ,r.t_dlcontrid
                 ,r.t_sfcontr_root_id
                 ,r.t_sfcontr_root_number
                 ,r.t_fiid),
      requirement as
       (select r.t_clientid
              ,r.t_dlcontrid
              ,r.t_sfcontr_root_id
              ,r.t_sfcontr_root_number
              ,r.t_fiid
              ,f.t_name
              ,a.t_isin
              ,r.amount
              ,case
                 when f.t_fi_kind = 1 /*Валюты*/
                  then
                  f.t_fiid
                 else
                  f.t_facevaluefi
               end as t_facevaluefi
              ,case
                 when f.t_fi_kind = 1 /*Валюты*/
                  then
                  1
                 else
                  f.t_facevalue
               end as t_facevalue
              ,f.t_ccy
              ,fk.t_fi_kind
              ,fk.t_name as t_fi_kind_name
              ,av.t_avoirkind
              ,av.t_name as t_avoirkind_name
              ,av.t_root t_avrkind_root
          from requirement2 r
         inner join dfininstr_dbt f
            on f.t_fiid = r.t_fiid
          left join dfikinds_dbt fk
            on fk.t_fi_kind = f.t_fi_kind
          left join davrkinds_dbt av
            on av.t_fi_kind = f.t_fi_kind
           and f.t_avoirkind = av.t_avoirkind
          left join davoiriss_dbt a
            on a.t_fiid = f.t_fiid),
      all_asset1 as
       (select t1.t_party
              ,t1.t_dlcontrid
              ,t1.t_sfcontr_root_id
              ,t1.t_sfcontr_root_number
              ,t1.t_fiid
              ,t1.t_name
              ,t1.t_isin
              ,t1.t_rest
              ,t1.t_facevaluefi
              ,t1.t_facevalue
              ,t1.t_ccy
              ,t1.t_fi_kind
              ,t1.t_fi_kind_name
              ,t1.t_avoirkind
              ,t1.t_avoirkind_name
              ,t1.t_avrkind_root
              ,1 as asset_type
          from avoiriss_asset t1 --остатки по ценным бумагам
        union all
        select t2.t_party --идентификатор клиента
              ,t2.t_dlcontrid
              ,t2.t_sfcontr_root_id
              ,t2.t_sfcontr_root_number
              ,t2.t_fiid --идентификатор ценной бумаги
              ,t2.t_name --наименование ценной бумаги
              ,null                     t_isin --ISIN ценной бумаги
              ,t2.t_rest --кол-во ценных бумаг на договоре клиента в рамках департамента(он всегда =1)
              ,t2.t_facevaluefi --валюта номинала
              ,t2.t_facevalue --номинал
              ,t2.t_ccy
              ,t2.t_fi_kind
              ,t2.t_fi_kind_name
              ,t2.t_avoirkind
              ,t2.t_avoirkind_name
              ,t2.t_avrkind_root
              ,2                        as asset_type
          from acc_asset t2 --денежные остатки на счетах                                       
        union all
        select t3.t_clientid
              ,t3.t_dlcontrid
              ,t3.t_sfcontr_root_id
              ,t3.t_sfcontr_root_number
              ,t3.t_fiid
              ,t3.t_name
              ,t3.t_isin
              ,t3.amount
              ,t3.t_facevaluefi
              ,t3.t_facevalue
              ,t3.t_ccy
              ,t3.t_fi_kind
              ,t3.t_fi_kind_name
              ,t3.t_avoirkind
              ,t3.t_avoirkind_name
              ,t3.t_avrkind_root
              ,3 as asset_type
          from requirement t3),
      all_asset as
       (select t1.t_party
              ,t1.t_dlcontrid
              ,t1.t_sfcontr_root_id
              ,t1.t_sfcontr_root_number
              ,t1.t_fiid
              ,t1.t_name
              ,t1.t_isin
              ,sum(case
                     when t1.asset_type in (1, 2) then
                      t1.t_rest
                     else
                      0
                   end) as t_rest --кол-во
              ,sum(case
                     when t1.asset_type = 3 then
                      t1.t_rest
                     else
                      0
                   end) as t_requirement_rest --баланс требований/обязательств (корректировка к кол-ву)
              ,t1.t_facevaluefi
              ,t1.t_facevalue
              ,t1.t_ccy
              ,t1.t_fi_kind
              ,t1.t_fi_kind_name
              ,t1.t_avoirkind
              ,t1.t_avoirkind_name
              ,t1.t_avrkind_root
          from all_asset1 t1
         group by t1.t_party
                 ,t1.t_dlcontrid
                 ,t1.t_sfcontr_root_id
                 ,t1.t_sfcontr_root_number
                 ,t1.t_fiid
                 ,t1.t_name
                 ,t1.t_isin
                 ,t1.t_facevaluefi
                 ,t1.t_facevalue
                 ,t1.t_ccy
                 ,t1.t_fi_kind
                 ,t1.t_fi_kind_name
                 ,t1.t_avoirkind
                 ,t1.t_avoirkind_name
                 ,t1.t_avrkind_root),
      rate1 as
       ( --котировка по ценной бумаге
        select r.t_rateid
               ,r.t_fiid
               ,r.t_otherfi
               ,r.t_type
               ,r.t_isrelative --jzotov
               ,r.t_market_place
               ,r.t_rate / power(10, r.t_point) as t_rate
               ,r.t_sincedate
               ,r.t_name
          from dratedef_dbt r --котировка по ценной бумаге
        union all
        select rh.t_rateid
               ,null as t_fiid
               ,null as t_otherfi
               ,null as t_type
               ,null as t_isrelative --jzotov
               ,null as t_market_place
               ,rh.t_rate / power(10, rh.t_point) as t_rate
               ,rh.t_sincedate
               ,null as t_name
          from dratehist_dbt rh --старое значение котировки по ценном бумаге
        ),
      rate2 as
       ( --котировка ценной бумаги с историчностью
        select distinct r.t_rateid
                        ,max(r.t_fiid) over(partition by r.t_rateid) as t_fiid
                        ,max(r.t_otherfi) over(partition by r.t_rateid) as t_otherfi
                        ,max(r.t_type) over(partition by r.t_rateid) as t_type
                        ,max(r.t_isrelative) over(partition by r.t_rateid) as t_isrelative --jzotov
                        ,max(r.t_market_place) over(partition by r.t_rateid) as t_market_place
                        ,max(r.t_name) over(partition by r.t_rateid) as t_name
                        ,r.t_rate
                        ,r.t_sincedate
                        ,nvl((lead(t_sincedate) over(partition by r.t_rateid order by r.t_sincedate)) - 1, to_date('31.12.9999', 'dd.mm.yyyy')) as dend
          from rate1 r
         where 1 = 1
        -- Типы курсов (t_typr): select * from dratetype_dbt
        ),
      rate as
       ( --котировка ценной бумаги на дату
        select *
          from (select r.t_fiid
                        ,r.t_otherfi
                        ,r.t_type
                        ,r.t_isrelative --jzotov
                        ,r.t_market_place
                        ,r.t_rate
                        ,r.t_sincedate
                        ,f.t_ccy
                        ,r.t_name
                         --Защита от дублирования курса. Приоритет=t_fiid: 0-рубли, 7-доллары, 8-евро.
                        ,row_number() over(partition by r.t_otherfi, r.t_type, r.t_market_place order by r.t_fiid) as rn
                    from rate2 r
                    join dfininstr_dbt f
                      on f.t_fiid = r.t_fiid
                   where --p_repdate between r.t_sincedate and r.dend
                   p_repdate between r.t_sincedate and r.dend
                  and ((r.t_type=1001/*Мотивированное суждение*/ and r.t_market_place=0)
                    or (r.t_type=1/*Рыночная цена*/ and r.t_market_place=2 /*Мосбиржа*/)
                    or (r.t_type=21/*Рыночная цена для НДФЛ из СПБ*/ and r.t_market_place=151337 /*СПБ*/)
                    or (r.t_type=1002/*Цена Bloomberg для ф.707*/ and r.t_market_place=0)                  
                    or (r.t_type=23/*Цена закрытия Bloomberg*/ and r.t_market_place=0)
                    or (r.t_type=7/*ЦБ РФ*/ and r.t_market_place=0)
                    or (r.t_type=15/*НКД на одну ц/б*/ and r.t_market_place=0)))
         where rn = 1),
      rate_from_any as
       (select rr.*
          from (select rt.t_otherfi
                      ,case
                         when av.t_root = 17 then
                          chr(88)
                         else
                          chr(0)
                       end as t_isrelative
                      ,rt.t_rate
                      ,rt.t_sincedate
                      ,rt.t_fiid
                      ,rt.t_type as t_type /*Цена Bloomberg для ф.707*/
                      ,rt.t_market_place
                       --, max(rt.t_sincedate)over(partition by rt.t_otherfi) max_date
                      ,f.t_ccy
                      ,row_number() over(partition by rt.t_otherfi order by rt.t_sincedate desc) rn
                  from itt_dratedef_dbt rt
                  join dfininstr_dbt f
                    on f.t_fiid = rt.t_fiid
                  join dfininstr_dbt ff
                    on ff.t_fiid = rt.t_otherfi
                  join dfikinds_dbt fk
                    on fk.t_fi_kind = ff.t_fi_kind
                  join davrkinds_dbt av
                    on av.t_fi_kind = ff.t_fi_kind
                   and ff.t_avoirkind = av.t_avoirkind
                 where rt.t_sincedate <= p_repdate) rr
         where rn = 1),
      portfolio as
       (select /*+ use_hash(r1001,rm,rs,rb,r23,rnkd,rst)*/ s.t_dlcontrid
              ,s.t_sfcontr_root_number
              ,c.t_partyid
              ,c.t_legalform
              ,c.t_shortname
              ,c.kval is_qual
              ,c.t_notresident --рез/нерез
              ,t_okato
              ,s.t_fiid ----идентификатор ценной бумаги
              ,s.t_name as fiid_name ----наименование ценной бумаги
              ,s.t_isin --ISIN ценной бумаги
              ,s.t_rest --t_rest кол-во
              ,s.t_requirement_rest --баланс требований/обязательств (корректировка к кол-ву)
              ,s.t_facevaluefi --валюта номинала 
              ,s.t_facevalue --номинал
              ,case
                 when s.t_facevaluefi != 0 /*валюта*/
                  then
                  coalesce(r1001.t_fiid, rs.t_fiid, rm.t_fiid, rb.t_fiid, rst.t_fiid, r23.t_fiid)
                 when s.t_facevaluefi = 0 /*рубли*/
                  then
                  coalesce(r1001.t_fiid, rm.t_fiid, rs.t_fiid, rb.t_fiid, rst.t_fiid, r23.t_fiid)
               end t_rate_fiid -- валюта приоритетного курса
              ,case
                 when s.t_facevaluefi != 0 /*валюта*/
                  then
                  coalesce(r1001.t_ccy, rs.t_ccy, rm.t_ccy, rb.t_ccy, rst.t_ccy, r23.t_ccy)
                 when s.t_facevaluefi = 0 /*рубли*/
                  then
                  coalesce(r1001.t_ccy, rm.t_ccy, rs.t_ccy, rb.t_ccy, rst.t_ccy, r23.t_ccy)
               end t_rate_ccy --  код валюты  приоритетного курса
              ,case
                 when s.t_facevaluefi != 0 /*валюта*/
                  then
                  coalesce(nvl2(r1001.t_rate, r1001.t_isrelative, null)
                          ,nvl2(rs.t_rate, rs.t_isrelative, null)
                          ,nvl2(rm.t_rate, rm.t_isrelative, null)
                          ,nvl2(rb.t_rate, rb.t_isrelative, null)
                          ,nvl2(rst.t_rate, rst.t_isrelative, null)
                          ,nvl2(r23.t_rate, r23.t_isrelative, null))
                 when s.t_facevaluefi = 0 /*рубли*/
                  then
                  coalesce(nvl2(r1001.t_rate, r1001.t_isrelative, null)
                          ,nvl2(rm.t_rate, rm.t_isrelative, null)
                          ,nvl2(rs.t_rate, rs.t_isrelative, null)
                          ,nvl2(rb.t_rate, rb.t_isrelative, null)
                          ,nvl2(rst.t_rate, rst.t_isrelative, null)
                          ,nvl2(r23.t_rate, r23.t_isrelative, null))
               end t_rate_isrelative -- признак относитльности приоритетного курса (курс в процентах от номинала) 
              ,case
                 when s.t_facevaluefi != 0 /*валюта*/
                  then
                  coalesce(r1001.t_rate, rs.t_rate, rm.t_rate, rb.t_rate, rst.t_rate, r23.t_rate)
                 when s.t_facevaluefi = 0 /*рубли*/
                  then
                  coalesce(r1001.t_rate, rm.t_rate, rs.t_rate, rb.t_rate, rst.t_rate, r23.t_rate)
               end t_rate -- приоритетный курс 
              ,case
                 when s.t_facevaluefi != 0 /*валюта*/
                  then
                  coalesce(r1001.t_type, rs.t_type, rm.t_type, rb.t_type, rst.t_type, r23.t_type)
                 when s.t_facevaluefi = 0 /*рубли*/
                  then
                  coalesce(r1001.t_type, rm.t_type, rs.t_type, rb.t_type, rst.t_type, r23.t_type)
               end t_rate_type -- приоритетный тип цены /*Рыночная цена / Цена Bloomberg для ф.707*/   
              ,case
                 when s.t_facevaluefi != 0 /*валюта*/
                  then
                  coalesce(r1001.t_market_place, rs.t_market_place, rm.t_market_place, rb.t_market_place, r23.t_market_place, 0)
                 when s.t_facevaluefi = 0 /*рубли*/
                  then
                  coalesce(r1001.t_market_place, rm.t_market_place, rs.t_market_place, rb.t_market_place, r23.t_market_place, 0)
               end t_rate_market_place -- приоритетный MP - market place /*СПБ / Мосбиржа/ Bloomberg*/
              ,case
                 when s.t_facevaluefi != 0 /*валюта*/
                  then
                  coalesce(r1001.t_sincedate, rs.t_sincedate, rm.t_sincedate, rb.t_sincedate, rst.t_sincedate, r23.t_sincedate)
                 when s.t_facevaluefi = 0 /*рубли*/
                  then
                  coalesce(r1001.t_sincedate, rm.t_sincedate, rs.t_sincedate, rb.t_sincedate, rst.t_sincedate, r23.t_sincedate)
               end t_rate_sincedate -- приоритетный дата котировки 
              ,rm.t_name || ' ' || rm.t_rate || ', ' || rs.t_name || ' ' || rs.t_rate || ', ' || rb.t_name || ' ' || rb.t_rate as t_rate_definition_lst
              ,s.t_fi_kind
              ,s.t_fi_kind_name
              ,s.t_avoirkind
              ,s.t_avoirkind_name
              ,s.t_avrkind_root
              ,rnkd.t_isrelative as t_nkd_rate_isrelative
              ,rnkd.t_rate as t_nkd_rate
              ,rnkd.t_fiid as t_nkd_rate_fiid
              ,rnkd.t_ccy as t_nkd_rate_ccy
              ,c.t_party_code
        --select c.*, s.*, rm.*, rs.*, rb.* 
          from client c
         inner join all_asset s
            on s.t_party = c.t_partyid
          left join rate r1001
            on r1001.t_otherfi = s.t_fiid
           and r1001.t_type = 1001 /*Мотивированное суждение*/
           and r1001.t_market_place = 0
        --and r1001.rn = 1
          left join rate rm
            on rm.t_otherfi = s.t_fiid
           and rm.t_type = 1 /*Рыночная цена*/
           and rm.t_market_place = 2 /*Мосбиржа*/
        -- and rm.rn = 1
          left join rate rs 
            on rs.t_otherfi = s.t_fiid
           and rs.t_type = 21 /*Рыночная цена для НДФЛ из СПБ*/
           and rs.t_market_place = 151337 /*СПБ*/
        --and rs.rn = 1
          left join rate rb
            on rb.t_otherfi = s.t_fiid
           and rb.t_type = 1002 /*Цена Bloomberg для ф.707*/
           and rb.t_market_place = 0
        --and rb.rn = 1
          left join rate_from_any rst
            on rst.t_otherfi = s.t_fiid -- and rs.t_fiid =  s.t_facevaluefi  
          left join rate r23
            on r23.t_otherfi = s.t_fiid
           and r23.t_type = 23 /*Цена закрытия Bloomberg7*/
           and r23.t_market_place = 0
        -- and r23.rn = 1
          left join rate rnkd
            on rnkd.t_otherfi = s.t_fiid
           and rnkd.t_type = 15 /*НКД на одну ц/б*/
           and rnkd.t_market_place = 0
        --and rnkd.rn = 1
        )
      select /*+ use_hash(rcb,nkd_rcb)*/ its_log.nextval
            ,v_id_rep_pack
            ,p.t_dlcontrid
            ,p.t_sfcontr_root_number
            ,p.t_partyid
            ,p.t_party_code
            ,p.t_legalform
            ,p.t_shortname
            ,p.is_qual
            ,p.t_notresident --рез/нерез
            ,p.t_okato
            ,p.t_fiid ----идентификатор ценной бумаги
            ,p.fiid_name ----наименование ценной бумаги
            ,p.t_isin --ISIN ценной бумаги
            ,p.t_rest --остаток 
            ,p.t_facevaluefi --ID валюты номинала   
            ,ffv.t_ccy t_facevaluefi_ccy --валюта номинала
            ,p.t_facevalue --номинал  
            ,p.t_rate_fiid -- приоритетный фин инструмент
            ,p.t_rate_ccy
            ,p.t_rate_isrelative
            ,p.t_rate
            ,p.t_rate_type
            ,p.t_rate_market_place
            ,p.t_rate_sincedate
            ,case
               when p.t_rate_sincedate < p_repdate - 90 then
                1
             end is_rate_sincedate90
            ,p.t_rate_definition_lst
            ,p.t_fi_kind
            ,p.t_avoirkind
            ,p.t_avrkind_root
            ,nvl(p.t_avoirkind_name, p.t_fi_kind_name) as t_kind_name
             /*--------стоимость ценной бумаги-------------*/
             --------------
            ,case
             --when p.t_avrkind_root = 17 /*все виды облигаций*/ then p.t_rest * p.t_facevalue / 100
               when p.t_rate_isrelative = chr(88)
                    and p.t_rate_type != 1001 /*относительный курс*/
                then
                p.t_facevalue / 100
               else
                1
             end * case
               when p.t_fi_kind = 1 then
                1 --для валюты
               else
                p.t_rate
             end as rate_abs --абсолютный курс
             --------------  
            ,case
             --when p.t_avrkind_root = 17 /*все виды облигаций*/ then p.t_rest * p.t_facevalue / 100
               when p.t_rate_isrelative = chr(88)
                    and p.t_rate_type != 1001 /*относительный курс*/
                then
                p.t_rest * p.t_facevalue / 100
               else
                p.t_rest
             end * case
               when p.t_fi_kind = 1 then
                1 --для валюты
               else
                p.t_rate
             end as summ
             --------------  
            ,case
             --when p.t_avrkind_root = 17 /*все виды облигаций*/ then p.t_rest * p.t_facevalue / 100
               when p.t_rate_isrelative = chr(88)
                    and p.t_rate_type != 1001 /*относительный курс*/
                then
                p.t_rest * p.t_facevalue / 100
               else
                p.t_rest
             end * case
               when p.t_fi_kind = 1 then
                1 --для валюты
               else
                p.t_rate
             end * case
               when p.t_rate_fiid = 0 then
                1 --если актив котируется в рублях
               when p.t_fi_kind = 1
                    and p.t_fiid = 0 then
                1 --если это собственно рубли    
               else
                rcb.t_rate
             end as summ_rur_cb
             --------------    
            ,case
               when p.t_rate_fiid = 0 then
                1 --если актив котируется в рублях
               when p.t_fi_kind = 1
                    and p.t_fiid = 0 then
                1 --если это собственно рубли    
               else
                rcb.t_rate
             end as t_rate_cb
             --------------    
             /*--------------------------------------------*/
             /*----НКД для ценнной бумаги-------*/
            ,p.t_nkd_rate_isrelative
            ,p.t_nkd_rate
            ,p.t_nkd_rate_fiid
            ,p.t_nkd_rate_ccy
             --------------
            ,case
               when p.t_nkd_rate_isrelative = chr(88)
                    and p.t_rate_type != 1001 /*относительный курс*/
                then
                p.t_rest * p.t_facevalue / 100
               else
                (p.t_rest + p.t_requirement_rest)
             end * p.t_nkd_rate as nkd_summ
             --------------  
            ,case
               when p.t_nkd_rate_isrelative = chr(88)
                    and p.t_rate_type != 1001 /*относительный курс*/
                then
                p.t_rest * p.t_facevalue / 100
               else
                (p.t_rest + p.t_requirement_rest)
             end * p.t_nkd_rate * case
               when p.t_nkd_rate_fiid = 0 then
                1 --если НКД котируется в рублях
               else
                nkd_rcb.t_rate
             end as nkd_summ_rur_cb
             --------------    
            ,case
               when p.t_nkd_rate_fiid = 0 then
                1 --если НКД котируется в рублях
               else
                nkd_rcb.t_rate
             end as t_nkd_rate_cb --    
             --------------
             /*---------------------------------*/
             /*-----Требования и обязательства для одной ценной бумаги-----*/
            ,p.t_requirement_rest --баланс требований/обязательств (корректировка к кол-ву)
             --------------
            ,case
             --when p.t_avrkind_root = 17 /*все виды облигаций*/ then p.t_requirement_rest * p.t_facevalue / 100
               when p.t_rate_isrelative = chr(88)
                    and p.t_rate_type != 1001 /*относительный курс*/
                then
                p.t_requirement_rest * p.t_facevalue / 100
               else
                p.t_requirement_rest
             end * case
               when p.t_fi_kind = 1 then
                1 --для валюты
               else
                p.t_rate
             end as requirement_summ
             --------------  
            ,case
             --             when p.t_avrkind_root = 17 /*все виды облигаций*/ then p.t_rest * p.t_facevalue / 100
               when p.t_rate_isrelative = chr(88)
                    and p.t_rate_type != 1001 /*относительный курс*/
                then
                p.t_requirement_rest * p.t_facevalue / 100
               else
                p.t_requirement_rest
             end * case
               when p.t_fi_kind = 1 then
                1 --для валюты
               else
                p.t_rate
             end * case
               when p.t_rate_fiid = 0 then
                1 --если актив котируется в рублях
               when p.t_fi_kind = 1
                    and p.t_fiid = 0 then
                1 --если это собственно рубли    
               else
                rcb.t_rate
             end as requirement_summ_rur_cb
             --------------    
             --признак иностр компании
            ,case
               when it_rcb_portf_by_cat.get_priz_notrezident(p.t_isin) = chr(88) /*Нерезидент*/ --issr not null                             
                    or p.t_isin is not null
                    and substr(p.t_isin, 1, 2) != 'RU'
                    and issr.t_partyid is not null
                    or issr.t_partyid is null
                    and substr(p.t_isin, 1, 2) != 'RU' then
                1
               else
                0
             end foreign_priz
             --признак ПАИ-ETF
            ,case
               when p.t_avrkind_root in (16) /*паи- etf*/
                    and ru.isincode is not null then
                1
               else
                0
             end as etf_priz
      /*------------------------------------------------------------*/
        from portfolio p
        left join rate rcb
          on (rcb.t_otherfi = p.t_rate_fiid and t_fi_kind != 1 /*для невалют*/
             or rcb.t_otherfi = p.t_fiid and t_fi_kind = 1 /*для валют*/
             )
            --rcb.t_otherfi=p.t_rate_fiid    
         and rcb.t_fiid = 0
         and rcb.t_type = 7 /*ЦБ РФ*/
         and rcb.t_market_place = 0 /*Const для t_type=7*/
      -- and rcb.rn = 1
        left join rate nkd_rcb
          on nkd_rcb.t_otherfi = p.t_nkd_rate_fiid
         and nkd_rcb.t_fiid = 0
         and nkd_rcb.t_type = 7 /*ЦБ РФ*/
         and nkd_rcb.t_market_place = 0 /*Const для t_type=7*/
      -- and nkd_rcb.rn = 1
        left join dfininstr_dbt f
          on f.t_fiid = p.t_fiid --ссылка на эмитента
        left join dfininstr_dbt ffv
          on ffv.t_fiid = p.t_facevaluefi --ссылка на Валюта номинала
        left join dparty_dbt issr
          on issr.t_partyid = f.t_issuer --эмитент
        left join sofr_info_fintoolreferencedata ru
          on ru.isincode = p.t_isin
         and ru.securitytype = 'ETF'
       where 1 = 1
      --  and p.t_partyid = 152956 --Каленский Анатолий
      -- and p.t_fi_kind_name like '%блигаци%'
      --and p.t_fiid = 1454
      -- and p.t_partyid = 148590
      ;
    v_cnt := sql%rowcount;
    it_log.log('insert into itt_rcb_portf_by_cat end  sql%rowcount = ' || v_cnt);
    if v_cnt = 0
    then
      raise_application_error(-20001
                             ,'Данные для отчета за дату ' || to_char(p_repdate, 'dd.mm.yyyy') || ' отсутствуют.');
    end if;
    v_itt_rcb_portf_by_cat_pack.end_date := sysdate;
    insert into itt_rcb_portf_by_cat_pack values v_itt_rcb_portf_by_cat_pack;
    it_log.log('END make_process');
    return v_id_rep_pack;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  function make_report_cb_portf_by_cat(p_id_rcb_portf_by_cat_pack in number) return number is
    v_id_file     number;
    v_file_dir    varchar2(4000);
    v_create_user varchar2(250) := 1;
    v_file_name   varchar2(1000);
    i             number;
    v_clob        clob;
  begin
    it_log.log('START make_report_cb_portf_by_cat ');
    v_file_name   := '6_ЭП_portf_' || to_char(s_rep_date, 'YYYYMMDD') || '_' || to_char(sysdate, 'hh24miss') || '.csv';
    v_create_user := 1;
    dbms_lob.createtemporary(lob_loc => v_clob, cache => true);
    /*dbms_lob.append(v_clob,'Признаки групп; '||chr(13)||chr(10)); 
    dbms_lob.append(v_clob,'№;'                                      || 
                           'Клиент: физическое или юридическое лицо;'||
                           'Клиент: квал или неквал инвестор;'       ||         
                           'Клиент: резидент или нерезидент;'        ||
                           'Размер счета клиента, руб;'              ||
                           'Код региона (страны);'                   ||
                           'Объем портфеля, руб;'                    ||
                           'Количество клиентов;'                    ||chr(13)||chr(10));
                                                    
    dbms_lob.append(v_clob,'1'  ||';'||
                           '2'  ||';'||
                           '3'  ||';'||
                           '4'  ||';'||
                           '5'  ||';'||
                           '6'  ||';'||
                           '7'  ||';'||
                           '8'  ||';'||chr(13)||chr(10)
                      ); 
    dbms_lob.append(v_clob,'ДУ/Брокер'  ||';'||
                           'ДУ/Брокер'  ||';'||
                           'ДУ/Брокер'  ||';'||
                           'ДУ/Брокер'  ||';'||
                           'ДУ/Брокер'  ||';'||
                           'ДУ/Брокер'  ||';'||
                           'ДУ/Брокер'  ||';'||
                           'ДУ/Брокер'  ||';'||chr(13)||chr(10)
                      );                  
    dbms_lob.append(v_clob,''  ||';'||
                           'Текстовое: ФИЗ, ЮР'       ||';'||
                           'Текстовое: КВАЛ, НЕКВАЛ'  ||';'||
                           'Текстовое: РЕЗ, НЕРЕЗ'    ||';'||
                           'Числовое'                 ||';'||
                           'Числовое'                 ||';'||
                           'Числовое'                 ||';'||
                           'Числовое'                 ||';'||chr(13)||chr(10)
                      ); */
    i := 0;
    for cCur in (with rec1 as
                    (select t_partyid
                          ,t_legalform
                          ,upper(is_qual) is_qual
                          ,case
                             when upper(t_notresident) = 'РЕЗИДЕНТ' then
                              'РЕЗ'
                             else
                              'НЕРЕЗ'
                           end t_notresident
                          ,t_okato
                          ,sum( /*ЦБ и остатки денежных средств на счетах*/ nvl(summ_rur_cb, 0) +
                               /*НКД*/
                                case
                                  when r.t_avrkind_root = 17 /*все виды облигаций*/
                                   then
                                   nvl(r.nkd_summ_rur_cb, 0)
                                  else
                                   0
                                end +
                               /*Баланс требований и обязательств*/
                                nvl(r.requirement_summ_rur_cb, 0)) t_summ_rur_cb
                      from itt_rcb_portf_by_cat_rec r
                     where r.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack
                     group by t_partyid
                             ,t_legalform
                             ,upper(is_qual)
                             ,case
                                when upper(t_notresident) = 'РЕЗИДЕНТ' then
                                 'РЕЗ'
                                else
                                 'НЕРЕЗ'
                              end
                             ,t_okato),
                   rec as
                    (select r.t_legalform
                          ,r.is_qual
                          ,r.t_notresident
                          ,case
                             when round(r.t_summ_rur_cb) < 1 then
                              0
                             when round(r.t_summ_rur_cb) between 1 and 9999 then
                              1
                             when round(r.t_summ_rur_cb) between 10000 and 99999 then
                              10000
                             when round(r.t_summ_rur_cb) between 100000 and 999999 then
                              100000
                             when round(r.t_summ_rur_cb) between 1000000 and 5999999 then
                              1000000
                             when round(r.t_summ_rur_cb) between 6000000 and 9999999 then
                              6000000
                             when round(r.t_summ_rur_cb) between 10000000 and 99999999 then
                              10000000
                             when round(r.t_summ_rur_cb) between 100000000 and 499999999 then
                              100000000
                             when round(r.t_summ_rur_cb) between 500000000 and 999999999 then
                              500000000
                             when round(r.t_summ_rur_cb) >= 1000000000 then
                              1000000000
                           end portfolio_range
                          ,r.t_okato
                          ,round(sum(r.t_summ_rur_cb)) t_summ_rur_cb
                          ,count(1) cnt
                      from rec1 r
                     group by r.t_legalform
                             ,is_qual
                             ,r.t_notresident
                             ,case
                                when round(r.t_summ_rur_cb) < 1 then
                                 0
                                when round(r.t_summ_rur_cb) between 1 and 9999 then
                                 1
                                when round(r.t_summ_rur_cb) between 10000 and 99999 then
                                 10000
                                when round(r.t_summ_rur_cb) between 100000 and 999999 then
                                 100000
                                when round(r.t_summ_rur_cb) between 1000000 and 5999999 then
                                 1000000
                                when round(r.t_summ_rur_cb) between 6000000 and 9999999 then
                                 6000000
                                when round(r.t_summ_rur_cb) between 10000000 and 99999999 then
                                 10000000
                                when round(r.t_summ_rur_cb) between 100000000 and 499999999 then
                                 100000000
                                when round(r.t_summ_rur_cb) between 500000000 and 999999999 then
                                 500000000
                                when round(r.t_summ_rur_cb) >= 1000000000 then
                                 1000000000
                              end
                             ,r.t_okato)
                   select r.t_legalform
                         ,r.is_qual
                         ,r.t_notresident
                         ,r.portfolio_range
                         ,case
                            when portfolio_range = 0 then
                             'пустые счета'
                            when portfolio_range = 1 then
                             'от 0 до 10 тыс.'
                            when portfolio_range = 10000 then
                             'от 10 тыс. до 100 тыс.'
                            when portfolio_range = 100000 then
                             'от 100 тыс. до 1 млн'
                            when portfolio_range = 1000000 then
                             'от 1 млн до 6 млн'
                            when portfolio_range = 6000000 then
                             'от 6 млн до 10 млн'
                            when portfolio_range = 10000000 then
                             'от 10 млн до 100 млн'
                            when portfolio_range = 100000000 then
                             'от 100 млн до 500 млн'
                            when portfolio_range = 500000000 then
                             'от 500 млн до 1 млрд'
                            when portfolio_range = 1000000000 then
                             '1 млрд +'
                          end as portfolio_range_str
                         ,r.t_okato
                         ,r.t_summ_rur_cb
                         ,r.cnt
                     from rec r
                    order by --Код региона (страны) Размер счета клиента, руб Клиент: резидент или нерезидент Клиент: квал или неквал инвестор  Клиент: физическое или юридическое лицо  
                             r.t_okato desc
                            ,r.portfolio_range
                            ,r.t_notresident
                            ,r.is_qual
                            ,r.t_legalform)
    loop
      i := i + 1;
      dbms_lob.append(v_clob
                     ,it_rsl_string.GetCell(i) --Уникальный номер строки
                      || it_rsl_string.GetCell(cCur.t_Legalform) --Указывается признак группы клиентов: физические или юридические лица
                      || it_rsl_string.GetCell(cCur.Is_Qual) --квалифицированные или неквалифицированные инвесторы
                      || it_rsl_string.GetCell(cCur.t_Notresident) --резидент или нерезидент (по валютному законодательству)
                      || it_rsl_string.GetCell(cCur.portfolio_range_str) --Заполняется одним из сл значений совокупного объема портфеля активов клиента: 
                      || it_rsl_string.GetCell(cCur.t_Okato) --Код региона (страны)
                      || it_rsl_string.GetCell(cCur.t_summ_rur_cb) --объем портфеля по клиенту
                      || it_rsl_string.GetCell(cCur.Cnt, true) -- Количество клиентов по договору на брокерское обслуживание или договору доверительного управления. 
                      );
    end loop;
    if dbms_lob.getlength(v_clob) > 0
    then
      dbms_lob.trim(v_clob, dbms_lob.getlength(v_clob) - 2); -- Удаляем последние chr(13)||chr(10))
      it_log.log('it_file.insert_file');
      v_id_file := it_file.insert_file(p_file_dir => v_file_dir
                                      ,p_file_name => v_file_name
                                      ,p_file_clob => v_clob
                                      ,p_from_system => it_file.C_SOFR_DB
                                      ,p_from_module => $$plsql_unit
                                      ,p_to_system => it_file.C_SOFR_RSBANK
                                      ,p_to_module => null
                                      ,p_create_user => v_create_user);
      it_log.log('v_id_file=' || v_id_file);
      it_log.log(p_msg => 'v_clob by_cat', p_msg_clob => v_clob);
      update itt_rcb_portf_by_cat_pack p set p.id_file_cb_portf_by_cat = v_id_file where p.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack;
    else
      raise_application_error(-20003, 'Данных для отчета нет');
    end if;
    it_log.log('END make_report_cb_portf_by_cat ');
    return v_id_file;
  exception
    when others then
      /*if dbms_lob.isopen(lob_loc => v_clob) = 1
      then
        dbms_lob.close(lob_loc => v_clob);
      end if;*/
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  function make_report_cover(p_id_rcb_portf_by_cat_pack in number) return number is
    v_id_file     number;
    v_file_dir    varchar2(4000);
    v_create_user varchar2(250) := 1;
    v_file_name   varchar2(1000);
    v_clob        clob;
  begin
    v_create_user := 1;
    v_file_name   := '6_ЭП_cover_' || to_char(s_rep_date, 'YYYYMMDD') || '_' || to_char(sysdate, 'hh24miss') || '.csv';
    dbms_lob.createtemporary(lob_loc => v_clob, cache => true);
    --Блок статистики--   
    /*
    ??  всего бумаг во всех портфелях в штуках
    ??  всего клиентов в штуках
    ??  бумаг у которых нашлись котировки в штуках
    ??  бумаги у которых нет котировок в штуках
    ??  бумаг у которых котировка старше -90 дней от даты отчета
    ??  валюты у которых нет валютной котировки
    */
    dbms_lob.append(v_clob, 'Блок статистики' || chr(13) || chr(10));
    for c1 in (with t as
                  (select rr.t_rate_ccy
                    from itt_rcb_portf_by_cat_rec rr
                   where 1 = 1
                     and rr.t_rate_ccy is not null
                     and rr.t_rate is null
                   group by rr.t_rate_ccy)
                 select sum(r.t_rest) as t_rest_sum --всего бумаг во всех портфелях в штуках
                       ,count(distinct r.t_partyid) as t_partyid_count --всего клиентов в штуках 
                       ,count(r.t_rate) as t_rate_count --бумаг у которых нашлись котировки в штуках             
                       ,sum(case
                              when r.t_rate is null then
                               1
                              else
                               0
                            end) as t_rate_is_null_count --бумаги у которых нет котировок в штуках
                       ,sum(case
                              when r.is_rate_sincedate90 is not null then
                               1
                              else
                               0
                            end) as t_rate_more90 --бумаг у которых котировка старше -90 дней от даты отчета                 
                       ,(select rtrim(xmlelement("XML", xmlagg(xmlelement("RATE", t_rate_ccy || ', '))).extract('/XML//RATE/text()').getstringval()
                                      ,', ')
                           from t) t_rate_list
                   from itt_rcb_portf_by_cat_rec r
                  where r.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack
                  group by t_rate_ccy
                          ,t_rate)
    loop
      dbms_lob.append(v_clob
                     ,c1.t_rest_sum || ';' || c1.t_partyid_count || ';' || c1.t_rate_count || ';' || c1.t_rate_is_null_count || ';' ||
                      c1.t_rate_more90 || ';' || c1.t_rate_list || ';' || chr(13) || chr(10));
    end loop;
    --Блок котировки--
    /*
    ISIN  ISIN
          ключ в справочнике СОФРА 
          тикер (если нет ISINа)
          котировка найденная СОФРом
          колонка для проставления котировки сотрудником ОД    
    */
    dbms_lob.append(v_clob, chr(13) || chr(10) || 'Блок котировки' || chr(13) || chr(10));
    for c2 in (select r.t_isin
                     ,r.t_fiid_name
                     ,r.t_fiid
                     ,r.t_rate
                 from itt_rcb_portf_by_cat_rec r
                where r.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack)
    loop
      dbms_lob.append(v_clob, c2.t_isin || ';' || c2.t_fiid_name || ';' || c2.t_fiid || ';' || c2.t_rate || ';');
    end loop;
    v_id_file := it_file.insert_file(p_file_dir => v_file_dir
                                    ,p_file_name => v_file_name
                                    ,p_file_clob => v_clob
                                    ,p_from_system => it_file.C_SOFR_DB
                                    ,p_from_module => $$plsql_unit
                                    ,p_to_system => it_file.C_SOFR_RSBANK
                                    ,p_to_module => null
                                    ,p_create_user => v_create_user);
    it_log.log(p_msg => 'v_clob cover', p_msg_clob => v_clob);
    update itt_rcb_portf_by_cat_pack p set p.id_file_cover = v_id_file where p.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack;
    if dbms_lob.getlength(v_clob) = 0
    then
      raise_application_error(-20004, 'Данных для отчета нет');
    end if;
    return v_id_file;
  exception
    when others then
      /*if dbms_lob.isopen(lob_loc => v_clob) = 1
      then
        dbms_lob.close(lob_loc => v_clob);
      end if;*/
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  function print_tab (p_table_in          in   varchar2
                      ,p_where_in          in   varchar2 default null
                      ,p_colname_like_in   in   varchar2 := '%'
                      )return clob
  is

     c_min_length   constant number := 10;

     type columns_tt is table of all_tab_columns%rowtype
        index by pls_integer;

     g_columns               columns_tt;
     g_cursor                integer        := dbms_sql.open_cursor;
     
     g_header                clob;
     g_select_list           clob;
     g_clob                  clob;

     function is_string(row_in in integer) return boolean
     is
     begin
       return (g_columns(row_in).data_type in ('CHAR', 'VARCHAR2', 'VARCHAR','CLOB'));
     end;

     function is_number(row_in in integer)return boolean
     is
     begin
       return (g_columns(row_in).data_type in('FLOAT', 'INTEGER', 'NUMBER'));
     end;

     function is_date(row_in in integer)return boolean
     is
     begin
       return (g_columns(row_in).data_type in ('DATE', 'TIMESTAMP'));
     end;

     procedure load_column_info
     is
        v_dot_location   number;
        v_owner          varchar2(100);
        v_table          varchar2(100);
        v_index          number;
        --
        no_such_table    exception;
        pragma exception_init (no_such_table, -942);
     begin
        v_dot_location := instr (p_table_in, '.');

        if v_dot_location > 0
        then
           v_owner := substr (p_table_in, 1, v_dot_location - 1);
           v_table := substr (p_table_in, v_dot_location + 1);
        else
           v_owner := user;
           v_table := p_table_in;
        end if;
        
        select *
        bulk collect into g_columns
          from all_tab_columns
         where owner = v_owner
           and table_name = upper(v_table)
           and column_name like nvl (p_colname_like_in, '%')
        order by column_id ;

        v_index := g_columns.first;

        if v_index is null
        then
           raise no_such_table;
        else           
           while (v_index is not null)
           loop
              if g_select_list is null
              then
                 g_select_list := g_columns(v_index).column_name;
              else
                 g_select_list := g_select_list||', '||g_columns (v_index).column_name;
              end if;                        
              
              g_header := g_header||g_columns (v_index).column_name||';';                    
              v_index := g_columns.next(v_index);
           end loop;
        end if;
      --  dbms_output.put_line('***g_select_list '||g_select_list);
     end;

     procedure construct_and_parse_query
     is
       v_where_clause   clob := ltrim (lower (p_where_in));
       v_query          clob;
     begin
       if v_where_clause is not null then
         v_where_clause := 'where '||ltrim (v_where_clause, 'where');
       end if;
       
       v_query :=
              'select '
           || g_select_list
           || '  from '
           || p_table_in
           || ' '
           || v_where_clause;
       
       dbms_sql.parse (g_cursor, v_query, dbms_sql.native);
     exception
        when others
        then
           dbms_output.put_line ('Error parsing query:');
           dbms_output.put_line (v_query);
           raise;
     end;

     procedure define_columns_and_execute
     is
        v_index      number;
        v_exec       number;
     begin    
        v_index := g_columns.first;

        while (v_index is not null)
        loop
           if is_string (v_index)
           then
              dbms_sql.define_column (g_cursor
                                     ,v_index
                                     ,'a'
                                     ,g_columns(v_index).data_length
                                     );
           elsif is_number(v_index)
           then
              dbms_sql.define_column(g_cursor, v_index, 1);
           elsif is_date (v_index)
           then
              dbms_sql.define_column(g_cursor, v_index, sysdate);
           end if;
           v_index := g_columns.next(v_index);
        end loop;

        v_exec := dbms_sql.execute(g_cursor);
     end;
     
     procedure get_clob is
       v_clob_value       clob;     
       v_str_value        varchar2(4000);  
       v_number_value     number;
       v_date_value       date;
       
       v_fetch_row        integer;
       v_index            number;
       v_one_row_string   clob;
     begin
       it_log.log('Start get_clob');
       g_clob := null;      
       dbms_lob.createtemporary(lob_loc => g_clob
                             , cache    => true
                             , dur      => dbms_lob.lob_readwrite
                               ); 
       dbms_lob.append(g_clob,g_header||chr(13)||chr(10));                    
       loop        
         v_fetch_row := dbms_sql.fetch_rows(g_cursor);
         exit when v_fetch_row = 0;
        
         v_one_row_string := null;
         v_index := g_columns.first;

         while (v_index is not null)
         loop  
           --dbms_output.put_line ('   g_cursor.column_name = '||g_columns(v_index).column_name||' - '||$$PLSQL_LINE);         
           if is_string(v_index) then
             dbms_sql.column_value(g_cursor, v_index, v_str_value); 
             v_clob_value := to_clob(v_str_value);          
           elsif is_number(v_index) then
             dbms_sql.column_value(g_cursor, v_index, v_number_value);
             v_clob_value := to_clob(v_number_value);
           elsif is_date(v_index) then
             dbms_sql.column_value(g_cursor, v_index, v_date_value);
             v_clob_value := to_clob(v_date_value);
           end if;
           v_one_row_string := v_one_row_string||to_clob(';')||to_clob(nvl(v_clob_value,';'));  
           
           v_index := g_columns.next(v_index);
         end loop; 
         
         if nvl(dbms_lob.getlength(lob_loc => v_one_row_string),0) != 0 then 
           dbms_lob.append(g_clob,ltrim(v_one_row_string,';')||chr(13)||chr(10)); 
         end if;
       end loop;
                   
       it_log.log(p_msg_clob => g_clob);         
     exception 
       when others then
         it_error.put_error_in_stack;
         -- v_error_msg := substr(sqlerrm,1,4000);
         --dbms_output.put_line ('ERROR: '||substr(v_error_msg,1,3900)||' - '||$$PLSQL_LINE);                
        
        it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
        raise;  
     end;
     
     procedure cleanup
     is
     begin   
       dbms_sql.close_cursor (g_cursor);
     end cleanup;
     
  begin
     it_log.log('START print_tab p_table_in = '||p_table_in||' p_where_in = '||p_where_in||' p_colname_like_in = '||p_colname_like_in); 
     load_column_info;
     construct_and_parse_query;
     define_columns_and_execute;
     get_clob;
     cleanup;
     it_log.log('END print_tab ');
     return g_clob;
  exception
     when others
     then
        cleanup;
        raise;
  end;

  function make_report_detail(p_id_rcb_portf_by_cat_pack in number
                             ,p_part                     integer) return number is
    v_id_file number;
    v_count_part constant number := 500000;
    v_rep       clob;
    v_header    clob;
    v_file_name varchar2(2000);
  begin
    it_log.log('START make_report_detail p_id_rcb_portf_by_cat_pack ' || p_id_rcb_portf_by_cat_pack || ' Part ' || p_part);
    v_file_name := '6_ЭП_detail' || p_part || '_' || to_char(s_rep_date, 'YYYYMMDD') || '_' || to_char(sysdate, 'hh24miss') || '.csv';
    dbms_lob.createtemporary(lob_loc => v_rep, cache => true);
    for cur in (select t.*
                  from (select rownum npp
                              ,t.*
                          from ITT_RCB_PORTF_BY_CAT_REC t
                         where id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack
                         order by t_partyid
                                 ,t_dlcontrid
                                 ,t_fiid) t
                 where npp between ((p_part - 1) * v_count_part + 1) and (p_part * v_count_part)
                 order by npp)
    loop
      dbms_lob.append(v_rep
                     ,it_rsl_string.GetCell(cur.npp) || it_rsl_string.GetCell(cur.t_dlcontrid) || it_rsl_string.GetCell(cur.t_dlcontr_name) ||
                      it_rsl_string.GetCell(cur.t_partyid) || it_rsl_string.GetCell(cur.t_party_code) || it_rsl_string.GetCell(cur.t_shortname) ||
                      it_rsl_string.GetCell(cur.t_legalform) || it_rsl_string.GetCell(cur.is_qual) || it_rsl_string.GetCell(cur.t_notresident) ||
                      it_rsl_string.GetCell(cur.t_okato) || it_rsl_string.GetCell(cur.t_fiid) || it_rsl_string.GetCell(cur.t_fiid_name) ||
                      it_rsl_string.GetCell(cur.t_isin) || it_rsl_string.GetCell(cur.t_rest) || it_rsl_string.GetCell(cur.t_facevaluefi_ccy) ||
                      it_rsl_string.GetCell(cur.t_facevalue) || it_rsl_string.GetCell(cur.t_rate_ccy) ||
                      it_rsl_string.GetCell(cur.t_rate_isrelative) || it_rsl_string.GetCell(cur.t_rate) || it_rsl_string.GetCell(cur.t_rate_type) ||
                      it_rsl_string.GetCell(cur.t_rate_market_place) || it_rsl_string.GetCell(cur.t_rate_sincedate) ||
                      it_rsl_string.GetCell(cur.is_rate_sincedate90) || it_rsl_string.GetCell(cur.t_rate_definition_lst) ||
                      it_rsl_string.GetCell(cur.t_fi_kind) || it_rsl_string.GetCell(cur.t_avoirkind) || it_rsl_string.GetCell(cur.t_avrkind_root) ||
                      it_rsl_string.GetCell(cur.t_kind_name) || it_rsl_string.GetCell(cur.summ) || it_rsl_string.GetCell(cur.summ_rur_cb) ||
                      it_rsl_string.GetCell(cur.t_rate_cb) || it_rsl_string.GetCell(cur.t_nkd_rate_isrelative) ||
                      it_rsl_string.GetCell(cur.t_nkd_rate) || it_rsl_string.GetCell(cur.t_nkd_rate_fiid) ||
                      it_rsl_string.GetCell(cur.t_nkd_rate_ccy) || it_rsl_string.GetCell(cur.nkd_summ) || it_rsl_string.GetCell(cur.nkd_summ_rur_cb) ||
                      it_rsl_string.GetCell(cur.t_nkd_rate_cb) || it_rsl_string.GetCell(cur.t_requirement_rest) ||
                      it_rsl_string.GetCell(cur.requirement_summ) || it_rsl_string.GetCell(cur.requirement_summ_rur_cb) ||
                      it_rsl_string.GetCell(cur.is_upd) || it_rsl_string.GetCell(cur.rate_abs) || it_rsl_string.GetCell(cur.foreign_priz) ||
                      it_rsl_string.GetCell(cur.etf_priz, true));
    end loop;
    if dbms_lob.getlength(v_rep) != 0
    then
      dbms_lob.trim(v_rep, dbms_lob.getlength(v_rep) - 2); -- Удаляем последние chr(13)||chr(10))
      v_id_file := it_file.insert_file(p_file_clob => v_rep
                                      ,p_file_name => v_file_name
                                      ,p_from_system => it_file.C_SOFR_DB
                                      ,p_from_module => $$plsql_unit
                                      ,p_to_system => it_file.C_SOFR_RSBANK
                                      ,p_to_module => null
                                      ,p_create_user => user);
      it_log.log('v_id_file=' || v_id_file);
    else
      it_log.log(' NO DATA');
    end if;
    it_log.log('END make_report_detail');
    return v_id_file;
  exception
    when others then
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      /*if dbms_lob.isopen(lob_loc => s_rep_detail) = 1
      then
        dbms_lob.close(lob_loc => s_rep_detail);
      end if;*/
      raise;
  end;

  function make_report_detail_old(p_id_rcb_portf_by_cat_pack in number) return number is
    v_id_file     number;
    v_file_dir    varchar2(4000);
    v_create_user varchar2(250) := 1;
    v_file_name   varchar2(1000);
    v_clob        clob;
  begin
    it_log.log('START make_report_detail p_id_rcb_portf_by_cat_pack ' || p_id_rcb_portf_by_cat_pack);
    v_file_name   := '6_ЭП_detail_' || to_char(s_rep_date, 'YYYYMMDD') || '_' || to_char(sysdate, 'hh24miss') || '.csv';
    v_create_user := 1;
    dbms_lob.createtemporary(lob_loc => v_clob, cache => true);
    for cCur in (select rownum
                       ,r.t_legalform as isLegalEntiy
                       ,r.is_qual as isQual
                       ,r.t_notresident as isResident
                       ,case
                          when round(r.summ_rur_cb) < 1 then
                           'пустые счета'
                          when round(r.summ_rur_cb) between 1 and 9999 then
                           'от 0 до 10 тыс.'
                          when round(r.summ_rur_cb) between 10000 and 99999 then
                           'от 10 тыс. до 100 тыс.'
                          when round(r.summ_rur_cb) between 100000 and 999999 then
                           'от 100 тыс. до 1 млн'
                          when round(r.summ_rur_cb) between 1000000 and 5999999 then
                           'от 1 млн до 6 млн'
                          when round(r.summ_rur_cb) between 6000000 and 9999999 then
                           'от 6 млн до 10 млн'
                          when round(r.summ_rur_cb) between 10000000 and 99999999 then
                           'от 10 млн до 100 млн'
                          when round(r.summ_rur_cb) between 100000000 and 499999999 then
                           'от 100 млн до 500 млн'
                          when round(r.summ_rur_cb) between 500000000 and 999999999 then
                           'от 500 млн до 1 млрд'
                          when round(r.summ_rur_cb) >= 1000000000 then
                           '1 млрд +'
                        end as portfolio_range
                       ,r.t_okato as countryCD
                       ,r.t_isin as isin
                       ,r.t_fiid as insID
                       ,r.t_rate_ccy as ccy
                       ,r.t_facevaluefi
                       ,r.t_facevalue
                       ,r.t_rest as qty
                       ,r.t_rate as qt
                       ,r.t_rate_market_place as qtSrc
                       ,r.t_rate_sincedate as qtDte
                       ,r.t_rate_definition_lst as qtLst
                       ,null as AmortizedBondVal
                       ,r.t_fiid_name as insTyp
                       ,r.summ as amt
                       ,r.summ_rur_cb as amtRUB
                   from itt_rcb_portf_by_cat_rec r
                  where r.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack)
    loop
      dbms_lob.append(v_clob
                     ,cCur.rownum || ';' || cCur.isLegalEntiy || ';' || cCur.IsQual || ';' || cCur.isResident || ';' || cCur.Portfolio_Range || ';' ||
                      cCur.countryCD || ';' || cCur.Isin || ';' || cCur.Insid || ';' || cCur.Ccy || ';' || cCur.t_facevaluefi || ';' ||
                      cCur.t_facevalue || ';' || cCur.Qty || ';' || cCur.Qt || ';' || cCur.Qtsrc || ';' || cCur.Qtdte || ';' || cCur.qtLst || ';' ||
                      cCur.Amortizedbondval || ';' || cCur.Instyp || ';' || cCur.Amt || ';' || cCur.Amtrub || ';' || chr(13) || chr(10));
    end loop;
    v_id_file := it_file.insert_file(p_file_dir => v_file_dir
                                    ,p_file_name => v_file_name
                                    ,p_file_clob => v_clob
                                    ,p_from_system => it_file.C_SOFR_DB
                                    ,p_from_module => $$plsql_unit
                                    ,p_to_system => it_file.C_SOFR_RSBANK
                                    ,p_to_module => null
                                    ,p_create_user => v_create_user);
    it_log.log('v_id_file=' || v_id_file);
    it_log.log(p_msg => 'v_clob detail', p_msg_clob => v_clob);
    update itt_rcb_portf_by_cat_pack p set p.id_file_detail = v_id_file where p.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack;
    if v_clob is null
    then
      raise_application_error(-20003, 'Данных для отчета нет');
    end if;
    it_log.log('END make_report_detail');
    return v_id_file;
  exception
    when others then
      /*if dbms_lob.isopen(lob_loc => v_clob) = 1
      then
        dbms_lob.close(lob_loc => v_clob);
      end if;*/
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      raise;
  end;

  --update облигаций из за неверных котировок облигаций
  procedure update_security(p_pack_id in varchar2) is
    vrate_usd_val number;
  begin
    it_log.log(p_msg => 'START run p_pack_id = ' || p_pack_id);
    with rate1 as
     ( --котировка по ценной бумаге
      select r.t_rateid
             ,r.t_fiid
             ,r.t_otherfi
             ,r.t_type
             ,r.t_isrelative --jzotov
             ,r.t_market_place
             ,r.t_rate / power(10, r.t_point) as t_rate
             ,r.t_sincedate
             ,r.t_name
        from dratedef_dbt r --котировка по ценной бумаге
      union all
      select rh.t_rateid
             ,null as t_fiid
             ,null as t_otherfi
             ,null as t_type
             ,null as t_isrelative --jzotov
             ,null as t_market_place
             ,rh.t_rate / power(10, rh.t_point) as t_rate
             ,rh.t_sincedate
             ,null as t_name
        from dratehist_dbt rh --старое значение котировки по ценном бумаге
      ),
    rate2 as
     ( --котировка ценной бумаги с историчностью
      select r.t_rateid
             ,max(r.t_fiid) over(partition by r.t_rateid) as t_fiid
             ,max(r.t_otherfi) over(partition by r.t_rateid) as t_otherfi
             ,max(r.t_type) over(partition by r.t_rateid) as t_type
             ,max(r.t_isrelative) over(partition by r.t_rateid) as t_isrelative --jzotov
             ,max(r.t_market_place) over(partition by r.t_rateid) as t_market_place
             ,max(r.t_name) over(partition by r.t_rateid) as t_name
             ,r.t_rate
             ,r.t_sincedate
             ,nvl((lead(t_sincedate) over(partition by r.t_rateid order by r.t_sincedate)) - 1, to_date('31.12.9999', 'dd.mm.yyyy')) as dend
        from rate1 r
       where 1 = 1
      -- Типы курсов (t_typr): select * from dratetype_dbt
      ),
    rate as
     ( --котировка ценной бумаги на дату
      select r.t_fiid
             ,r.t_otherfi
             ,r.t_type
             ,r.t_isrelative --jzotov
             ,r.t_market_place
             ,r.t_rate
             ,r.t_sincedate
              --                     , f.t_ccy
             ,r.t_name
              --Защита от дублирования курса. Приоритет=t_fiid: 0-рубли, 7-доллары, 8-евро.
             ,row_number() over(partition by r.t_otherfi, r.t_type, r.t_market_place order by r.t_fiid) as rn
        from rate2 r
       where --p_repdate between r.t_sincedate and r.dend
       s_rep_date between r.t_sincedate and r.dend
    and r.t_type = 7 /*ЦБ РФ*/
    and r.t_market_place = 0
    and r.t_otherfi = 7
    and r.t_fiid = 0)
    select t_rate into vrate_usd_val from rate r where r.rn = 1;
  --только для физиков
    update itt_rcb_portf_by_cat_rec r
       set r.rate_abs        = r.t_facevalue
          ,r.summ            = r.t_facevalue * r.t_rest
          ,r.summ_rur_cb = r.t_facevalue * r.t_rest * case
                             when r.t_rate_cb > 1
                                  and r.t_facevaluefi = 0 then
                              1
                             when r.t_rate_cb = 1
                                  and r.t_facevaluefi = 7 then
                              vrate_usd_val
                             when r.t_rate_cb > 1
                                  and r.t_facevaluefi > 0 then
                              t_rate_cb
                             else
                              r.t_rate_cb
                           end
          ,r.t_nkd_rate      = r.t_nkd_rate / 100
          ,r.nkd_summ        = r.t_nkd_rate * r.t_rest / 100
          ,r.nkd_summ_rur_cb = r.t_nkd_rate * r.t_rest * case
                                 when r.t_rate_cb > 1
                                      and r.t_facevaluefi = 0 then
                                  1
                                 when r.t_rate_cb = 1
                                      and r.t_facevaluefi = 7 then
                                  vrate_usd_val
                                 when r.t_rate_cb > 1
                                      and r.t_facevaluefi > 0 then
                                  t_rate_cb
                                 else
                                  r.t_rate_cb
                               end / 100
          ,r.t_rate_cb = case
                           when r.t_rate_cb > 1
                                and r.t_facevaluefi = 0 then
                            1
                           when r.t_rate_cb = 1
                                and r.t_facevaluefi = 7 then
                            63.0975
                           when r.t_rate_cb > 1
                                and r.t_facevaluefi > 0 then
                            t_rate_cb
                           else
                            r.t_rate_cb
                         end
          ,r.is_upd          = 1
     where r.id_rcb_portf_by_cat_pack = p_pack_id
       and r.t_avrkind_root = 17
       and r.t_legalform = 'ФИЗ'
       and r.t_rate_isrelative = 'X'
          -- and (r.rate_abs-2*r.t_facevalue)>0 --3493
       and (r.t_rate >= 150) --4246
    ;
    update itt_rcb_portf_by_cat_rec r
       set r.t_rate_cb   = vrate_usd_val
          ,r.summ_rur_cb = summ * vrate_usd_val
     where 1 = 1 --r.t_isin  in  ('RU000A0JXU14','RU000A0ZYYN4')  
       and r.id_rcb_portf_by_cat_pack = p_pack_id
       and r.t_avrkind_root = 17
          -- and r.t_legalform = 'ФИЗ' 
       and r.t_facevaluefi > 1
       and r.t_rate_cb = 1
       and r.t_avrkind_root = 17;
    it_log.log(p_msg => 'END ');
  end;

  function run(p_rep_date in date) return clob is
    pragma autonomous_transaction;
    v_id_file_by_cat number;
    v_id_file_cover  number;
    v_id_file_detail number;
    v_id_rep_pack    number;
    v_error          varchar2(2000);
    v_n              integer;
    v_xres           xmltype;
    v_sel_detail     varchar2(32000);
  begin
    it_log.log('START run ' || p_rep_date);
    it_error.clear_error_stack;
    s_rep_date := p_rep_date;
    -- Удаляем все расчеты старше 1 дня
    for rep in (select r.id_rcb_portf_by_cat_pack from itt_rcb_portf_by_cat_pack r where r.start_date < sysdate - 1 order by r.start_date)
    loop
      clear_report(rep.id_rcb_portf_by_cat_pack);
    end loop;
    /*s_rep_cb_portf_by_cat := null; 
    s_rep_cover_stat      := null;  
    s_rep_detail          := null;*/
    v_id_rep_pack := make_process(p_rep_date);
    --по портфелю 
    it_log.log('Report by_cat ');
    v_id_file_by_cat := make_report_cb_portf_by_cat(p_id_rcb_portf_by_cat_pack => v_id_rep_pack);
    it_log.log('v_id_file_by_cat ' || v_id_file_by_cat);
    /* select f.file_clob
     into s_rep_cb_portf_by_cat 
    from  itt_file f 
    where f.id_file = v_id_file_by_cat;*/
    --котировки/статистика 
    --it_log.log('Report cover ');
    --v_id_file_cover := make_report_cover(p_id_rcb_portf_by_cat_pack => v_id_rep_pack);
    /*select f.file_clob
     into s_rep_cover_stat  
    from  itt_file f 
    where f.id_file = v_id_file_cover; */
    --<обновляем неверные котировки в облигациях 
    update_security(v_id_rep_pack);
    --развернутый 
    it_log.log('Report detail ');
    v_n := 1;
    loop
      v_id_file_detail := it_rcb_portf_by_cat.make_report_detail(v_id_rep_pack, v_n);
      exit when v_id_file_detail is null;
      v_sel_detail := v_sel_detail || case
                        when v_sel_detail is not null then
                         ' union all '
                      end || 'select ' || v_n || ' as part, ' || v_id_file_detail || ' as id_file from dual';
      v_n          := v_n + 1;
    end loop;
    execute immediate 'select xmlelement("XML", xmlattributes( :1 as "id_rep_pack", :2 as "id_file_by_cat", :3 as "id_file_cover"),
                      XMLAGG(XMLELEMENT("file_detail", xmlattributes(c.part as "part", c.id_file as "id_file_detail"))))
           from (' || v_sel_detail || ') c'
      into v_xres
      using v_id_rep_pack, v_id_file_by_cat, v_id_file_cover;
    update itt_rcb_portf_by_cat_pack p set p.meta = v_xres.getClobVal() where p.id_rcb_portf_by_cat_pack = v_id_rep_pack;
    it_log.log('END run ');
    commit;
    return v_xres.getClobVal();
  exception
    when others then
      rollback;
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
      v_error := substr(it_error.get_error_stack, instr(it_error.get_error_stack, 'ORA-'), 2000);
      v_error := substr(nvl(v_error, it_error.get_error_stack), 1, 2000);
      select xmlelement("SOFRError", v_error) into v_xres from dual;
      return v_xres.getClobVal();
  end;

  -- Удаление расчетов и csv отчета 
  procedure clear_report(p_id_rcb_portf_by_cat_pack in number) as
    pragma autonomous_transaction;
    vr_portf_by_cat itt_rcb_portf_by_cat_pack%rowtype;
    vx_meta         xmltype;
    vx_node         dbms_xmldom.DOMNodeList;
    vx_element      dbms_xmldom.DOMElement;
    vc_id_file      varchar2(50);
    procedure del_itt_file(p_id_file number) as
      v_file_name itt_file.file_name%type;
    begin
      if p_id_file is not null
      then
        it_log.log('Delete from itt_file id_file = ' || p_id_file);
        delete from itt_file f
         where f.id_file = p_id_file
           and f.file_code is null
        returning f.file_name into v_file_name;
        it_log.log('Deleted ' || v_file_name);
      end if;
    end;
  
  begin
    it_log.log('Start  p_id_rcb_portf_by_cat_pack ' || p_id_rcb_portf_by_cat_pack);
    select * into vr_portf_by_cat from itt_rcb_portf_by_cat_pack p where p.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack;
    del_itt_file(vr_portf_by_cat.id_file_cb_portf_by_cat);
    del_itt_file(vr_portf_by_cat.id_file_cover);
    del_itt_file(vr_portf_by_cat.id_file_detail);
    if dbms_lob.getlength(vr_portf_by_cat.meta) > 0
    then
      vx_meta := xmltype(vr_portf_by_cat.meta);
      select EXTRACTVALUE(vx_meta, '/XML/@id_file_by_cat') into vc_id_file from dual;
      del_itt_file(vc_id_file);
      select EXTRACTVALUE(vx_meta, '/XML/@id_file_cover') into vc_id_file from dual;
      del_itt_file(vc_id_file);
      vx_node := dbms_xmldom.getChildNodes(dbms_xmldom.getNodeFromFragment(vx_meta.extract('//file_detail')));
      for i in 0 .. dbms_xmldom.getLength(vx_node) - 1
      loop
        vx_element := dbms_xmldom.makeelement(dbms_xmldom.item(vx_node, i));
        vc_id_file := dbms_xmldom.getAttribute(vx_element, 'id_file_detail');
        del_itt_file(vc_id_file);
      end loop;
    end if;
    it_log.log('Delete from itt_rcb_portf_by_cat_rec ');
    delete from itt_rcb_portf_by_cat_rec r where r.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack;
    delete from itt_rcb_portf_by_cat_pack p where p.id_rcb_portf_by_cat_pack = p_id_rcb_portf_by_cat_pack;
    commit;
    it_log.log('End ');
  exception
    when others then
      rollback;
      it_error.put_error_in_stack;
      it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
  end;

end it_rcb_portf_by_cat;
/

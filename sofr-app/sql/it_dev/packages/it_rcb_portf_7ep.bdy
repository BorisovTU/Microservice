create or replace package body it_rcb_portf_7ep is

	/**************************************************************************************************\
   Отчет -7_ЭП_Обследование портрета клиента брокера
   **************************************************************************************************
   Изменения:
   ---------------------------------------------------------------------------------------------------
   Дата        Автор            Jira                          Описание
   ----------  ---------------  ---------------------------   ----------------------------------------     
   22.08.2022  Зыков   М.В.     BIQ-12884                      Доработка отчета 
   18.08.2022  Зыков   М.В.     BIQ-12884                      Доработка отчета 
   16.08.2022  Зыков   М.В.     BIQ-12884                      Доработка отчета 
   11.08.2022  Зотов   Ю.Н.     BIQ-12884                      Исправлена ошибка: русские буквы (ер) заменены на английские (ep) в объектах itt_rcb_portf_7ep_clnt, itt_rcb_portf_7ep_deal  
   05.08.2022  Зыков   М.В.     BIQ-12884                      Создание
  */
	s_rep_date date;

	function get_agegrp(p_date date
										 ,p_born date) return number deterministic as
		pragma udf;
		v_agegrp number;
		v_age    number := TRUNC(months_between(p_date, p_born) / 12);
	begin
		case
			when v_age <= 20 then
				v_agegrp := 1;
			when v_age <= 30 then
				v_agegrp := 2;
			when v_age <= 40 then
				v_agegrp := 3;
			when v_age <= 50 then
				v_agegrp := 4;
			when v_age <= 60 then
				v_agegrp := 5;
			else
				v_agegrp := 6;
		end case;
		return v_agegrp;
	end;

	function make_process(p_dtbegin       date
											 ,p_dtend         date
											 ,p_pack_id_begin in number
											 ,p_pack_id_end   in number) return number is
		v_id_rep_pack               number;
		v_cnt                       number;
		v_itt_rcb_portf_by_cat_pack itt_rcb_portf_by_cat_pack%rowtype;
	begin
		it_log.log('START');
		v_id_rep_pack                                        := its_main.nextval();
		v_itt_rcb_portf_by_cat_pack                          := null;
		v_itt_rcb_portf_by_cat_pack.id_rcb_portf_by_cat_pack := v_id_rep_pack;
		v_itt_rcb_portf_by_cat_pack.report_date              := p_dtend;
		v_itt_rcb_portf_by_cat_pack.start_date               := sysdate;
		v_itt_rcb_portf_by_cat_pack.create_user              := 1;
		-- Формирование отчета
		it_log.log('Формирование отчета');
		insert into itt_rcb_portf_7ep_clnt
			(id_rcb_portf_7ep_pack
			,t_partyid
			,t_dlcontrid
			,t_dlcontr_name
			,sfcontr_begin
			,sfcontr_end
			,client_agegrp
			,client_birth_date
			,client_gender
			,portf_begin
			,portf_end
			,portf_end_frgn
			,portf_end_etf
			,cash_end
			,cash_end_frgn
			,fii_count
			 --,portf_add
			,portf_add_cnt)
			with pack_end_fii_count as
			 (select r.t_partyid
							,count(distinct case
											 when round(nvl(r.summ_rur_cb, 0) + case
																		when r.t_avrkind_root = 17 then
																		 nvl(r.nkd_summ_rur_cb, 0)
																		else
																		 0
																	end + nvl(r.requirement_summ_rur_cb, 0)
																 ,2) != 0 -- не 0 оценка 
														and r.t_fiid != 0 then -- не рубли 
												r.t_fiid
											 else
												null
										 end) fii_count
					from itt_rcb_portf_by_cat_rec r
				 inner join dparty_dbt cli
						on cli.t_partyid = r.t_partyid -- клиент
				 where 1 = 1
					 and cli.t_legalform = 2 --'ФИЗ'
					 and r.id_rcb_portf_by_cat_pack = p_pack_id_end -- 1584312
				 group by r.t_partyid),
			pack_end as
			 (select r.t_partyid
							,r.t_dlcontrid
							,r.t_dlcontr_name
							 --признак Ден.средств
							,case
								 when r.t_fi_kind = 1 then --'Валюты'
									1
								 else
									0
							 end cash_prz
							,case
								 when r.t_fi_kind = 1
											and r.t_fiid != 0 then --'Иностр.вал'
									1
								 else
									0
							 end cash_prz_frgn
							 --признак иностр компании
							,r.foreign_priz
							,r.etf_priz
							,sum(round( --/*ЦБ и остатки денежных средств на счетах*/
												 nvl(summ_rur_cb, 0) +
												 /*НКД*/
													case
														when r.t_avrkind_root = 17 /*все виды облигаций*/
														 then
														 nvl(r.nkd_summ_rur_cb, 0)
														else
														 0
													end +
												 /*Баланс требований и обязательств*/
													nvl(r.requirement_summ_rur_cb, 0)
												,2)) summ_rur
					from itt_rcb_portf_by_cat_rec r
				 inner join dparty_dbt cli
						on cli.t_partyid = r.t_partyid -- клиент
				 where 1 = 1
					 and cli.t_legalform = 2 --'ФИЗ'
					 and r.id_rcb_portf_by_cat_pack = p_pack_id_end -- 1584312
				 group by r.t_partyid
								 ,r.t_dlcontrid
								 ,r.t_dlcontr_name
								 ,case
										when r.t_fi_kind = 1 then -- 'Валюты'
										 1
										else
										 0
									end
								 ,case
										when r.t_fi_kind = 1
												 and r.t_fiid != 0 then --'Иностр.вал'
										 1
										else
										 0
									end
								 ,r.foreign_priz
								 ,r.etf_priz),
			pack_begin as
			 (select r.t_partyid
							,r.t_dlcontrid
							,sum(round( --/*ЦБ и остатки денежных средств на счетах*/
												 nvl(summ_rur_cb, 0) +
												 /*НКД*/
													case
														when r.t_avrkind_root = 17 /*все виды облигаций*/
														 then
														 nvl(r.nkd_summ_rur_cb, 0)
														else
														 0
													end +
												 /*Баланс требований и обязательств*/
													nvl(r.requirement_summ_rur_cb, 0)
												,2)) summ_rur
					from itt_rcb_portf_by_cat_rec r
				 inner join dparty_dbt cli
						on cli.t_partyid = r.t_partyid -- клиент
				 where 1 = 1
					 and cli.t_legalform = 2 --'ФИЗ'
					 and r.id_rcb_portf_by_cat_pack = p_pack_id_begin -- 1406118
				 group by r.t_partyid
								 ,r.t_dlcontrid),
			portf_add_cnt as
			 (select pe.t_partyid
							,count(distinct x.t_id) + count(distinct cb.t_dealid) as cnt
					from pack_end pe
					left join dnptxop_dbt x
						on x.t_client = pe.t_partyid
					 and x.t_dockind = 4607
					 and x.t_subkind_operation = 10
					 and x.t_operdate between p_dtbegin and p_dtend
					left join ddl_tick_dbt cb
						on cb.t_bofficekind = 127
					 and cb.t_dealtype = 2011
					 and cb.t_dealcode not like '%_NDFL'
					 and cb.t_dealdate between p_dtbegin and p_dtend
					 and cb.t_clientid = pe.t_partyid
				 group by pe.t_partyid)
			select v_id_rep_pack -- id_cbr                                                        id_rcb_portf_7ep_pack
						,pe.t_partyid --client_id    -- sfcontr_id                                      t_partyid
						,pe.t_dlcontrid --                                                              t_dlcontrid
						,pe.t_dlcontr_name -- sfcontr_name                                              t_dlcontr_name
						,min(c.t_datebegin) --sfcontr_begin                                             sfcontr_begin
						,min(c.t_dateclose) --sfcontr_end                                               sfcontr_end
						,it_rcb_portf_7ep.get_agegrp(p_dtend, prs.t_born) -- cclient_agegrp           client_agegrp
						,prs.t_born -- client_birth_date                                                 client_birth_date
						,prs.t_ismale --client_gender                                                    client_gender
						,nvl(sum(pb.summ_rur), 0) --portf_begin                                          portf_begin
						,nvl(sum(pe.summ_rur), 0) --portf_end                                            portf_end
						,nvl(sum(decode(pe.foreign_priz, 1, pe.summ_rur, 0)), 0) --portf_end_frgn        portf_end_frgn
						,nvl(sum(decode(pe.etf_priz, 1, pe.summ_rur, 0)), 0) --                          portf_end_etf
						,nvl(sum(decode(pe.cash_prz, 1, pe.summ_rur, 0)), 0) -- cash_end                 cash_end
						,nvl(sum(decode(pe.cash_prz_frgn, 1, pe.summ_rur, 0)), 0) -- cash_end_frgn       cash_end_frgn
						,pe_fcnt.fii_count -- fii_count                                                  fii_count)
						,nvl(addc.cnt, 0) as portf_add_cnt ----portf_add_cnt
				from pack_end pe
				join pack_end_fii_count pe_fcnt
					on pe.t_partyid = pe_fcnt.t_partyid
				left join pack_begin pb
					on pb.t_dlcontrid = pe.t_dlcontrid
				 and pb.t_partyid = pb.t_partyid
				left join ddlcontr_dbt dc
					on dc.t_dlcontrid = pe.t_dlcontrid
				left join dsfcontr_dbt c
					on c.t_id = dc.t_sfcontrid
				left join dpersn_dbt prs
					on prs.t_personid = pe.t_partyid
				left join portf_add_cnt addc
					on pe.t_partyid = addc.t_partyid
			 group by pe.t_partyid --client_id
							 ,pe.t_dlcontrid
							 ,pe.t_dlcontr_name
							 ,it_rcb_portf_7ep.get_agegrp(p_dtend, prs.t_born)
							 ,prs.t_born -- client_birth_date
							 ,prs.t_ismale --client_gender
							 ,pe_fcnt.fii_count
							 ,nvl(addc.cnt, 0);
		v_cnt := sql%rowcount;
		it_log.log('insert into ITT_RCB_PORTF_7ЕР_CLNT end  sql%rowcount = ' || v_cnt);
		if v_cnt = 0
		then
			raise_application_error(-20001, 'Данные для отчета за дату  отсутствуют.');
		end if;
		---------------------------------------------------------------------------------
		insert into itt_rcb_portf_7ep_deal
			(id_rcb_portf_7ep_pack
			,t_partyid
			,sfcontr_id
			,t_dealdate
			,cnt_deal
			,cnt_margin
			,cnt_short_sel
			,cnt_margin_call)
			with PARTY as -- Положительные портфели клиентов
			 (select t.T_PARTYID
							,sum(t.portf_end) as portf
					from itt_rcb_portf_7ep_clnt t
				 where t.id_rcb_portf_7ep_pack = v_id_rep_pack
				 group by t.T_PARTYID
				having sum(t.portf_end) > 0),
			contr as -- Договоры по положительным портфелям
			 (select distinct t.T_PARTYID
							,sf.t_Id sfcontr_id
					from itt_rcb_portf_7ep_clnt t
					join PARTY p
						on p.T_PARTYID = t.t_partyid
					join ddlcontrmp_dbt mp
						on mp.t_dlcontrid = t.t_dlcontrid
					join dsfcontr_dbt sf
						on sf.t_id = mp.t_sfcontrid
				 where t.id_rcb_portf_7ep_pack = v_id_rep_pack)
			select v_id_rep_pack
						,contr.T_PARTYID
						,contr.sfcontr_id
						,coalesce(tick1.t_dealdate, tick2.t_dealdate, tick3.t_date, tick4.t_date) t_dealdate
						,count(distinct coalesce(tick1.t_dealid, tick2.t_dealid, tick3.t_id, tick4.t_id)) cnt_deal
						,count(distinct case
										 when tick3.t_kind = 32715 then
											tick3.t_id
									 end) + count(distinct case
																	when Attr.t_NumInList = 1 then
																	 tick1.t_dealid
																end) as cnt_margin
						,count(distinct case
										 when Attr.t_NumInList = 1 then
											tick1.t_dealid
									 end) as cnt_short_sel
						,0 as cnt_margin_call
				from contr
				left join ddl_tick_dbt tick1
					on tick1.t_ClientId = contr.T_PARTYID
				 and tick1.t_ClientContrId = contr.sfcontr_id
				 and tick1.t_BOfficeKind = 101
				 and tick1.t_dealdate between p_dtbegin and p_dtend
				left join ddl_tick_dbt tick2
					on tick2.t_PartyId = contr.T_PARTYID
				 and tick2.t_PartyContrId = contr.sfcontr_id
				 and tick2.t_BOfficeKind = 101
				 and tick2.t_dealdate between p_dtbegin and p_dtend
				left join ddvndeal_dbt tick3 --  валютный рынок(селки с валютой, внебиржевые форварды, своп)
					on tick3.t_client = contr.T_PARTYID
				 and tick3.t_clientcontr = contr.sfcontr_id
				 and tick3.t_date between p_dtbegin and p_dtend
				left join ddvdeal_dbt tick4 --  срочный рынок (фьючерсы, опционы)
					on tick4.t_client = contr.T_PARTYID
				 and tick4.t_clientcontr = contr.sfcontr_id
				 and tick4.t_date between p_dtbegin and p_dtend
				left join dobjatcor_dbt AtCor
					on AtCor.t_ObjectType = 101 -- OBJTYPE_SECDEAL (сделки БО ЦБ)
				 and AtCor.t_GroupID = 103 -- Номер категории, тут меняем на нужную
				 and AtCor.t_Object = LPAD(tick1.t_dealid, 34, '0') --t_DealID - это id сделки, берем его из ddl_tick_dbt
				left join dobjattr_dbt Attr
					on Attr.t_AttrID = AtCor.t_AttrID
				 and Attr.t_ObjectType = AtCor.t_ObjectType
				 and Attr.t_GroupID = AtCor.t_GroupID
			 where (tick1.t_dealid is not null or tick2.t_dealid is not null or tick3.t_id is not null or tick4.t_id is not null)
				 and (AtCor.t_ValidFromDate is null or AtCor.t_ValidFromDate = (select max(t.T_ValidFromDate) -- Для историчных категорий нужно проверять, что это последнее значение
																																					from DOBJATCOR_DBT t
																																				 where t.T_ObjectType = AtCor.T_ObjectType
																																					 and t.T_GroupID = AtCor.T_GroupID
																																					 and t.t_Object = AtCor.t_Object
																																					 and t.T_ValidFromDate <= p_dtend))
			 group by contr.T_PARTYID
							 ,contr.sfcontr_id
							 ,coalesce(tick1.t_dealdate, tick2.t_dealdate, tick3.t_date, tick4.t_date);
		insert into itt_rcb_portf_by_cat_pack values v_itt_rcb_portf_by_cat_pack;
		it_log.log('END make_process');
		return v_id_rep_pack;
	exception
		when others then
			raise;
	end;

	function make_report_7ep(p_dtbegin date
													,p_dtend   date
													,p_id_pack in number) return number is
		v_clob00      clob := '0;0;0;0;0;0;0;0;0;0;0;0' || chr(13) || chr(10);
		v_clob        clob;
		v_clob2       clob;
		v_clob3       clob;
		v_clob31      clob;
		v_clob4       clob;
		v_clob7       clob;
		v_clob8       clob;
		v_clob9       clob;
		v_clob10      clob;
		v_clob16on    clob;
		v_clob16off   clob;
		v_create_user number;
		v_id_file     number;
		i             number;
		v_cnt_month   integer := MONTHS_BETWEEN(last_day(p_dtend), round(p_dtbegin, 'MONTH') - 1); -- кол во полных месяцев
		function num_to_xls(p_num number) return varchar2 as
		begin
			return replace(to_char(p_num), '.', ',');
		end;
	
	begin
		it_log.log('START make_report_7ep ');
		v_create_user := 1;
		dbms_lob.createtemporary(lob_loc => v_clob, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob2, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob3, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob31, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob4, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob7, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob8, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob9, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob10, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob16on, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.createtemporary(lob_loc => v_clob16off, cache => true, dur => dbms_lob.lob_readwrite);
		dbms_lob.append(v_clob
									 ,'Основная информация о компании;' || chr(13) || chr(10) || 'ИНН  компании:;' || chr(13) || chr(10) || 'Наименование  компании:;' || chr(13) || chr(10) ||
										'ФИО исполнителя, ответственного за заполнение анкеты:;' || chr(13) || chr(10) || 'Телефон исполнителя, ответственного за заполнение анкеты:;' || chr(13) ||
										chr(10) || 'Адрес электронной почты исполнителя, ответственного за заполнение анкеты:;' || chr(13) || chr(10) ||
										'Анкета обследования брокерских компаний по портрету клиента;' || chr(13) || chr(10) || '№ п/п;;Мужчины;;;;;;Женщины;;;;;;Комментарии' || chr(13) || chr(10) ||
										';;"(0;20)";"[20;30)";"[30;40)";"[40;50)";"[50;60)";"[60;?)";"(0;20)";"[20;30)";"[30;40)";"[40;50)";"[50;60)";"[60;?)"' || chr(13) || chr(10));
		dbms_lob.append(v_clob
									 ,'1;Укажите количество клиентов соответствующей группы по размеру активов на брокерском счете' || chr(13) || chr(10));
		for cCur in (select p as line
											 ,line_name
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as cm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as cm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as cm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as cm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as cm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as cm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as cf1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as cf2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as cf3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as cf4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as cf5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as cf6
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 portf_end
															else
															 0
														end) as pm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 portf_end
															else
															 0
														end) as pm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 portf_end
															else
															 0
														end) as pm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 portf_end
															else
															 0
														end) as pm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 portf_end
															else
															 0
														end) as pm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 portf_end
															else
															 0
														end) as pm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 portf_end
															else
															 0
														end) as pf1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 portf_end
															else
															 0
														end) as pf2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 portf_end
															else
															 0
														end) as pf3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 portf_end
															else
															 0
														end) as pf4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 portf_end
															else
															 0
														end) as pf5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 portf_end
															else
															 0
														end) as pf6
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 portf_end_frgn
															else
															 0
														end) as pfm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 portf_end_frgn
															else
															 0
														end) as pfm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 portf_end_frgn
															else
															 0
														end) as pfm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 portf_end_frgn
															else
															 0
														end) as pfm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 portf_end_frgn
															else
															 0
														end) as pfm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 portf_end_frgn
															else
															 0
														end) as pfm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 portf_end_frgn
															else
															 0
														end) as pff1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 portf_end_frgn
															else
															 0
														end) as pff2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 portf_end_frgn
															else
															 0
														end) as pff3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 portf_end_frgn
															else
															 0
														end) as pff4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 portf_end_frgn
															else
															 0
														end) as pff5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 portf_end_frgn
															else
															 0
														end) as pff6
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 fii_count
															else
															 0
														end) as cfm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 fii_count
															else
															 0
														end) as cfm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 fii_count
															else
															 0
														end) as cfm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 fii_count
															else
															 0
														end) as cfm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 fii_count
															else
															 0
														end) as cfm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 fii_count
															else
															 0
														end) as cfm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 fii_count
															else
															 0
														end) as cff1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 fii_count
															else
															 0
														end) as cff2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 fii_count
															else
															 0
														end) as cff3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 fii_count
															else
															 0
														end) as cff4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 fii_count
															else
															 0
														end) as cff5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 fii_count
															else
															 0
														end) as cff6
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 d_c
															else
															 0
														end) as dcm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 d_c
															else
															 0
														end) as dcm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 d_c
															else
															 0
														end) as dcm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 d_c
															else
															 0
														end) as dcm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 d_c
															else
															 0
														end) as dcm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 d_c
															else
															 0
														end) as dcm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 d_c
															else
															 0
														end) as dcf1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 d_c
															else
															 0
														end) as dcf2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 d_c
															else
															 0
														end) as dcf3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 d_c
															else
															 0
														end) as dcf4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 d_c
															else
															 0
														end) as dcf5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 d_c
															else
															 0
														end) as dcf6
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 d_m
															else
															 0
														end) as dmm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 d_m
															else
															 0
														end) as dmm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 d_m
															else
															 0
														end) as dmm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 d_m
															else
															 0
														end) as dmm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 d_m
															else
															 0
														end) as dmm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 d_m
															else
															 0
														end) as dmm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 d_m
															else
															 0
														end) as dmf1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 d_m
															else
															 0
														end) as dmf2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 d_m
															else
															 0
														end) as dmf3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 d_m
															else
															 0
														end) as dmf4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 d_m
															else
															 0
														end) as dmf5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 d_m
															else
															 0
														end) as dmf6
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 d_s
															else
															 0
														end) as dsm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 d_s
															else
															 0
														end) as dsm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 d_s
															else
															 0
														end) as dsm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 d_s
															else
															 0
														end) as dsm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 d_s
															else
															 0
														end) as dsm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 d_s
															else
															 0
														end) as dsm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 d_s
															else
															 0
														end) as dsf1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 d_s
															else
															 0
														end) as dsf2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 d_s
															else
															 0
														end) as dsf3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 d_s
															else
															 0
														end) as dsf4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 d_s
															else
															 0
														end) as dsf5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 d_s
															else
															 0
														end) as dsf6
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 d_mc
															else
															 0
														end) as dmcm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 d_mc
															else
															 0
														end) as dmcm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 d_mc
															else
															 0
														end) as dmcm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 d_mc
															else
															 0
														end) as dmcm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 d_mc
															else
															 0
														end) as dmcm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 d_mc
															else
															 0
														end) as dmcm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 d_mc
															else
															 0
														end) as dmcf1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 d_mc
															else
															 0
														end) as dmcf2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 d_mc
															else
															 0
														end) as dmcf3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 d_mc
															else
															 0
														end) as dmcf4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 d_mc
															else
															 0
														end) as dmcf5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 d_mc
															else
															 0
														end) as dmcf6
									 from (select line.p
															 ,line.line_name
															 ,cln.client_agegrp
															 ,cln.client_gender
															 ,sum(cln.fii_count) / count(cln.t_partyid) as fii_count
															 ,count(cln.t_partyid) as cnt
															 ,sum(cln.portf_end) / 1000000000 as portf_end
															 ,sum(cln.portf_end_frgn) / 1000000000 as portf_end_frgn
															 ,sum(deal.d_c) as d_c
															 ,sum(case
																			when deal.d_m > 0 then
																			 1
																			else
																			 0
																		end) as d_m
															 ,sum(case
																			when deal.d_s > 0 then
																			 1
																			else
																			 0
																		end) as d_s
															 ,count(mgc.t_partyid) as d_mc
													 from (select 1 as p
																			 ,null as f
																			 ,0 as l
																			 ,'Отрицательный портфель' as line_name
																	 from dual
																 union all
																 select 2 as p
																			 ,0 as f
																			 ,0 as l
																			 ,'Пустые счета (0 руб.)' as line_name
																	 from dual
																 union all
																 select 3 as p
																			 ,0 as f
																			 ,10000 as l
																			 ,'(0 руб.; 10 тыс. руб.]' as line_name
																	 from dual
																 union all
																 select 4 as p
																			 ,10000 as f
																			 ,100000 as l
																			 ,'(10 тыс. руб.; 100 тыс. руб.]' as line_name
																	 from dual
																 union all
																 select 5 as p
																			 ,100000 as f
																			 ,1000000 as l
																			 ,'(100 тыс. руб.; 1 млн руб.]' as line_name
																	 from dual
																 union all
																 select 6 as p
																			 ,1000000 as f
																			 ,6000000 as l
																			 ,'(1 млн руб.; 6 млн руб.]' as line_name
																	 from dual
																 union all
																 select 7 as p
																			 ,6000000 as f
																			 ,10000000 as l
																			 ,'(6 млн руб.; 10 млн руб.]' as line_name
																	 from dual
																 union all
																 select 8 as p
																			 ,10000000 as f
																			 ,100000000 as l
																			 ,'(10 млн руб.; 100 млн руб.]' as line_name
																	 from dual
																 union all
																 select 9 as p
																			 ,100000000 as f
																			 ,null as l
																			 ,'(100 млн руб.; ?)' as line_name
																	 from dual) line
													 left join (select t.t_partyid
																					 ,t.client_agegrp
																					 ,t.client_gender
																					 ,t.fii_count
																					 ,round(sum(t.portf_end), 2) portf_end
																					 ,round(sum(t.portf_end_frgn), 2) portf_end_frgn
																			 from itt_rcb_portf_7ep_clnt t
																			where t.id_rcb_portf_7ep_pack = p_id_pack
																			group by t.t_partyid
																							,t.client_agegrp
																							,t.client_gender
																							,t.fii_count) cln
														 on (decode(line.f, line.l, 1, 0) = 0 and cln.portf_end > nvl(line.f, cln.portf_end - 1) and
																cln.portf_end <= decode(line.l, 0, -0.000001, nvl(line.l, cln.portf_end)))
														 or (decode(line.f, line.l, 1, 0) = 1 and cln.portf_end = line.f)
													 left join (select d.t_partyid
																					 ,sum(d.cnt_deal) as d_c
																					 ,sum(d.cnt_margin) as d_m
																					 ,sum(d.cnt_short_sel) as d_s
																			 from itt_rcb_portf_7ep_deal d
																			where d.id_rcb_portf_7ep_pack = p_id_pack
																			group by d.t_partyid) deal
														 on cln.t_partyid = deal.t_partyid
													 left join (select distinct d.t_partyid from itt_rcb_portf_7ep_mgc d) mgc
														 on cln.t_partyid = mgc.t_partyid
													group by line.p
																	,line.line_name
																	,cln.client_agegrp
																	,cln.client_gender)
									group by p
													,line_name
									order by p)
		loop
			dbms_lob.append(v_clob
										 ,';"' || cCur.line_name || '";' || cCur.cm1 || ';' || cCur.cm2 || ';' || cCur.cm3 || ';' || cCur.cm4 || ';' || cCur.cm5 || ';' || cCur.cm6 || ';' || cCur.cf1 || ';' ||
											cCur.cf2 || ';' || cCur.cf3 || ';' || cCur.cf4 || ';' || cCur.cf5 || ';' || cCur.cf6 || chr(13) || chr(10));
			dbms_lob.append(v_clob2
										 ,';"' || cCur.line_name || '";' || num_to_xls(cCur.pm1) || ';' || num_to_xls(cCur.pm2) || ';' || num_to_xls(cCur.pm3) || ';' || num_to_xls(cCur.pm4) || ';' ||
											num_to_xls(cCur.pm5) || ';' || num_to_xls(cCur.pm6) || ';' || num_to_xls(cCur.pf1) || ';' || num_to_xls(cCur.pf2) || ';' || num_to_xls(cCur.pf3) || ';' ||
											num_to_xls(cCur.pf4) || ';' || num_to_xls(cCur.pf5) || ';' || num_to_xls(cCur.pf6) || chr(13) || chr(10));
			dbms_lob.append(v_clob4
										 ,';"' || cCur.line_name || '";' || num_to_xls(cCur.pfm1) || ';' || num_to_xls(cCur.pfm2) || ';' || num_to_xls(cCur.pfm3) || ';' || num_to_xls(cCur.pfm4) || ';' ||
											num_to_xls(cCur.pfm5) || ';' || num_to_xls(cCur.pfm6) || ';' || num_to_xls(cCur.pff1) || ';' || num_to_xls(cCur.pff2) || ';' || num_to_xls(cCur.pff3) || ';' ||
											num_to_xls(cCur.pff4) || ';' || num_to_xls(cCur.pff5) || ';' || num_to_xls(cCur.pff6) || chr(13) || chr(10));
			dbms_lob.append(v_clob7
										 ,';"' || cCur.line_name || '";' || cCur.dmm1 || ';' || cCur.dmm2 || ';' || cCur.dmm3 || ';' || cCur.dmm4 || ';' || cCur.dmm5 || ';' || cCur.dmm6 || ';' ||
											cCur.dmf1 || ';' || cCur.dmf2 || ';' || cCur.dmf3 || ';' || cCur.dmf4 || ';' || cCur.dmf5 || ';' || cCur.dmf6 || chr(13) || chr(10));
			dbms_lob.append(v_clob8
										 ,';"' || cCur.line_name || '";' || cCur.dsm1 || ';' || cCur.dsm2 || ';' || cCur.dsm3 || ';' || cCur.dsm4 || ';' || cCur.dsm5 || ';' || cCur.dsm6 || ';' ||
											cCur.dsf1 || ';' || cCur.dsf2 || ';' || cCur.dsf3 || ';' || cCur.dsf4 || ';' || cCur.dsf5 || ';' || cCur.dsf6 || chr(13) || chr(10));
			dbms_lob.append(v_clob9
										 ,';"' || cCur.line_name || '";' || cCur.dmcm1 || ';' || cCur.dmcm2 || ';' || cCur.dmcm3 || ';' || cCur.dmcm4 || ';' || cCur.dmcm5 || ';' || cCur.dmcm6 || ';' ||
											cCur.dmcf1 || ';' || cCur.dmcf2 || ';' || cCur.dmcf3 || ';' || cCur.dmcf4 || ';' || cCur.dmcf5 || ';' || cCur.dmcf6 || chr(13) || chr(10));
			if cCur.line > 2
			then
				dbms_lob.append(v_clob3, ';"' || cCur.line_name || '";'|| chr(13) || chr(10) );
				dbms_lob.append(v_clob31, ';"' || cCur.line_name || '";'|| chr(13) || chr(10) );
				dbms_lob.append(v_clob10
											 ,';"' || cCur.line_name || '";' || num_to_xls(cCur.cfm1) || ';' || num_to_xls(cCur.cfm2) || ';' || num_to_xls(cCur.cfm3) || ';' || num_to_xls(cCur.cfm4) || ';' ||
												num_to_xls(cCur.cfm5) || ';' || num_to_xls(cCur.cfm6) || ';' || num_to_xls(cCur.cff1) || ';' || num_to_xls(cCur.cff2) || ';' || num_to_xls(cCur.cff3) || ';' ||
												num_to_xls(cCur.cff4) || ';' || num_to_xls(cCur.cff5) || ';' || num_to_xls(cCur.cff6) || chr(13) || chr(10));
			end if;
		end loop;
		dbms_lob.append(v_clob
									 ,'2;Укажите объем активов соответствующей группы  по размеру портфелей на брокерском счете, млрд руб.' || chr(13) || chr(10));
		dbms_lob.append(v_clob, v_clob2);
		dbms_lob.append(v_clob
									 ,'3;Укажите средневзвешенную доходность клиентов соответствующей группы за период с 1 января по 30 июня 2022' || chr(13) || chr(10));
		dbms_lob.append(v_clob, v_clob3);
		dbms_lob.append(v_clob
									 ,'"''3.1";Укажите медианную доходность клиентов соответствующей группы за период с 1 января по 30 июня 2022' || chr(13) || chr(10));
		dbms_lob.append(v_clob, v_clob31);
		dbms_lob.append(v_clob
									 ,'4;Укажите объем иностранных активов в портфеле каждой группы клиентов, млрд руб.' || chr(13) || chr(10));
		dbms_lob.append(v_clob, v_clob4);
		for cCur in (select sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as pfm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as pfm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as pfm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as pfm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as pfm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as pfm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as pff1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as pff2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as pff3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as pff4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as pff5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as pff6
									 from (select client_agegrp
															 ,client_gender
															 ,sum(cnt) / count(*) / v_cnt_month as cnt
													 from (select c.T_PARTYID
																			 ,c.client_agegrp
																			 ,c.client_gender
																			 ,count(distinct d.t_dealdate) cnt
																	 from (select t.T_PARTYID -- Положительный портфель
																							 ,t.client_agegrp
																							 ,t.client_gender
																							 ,round(sum(t.portf_end), 2) as portf
																					 from itt_rcb_portf_7ep_clnt t
																					where t.id_rcb_portf_7ep_pack = p_id_pack
																					group by t.T_PARTYID
																									,t.client_agegrp
																									,t.client_gender
																				 having round(sum(t.portf_end), 2) > 0) c
																	 left join itt_rcb_portf_7ep_deal d
																		 on d.id_rcb_portf_7ep_pack = p_id_pack
																		and c.t_partyid = d.t_partyid
																	group by c.t_partyid
																					,c.client_agegrp
																					,c.client_gender)
													group by client_agegrp
																	,client_gender))
		loop
			dbms_lob.append(v_clob
										 ,'5;Укажите среднее количество дней в месяц, когда в интересах клиента с положительным портфелем заключалась хотя бы одна сделка за период с 1 января по 30 июня 2022 года, дней;' ||
											num_to_xls(cCur.pfm1) || ';' || num_to_xls(cCur.pfm2) || ';' || num_to_xls(cCur.pfm3) || ';' || num_to_xls(cCur.pfm4) || ';' || num_to_xls(cCur.pfm5) || ';' ||
											num_to_xls(cCur.pfm6) || ';' || num_to_xls(cCur.pff1) || ';' || num_to_xls(cCur.pff2) || ';' || num_to_xls(cCur.pff3) || ';' || num_to_xls(cCur.pff4) || ';' ||
											num_to_xls(cCur.pff5) || ';' || num_to_xls(cCur.pff6) || chr(13) || chr(10));
		end loop;
		dbms_lob.append(v_clob
									 ,'6;Укажите количество клиентов с положительным портфелем, совершивших в месяц соответствующее количество сделок за период с 1 января по 30 июня 2022 года' ||
										chr(13) || chr(10));
		for cCur in (select p as line
											 ,line_name
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as cm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as cm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as cm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as cm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as cm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as cm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as cf1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as cf2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as cf3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as cf4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as cf5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as cf6
									 from (select l.p
															 ,l.line_name
															 ,c.client_agegrp
															 ,c.client_gender
															 ,count(*) cnt
													 from (select 1 as p
																			 ,0 as f
																			 ,0 as l
																			 ,'0 сделок' as line_name
																	 from dual
																 union all
																 select 2 as p
																			 ,1 as f
																			 ,5 as l
																			 ,'от 1 до 5 сделок' as line_name
																	 from dual
																 union all
																 select 3 as p
																			 ,6 as f
																			 ,50 as l
																			 ,'от 6 до 50 сделок' as line_name
																	 from dual
																 union all
																 select 4 as p
																			 ,51 as f
																			 ,200 as l
																			 ,'от 51 до 200 сделок' as line_name
																	 from dual
																 union all
																 select 5 as p
																			 ,201 as f
																			 ,2000 as l
																			 ,'от 201 до 2000 сделок' as line_name
																	 from dual
																 union all
																 select 6 as p
																			 ,2001 as f
																			 ,null as l
																			 ,'2001 и более сделок' as line_name
																	 from dual) l
													 left join (select c.T_PARTYID
																					 ,c.client_agegrp
																					 ,c.client_gender
																					 ,nvl(CEIL(sum(d.cnt_deal) / v_cnt_month), 0) as cnt
																			 from (select t.T_PARTYID -- Положительный портфель
																									 ,t.client_agegrp
																									 ,t.client_gender
																									 ,round(sum(t.portf_end), 2) as portf
																							 from itt_rcb_portf_7ep_clnt t
																							where t.id_rcb_portf_7ep_pack = p_id_pack -- 3777028 --
																							group by t.T_PARTYID
																											,t.client_agegrp
																											,t.client_gender
																						 having round(sum(t.portf_end), 2) > 0) c
																			 left join itt_rcb_portf_7ep_deal d
																				 on d.id_rcb_portf_7ep_pack = p_id_pack -- 3777028 -
																				and c.t_partyid = d.t_partyid
																			group by c.t_partyid
																							,c.client_agegrp
																							,c.client_gender) c
														 on c.cnt >= l.f
														and c.cnt <= nvl(l.l, c.cnt)
													group by l.p
																	,l.line_name
																	,c.client_agegrp
																	,c.client_gender)
									group by p
													,line_name
									order by p)
		loop
			dbms_lob.append(v_clob
										 ,';"' || cCur.line_name || '";' || cCur.cm1 || ';' || cCur.cm2 || ';' || cCur.cm3 || ';' || cCur.cm4 || ';' || cCur.cm5 || ';' || cCur.cm6 || ';' || cCur.cf1 || ';' ||
											cCur.cf2 || ';' || cCur.cf3 || ';' || cCur.cf4 || ';' || cCur.cf5 || ';' || cCur.cf6 || chr(13) || chr(10));
		end loop;
		dbms_lob.append(v_clob
									 ,'7;Укажите количество клиентов, совершивших хотя бы одну маржинальную сделку за период с 1 января по 30 июня 2022 года' || chr(13) || chr(10));
		dbms_lob.append(v_clob, v_clob7);
		dbms_lob.append(v_clob
									 ,'8;Укажите количество клиентов, осуществивших хотя бы 1 короткую продажу ценных бумаг за период с 1 января по 30 июня 2022 года' || chr(13) || chr(10));
		dbms_lob.append(v_clob, v_clob8);
		dbms_lob.append(v_clob
									 ,'9;Укажите количество клиентов, у которых брокер принудительно закрывал позиции в рамках маржинальной торговли  за период с 1 января по 30 июня 2022 года' ||
										chr(13) || chr(10));
		dbms_lob.append(v_clob, v_clob9);
		dbms_lob.append(v_clob
									 ,'10;Укажите среднее количество инструментов в портфеле клиентов с положительным портфелем' || chr(13) || chr(10));
		dbms_lob.append(v_clob, v_clob10);
		dbms_lob.append(v_clob
									 ,'11;Укажите количество клиентов с положительным портфелем, у которых соответствующая доля портфеля приходится на инструменты коллективного инвестирования (ETF/ПИФ)' ||
										chr(13) || chr(10));
		for cCur in (select p as line
											 ,line_name
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as cm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as cm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as cm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as cm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as cm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as cm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as cf1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as cf2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as cf3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as cf4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as cf5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as cf6
									 from (select l.p
															 ,l.line_name
															 ,c.client_agegrp
															 ,c.client_gender
															 ,count(c.T_PARTYID) as cnt
													 from (select 1 as p
																			 ,0 as f
																			 ,0 as l
																			 ,'''0%' as line_name
																	 from dual
																 union all
																 select 2 as p
																			 ,1 as f
																			 ,9 as l
																			 ,'(0%; 10%]' as line_name
																	 from dual
																 union all
																 select 3 as p
																			 ,10 as f
																			 ,34 as l
																			 ,'(10%; 35%]' as line_name
																	 from dual
																 union all
																 select 4 as p
																			 ,35 as f
																			 ,59 as l
																			 ,'(35%; 60%]' as line_name
																	 from dual
																 union all
																 select 5 as p
																			 ,60 as f
																			 ,89 as l
																			 ,'(60%; 90%]' as line_name
																	 from dual
																 union all
																 select 6 as p
																			 ,90 as f
																			 ,101 as l
																			 ,'более 90%' as line_name
																	 from dual) l
													 left join (select t.T_PARTYID -- Положительный портфель
																					 ,t.client_agegrp
																					 ,t.client_gender
																					 ,CEIL(sum(t.portf_end_etf) / sum(t.portf_end) * 100) as perc_eft
																			 from itt_rcb_portf_7ep_clnt t
																			where t.id_rcb_portf_7ep_pack = p_id_pack
																			group by t.T_PARTYID
																							,t.client_agegrp
																							,t.client_gender
																		 having round(sum(t.portf_end), 2) > 0) c
														 on c.perc_eft >= l.f
														and c.perc_eft <= l.l
													group by l.p
																	,l.line_name
																	,c.client_agegrp
																	,c.client_gender)
									group by p
													,line_name
									order by p)
		loop
			dbms_lob.append(v_clob
										 ,';"' || cCur.line_name || '";' || cCur.cm1 || ';' || cCur.cm2 || ';' || cCur.cm3 || ';' || cCur.cm4 || ';' || cCur.cm5 || ';' || cCur.cm6 || ';' || cCur.cf1 || ';' ||
											cCur.cf2 || ';' || cCur.cf3 || ';' || cCur.cf4 || ';' || cCur.cf5 || ';' || cCur.cf6 || chr(13) || chr(10));
		end loop;
		dbms_lob.append(v_clob
									 ,'12;Укажите количество клиентов с положительным портфелем, использующих инвестиционные рекомендации (подписку на сервис инвестиционных рекомендаций) по состоянию на 30.06.2022 ;' ||
										v_clob00);
		dbms_lob.append(v_clob
									 ,'13;Укажите количество клиентов с положительным портфелем, использующих стратегии автоследования по состоянию на 30.06.2022;' || v_clob00);
		dbms_lob.append(v_clob
									 ,'14;Укажите количество клиентов с положительным портфелем, использующих алгоритмическую торговлю, по состоянию на 30.06.2022;' || v_clob00);
		dbms_lob.append(v_clob
									 ,'15;Укажите количество действующих клиентов с положительным портфелем, использовавших следующие способы взаимодействия с брокером за период с 1 января по 30 июня 2022 года' ||
										chr(13) || chr(10));
		dbms_lob.append(v_clob, ';Приложения для смартфона;' || v_clob00);
		dbms_lob.append(v_clob, ';Личный кабинет, доступный через браузер;' || v_clob00);
		for cCur in (select sum(case
															when client_gender = 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as cm1
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as cm2
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as cm3
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as cm4
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as cm5
											 ,sum(case
															when client_gender = 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as cm6
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 1 then
															 cnt
															else
															 0
														end) as cf1
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 2 then
															 cnt
															else
															 0
														end) as cf2
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 3 then
															 cnt
															else
															 0
														end) as cf3
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 4 then
															 cnt
															else
															 0
														end) as cf4
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 5 then
															 cnt
															else
															 0
														end) as cf5
											 ,sum(case
															when client_gender != 'X'
																	 and client_agegrp = 6 then
															 cnt
															else
															 0
														end) as cf6
									 from (select c.client_agegrp
															 ,c.client_gender
															 ,count(c.T_PARTYID) as cnt
													 from (select distinct t.T_PARTYID -- Положительный портфель
																								,t.client_agegrp
																								,t.client_gender
																	 from itt_rcb_portf_7ep_clnt t
																	where t.id_rcb_portf_7ep_pack = p_id_pack
																	group by t.T_PARTYID
																					,t.client_agegrp
																					,t.client_gender
																 having round(sum(t.portf_end), 2) > 0) c
													 join itt_rcb_portf_7ep_deal d
														 on d.id_rcb_portf_7ep_pack = p_id_pack -- 3777028 -
														and c.t_partyid = d.t_partyid
													where d.cnt_deal > 0
													group by c.client_agegrp
																	,c.client_gender))
		loop
			dbms_lob.append(v_clob
										 ,';"Торговые платформы (Quik и прочие)";' || cCur.cm1 || ';' || cCur.cm2 || ';' || cCur.cm3 || ';' || cCur.cm4 || ';' || cCur.cm5 || ';' || cCur.cm6 || ';' ||
											cCur.cf1 || ';' || cCur.cf2 || ';' || cCur.cf3 || ';' || cCur.cf4 || ';' || cCur.cf5 || ';' || cCur.cf6 || chr(13) || chr(10));
		end loop;
		dbms_lob.append(v_clob, ';Телефон;' || v_clob00);
		dbms_lob.append(v_clob, ';Оффлайн;' || v_clob00);
		dbms_lob.append(v_clob, ';Иные способы;' || v_clob00);
		dbms_lob.append(v_clob
									 ,'16;Укажите количество клиентов, заключивших за период с 1 января по 30 июня 2022 года договор на брокерское обслуживание соответствующим способом' || chr(13) ||
										chr(10));
		for cCur in (with party as
										(select /*+ materialize*/
										 *
											from (select t.T_PARTYID
																	,t.client_agegrp
																	,t.client_gender
																	,min(t.sfcontr_begin) as dt
															from itt_rcb_portf_7ep_clnt t
														 where t.id_rcb_portf_7ep_pack = p_id_pack
														 group by t.T_PARTYID
																		 ,t.client_agegrp
																		 ,t.client_gender)
										 where dt >= to_date('01.01.2022', 'dd.mm.yyyy'))
									 select c_on
												 ,sum(case
																when client_gender = 'X'
																		 and client_agegrp = 1 then
																 cnt
																else
																 0
															end) as cm1
												 ,sum(case
																when client_gender = 'X'
																		 and client_agegrp = 2 then
																 cnt
																else
																 0
															end) as cm2
												 ,sum(case
																when client_gender = 'X'
																		 and client_agegrp = 3 then
																 cnt
																else
																 0
															end) as cm3
												 ,sum(case
																when client_gender = 'X'
																		 and client_agegrp = 4 then
																 cnt
																else
																 0
															end) as cm4
												 ,sum(case
																when client_gender = 'X'
																		 and client_agegrp = 5 then
																 cnt
																else
																 0
															end) as cm5
												 ,sum(case
																when client_gender = 'X'
																		 and client_agegrp = 6 then
																 cnt
																else
																 0
															end) as cm6
												 ,sum(case
																when client_gender != 'X'
																		 and client_agegrp = 1 then
																 cnt
																else
																 0
															end) as cf1
												 ,sum(case
																when client_gender != 'X'
																		 and client_agegrp = 2 then
																 cnt
																else
																 0
															end) as cf2
												 ,sum(case
																when client_gender != 'X'
																		 and client_agegrp = 3 then
																 cnt
																else
																 0
															end) as cf3
												 ,sum(case
																when client_gender != 'X'
																		 and client_agegrp = 4 then
																 cnt
																else
																 0
															end) as cf4
												 ,sum(case
																when client_gender != 'X'
																		 and client_agegrp = 5 then
																 cnt
																else
																 0
															end) as cf5
												 ,sum(case
																when client_gender != 'X'
																		 and client_agegrp = 6 then
																 cnt
																else
																 0
															end) as cf6
										 from (select c.client_agegrp
																 ,c.client_gender
																 ,nvl(fc.c_on, 0) as c_on
																 ,count(distinct c.T_PARTYID) as cnt
														 from party c
														 left join (select t.t_partyid
																						 ,max(case
																										when UTL_RAW.CAST_TO_VARCHAR2(n.t_text) like '%Технический пользователь НАБС%' then
																										 1
																										else
																										 0
																									end) as c_on
																				 from itt_rcb_portf_7ep_clnt t
																				 join dnotetext_dbt n
																					 on n.t_notekind = 150 /*Финансовый консультант*/
																					and n.t_objecttype = 207 /*договор ДБО*/
																					and n.t_documentid = lpad(t.t_dlcontrid, 34, '0')
																				where t.id_rcb_portf_7ep_pack = p_id_pack --2502404
																					and t.t_partyid in (select t_partyid from party)
																				group by t.t_partyid) fc
															 on fc.t_partyid = c.t_partyid
														group by c.client_agegrp
																		,c.client_gender
																		,nvl(fc.c_on, 0))
										group by c_on)
		loop
			if cCur.c_On = 1
			then
				v_clob16on := cCur.cm1 || ';' || cCur.cm2 || ';' || cCur.cm3 || ';' || cCur.cm4 || ';' || cCur.cm5 || ';' || cCur.cm6 || ';' || cCur.cf1 || ';' || cCur.cf2 || ';' ||
											cCur.cf3 || ';' || cCur.cf4 || ';' || cCur.cf5 || ';' || cCur.cf6 || chr(13) || chr(10);
			else
				v_clob16off := cCur.cm1 || ';' || cCur.cm2 || ';' || cCur.cm3 || ';' || cCur.cm4 || ';' || cCur.cm5 || ';' || cCur.cm6 || ';' || cCur.cf1 || ';' || cCur.cf2 || ';' ||
											 cCur.cf3 || ';' || cCur.cf4 || ';' || cCur.cf5 || ';' || cCur.cf6 || chr(13) || chr(10);
			end if;
		end loop;
		dbms_lob.append(v_clob, ';"Офлайн, в т.ч.";' || v_clob16off);
		dbms_lob.append(v_clob, ';"Офисы брокера";' || v_clob00);
		dbms_lob.append(v_clob, ';"Офисы компаний группы";' || v_clob16off);
		dbms_lob.append(v_clob, ';"Офисы агентов";' || v_clob00);
		dbms_lob.append(v_clob, ';"Прочие";' || v_clob00);
		dbms_lob.append(v_clob, ';"Онлайн, в т.ч.";' || v_clob16on);
		dbms_lob.append(v_clob, ';"Личный кабинет/мобильное приложение брокера, в т.ч.";' || v_clob16on);
		dbms_lob.append(v_clob, ';"Идентификация с помощью ЕСИА";' || v_clob00);
		dbms_lob.append(v_clob, ';"Идентификация с помощью СМЭВ";' || v_clob00);
		dbms_lob.append(v_clob
									 ,';"Индентификация как действующего клиента кредитной организации (в случае совмещения лицензий КО и брокера)";' || v_clob16on);
		dbms_lob.append(v_clob, ';"Прочие виды идентификации";' || v_clob00);
		dbms_lob.append(v_clob, ';"Личный кабинет/мобильное приложение компаний группы, в т.ч.";' || v_clob00);
		dbms_lob.append(v_clob, ';"Идентификация с помощью ЕСИА";' || v_clob00);
		dbms_lob.append(v_clob, ';"Идентификация с помощью СМЭВ";' || v_clob00);
		dbms_lob.append(v_clob
									 ,';"Индентификация как действующего клиента кредитной организации (в случае совмещения лицензий КО и брокера)";' || v_clob00);
		dbms_lob.append(v_clob, ';"Прочие виды идентификации";' || v_clob00);
		dbms_lob.append(v_clob, ';"Прочие источники";' || v_clob00);
		dbms_lob.append(v_clob, ';"Идентификация с помощью ЕСИА";' || v_clob00);
		dbms_lob.append(v_clob, ';"Идентификация с помощью СМЭВ";' || v_clob00);
		dbms_lob.append(v_clob, ';"Прочие виды идентификации";' || v_clob00);
		dbms_lob.append(v_clob
									 ,'17;Укажите количество клиентов, которые пополняли свои счета за период с 1 января по 30 июня 2022 года соответствующее количество раз, в зависимости от года открытия счета;Клиенты, открывшие счета в 1 полугодии 2022 года;;;Клиенты, открывшие счета в 2021 году;;;Клиенты, открывшие счета в 2020 году;;;Клиенты, открывшие счета в 2019 году и ранее' ||
										chr(13) || chr(10));
		for cCur in (select p as line
											 ,line_name
											 ,sum(case
															when dt_grp = 1 then
															 cnt
															else
															 0
														end) as cg1
											 ,sum(case
															when dt_grp = 2 then
															 cnt
															else
															 0
														end) as cg2
											 ,sum(case
															when dt_grp = 3 then
															 cnt
															else
															 0
														end) as cg3
											 ,sum(case
															when dt_grp = 4 then
															 cnt
															else
															 0
														end) as cg4
									 from (select l.p
															 ,l.line_name
															 ,c.dt_grp
															 ,count(c.T_PARTYID) cnt
													 from (select 1 as p
																			 ,0 as f
																			 ,0 as l
																			 ,'не пополняли' as line_name
																	 from dual
																 union all
																 select 2 as p
																			 ,1 as f
																			 ,1 as l
																			 ,'1 раз' as line_name
																	 from dual
																 union all
																 select 3 as p
																			 ,2 as f
																			 ,3 as l
																			 ,'2-3 раза' as line_name
																	 from dual
																 union all
																 select 4 as p
																			 ,4 as f
																			 ,10 as l
																			 ,'4-10 раз' as line_name
																	 from dual
																 union all
																 select 5 as p
																			 ,11 as f
																			 ,null as l
																			 ,'более 10 раз' as line_name
																	 from dual) l
													 left join (select T_PARTYID
																					 ,case
																							when dt >= to_date('01.01.2022', 'dd.mm.yyyy') then
																							 1
																							when dt >= to_date('01.01.2021', 'dd.mm.yyyy') then
																							 2
																							when dt >= to_date('01.01.2020', 'dd.mm.yyyy') then
																							 3
																							else
																							 4
																						end as dt_grp
																					 ,cnt
																			 from (select t.T_PARTYID
																									 ,min(t.sfcontr_begin) as dt
																									 ,t.portf_add_cnt cnt
																							 from itt_rcb_portf_7ep_clnt t
																							where t.id_rcb_portf_7ep_pack = p_id_pack
																							group by t.T_PARTYID
																											,t.portf_add_cnt)) c
														 on c.cnt >= l.f
														and c.cnt <= nvl(l.l, c.cnt)
													group by l.p
																	,l.line_name
																	,c.dt_grp)
									group by p
													,line_name
									order by p)
		loop
			dbms_lob.append(v_clob, ';"' || cCur.line_name || '";' || cCur.cg1 || ';;;' || cCur.cg2 || ';;;' || cCur.cg3 || ';;;' || cCur.cg4 || chr(13) || chr(10));
		end loop;
		it_log.log('it_file.insert_file');
		v_id_file := it_file.insert_file(p_file_dir => null
																		,p_file_name => null
																		,p_file_clob => v_clob
																		,p_from_system => it_file.C_SOFR_DB
																		,p_from_module => $$plsql_unit
																		,p_to_system => it_file.C_SOFR_RSBANK
																		,p_to_module => null
																		,p_create_user => v_create_user);
		it_log.log('v_id_file=' || v_id_file);
		update itt_rcb_portf_by_cat_pack p set p.id_file_cb_portf_by_cat = v_id_file where p.id_rcb_portf_by_cat_pack = p_id_pack;
		it_log.log('END make_report_7ep ');
		return v_id_file;
	exception
		when others then
			if dbms_lob.isopen(lob_loc => v_clob) = 1
			then
				dbms_lob.close(lob_loc => v_clob);
			end if;
			it_error.put_error_in_stack;
			it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
			raise;
	end;

	function run(p_dtbegin date default to_date('01.01.2022', 'dd.mm.yyyy')
							,p_dtend   date default to_date('30.06.2022', 'dd.mm.yyyy')) return number is
		v_pack_id_6ep_begin number;
		v_pack_id_6ep_end   number;
		v_pack_id_7ep       number;
		v_clob              clob;
		v_error             clob;
		v_id_file           number;
	begin
		it_log.log('Start: p_dtbegin = ' || to_char(p_dtbegin, 'dd.mm.yyyy') || ' p_dtend = ' || to_char(p_dtend, 'dd.mm.yyyy'));
		it_error.clear_error_stack;
		--запускаем расчет 6ЭП
		v_pack_id_6ep_begin := it_rcb_portf_by_cat.make_process(p_repdate => p_dtbegin - 1);
		it_log.log('Для формы 6ЭП (it_rcb_portf_by_cat)' || to_char(p_dtbegin - 1, 'dd.mm.yyyy') || ' v_pack_id = ' || v_pack_id_6ep_begin);
		v_pack_id_6ep_end := it_rcb_portf_by_cat.make_process(p_repdate => p_dtend);
		it_log.log('Для формы 6ЭП (it_rcb_portf_by_cat)' || to_char(p_dtend, 'dd.mm.yyyy') || ' v_pack_id = ' || v_pack_id_6ep_end);
		--формируем разовую выгрузку для ЦБ
		-- Детализация
		v_pack_id_7ep := make_process(p_dtbegin, p_dtend, v_pack_id_6ep_begin, v_pack_id_6ep_end);
		v_id_file     := make_report_7ep(p_dtbegin, p_dtend, v_pack_id_7ep);
		it_log.log('форма 7ЭП END  v_pack_id_7ep = ' || v_pack_id_7ep || ' v_id_file:=' || v_id_file);
		commit;
		return v_id_file;
	exception
		when others then
			it_error.put_error_in_stack;
			it_log.log(p_msg_type => it_log.C_MSG_TYPE__ERROR);
			raise;
	end;

end;
/

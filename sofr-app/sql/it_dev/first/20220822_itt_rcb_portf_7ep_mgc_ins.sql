delete from itt_rcb_portf_7ep_mgc;
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('30093');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('40789');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('43062');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('44005');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('44551');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('48263');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('48263');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('412815');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('413024');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('413783');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('413817');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('414380');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('415203');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('415335');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('415524');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('418031');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('418210');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('418241');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('418246');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('418291');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('421397');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('421819');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('422024');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('422269');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('426264');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('426285');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('427614');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('428105');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('428869');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('429340');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('429771');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('434310');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('448004');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('488427');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('615204');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('615400');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('621540');
insert into itt_rcb_portf_7ep_mgc (t_clientcode) values ('629094');
update itt_rcb_portf_7ep_mgc t
	 set t.t_partyid =
			 (select distinct sf.t_partyid
					from ddlcontrmp_dbt mp
							,dsfcontr_dbt   sf
				 where mp.t_mpcode = t.t_clientcode
					 and sf.t_id = mp.t_sfcontrid
					 and sf.t_servkind = 1
					 and sf.t_servkindsub = 8);
commit;

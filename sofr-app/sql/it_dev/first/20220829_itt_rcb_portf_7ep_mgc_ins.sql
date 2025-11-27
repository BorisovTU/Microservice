delete from itt_rcb_portf_7ep_mgc where t_clientcode is null;
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (140530);
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (140384);
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (140282);
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (137275);
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (140475);
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (137421);
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (144344);
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (148602);
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (144468);
insert into itt_rcb_portf_7ep_mgc (t_partyid) values (144055);
update itt_rcb_portf_7ep_mgc t
	 set t.t_partyid =
			 (select distinct sf.t_partyid
					from ddlcontrmp_dbt mp
							,dsfcontr_dbt   sf
				 where mp.t_mpcode = t.t_clientcode
					 and sf.t_id = mp.t_sfcontrid
					 and sf.t_servkind = 1
					 and sf.t_servkindsub = 8)
    where t.t_partyid is null ;
commit;

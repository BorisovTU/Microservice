create global temporary table d724_metall_details (
 t_contr_groupid      varchar2(100),
 t_client_groupid     varchar2(100),
 t_clientcode         varchar2(35),
 t_name               varchar2(320),
 t_parent_sf_id       number(10),
 t_sf_id              number(10),
 t_account            varchar2(20),
 t_rest               number(32, 12),
 t_fiid               number(10),
 t_rest_rub           number(32, 12)
) on commit preserve rows;
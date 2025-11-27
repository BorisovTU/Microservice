create or replace package commiss_utils as 

  procedure save_one_time_commiss (
    pio_id              in out dsfdef_dbt.t_id%type,
    p_feetype                  dsfdef_dbt.t_feetype%type,
    p_commnumber               dsfdef_dbt.t_commnumber%type,
    p_code                     dsfdef_dbt.t_code%type,
    p_sum                      dsfdef_dbt.t_sum%type,
    p_fiid                     dsfdef_dbt.t_fiid_sum%type,
    p_date                     dsfdef_dbt.t_datefee%type,
    p_payer_contract_id        dsfdef_dbt.t_sfcontrid%type,
    p_service_type             sf_commiss_additional_info.contract_service_type%type
  );

end commiss_utils;
/

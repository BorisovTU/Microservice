create or replace package rudata_read as

  function get_seccode_mmvb (
    p_isin sofr_info_instruments.isin%type
  ) return sofr_info_instruments.seccode%type deterministic;
  
  function get_seccode_spb (
    p_isin sofr_info_instruments.isin%type
  ) return sofr_info_instruments.seccode%type deterministic;
  
  function prepare_temp_table(
    p_FlAll in number, 
    p_Mode in number, 
    p_ISIN_LSIN in varchar2
  ) return varchar2;
  
  -- Обновление эмитентов по данным из SOFR_INFO_EMITENTS
  procedure  update_emitents ;
 
 -- Обновление ценных бумаг по данным из SOFR_INFO_FINTOOLREFERENCEDATA
  procedure update_fininstr ;
   
end rudata_read;
/
